import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import axios from 'axios';
import * as cheerio from 'cheerio';
import { db } from '../lib/db.js';
import { config } from '../config.js';

const PSX_HISTORICAL_URL = 'https://dps.psx.com.pk/historical';

function normalizeDate(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function fmtDate(d) {
  return d.toISOString().slice(0, 10);
}

function nextDate(yyyyMmDd) {
  const d = new Date(`${yyyyMmDd}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  return fmtDate(d);
}

function parseFloatSafe(text) {
  if (text == null) return null;
  const cleaned = String(text).replaceAll(',', '').replaceAll('%', '').trim();
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function parseCsvLine(line) {
  const out = [];
  let cur = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      const next = line[i + 1];
      if (inQuotes && next === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function chooseCsvPath() {
  const candidates = [
    process.env.PSX_CSV_PATH,
    path.resolve(config.rootDir, '..', 'new_psx_historical_.csv'),
    path.resolve(config.rootDir, 'data', 'psx_historical.csv')
  ].filter(Boolean);

  return candidates.find((p) => fs.existsSync(p)) || null;
}

const insertStockStmt = db.prepare(`
  INSERT OR REPLACE INTO stocks (
    symbol, ldcp, open, high, low, close, change, change_pct, volume, date, timestamp
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

export function getLatestDbDate() {
  const row = db.prepare('SELECT MAX(date) AS max_date FROM stocks').get();
  return normalizeDate(row?.max_date) || null;
}

export function getAllSymbols(limit = 2000) {
  const rows = db.prepare(`
    SELECT DISTINCT symbol
    FROM stocks
    WHERE symbol IS NOT NULL AND symbol <> ''
    ORDER BY symbol ASC
    LIMIT ?
  `).all(limit);
  return rows.map((r) => String(r.symbol || '').toUpperCase()).filter(Boolean);
}

export async function importCsvToDb(csvPath) {
  if (!csvPath || !fs.existsSync(csvPath)) {
    return { path: csvPath || null, imported: 0, skipped: 0, status: 'missing' };
  }

  const stream = fs.createReadStream(csvPath, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let imported = 0;
  let skipped = 0;
  let headerChecked = false;

  const tx = db.transaction((rows) => {
    for (const cols of rows) {
      try {
        const symbol = String(cols[0] || '').trim().toUpperCase();
        const date = normalizeDate(cols[9]);
        if (!symbol || !date) {
          skipped += 1;
          continue;
        }

        insertStockStmt.run(
          symbol,
          parseFloatSafe(cols[1]),
          parseFloatSafe(cols[2]),
          parseFloatSafe(cols[3]),
          parseFloatSafe(cols[4]),
          parseFloatSafe(cols[5]),
          parseFloatSafe(cols[6]),
          parseFloatSafe(cols[7]),
          parseFloatSafe(cols[8]),
          date,
          String(cols[10] || new Date().toISOString().replace('T', ' ').slice(0, 19)).trim()
        );
        imported += 1;
      } catch {
        skipped += 1;
      }
    }
  });

  const batch = [];
  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    const cols = parseCsvLine(line);

    if (!headerChecked) {
      headerChecked = true;
      const c0 = String(cols[0] || '').trim().toUpperCase();
      const c9 = String(cols[9] || '').trim().toUpperCase();
      if (c0 === 'SYMBOL' && c9 === 'DATE') {
        continue;
      }
    }

    if (cols.length < 11) {
      skipped += 1;
      continue;
    }

    batch.push(cols);
    if (batch.length >= 3000) {
      tx(batch.splice(0, batch.length));
    }
  }

  if (batch.length) tx(batch);

  return { path: csvPath, imported, skipped, status: 'ok' };
}

async function fetchHistoricalForDate(dateStr) {
  const res = await axios.post(
    PSX_HISTORICAL_URL,
    new URLSearchParams({ date: dateStr }).toString(),
    {
      timeout: 30000,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'User-Agent': 'Mozilla/5.0'
      }
    }
  );

  const $ = cheerio.load(res.data);
  const rows = [];

  $('table tbody tr').each((_, tr) => {
    const tds = $(tr).find('td');
    if (tds.length < 9) return;

    const symbol = $(tds[0]).text().trim().toUpperCase();
    if (!symbol) return;

    rows.push({
      symbol,
      ldcp: parseFloatSafe($(tds[1]).text()),
      open: parseFloatSafe($(tds[2]).text()),
      high: parseFloatSafe($(tds[3]).text()),
      low: parseFloatSafe($(tds[4]).text()),
      close: parseFloatSafe($(tds[5]).text()),
      change: parseFloatSafe($(tds[6]).text()),
      change_pct: parseFloatSafe($(tds[7]).text()),
      volume: parseFloatSafe($(tds[8]).text()),
      date: dateStr,
      timestamp: new Date().toISOString().replace('T', ' ').slice(0, 19)
    });
  });

  return rows;
}

export async function syncMissingHistoricalData({ startDate } = {}) {
  const latest = getLatestDbDate();
  const fromDate = normalizeDate(startDate) || latest || '2024-01-01';
  let cursor = fromDate;
  const today = fmtDate(new Date());

  const tx = db.transaction((rows) => {
    for (const r of rows) {
      insertStockStmt.run(
        r.symbol,
        r.ldcp,
        r.open,
        r.high,
        r.low,
        r.close,
        r.change,
        r.change_pct,
        r.volume,
        r.date,
        r.timestamp
      );
    }
  });

  let daysAttempted = 0;
  let daysWithData = 0;
  let rowsUpserted = 0;
  const errors = [];

  while (cursor <= today) {
    daysAttempted += 1;
    try {
      const rows = await fetchHistoricalForDate(cursor);
      if (rows.length) {
        tx(rows);
        rowsUpserted += rows.length;
        daysWithData += 1;
      }
    } catch (err) {
      errors.push({ date: cursor, error: String(err?.message || err) });
    }
    cursor = nextDate(cursor);
  }

  return {
    start_date: fromDate,
    end_date: today,
    days_attempted: daysAttempted,
    days_with_data: daysWithData,
    rows_upserted: rowsUpserted,
    latest_date_after_sync: getLatestDbDate(),
    errors: errors.slice(0, 10)
  };
}

let startupSyncPromise = null;

export function runStartupHistoricalSync() {
  if (startupSyncPromise) return startupSyncPromise;

  startupSyncPromise = (async () => {
    const csvPath = chooseCsvPath();
    const csv = await importCsvToDb(csvPath);
    const historical = await syncMissingHistoricalData();
    return {
      csv,
      historical
    };
  })().catch((err) => ({ error: String(err?.message || err) }));

  return startupSyncPromise;
}
