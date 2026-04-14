import axios from 'axios';
import * as cheerio from 'cheerio';
import { config } from '../config.js';

const DPS_HOME_URL = 'https://dps.psx.com.pk/';
const DPS_INDICES_URL = 'https://dps.psx.com.pk/indices';
const DPS_TIMESERIES_URL = 'https://dps.psx.com.pk/timeseries';
const USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';

const state = {
  cache: new Map()
};

function cacheKey(indexCode) {
  return String(indexCode || 'KSE100').trim().toUpperCase();
}

function parseNumber(value) {
  if (value == null) return null;
  const cleaned = String(value)
    .replace(/,/g, '')
    .replace(/\u00a0/g, ' ')
    .match(/[-+]?\d+(?:\.\d+)?/);
  return cleaned ? Number(cleaned[0]) : null;
}

function parsePercent(value) {
  const parsed = parseNumber(String(value || '').replace('%', ''));
  return Number.isFinite(parsed) ? parsed : null;
}

function round2(value) {
  return Number.isFinite(Number(value)) ? Number(Number(value).toFixed(2)) : null;
}

function parseAsOf(text) {
  const match = String(text || '').match(/As of\s+([A-Za-z]{3}\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s*[AP]M)/i);
  return match ? match[1].trim() : null;
}

function parseIndicesRow(html, indexCode) {
  const code = cacheKey(indexCode);
  const $ = cheerio.load(html || '');
  const rows = $('table tr').toArray();

  for (const row of rows) {
    const cols = $(row).find('td').toArray().map((c) => $(c).text().trim());
    if (cols.length < 6) continue;
    if (String(cols[0] || '').trim().toUpperCase() !== code) continue;

    return {
      code,
      high: parseNumber(cols[1]),
      low: parseNumber(cols[2]),
      current: parseNumber(cols[3]),
      change: parseNumber(cols[4]),
      change_pct: parsePercent(cols[5])
    };
  }

  return null;
}

function normalizeSeries(rawRows = []) {
  const rows = Array.isArray(rawRows) ? rawRows : [];
  const normalized = rows
    .map((r) => {
      const ts = Number(Array.isArray(r) ? r[0] : null);
      const close = Number(Array.isArray(r) ? r[1] : null);
      const volume = Number(Array.isArray(r) ? r[2] : null);
      const open = Number(Array.isArray(r) ? r[3] : null);

      if (!Number.isFinite(ts) || !Number.isFinite(close)) return null;
      const dateObj = new Date(ts * 1000);
      if (Number.isNaN(dateObj.getTime())) return null;

      return {
        ts,
        date: dateObj.toISOString().slice(0, 10),
        open: Number.isFinite(open) ? open : close,
        close,
        volume: Number.isFinite(volume) ? volume : null
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.ts - b.ts);

  return normalized;
}

function seriesToHistory(series = [], days = 365) {
  const safeDays = Math.max(7, Number(days || 365));
  if (!series.length) return [];

  const endTs = series[series.length - 1].ts;
  const startTs = endTs - (safeDays * 24 * 60 * 60);

  const filtered = series.filter((row) => row.ts >= startTs);

  return filtered.map((row, idx) => {
    const prev = idx > 0 ? filtered[idx - 1] : null;
    const high = Math.max(row.open, row.close);
    const low = Math.min(row.open, row.close);
    const dayChangePct = prev && Number.isFinite(prev.close) && prev.close !== 0
      ? ((row.close - prev.close) / prev.close) * 100
      : 0;

    return {
      date: row.date,
      open: round2(row.open),
      high: round2(high),
      low: round2(low),
      close: round2(row.close),
      volume: row.volume,
      change_pct: round2(dayChangePct)
    };
  });
}

function buildPayload(indexCode, raw, days) {
  const code = cacheKey(indexCode);
  const series = normalizeSeries(raw?.timeseriesRows || []);
  const latest = series.length ? series[series.length - 1] : null;
  const previous = series.length > 1 ? series[series.length - 2] : null;
  const row = raw?.indicesRow || null;

  const indexValue = row?.current ?? latest?.close ?? null;
  const change = row?.change ?? (
    latest && previous ? (latest.close - previous.close) : null
  );
  const changePct = row?.change_pct ?? (
    latest && previous && previous.close
      ? ((latest.close - previous.close) / previous.close) * 100
      : null
  );

  const high = row?.high ?? null;
  const low = row?.low ?? null;
  const previousClose = Number.isFinite(indexValue) && Number.isFinite(change)
    ? indexValue - change
    : (previous?.close ?? null);

  return {
    index_code: code,
    index_value: round2(indexValue),
    change: round2(change),
    change_percent: round2(changePct),
    volume: latest?.volume ?? null,
    high: round2(high),
    low: round2(low),
    previous_close: round2(previousClose),
    day_range: {
      low: round2(low),
      high: round2(high)
    },
    as_of: raw?.asOf || (latest ? `${latest.date} 5:00 PM` : null),
    date: latest?.date || null,
    history: seriesToHistory(series, days),
    source: 'dps.psx.com.pk',
    fetched_at: raw?.fetchedAt || new Date().toISOString()
  };
}

async function fetchMarketSummaryFromDps(indexCode) {
  const code = cacheKey(indexCode);
  const [homeRes, indicesRes, seriesRes] = await Promise.all([
    axios.get(DPS_HOME_URL, { timeout: 30000, headers: { 'User-Agent': USER_AGENT } }),
    axios.get(DPS_INDICES_URL, { timeout: 30000, headers: { 'User-Agent': USER_AGENT } }),
    axios.get(`${DPS_TIMESERIES_URL}/eod/${encodeURIComponent(code)}`, { timeout: 30000, headers: { 'User-Agent': USER_AGENT } })
  ]);

  const homeHtml = String(homeRes?.data || '');
  const indicesHtml = String(indicesRes?.data || '');
  const seriesRows = Array.isArray(seriesRes?.data?.data) ? seriesRes.data.data : [];

  const indicesRow = parseIndicesRow(indicesHtml, code);
  if (!indicesRow && !seriesRows.length) {
    throw new Error(`No DPS market summary data found for ${code}`);
  }

  return {
    indexCode: code,
    asOf: parseAsOf(homeHtml),
    indicesRow,
    timeseriesRows: seriesRows,
    fetchedAt: new Date().toISOString()
  };
}

function isFresh(entry) {
  if (!entry?.at) return false;
  const ttlMs = Math.max(1, Number(config.marketSummaryCacheMinutes || 5)) * 60 * 1000;
  return (Date.now() - entry.at) <= ttlMs;
}

export async function getMarketSummary({ indexCode = 'KSE100', days = 365, forceRefresh = false } = {}) {
  const code = cacheKey(indexCode);
  const existing = state.cache.get(code);

  if (!forceRefresh && existing && isFresh(existing)) {
    return {
      ...buildPayload(code, existing.raw, days),
      cache_status: 'hit'
    };
  }

  try {
    const raw = await fetchMarketSummaryFromDps(code);
    state.cache.set(code, { at: Date.now(), raw });
    return {
      ...buildPayload(code, raw, days),
      cache_status: existing ? 'refresh' : 'miss'
    };
  } catch (err) {
    if (existing) {
      return {
        ...buildPayload(code, existing.raw, days),
        cache_status: 'stale_fallback',
        warning: String(err?.message || err)
      };
    }
    throw err;
  }
}
