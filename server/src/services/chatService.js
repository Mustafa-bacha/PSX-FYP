import axios from 'axios';
import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import { db } from '../lib/db.js';
import { config } from '../config.js';
import { getLatestBusinessNewsFeed } from './sentimentIngestionService.js';
import { scrapeCompanyPage } from './companyScraperService.js';

const PROMPT_TEMPLATE = `You are an advanced PSX-focused RAG financial assistant.

Use only the retrieved evidence and conversation history. If evidence is weak, say so explicitly. Reference evidence by bracket numbers like [1], [2] where helpful.

Response requirements:
1) Use clear Markdown headings exactly in this order:
  ## Direct Answer
  ## Historical Data Evidence
  ## Recommendation
2) Length: aim for **200–320 words** normally; if the user asked for a full/deep/comprehensive outlook, use **280–420 words**. Do not repeat the same sentence across sections.
3) In **Historical Data Evidence**, organize with labeled bullets:
   - **CSV / price archive** — multi-timeframe returns, MA spread, volatility, volume ratio, date span (when csv_* or CSV-backed chunks exist).
   - **PSX DPS profile** — sector, P/E, YTD/1Y, 52w range, business description (when psx_dps_company_page chunks exist).
   - **Trend / database metrics** — latest session, breadth, sentiment aggregates when present.
   - **News** — macro + symbol headlines; flag if a headline is macro-only vs symbol-specific.
4) Use **bold** key metrics (**Close**, **Change%**, **MA20/MA60**, **P/E**, **Sentiment**, etc.).
5) If the user asks for related/peer names, synthesize **liquid_peer_universe** and **related_symbol_sentiment** chunks explicitly — do not invent tickers not in evidence.
6) Never guarantee returns. Educational tone only.
7) Non-financial small talk: brief reply, steer back to PSX.
8) For a specific symbol, stay on that symbol unless the user asks to compare.

Retrieved Evidence:
{retrieved_docs}

Conversation History:
{chat_history}

User Question:
{user_query}`;

const STOPWORDS = new Set([
  'a', 'an', 'the', 'is', 'are', 'was', 'were', 'be', 'to', 'for', 'of', 'in', 'on', 'at', 'from', 'and', 'or', 'with', 'about',
  'this', 'that', 'these', 'those', 'it', 'its', 'as', 'by', 'into', 'over', 'under', 'i', 'you', 'we', 'they', 'he', 'she', 'them',
  'my', 'your', 'our', 'their', 'what', 'which', 'how', 'why', 'when', 'where', 'who', 'should', 'would', 'could', 'can', 'may',
  'today', 'now', 'latest', 'current', 'tell', 'me', 'please'
]);

const FINANCE_KEYWORDS = [
  'psx', 'kse', 'market', 'stock', 'stocks', 'share', 'shares', 'ticker', 'symbol', 'price', 'close', 'open', 'high', 'low',
  'volume', 'sentiment', 'outlook', 'risk', 'returns', 'trend', 'momentum', 'support', 'resistance', 'portfolio',
  'sector', 'bullish', 'bearish', 'earnings', 'dividend', 'p/e', 'pe ratio', 'ma20', 'ma60', 'watchlist', 'buy', 'sell',
  'recommend', 'recommendation', 'recommendations', 'advice'
];

const OFFTOPIC_HINT_KEYWORDS = [
  'physics', 'chemistry', 'biology', 'math', 'mathematics', 'geography', 'history', 'recipe', 'poem', 'joke', 'astrology',
  'horoscope', 'movie', 'football', 'cricket', 'music', 'travel', 'weather', 'coding', 'javascript', 'python', 'java'
];

const FORBIDDEN_USER_VISIBLE_PHRASES = [
  /given the current database status[^\n.]*(?:\.|$)/ig,
  /csv fallback evidence[^\n.]*(?:\.|$)/ig,
  /database status[^\n.]*(?:\.|$)/ig,
  /degraded mode response[^\n.]*(?:\.|$)/ig,
  /repair\/?restore the sqlite database[^\n.]*(?:\.|$)/ig,
  /integrity issue[^\n.]*(?:\.|$)/ig,
  /news-only,?\s*no historical scoring[^\n.]*(?:\.|$)/ig,
  /analysis is based on limited evidence[^\n.]*(?:\.|$)/ig,
  /local database is corrupted[^\n.]*(?:\.|$)/ig,
  /database-backed analysis[^\n.]*(?:\.|$)/ig
];

const csvFallbackCache = {
  path: null,
  mtimeMs: 0,
  snapshot: null,
  loadingPromise: null
};

/** Per-symbol OHLCV rows from incremental CSV (key: path|mtime|SYMBOL). */
const csvSymbolSeriesCache = new Map();
const csvSymbolSeriesLoading = new Map();

/** PSX DPS company page evidence (avoid hammering dps.psx.com.pk). */
const psxCompanyDocCache = new Map();
const PSX_COMPANY_CACHE_TTL_MS = Number(process.env.PSX_COMPANY_RAG_CACHE_MS || 45 * 60 * 1000);

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

function parseNum(value) {
  if (value == null) return null;
  const cleaned = String(value).replaceAll(',', '').replaceAll('%', '').trim();
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function normalizeDate(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function pickCsvFallbackPath() {
  const candidates = [
    process.env.PSX_CSV_PATH,
    config.incrementalCsvPath,
    path.resolve(config.rootDir, 'data', 'new_psx_historical_.csv'),
    path.resolve(config.rootDir, '..', 'new_psx_historical_.csv'),
    path.resolve(config.rootDir, 'data', 'psx_historical.csv')
  ].filter(Boolean);

  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) return p;
    } catch {
      // ignore
    }
  }
  return null;
}

async function loadCsvMarketSnapshot() {
  const csvPath = pickCsvFallbackPath();
  if (!csvPath) return null;

  let stat;
  try {
    stat = fs.statSync(csvPath);
  } catch {
    return null;
  }

  if (
    csvFallbackCache.snapshot
    && csvFallbackCache.path === csvPath
    && csvFallbackCache.mtimeMs === Number(stat.mtimeMs || 0)
  ) {
    return csvFallbackCache.snapshot;
  }

  if (csvFallbackCache.loadingPromise) {
    return csvFallbackCache.loadingPromise;
  }

  csvFallbackCache.loadingPromise = (async () => {
    const stream = fs.createReadStream(csvPath, { encoding: 'utf-8' });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    const latestBySymbol = new Map();
    let firstLine = true;

    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      const cols = parseCsvLine(line);
      if (cols.length < 10) continue;

      if (firstLine) {
        firstLine = false;
        const c0 = String(cols[0] || '').trim().toUpperCase();
        const c9 = String(cols[9] || '').trim().toUpperCase();
        if (c0 === 'SYMBOL' && c9 === 'DATE') continue;
      }

      const symbol = String(cols[0] || '').trim().toUpperCase();
      const date = normalizeDate(cols[9]);
      if (!symbol || !date) continue;

      const row = {
        symbol,
        date,
        close: parseNum(cols[5]),
        change_pct: parseNum(cols[7]),
        volume: parseNum(cols[8])
      };

      const prev = latestBySymbol.get(symbol);
      if (!prev || String(row.date) > String(prev.date)) {
        latestBySymbol.set(symbol, row);
      }
    }

    const rows = Array.from(latestBySymbol.values());
    if (!rows.length) return null;

    const latestDate = rows.reduce((mx, r) => (r.date > mx ? r.date : mx), rows[0].date);
    const usableChangeRows = rows.filter((r) => Number.isFinite(r.change_pct));
    const avgChange = usableChangeRows.length
      ? (usableChangeRows.reduce((s, r) => s + Number(r.change_pct || 0), 0) / usableChangeRows.length)
      : null;

    const snapshot = {
      csvPath,
      latestDate,
      bySymbol: latestBySymbol,
      rows,
      advancers: rows.filter((r) => Number(r.change_pct || 0) > 0).length,
      decliners: rows.filter((r) => Number(r.change_pct || 0) < 0).length,
      avgChange,
      topGainers: [...rows].filter((r) => Number.isFinite(r.change_pct)).sort((a, b) => Number(b.change_pct || 0) - Number(a.change_pct || 0)).slice(0, 8),
      topLosers: [...rows].filter((r) => Number.isFinite(r.change_pct)).sort((a, b) => Number(a.change_pct || 0) - Number(b.change_pct || 0)).slice(0, 8),
      topVolume: [...rows].filter((r) => Number.isFinite(r.volume)).sort((a, b) => Number(b.volume || 0) - Number(a.volume || 0)).slice(0, 8)
    };

    csvFallbackCache.path = csvPath;
    csvFallbackCache.mtimeMs = Number(stat.mtimeMs || 0);
    csvFallbackCache.snapshot = snapshot;
    return snapshot;
  })();

  try {
    return await csvFallbackCache.loadingPromise;
  } finally {
    csvFallbackCache.loadingPromise = null;
  }
}

/**
 * Load descending-dated price rows for one symbol from the incremental CSV (full series scan, cached per file mtime).
 */
async function loadCsvSeriesForSymbol(symbol) {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym || sym === 'MARKET') return [];

  const csvPath = pickCsvFallbackPath();
  if (!csvPath) return [];

  let stat;
  try {
    stat = fs.statSync(csvPath);
  } catch {
    return [];
  }

  const cacheKey = `${csvPath}|${Number(stat.mtimeMs || 0)}|${sym}`;
  if (csvSymbolSeriesCache.has(cacheKey)) {
    return csvSymbolSeriesCache.get(cacheKey);
  }
  if (csvSymbolSeriesLoading.has(cacheKey)) {
    return csvSymbolSeriesLoading.get(cacheKey);
  }

  const promise = (async () => {
    const stream = fs.createReadStream(csvPath, { encoding: 'utf-8' });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    const collected = [];
    let firstLine = true;

    for await (const line of rl) {
      if (!line || !line.trim()) continue;
      const cols = parseCsvLine(line);
      if (cols.length < 10) continue;

      if (firstLine) {
        firstLine = false;
        const c0 = String(cols[0] || '').trim().toUpperCase();
        const c9 = String(cols[9] || '').trim().toUpperCase();
        if (c0 === 'SYMBOL' && c9 === 'DATE') continue;
      }

      const rowSym = String(cols[0] || '').trim().toUpperCase();
      if (rowSym !== sym) continue;

      const date = normalizeDate(cols[9]);
      if (!date) continue;

      collected.push({
        date,
        close: parseNum(cols[5]),
        change_pct: parseNum(cols[7]),
        volume: parseNum(cols[8])
      });
    }

    collected.sort((a, b) => String(b.date).localeCompare(String(a.date)));
    const trimmed = collected.slice(0, 130);
    csvSymbolSeriesCache.set(cacheKey, trimmed);
    return trimmed;
  })();

  csvSymbolSeriesLoading.set(cacheKey, promise);
  try {
    return await promise;
  } finally {
    csvSymbolSeriesLoading.delete(cacheKey);
  }
}

function buildCsvDeepDocsForSymbol(scope, rows) {
  if (!rows?.length) return [];

  const latest = rows[0];
  const closeAt5 = rows[Math.min(4, rows.length - 1)]?.close;
  const closeAt20 = rows[Math.min(19, rows.length - 1)]?.close;
  const closeAt60 = rows[Math.min(59, rows.length - 1)]?.close;

  const ret5 = pctChange(latest?.close, closeAt5);
  const ret20 = pctChange(latest?.close, closeAt20);
  const ret60 = pctChange(latest?.close, closeAt60);

  const closes20 = rows.slice(0, 20).map((r) => Number(r.close));
  const closes60 = rows.slice(0, 60).map((r) => Number(r.close));
  const ma20 = average(closes20);
  const ma60 = average(closes60);
  const maSpreadPct = pctChange(ma20, ma60);

  const vol20 = stdDev(rows.slice(0, 20).map((r) => Number(r.change_pct)));
  const avgVol20 = average(rows.slice(0, 20).map((r) => Number(r.volume)));
  const volRatio = Number.isFinite(Number(latest?.volume)) && Number.isFinite(avgVol20) && avgVol20 > 0
    ? Number(latest.volume) / avgVol20
    : null;

  const regime = deriveRegime({
    ret20,
    ret60,
    maSpreadPct,
    sent30: null,
    vol20
  });

  const closesAll = rows.map((r) => Number(r.close)).filter((n) => Number.isFinite(n));
  const minC = closesAll.length ? Math.min(...closesAll) : null;
  const maxC = closesAll.length ? Math.max(...closesAll) : null;
  const avgC = average(closesAll);
  const avgChg = average(rows.map((r) => Number(r.change_pct)).filter((n) => Number.isFinite(n)));
  const lastDate = latest?.date;
  const firstDate = rows[rows.length - 1]?.date;
  const csvLabel = path.basename(pickCsvFallbackPath() || 'psx_historical.csv');

  return [
    {
      id: `csvdeep::${scope}::multi`,
      source: 'csv_symbol_multi_timeframe_metrics',
      type: 'historical',
      scope,
      published_at: lastDate,
      text: `${scope} CSV-backed (${csvLabel}) multi-timeframe: 5-session return ${formatNum(ret5)}%, 20-session ${formatNum(ret20)}%, 60-session ${formatNum(ret60)}%, MA20 ${formatNum(ma20)} vs MA60 ${formatNum(ma60)} (spread ${formatNum(maSpreadPct)}%), as of ${lastDate}.`
    },
    {
      id: `csvdeep::${scope}::regime`,
      source: 'csv_symbol_regime_inference',
      type: 'historical',
      scope,
      published_at: lastDate,
      text: `${scope} CSV-backed regime: ${regime.regime} (confidence ${regime.confidence}%, bullish ${regime.bullishSignals}, bearish ${regime.bearishSignals}), 20-session stdev of daily % ${formatNum(vol20)}%, volume vs 20d avg ${formatNum(volRatio)}x.`
    },
    {
      id: `csvdeep::${scope}::rollup`,
      source: 'csv_symbol_history_rollup',
      type: 'historical',
      scope,
      published_at: lastDate,
      text: `${scope} CSV archive span ${firstDate}→${lastDate}: ${rows.length} rows in file window, avg close ${formatNum(avgC)}, range ${formatNum(minC)}-${formatNum(maxC)} PKR, mean daily change ${formatNum(avgChg)}%.`
    }
  ];
}

async function getCsvFallbackHistoricalDocs(scope) {
  const snapshot = await loadCsvMarketSnapshot();

  const docs = [];
  if (scope === 'MARKET') {
    if (!snapshot) return [];
    docs.push({
      id: 'csv::market::breadth',
      source: 'csv_market_breadth',
      type: 'historical',
      published_at: snapshot.latestDate,
      text: `CSV market breadth (${snapshot.latestDate}): advancers ${snapshot.advancers}, decliners ${snapshot.decliners}, average change ${formatNum(snapshot.avgChange)}%.`
    });
    docs.push({
      id: 'csv::market::gainers',
      source: 'csv_market_gainers',
      type: 'historical',
      published_at: snapshot.latestDate,
      text: `Top gainers by latest change%: ${snapshot.topGainers.slice(0, 5).map((r) => `${r.symbol} (${formatNum(r.change_pct)}%)`).join(', ')}.`
    });
    docs.push({
      id: 'csv::market::losers',
      source: 'csv_market_losers',
      type: 'historical',
      published_at: snapshot.latestDate,
      text: `Top losers by latest change%: ${snapshot.topLosers.slice(0, 5).map((r) => `${r.symbol} (${formatNum(r.change_pct)}%)`).join(', ')}.`
    });
    docs.push({
      id: 'csv::market::active',
      source: 'csv_market_active',
      type: 'historical',
      published_at: snapshot.latestDate,
      text: `Most active by volume: ${snapshot.topVolume.slice(0, 5).map((r) => `${r.symbol} (vol ${formatNum(r.volume, 0)})`).join(', ')}.`
    });
    return docs;
  }

  const series = await loadCsvSeriesForSymbol(scope);
  if (series.length >= 5) {
    let dbLatest = null;
    try {
      const row = db.prepare('SELECT MAX(date) AS d FROM stocks WHERE symbol = ?').get(scope);
      dbLatest = row?.d != null ? String(row.d).slice(0, 10) : null;
    } catch {
      dbLatest = null;
    }
    const csvLatest = series[0]?.date;
    const csvStale = Boolean(dbLatest && csvLatest && String(csvLatest) < String(dbLatest));

    if (csvStale) {
      docs.push({
        id: `csv::${scope}::staleness`,
        source: 'csv_staleness_guardrail',
        type: 'historical',
        scope,
        published_at: csvLatest,
        text: `${scope}: incremental CSV last bar is **${csvLatest}** while SQLite has prices through **${dbLatest}**. Omitted CSV momentum/regime metrics that would **conflict** with live DB; use DB + PSX DPS for current returns.`
      });
      const closesAll = series.map((r) => Number(r.close)).filter((n) => Number.isFinite(n));
      if (closesAll.length) {
        const firstDate = series[series.length - 1]?.date;
        docs.push({
          id: `csv::${scope}::archival_only`,
          source: 'csv_symbol_archival_rollup',
          type: 'historical',
          scope,
          published_at: csvLatest,
          text: `${scope} CSV archival window only (${firstDate}→${csvLatest}, ${series.length} bars): avg close ${formatNum(average(closesAll))}, range ${formatNum(Math.min(...closesAll))}-${formatNum(Math.max(...closesAll))} PKR.`
        });
      }
    } else {
      docs.push(...buildCsvDeepDocsForSymbol(scope, series));
    }
  } else if (snapshot) {
    const row = snapshot.bySymbol.get(String(scope || '').toUpperCase());
    if (row) {
      docs.push({
        id: `csv::${scope}::latest`,
        source: 'csv_symbol_latest',
        type: 'historical',
        scope,
        published_at: row.date,
        text: `${scope} latest CSV record (${row.date}): close ${formatNum(row.close)} PKR, change ${formatNum(row.change_pct)}%, volume ${formatNum(row.volume, 0)}.`
      });
    }
  }

  return docs;
}

async function getPsxCompanyPageDocs(scope) {
  if (!scope || scope === 'MARKET') return [];

  const sym = String(scope).toUpperCase();
  const cached = psxCompanyDocCache.get(sym);
  if (cached && (Date.now() - cached.at) < PSX_COMPANY_CACHE_TTL_MS) {
    return cached.docs;
  }

  try {
    const data = await scrapeCompanyPage(sym);
    const url = `https://dps.psx.com.pk/company/${sym}`;
    const quoteBits = [
      `${sym} PSX DPS quote`,
      data.company_name ? `name: ${normalizeText(data.company_name)}` : '',
      data.sector ? `sector: ${normalizeText(data.sector)}` : '',
      data.as_of ? `timing: ${normalizeText(data.as_of)}` : '',
      Number.isFinite(data.close) ? `close ${formatNum(data.close)} PKR` : '',
      Number.isFinite(data.change_pct) ? `day change ${formatNum(data.change_pct)}%` : '',
      Number.isFinite(data.volume) ? `volume ${formatNum(data.volume, 0)}` : '',
      Number.isFinite(data.pe_ratio) ? `P/E TTM ${formatNum(data.pe_ratio)}` : '',
      Number.isFinite(data.ytd_change_pct) ? `YTD ${formatNum(data.ytd_change_pct)}%` : '',
      Number.isFinite(data.year_change_pct) ? `1Y ${formatNum(data.year_change_pct)}%` : '',
      data.year_range?.low != null && data.year_range?.high != null
        ? `52w ${formatNum(data.year_range.low)}-${formatNum(data.year_range.high)} PKR`
        : ''
    ].filter(Boolean);

    const docs = [
      {
        id: `psx::${sym}::quote`,
        source: 'psx_dps_company_page',
        type: 'report',
        scope: sym,
        published_at: data.fetched_at,
        text: `${quoteBits.join('; ')}. Source: ${url}`
      }
    ];

    if (data.profile?.business_description) {
      docs.push({
        id: `psx::${sym}::business`,
        source: 'psx_dps_company_page',
        type: 'report',
        scope: sym,
        published_at: data.fetched_at,
        text: `${sym} business description (PSX DPS): ${truncateText(normalizeText(data.profile.business_description), 520)}`
      });
    }

    psxCompanyDocCache.set(sym, { at: Date.now(), docs });
    return docs;
  } catch {
    return [];
  }
}

function normalizeScope(stock) {
  const raw = String(stock || '').trim().toUpperCase();
  if (!raw || ['MARKET', 'ALL', 'OVERALL', 'PSX', 'KSE', 'GENERAL'].includes(raw)) return 'MARKET';
  return raw;
}

const RESERVED_SCOPE_WORDS = new Set([
  'MARKET', 'STOCK', 'STOCKS', 'PSX', 'KSE', 'THIS', 'THAT', 'WITH', 'WHAT', 'WHY', 'HOW', 'WHEN', 'WHERE', 'LATEST', 'TODAY', 'HISTORY'
]);

function inferScopeFromQuestion(question, history = []) {
  const q = String(question || '');
  const tokenRegex = /\b[A-Za-z]{3,5}\b/g;

  const extractSymbols = (text) => {
    const tokens = String(text || '').match(tokenRegex) || [];
    return tokens
      .map((t) => t.toUpperCase())
      .filter((t) => isLikelyEquitySymbol(t) && !RESERVED_SCOPE_WORDS.has(t));
  };

  const inQuestion = extractSymbols(q);
  if (inQuestion.length) {
    const row = db.prepare(`
      SELECT symbol
      FROM stocks
      WHERE symbol IN (${inQuestion.map(() => '?').join(',')})
      LIMIT 1
    `).get(...inQuestion);
    if (row?.symbol) return String(row.symbol).toUpperCase();
  }

  const recent = Array.isArray(history)
    ? history
      .slice(-16)
      .filter((msg) => String(msg?.role || '').toLowerCase() === 'user')
      .reverse()
    : [];

  const asksGenericFinanceFollowup = /(recommend|recommendation|recommendations|advice|outlook|risk|trend|view|take)/i.test(q);
  if (asksGenericFinanceFollowup && !inQuestion.length && recent.length) {
    const immediatePrior = String(recent[0]?.content || '');
    if (/\b(market|psx|kse|overall)\b/i.test(immediatePrior)) {
      return 'MARKET';
    }
  }

  if (!/(this\s+stock|this\s+stocks|that\s+stock|that\s+stocks|it\b)/i.test(q)) return null;

  for (const msg of recent) {
    const candidates = extractSymbols(msg?.content);
    if (!candidates.length) continue;
    const row = db.prepare(`
      SELECT symbol
      FROM stocks
      WHERE symbol IN (${candidates.map(() => '?').join(',')})
      LIMIT 1
    `).get(...candidates);
    if (row?.symbol) return String(row.symbol).toUpperCase();
  }

  return null;
}

function formatNum(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '—';
  return n.toFixed(digits);
}

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function removeForbiddenPhrases(value) {
  let text = String(value || '');
  for (const pattern of FORBIDDEN_USER_VISIBLE_PHRASES) {
    text = text.replace(pattern, ' ');
  }
  return text.replace(/\s+/g, ' ').trim();
}

function isLikelyEquitySymbol(symbol) {
  const s = String(symbol || '').trim().toUpperCase();
  if (!s) return false;
  if (s.length < 3 || s.length > 5) return false;
  return /^[A-Z]+$/.test(s);
}

function stripHtml(value) {
  return String(value || '')
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/\s+/g, ' ')
    .trim();
}

function truncateText(value, maxLen = 220) {
  const clean = normalizeText(value);
  if (!clean || clean.length <= maxLen) return clean;
  return `${clean.slice(0, Math.max(60, maxLen)).trim()}…`;
}

function dedupeDocs(docs = []) {
  const seen = new Set();
  const output = [];
  for (const doc of docs || []) {
    const source = String(doc?.source || '').toLowerCase().trim();
    const type = String(doc?.type || '').toLowerCase().trim();
    const scope = String(doc?.scope || '').toUpperCase().trim();
    const text = normalizeText(doc?.text).toLowerCase();
    if (!text) continue;
    const key = `${scope}|${type}|${source}|${text}`;
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(doc);
  }
  return output;
}

function tokenize(text) {
  return normalizeText(text)
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 2 && !STOPWORDS.has(t));
}

function parseDateValue(input) {
  const d = new Date(input || '');
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function recencyBoost(dateStr) {
  const d = parseDateValue(dateStr);
  if (!d) return 0;
  const ageDays = Math.max(0, (Date.now() - d.getTime()) / (1000 * 60 * 60 * 24));
  if (ageDays <= 1) return 0.28;
  if (ageDays <= 3) return 0.22;
  if (ageDays <= 7) return 0.15;
  if (ageDays <= 30) return 0.08;
  return 0;
}

function average(values) {
  const nums = (Array.isArray(values) ? values : []).map((v) => Number(v)).filter((n) => Number.isFinite(n));
  if (!nums.length) return null;
  return nums.reduce((s, n) => s + n, 0) / nums.length;
}

function stdDev(values) {
  const nums = (Array.isArray(values) ? values : []).map((v) => Number(v)).filter((n) => Number.isFinite(n));
  if (nums.length < 2) return null;
  const mean = average(nums);
  const variance = nums.reduce((s, n) => s + ((n - mean) ** 2), 0) / nums.length;
  return Math.sqrt(variance);
}

function pctChange(latest, base) {
  const a = Number(latest);
  const b = Number(base);
  if (!Number.isFinite(a) || !Number.isFinite(b) || b === 0) return null;
  return ((a / b) - 1) * 100;
}

function deriveRegime({ ret20, ret60, maSpreadPct, sent30, vol20 }) {
  let bullish = 0;
  let bearish = 0;

  if (Number.isFinite(ret20)) {
    if (ret20 > 1.2) bullish += 1;
    if (ret20 < -1.2) bearish += 1;
  }
  if (Number.isFinite(ret60)) {
    if (ret60 > 2.5) bullish += 1;
    if (ret60 < -2.5) bearish += 1;
  }
  if (Number.isFinite(maSpreadPct)) {
    if (maSpreadPct > 1.0) bullish += 1;
    if (maSpreadPct < -1.0) bearish += 1;
  }
  if (Number.isFinite(sent30)) {
    if (sent30 > 0.08) bullish += 1;
    if (sent30 < -0.08) bearish += 1;
  }

  const confidence = Math.min(100, Math.round((Math.max(bullish, bearish) / 4) * 100));

  let regime = 'mixed / range-bound';
  if (bullish >= 3 && bullish > bearish) regime = 'bullish continuation';
  else if (bearish >= 3 && bearish > bullish) regime = 'bearish pressure';
  else if (Number.isFinite(vol20) && vol20 > 2.8) regime = 'high-volatility transition';

  return { regime, confidence, bullishSignals: bullish, bearishSignals: bearish };
}

function getSymbolAdvancedDocs(scope) {
  const rows = db.prepare(`
    SELECT date, close, change_pct, volume
    FROM stocks
    WHERE symbol = ?
    ORDER BY date DESC
    LIMIT 90
  `).all(scope);

  if (!rows.length) return [];

  const latest = rows[0];
  const closeAt5 = rows[Math.min(4, rows.length - 1)]?.close;
  const closeAt20 = rows[Math.min(19, rows.length - 1)]?.close;
  const closeAt60 = rows[Math.min(59, rows.length - 1)]?.close;

  const ret5 = pctChange(latest?.close, closeAt5);
  const ret20 = pctChange(latest?.close, closeAt20);
  const ret60 = pctChange(latest?.close, closeAt60);

  const closes20 = rows.slice(0, 20).map((r) => Number(r.close));
  const closes60 = rows.slice(0, 60).map((r) => Number(r.close));
  const ma20 = average(closes20);
  const ma60 = average(closes60);
  const maSpreadPct = pctChange(ma20, ma60);

  const vol20 = stdDev(rows.slice(0, 20).map((r) => Number(r.change_pct)));
  const avgVol20 = average(rows.slice(0, 20).map((r) => Number(r.volume)));
  const volRatio = Number.isFinite(Number(latest?.volume)) && Number.isFinite(avgVol20) && avgVol20 > 0
    ? Number(latest.volume) / avgVol20
    : null;

  const sent7 = db.prepare(`
    SELECT AVG(score) AS avg_score,
           COUNT(*) AS total,
           SUM(CASE WHEN label='positive' THEN 1 ELSE 0 END) AS positive_count,
           SUM(CASE WHEN label='negative' THEN 1 ELSE 0 END) AS negative_count,
           SUM(CASE WHEN label='neutral' THEN 1 ELSE 0 END) AS neutral_count
    FROM sentiment
    WHERE symbol = ?
      AND analyzed_at >= datetime('now', '-7 day')
  `).get(scope);

  const sent30 = db.prepare(`
    SELECT AVG(score) AS avg_score,
           COUNT(*) AS total
    FROM sentiment
    WHERE symbol = ?
      AND analyzed_at >= datetime('now', '-30 day')
  `).get(scope);

  const regime = deriveRegime({
    ret20,
    ret60,
    maSpreadPct,
    sent30: Number(sent30?.avg_score),
    vol20
  });

  const docs = [
    {
      id: `hist::${scope}::multi_timeframe`,
      source: 'symbol_multi_timeframe_metrics',
      type: 'historical',
      scope,
      published_at: latest?.date,
      text: `${scope} multi-timeframe performance: 5-session return ${formatNum(ret5)}%, 20-session return ${formatNum(ret20)}%, 60-session return ${formatNum(ret60)}%, MA20 ${formatNum(ma20)} vs MA60 ${formatNum(ma60)} (spread ${formatNum(maSpreadPct)}%).`
    },
    {
      id: `hist::${scope}::regime`,
      source: 'symbol_regime_inference',
      type: 'historical',
      scope,
      published_at: latest?.date,
      text: `${scope} current regime inference: ${regime.regime} (confidence ${regime.confidence}%, bullish signals ${regime.bullishSignals}, bearish signals ${regime.bearishSignals}), 20-session volatility ${formatNum(vol20)}%, volume ratio vs 20-session average ${formatNum(volRatio)}x.`
    }
  ];

  if (sent7?.total) {
    docs.push({
      id: `hist::${scope}::sentiment_7d_30d`,
      source: 'symbol_sentiment_regime',
      type: 'historical',
      scope,
      published_at: latest?.date,
      text: `${scope} sentiment regime: last 7 days avg score ${formatNum(sent7.avg_score, 3)} (${sent7.positive_count || 0} positive, ${sent7.negative_count || 0} negative, ${sent7.neutral_count || 0} neutral), last 30 days avg score ${formatNum(sent30?.avg_score, 3)} from ${sent30?.total || 0} items.`
    });
  }

  return docs;
}

function getMarketAdvancedDocs() {
  const breadthSeries = db.prepare(`
    WITH last_dates AS (
      SELECT date
      FROM stocks
      GROUP BY date
      ORDER BY date DESC
      LIMIT 7
    )
    SELECT
      s.date,
      SUM(CASE WHEN s.change_pct > 0 THEN 1 ELSE 0 END) AS advancers,
      SUM(CASE WHEN s.change_pct < 0 THEN 1 ELSE 0 END) AS decliners,
      AVG(s.change_pct) AS avg_change
    FROM stocks s
    JOIN last_dates d ON d.date = s.date
    GROUP BY s.date
    ORDER BY s.date DESC
  `).all();

  const sent3 = db.prepare(`
    SELECT AVG(score) AS avg_score,
           COUNT(*) AS total,
           SUM(CASE WHEN label='positive' THEN 1 ELSE 0 END) AS positive_count,
           SUM(CASE WHEN label='negative' THEN 1 ELSE 0 END) AS negative_count
    FROM sentiment
    WHERE analyzed_at >= datetime('now', '-3 day')
  `).get();

  const sent7 = db.prepare(`
    SELECT AVG(score) AS avg_score,
           COUNT(*) AS total,
           SUM(CASE WHEN label='positive' THEN 1 ELSE 0 END) AS positive_count,
           SUM(CASE WHEN label='negative' THEN 1 ELSE 0 END) AS negative_count
    FROM sentiment
    WHERE analyzed_at >= datetime('now', '-7 day')
  `).get();

  const docs = [];
  if (breadthSeries.length) {
    const latest = breadthSeries[0];
    const meanBreadth = average(breadthSeries.map((r) => Number(r.advancers || 0) - Number(r.decliners || 0)));
    const latestBreadth = Number(latest.advancers || 0) - Number(latest.decliners || 0);
    const breadthDelta = Number.isFinite(meanBreadth) ? latestBreadth - meanBreadth : null;

    docs.push({
      id: 'hist::market::breadth_trend_7d',
      source: 'market_breadth_trend_7d',
      type: 'historical',
      published_at: latest.date,
      text: `PSX breadth trend (last 7 sessions): latest breadth ${latestBreadth} (advancers ${latest.advancers || 0}, decliners ${latest.decliners || 0}), 7-session average breadth ${formatNum(meanBreadth, 1)}, breadth delta vs average ${formatNum(breadthDelta, 1)}, latest average change ${formatNum(latest.avg_change)}%.`
    });

    docs.push({
      id: 'hist::market::breadth_series_7d',
      source: 'market_breadth_series_7d',
      type: 'historical',
      published_at: latest.date,
      text: `Breadth sequence newest->older: ${breadthSeries.slice(0, 5).map((r) => `${r.date}: ${(Number(r.advancers || 0) - Number(r.decliners || 0))}`).join(' | ')}.`
    });
  }

  if (sent7?.total) {
    const momentum = Number(sent3?.avg_score) - Number(sent7?.avg_score);
    docs.push({
      id: 'hist::market::sentiment_regime_3d_7d',
      source: 'market_sentiment_regime',
      type: 'historical',
      published_at: new Date().toISOString(),
      text: `Market sentiment regime: 3-day avg score ${formatNum(sent3?.avg_score, 3)} (${sent3?.positive_count || 0} positive / ${sent3?.negative_count || 0} negative), 7-day avg score ${formatNum(sent7?.avg_score, 3)} (${sent7?.positive_count || 0} positive / ${sent7?.negative_count || 0} negative), short-term momentum vs 7-day baseline ${formatNum(momentum, 3)}.`
    });
  }

  return docs;
}

function getStockSnapshot(symbol) {
  const latest = db.prepare(`
    SELECT symbol, date, close, change_pct, volume
    FROM stocks
    WHERE symbol = ?
    ORDER BY date DESC
    LIMIT 1
  `).get(symbol);

  const sentiment = db.prepare(`
    SELECT AVG(score) AS avg_score,
           SUM(CASE WHEN label='positive' THEN 1 ELSE 0 END) AS positive_count,
           SUM(CASE WHEN label='negative' THEN 1 ELSE 0 END) AS negative_count,
           SUM(CASE WHEN label='neutral' THEN 1 ELSE 0 END) AS neutral_count
    FROM sentiment
    WHERE symbol = ?
  `).get(symbol);

  const recentHeadlines = db.prepare(`
    SELECT headline, label, score, source, analyzed_at
    FROM sentiment
    WHERE symbol = ?
    ORDER BY analyzed_at DESC
    LIMIT 8
  `).all(symbol);

  return { latest, sentiment, recentHeadlines };
}

function detectIntent(question) {
  const rawQ = String(question || '').trim();
  const q = rawQ.toLowerCase();
  if (!q) return 'unknown';
  if (/^(hi|hello|hey|salam|assalam|aoa)\b/.test(q)) return 'greeting';

  const hasFinanceKeyword = FINANCE_KEYWORDS.some((kw) => {
    const escaped = String(kw).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`\\b${escaped}\\b`, 'i').test(q);
  });
  const symbolTokens = (rawQ.match(/\b[A-Z]{3,5}\b/g) || [])
    .map((t) => t.toUpperCase())
    .filter((t) => isLikelyEquitySymbol(t) && !RESERVED_SCOPE_WORDS.has(t));

  if (hasFinanceKeyword || symbolTokens.length) return 'finance';

  const hasOfftopicHint = OFFTOPIC_HINT_KEYWORDS.some((kw) => q.includes(kw));
  const asksGeneralDefinition = /^(what is|who is|define|explain|tell me about)\b/.test(q);
  if (hasOfftopicHint) return 'off_topic';
  if (asksGeneralDefinition && !hasFinanceKeyword && !symbolTokens.length) return 'off_topic';

  return 'unknown';
}

async function detectIntentWithGemini(question) {
  const apiKey = String(config.geminiApiKey || '').trim();
  if (!apiKey || !question) return null;

  const model = String(config.geminiIntentModel || 'gemini-2.0-flash-lite').trim();
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const prompt = [
    'Classify user intent for a Pakistan Stock Exchange assistant.',
    'Allowed labels: greeting, finance, off_topic.',
    'Return JSON only: {"label":"<one_label>","confidence":0..1}.',
    'Label finance for PSX/stock/market/investing/ticker analysis questions.',
    'Label off_topic for unrelated general-knowledge questions (e.g., "what is physics?").',
    `User question: ${String(question || '').trim()}`
  ].join('\n');

  try {
    const resp = await axios.post(
      endpoint,
      {
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: {
          temperature: 0,
          maxOutputTokens: 80,
          responseMimeType: 'application/json'
        }
      },
      {
        timeout: config.geminiIntentTimeoutMs
      }
    );

    const text = String(
      resp?.data?.candidates?.[0]?.content?.parts?.[0]?.text
      || ''
    ).trim();
    if (!text) return null;

    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = null;
    }

    const rawLabel = String(parsed?.label || '').toLowerCase().trim();
    if (['greeting', 'finance', 'off_topic'].includes(rawLabel)) {
      return rawLabel;
    }
  } catch {
    return null;
  }

  return null;
}

async function classifyUserIntent(question) {
  const ruleIntent = detectIntent(question);
  if (ruleIntent !== 'unknown') return ruleIntent;

  if (!config.enableGeminiIntentGuard) return 'unknown';
  const geminiIntent = await detectIntentWithGemini(question);
  return geminiIntent || 'unknown';
}

function buildOffTopicReply({ scope, question }) {
  const cleanQ = normalizeText(question);
  return [
    `I’m focused on PSX market analysis, so I can’t reliably answer general questions like "${cleanQ}".`,
    scope === 'MARKET'
      ? 'Try asking about today’s PSX outlook, strongest momentum+sentiment stocks, sector risk, or a specific ticker.'
      : `Try asking about **${scope}** price trend, sentiment shift, key risks, or a comparison with related PSX names.`
  ].join(' ');
}

const CHAT_SYSTEM_PROMPT = 'You are a PSX RAG assistant. Use retrieved evidence only; cite bracket indices [1], [2]. Ground fundamentals in psx_dps_company_page chunks and price history in csv_* chunks. EXACT headings: ## Direct Answer, ## Historical Data Evidence, ## Recommendation.';

async function callGeminiChatCompletion({ prompt, maxTokens, temperature }) {
  const apiKey = String(config.geminiApiKey || '').trim();
  if (!apiKey) return null;

  const model = String(config.geminiChatModel || 'gemini-2.0-flash').trim();
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const payload = {
    contents: [{
      parts: [{
        text: `${CHAT_SYSTEM_PROMPT}\n\n${String(prompt || '').trim()}`
      }]
    }],
    generationConfig: {
      temperature: Number.isFinite(Number(temperature)) ? Number(temperature) : 0.6,
      maxOutputTokens: Number.isFinite(Number(maxTokens)) ? Number(maxTokens) : 640
    }
  };

  const resp = await axios.post(endpoint, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: Number(config.geminiChatTimeoutMs || 60000)
  });

  const text = String(
    resp?.data?.candidates?.[0]?.content?.parts?.[0]?.text
    || ''
  ).trim();
  return text || null;
}

async function callGroqChatCompletion({ prompt, maxTokens, temperature, groqApiKey, groqModel }) {
  const keyCandidates = Array.from(new Set([
    ...(Array.isArray(config.groqApiKeys) ? config.groqApiKeys : []),
    ...(Array.isArray(groqApiKey) ? groqApiKey : [groqApiKey])
  ].map((k) => String(k || '').trim()).filter(Boolean)));

  if (!keyCandidates.length) return null;

  let lastError = null;
  for (const key of keyCandidates) {
    try {
      const resp = await axios.post(
        'https://api.groq.com/openai/v1/chat/completions',
        {
          model: groqModel,
          messages: [
            {
              role: 'system',
              content: CHAT_SYSTEM_PROMPT
            },
            { role: 'user', content: prompt }
          ],
          temperature,
          max_tokens: maxTokens
        },
        {
          headers: {
            Authorization: `Bearer ${key}`,
            'Content-Type': 'application/json'
          },
          timeout: 60000
        }
      );

      const text = String(resp?.data?.choices?.[0]?.message?.content || '').trim();
      if (text) return text;
    } catch (err) {
      lastError = err;
    }
  }

  if (lastError) throw lastError;
  return null;
}

async function callPrimaryChatModel({ prompt, maxTokens, temperature, groqApiKey, groqModel }) {
  const provider = String(config.mainChatProvider || 'gemini').trim().toLowerCase();
  const order = provider === 'groq'
    ? ['groq', 'gemini']
    : ['gemini', 'groq'];

  let lastError = null;

  for (const p of order) {
    try {
      if (p === 'gemini') {
        const answer = await callGeminiChatCompletion({ prompt, maxTokens, temperature });
        if (answer) return { answer, provider: 'gemini' };
      } else if (p === 'groq') {
        const answer = await callGroqChatCompletion({ prompt, maxTokens, temperature, groqApiKey, groqModel });
        if (answer) return { answer, provider: 'groq' };
      }
    } catch (err) {
      lastError = err;
    }
  }

  return {
    answer: '',
    provider: 'none',
    error: lastError ? String(lastError?.message || lastError) : ''
  };
}

function formatHistory(history) {
  const rows = Array.isArray(history) ? history.slice(-8) : [];
  if (!rows.length) return 'No previous conversation.';
  return rows.map((m) => {
    const clean = removeForbiddenPhrases(String(m.content || '')).replace(/\s+/g, ' ').trim();
    return `${m.role === 'user' ? 'User' : 'Assistant'}: ${clean}`;
  }).join('\n');
}
function classifySentiment(context) {
  const text = String(context || '').toLowerCase();
  const pos = ['positive', 'growth', 'profit', 'resilient', 'up'].reduce((s, w) => s + (text.match(new RegExp(`\\b${w}\\b`, 'g')) || []).length, 0);
  const neg = ['negative', 'risk', 'loss', 'decline', 'down'].reduce((s, w) => s + (text.match(new RegExp(`\\b${w}\\b`, 'g')) || []).length, 0);
  if (pos > neg) return 'positive';
  if (neg > pos) return 'negative';
  return 'neutral';
}

function getRelatedSymbolDocs(scope) {
  if (!scope || scope === 'MARKET') return [];
  const prefix = `${scope.slice(0, 2)}%`;

  const rows = db.prepare(`
    SELECT symbol,
           AVG(score) AS avg_score,
           COUNT(*) AS items,
           MAX(analyzed_at) AS last_at
    FROM sentiment
    WHERE symbol LIKE ?
      AND symbol <> ?
    GROUP BY symbol
    HAVING items >= 2
    ORDER BY datetime(last_at) DESC
    LIMIT 5
  `).all(prefix, scope);

  const docs = [];
  for (const row of rows) {
    docs.push({
      id: `related::${scope}::sentiment::${row.symbol}`,
      source: 'related_symbol_sentiment',
      type: 'historical',
      scope: row.symbol,
      published_at: row.last_at,
      text: `Related symbol ${row.symbol} sentiment context: avg score ${formatNum(row.avg_score, 3)} from ${row.items || 0} items.`
    });
  }

  return docs;
}

function getLiquidPeerDocs(scope) {
  if (!scope || scope === 'MARKET') return [];
  try {
    const rows = db.prepare(`
      SELECT s.symbol, s.volume, s.date
      FROM stocks s
      JOIN (
        SELECT symbol, MAX(date) AS md
        FROM stocks
        GROUP BY symbol
      ) l ON l.symbol = s.symbol AND l.md = s.date
      WHERE s.symbol <> ?
        AND LENGTH(s.symbol) BETWEEN 3 AND 5
        AND s.symbol NOT GLOB '*[0-9]*'
        AND s.symbol NOT GLOB '*[^A-Z]*'
      ORDER BY s.volume DESC
      LIMIT 14
    `).all(scope);

    if (!rows.length) return [];

    const symbols = rows.map((r) => String(r.symbol).toUpperCase()).filter(isLikelyEquitySymbol);
    return [{
      id: `peer::${scope}::liquid_volume`,
      source: 'liquid_peer_universe',
      type: 'historical',
      scope,
      published_at: rows[0]?.date,
      text: `High-liquidity PSX names by latest-session volume (context for “related” comparisons vs ${scope}, not a correlation model): ${symbols.join(', ')}.`
    }];
  } catch {
    return [];
  }
}

function getHistoricalDocs(scope) {
  if (scope === 'MARKET') {
    const sentiment7d = db.prepare(`
      SELECT AVG(score) AS avg_score,
             COUNT(*) AS total,
             SUM(CASE WHEN label='positive' THEN 1 ELSE 0 END) AS positive_count,
             SUM(CASE WHEN label='negative' THEN 1 ELSE 0 END) AS negative_count,
             SUM(CASE WHEN label='neutral' THEN 1 ELSE 0 END) AS neutral_count
      FROM sentiment
      WHERE analyzed_at >= datetime('now', '-7 day')
    `).get();

    const breadth = db.prepare(`
      WITH latest_per_symbol AS (
        SELECT symbol, MAX(date) AS latest_date
        FROM stocks
        GROUP BY symbol
      )
      SELECT
        SUM(CASE WHEN s.change_pct > 0 THEN 1 ELSE 0 END) AS advancers,
        SUM(CASE WHEN s.change_pct < 0 THEN 1 ELSE 0 END) AS decliners,
        AVG(s.change_pct) AS avg_change_pct,
        MAX(s.date) AS market_date
      FROM stocks s
      JOIN latest_per_symbol l ON l.symbol = s.symbol AND l.latest_date = s.date
    `).get();

    const strongestComposite = db.prepare(`
      WITH recent AS (
        SELECT symbol, date, close, change_pct, volume,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
        FROM stocks
   WHERE LENGTH(symbol) BETWEEN 3 AND 5
          AND symbol NOT GLOB '*[0-9]*'
          AND symbol NOT GLOB '*[^A-Z]*'
      ),
      momentum AS (
        SELECT symbol,
               AVG(CASE WHEN rn <= 10 THEN change_pct END) AS mom10,
               AVG(CASE WHEN rn <= 30 THEN change_pct END) AS mom30,
               AVG(CASE WHEN rn <= 20 THEN volume END) AS avg_vol20,
               MAX(CASE WHEN rn = 1 THEN close END) AS latest_close,
               MAX(CASE WHEN rn = 1 THEN date END) AS latest_date
        FROM recent
        GROUP BY symbol
        HAVING COUNT(CASE WHEN rn <= 10 THEN 1 END) >= 8
      ),
      sent AS (
        SELECT symbol,
               AVG(score) AS avg_score,
               COUNT(*) AS items
        FROM sentiment
        WHERE analyzed_at >= datetime('now', '-30 day')
        GROUP BY symbol
      )
      SELECT
        m.symbol,
        m.mom10,
        m.mom30,
        m.avg_vol20,
        m.latest_close,
        m.latest_date,
        COALESCE(s.avg_score, 0) AS avg_score,
        COALESCE(s.items, 0) AS sentiment_items,
        ((COALESCE(m.mom10, 0) * 0.65) + (COALESCE(m.mom30, 0) * 0.20) + (COALESCE(s.avg_score, 0) * 8 * 0.15)) AS composite
      FROM momentum m
      LEFT JOIN sent s ON s.symbol = m.symbol
      WHERE COALESCE(m.latest_close, 0) >= 5
        AND COALESCE(m.avg_vol20, 0) >= 100000
      ORDER BY composite DESC, m.avg_vol20 DESC
      LIMIT 10
    `).all();

    const weakestComposite = db.prepare(`
      WITH recent AS (
        SELECT symbol, change_pct,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
        FROM stocks
   WHERE LENGTH(symbol) BETWEEN 3 AND 5
          AND symbol NOT GLOB '*[0-9]*'
          AND symbol NOT GLOB '*[^A-Z]*'
      ),
      momentum AS (
        SELECT symbol,
               AVG(CASE WHEN rn <= 10 THEN change_pct END) AS mom10,
               AVG(CASE WHEN rn <= 30 THEN change_pct END) AS mom30
        FROM recent
        GROUP BY symbol
        HAVING COUNT(CASE WHEN rn <= 10 THEN 1 END) >= 8
      ),
      sent AS (
        SELECT symbol, AVG(score) AS avg_score
        FROM sentiment
        WHERE analyzed_at >= datetime('now', '-30 day')
        GROUP BY symbol
      )
      SELECT
        m.symbol,
        ((COALESCE(m.mom10, 0) * 0.65) + (COALESCE(m.mom30, 0) * 0.20) + (COALESCE(s.avg_score, 0) * 8 * 0.15)) AS composite
      FROM momentum m
      LEFT JOIN sent s ON s.symbol = m.symbol
      ORDER BY composite ASC
      LIMIT 6
    `).all();

    const active = db.prepare(`
      SELECT s.symbol, s.close, s.change_pct, s.volume, s.date
      FROM stocks s
      JOIN (
        SELECT symbol, MAX(date) AS date
        FROM stocks
        GROUP BY symbol
      ) l ON l.symbol = s.symbol AND l.date = s.date
      WHERE LENGTH(s.symbol) BETWEEN 3 AND 5
        AND s.symbol NOT GLOB '*[0-9]*'
        AND s.symbol NOT GLOB '*[^A-Z]*'
        AND COALESCE(s.close, 0) >= 5
      ORDER BY s.volume DESC
      LIMIT 24
    `).all();

  const docs = [];
    if (sentiment7d?.total) {
      docs.push({
        id: 'hist::market::sentiment7d',
        source: 'sentiment_aggregate_7d',
        type: 'historical',
        published_at: new Date().toISOString(),
        text: `PSX market sentiment (last 7 days): avg score ${formatNum(sentiment7d.avg_score, 3)}, positive ${sentiment7d.positive_count || 0}, negative ${sentiment7d.negative_count || 0}, neutral ${sentiment7d.neutral_count || 0}, total items ${sentiment7d.total || 0}.`
      });
    }

    if (breadth?.market_date) {
      docs.push({
        id: 'hist::market::breadth',
        source: 'market_breadth_latest',
        type: 'historical',
        published_at: breadth.market_date,
        text: `Market breadth (${breadth.market_date}): advancers ${breadth.advancers || 0}, decliners ${breadth.decliners || 0}, average change ${formatNum(breadth.avg_change_pct)}%.`
      });
    }

    const strongestEquities = strongestComposite.filter((r) => isLikelyEquitySymbol(r.symbol));
    const weakestEquities = weakestComposite.filter((r) => isLikelyEquitySymbol(r.symbol));
    const activeEquities = active.filter((r) => isLikelyEquitySymbol(r.symbol));

    if (strongestEquities.length) {
      docs.push({
        id: 'hist::market::leaders',
        source: 'market_composite_strength',
        type: 'historical',
        published_at: strongestEquities[0]?.latest_date || new Date().toISOString(),
        text: `Top combined momentum+sentiment leaders: ${strongestEquities.slice(0, 6).map((r) => `${r.symbol} (${formatNum(r.composite, 2)})`).join(', ')}.`
      });

      for (const row of strongestEquities.slice(0, 6)) {
        docs.push({
          id: `hist::market::leader::${row.symbol}`,
          source: 'market_composite_strength_detail',
          type: 'historical',
          scope: row.symbol,
          published_at: row.latest_date,
          text: `${row.symbol}: composite ${formatNum(row.composite, 2)}, 10-session momentum ${formatNum(row.mom10)}%, 30-session momentum ${formatNum(row.mom30)}%, 30-day sentiment ${formatNum(row.avg_score, 3)}, latest close ${formatNum(row.latest_close)} PKR.`
        });
      }
    }

    if (weakestEquities.length) {
      docs.push({
        id: 'hist::market::laggards',
        source: 'market_composite_weakness',
        type: 'historical',
        published_at: new Date().toISOString(),
        text: `Risk watch laggards by composite trend: ${weakestEquities.slice(0, 4).map((r) => `${r.symbol} (${formatNum(r.composite, 2)})`).join(', ')}.`
      });
    }

    for (const row of activeEquities.slice(0, 10)) {
      docs.push({
        id: `hist::market::${row.symbol}`,
        source: 'stocks_latest_snapshot',
        type: 'historical',
        scope: row.symbol,
        published_at: row.date,
        text: `${row.symbol} latest session (${row.date}): close ${formatNum(row.close)} PKR, change ${formatNum(row.change_pct)}%, volume ${row.volume || 0}.`
      });
    }

    docs.push(...getMarketAdvancedDocs());

    return docs;
  }

  const latest = db.prepare(`
    SELECT symbol, date, close, change_pct, volume
    FROM stocks
    WHERE symbol = ?
    ORDER BY date DESC
    LIMIT 1
  `).get(scope);

  const rollup = db.prepare(`
    SELECT COUNT(*) AS rows_count,
           MIN(date) AS first_date,
           MAX(date) AS last_date,
           AVG(close) AS avg_close,
           MIN(close) AS min_close,
           MAX(close) AS max_close,
           AVG(change_pct) AS avg_change_pct
    FROM stocks
    WHERE symbol = ?
  `).get(scope);

  const momentum = db.prepare(`
    SELECT AVG(change_pct) AS avg_change_pct_30,
           AVG(volume) AS avg_volume_30,
           MIN(date) AS from_date,
           MAX(date) AS to_date
    FROM (
      SELECT date, change_pct, volume
      FROM stocks
      WHERE symbol = ?
      ORDER BY date DESC
      LIMIT 30
    ) t
  `).get(scope);

  const sentiment = db.prepare(`
    SELECT AVG(score) AS avg_score,
           COUNT(*) AS total,
           SUM(CASE WHEN label='positive' THEN 1 ELSE 0 END) AS positive_count,
           SUM(CASE WHEN label='negative' THEN 1 ELSE 0 END) AS negative_count,
           SUM(CASE WHEN label='neutral' THEN 1 ELSE 0 END) AS neutral_count,
           MAX(analyzed_at) AS last_sentiment_at
    FROM sentiment
    WHERE symbol = ?
  `).get(scope);

  const docs = [];
  if (latest) {
    docs.push({
      id: `hist::${scope}::latest`,
      source: 'stocks_latest',
      type: 'historical',
      scope,
      published_at: latest.date,
      text: `${scope} latest close ${formatNum(latest.close)} PKR on ${latest.date}, change ${formatNum(latest.change_pct)}%, volume ${latest.volume || 0}.`
    });
  }

  if (rollup?.rows_count) {
    docs.push({
      id: `hist::${scope}::rollup`,
      source: 'stocks_history_rollup',
      type: 'historical',
      scope,
      published_at: rollup.last_date,
      text: `${scope} historical profile: ${rollup.rows_count} records from ${rollup.first_date} to ${rollup.last_date}, average close ${formatNum(rollup.avg_close)}, range ${formatNum(rollup.min_close)}-${formatNum(rollup.max_close)} PKR, average daily change ${formatNum(rollup.avg_change_pct)}%.`
    });
  }

  if (momentum?.to_date) {
    docs.push({
      id: `hist::${scope}::momentum30`,
      source: 'stocks_momentum_30d',
      type: 'historical',
      scope,
      published_at: momentum.to_date,
      text: `${scope} recent 30-session momentum (${momentum.from_date} to ${momentum.to_date}): average change ${formatNum(momentum.avg_change_pct_30)}%, average volume ${formatNum(momentum.avg_volume_30, 0)}.`
    });
  }

  if (sentiment?.total) {
    docs.push({
      id: `hist::${scope}::sentiment`,
      source: 'sentiment_symbol_aggregate',
      type: 'historical',
      scope,
      published_at: sentiment.last_sentiment_at,
      text: `${scope} sentiment aggregate: avg score ${formatNum(sentiment.avg_score, 3)}, positive ${sentiment.positive_count || 0}, negative ${sentiment.negative_count || 0}, neutral ${sentiment.neutral_count || 0}, total ${sentiment.total || 0}.`
    });
  }

  const sentimentRows = db.prepare(`
    SELECT headline, label, score, source, analyzed_at
    FROM sentiment
    WHERE symbol = ?
    ORDER BY datetime(analyzed_at) DESC
    LIMIT 16
  `).all(scope);

  const seenHeadlines = new Set();
  for (const row of sentimentRows) {
    const headline = normalizeText(row?.headline);
    if (!headline) continue;
    const dedupeKey = `${String(row?.source || '').toLowerCase()}|${headline.toLowerCase()}`;
    if (seenHeadlines.has(dedupeKey)) continue;
    seenHeadlines.add(dedupeKey);

    docs.push({
      id: `hist::${scope}::headline::${row.analyzed_at || ''}::${String(row.headline || '').slice(0, 24)}`,
      source: row.source || 'sentiment_news',
      type: 'news',
      scope,
      published_at: row.analyzed_at,
      text: `${scope} headline (${row.label || 'neutral'}, score ${formatNum(row.score, 3)}): ${headline}`
    });
  }

  docs.push(...getSymbolAdvancedDocs(scope));

  return docs;
}

function scoreDoc(doc, questionTokens, scope, rawQuestion) {
  const text = normalizeText(doc?.text).toLowerCase();
  if (!text) return 0;
  if (!questionTokens.length) return 0.2 + recencyBoost(doc?.published_at);
  const q = normalizeText(rawQuestion).toLowerCase();

  const tokenSet = new Set(tokenize(text));
  let overlap = 0;
  for (const t of questionTokens) if (tokenSet.has(t)) overlap += 1;
  const overlapRatio = overlap / Math.max(1, questionTokens.length);

  let scopeBoost = 0;
  if (scope !== 'MARKET' && String(doc.scope || '').toUpperCase() === scope) scopeBoost += 0.24;
  if (scope !== 'MARKET' && text.includes(scope.toLowerCase())) scopeBoost += 0.16;

  const source = String(doc?.source || '').toLowerCase();
  if (scope !== 'MARKET' && String(doc.scope || '').toUpperCase() === scope) {
    if (source.includes('psx_dps_company_page')) scopeBoost += 0.38;
    if (source.includes('csv_symbol_multi_timeframe')) scopeBoost += 0.34;
    if (source.includes('csv_symbol_regime') || source.includes('csv_symbol_history_rollup')) scopeBoost += 0.2;
  }

  const exactPhraseBoost = text.includes(normalizeText(rawQuestion).toLowerCase()) ? 0.12 : 0;
  const typeBoost = doc.type === 'historical' ? 0.13 : doc.type === 'report' ? 0.12 : doc.type === 'news' ? 0.12 : 0.05;
  let intentBoost = 0;
  const recency = recencyBoost(doc?.published_at);
  if (/(strongest|best|top|momentum|trend|sentiment|outlook|historical)/.test(q) && (doc.type === 'historical' || doc.type === 'report')) intentBoost += 0.18;
  if (/(strongest|best|top|momentum|trend|sentiment|outlook|historical)/.test(q) && doc.type === 'news') intentBoost -= 0.05;
  if (/(risk|uncertainty|downside|threat|warning)/.test(q) && doc.type === 'news') intentBoost += 0.08;
  if (/(buy|recommend|entry|accumulate|pick)/.test(q) && source.includes('market_composite_strength_detail')) intentBoost += 0.22;
  if (/(buy|recommend|entry|accumulate|pick)/.test(q) && source.includes('market_composite_strength')) intentBoost += 0.16;
  if (/(deep|detailed|comprehensive|full|scenario|probability|confidence|multi|timeframe)/.test(q) && doc.type === 'historical') intentBoost += 0.16;
  if (/(deep|detailed|comprehensive|full|scenario|probability|confidence|multi|timeframe)/.test(q) && source.includes('csv_symbol')) intentBoost += 0.14;
  if (/(current|today|latest|now|fresh)/.test(q)) intentBoost += recency * 0.5;
  if (/(news|headline|sentiment)/.test(q) && doc.type === 'news') intentBoost += 0.12;
  if (/(news|headline|sentiment)/.test(q) && source.includes('sentiment_regime')) intentBoost += 0.1;
  if (source.includes('multi_timeframe') || source.includes('regime') || source.includes('breadth_trend')) intentBoost += 0.08;
  if (/(related|similar|peer|comparable|sector|business|p\/e|eps|fundamental)/.test(q) && doc.type === 'report') intentBoost += 0.2;
  if (/(related|similar|peer|comparable|liquid)/.test(q) && source.includes('liquid_peer')) intentBoost += 0.22;
  if (/(related|similar|peer|comparable)/.test(q) && source.includes('related_symbol')) intentBoost += 0.16;
  if (scope === 'MARKET' && doc.type === 'news') intentBoost += 0.12;
  if (/(outlook|risk|macro|headline|news|today|week)/.test(q) && doc.type === 'news') intentBoost += 0.1;
  if (scope === 'MARKET' && doc.type === 'news' && source.includes('sentiment_db')) intentBoost += 0.22;

  return overlapRatio + scopeBoost + exactPhraseBoost + typeBoost + intentBoost + recency;
}

async function getDailyNewsDocs(scope) {
  let feed = await getLatestBusinessNewsFeed({ limit: 80, forceRefresh: false });
  if (!Array.isArray(feed) || !feed.length) {
    feed = await getLatestBusinessNewsFeed({ limit: 80, forceRefresh: true });
  }

  const symbolRegex = scope !== 'MARKET'
    ? new RegExp(`(^|[^a-z0-9])${scope.toLowerCase()}([^a-z0-9]|$)`, 'i')
    : null;
  const marketRelevanceRegex = /(psx|kse|stock|market|inflation|gdp|exports|rupee|interest|policy|bank|oil|gas|industry|economy|imf|fiscal|current account|trade|manufacturing|pakistan|karachi|lahore|islamabad|sbp|fuel|power|utility|tax|budget|auction|foreign|invest|import|export|labou?r|price|consumer|business|finance)/i;

  const pushItem = (item, seen, list) => {
    const title = truncateText(stripHtml(item?.title), 150);
    const description = truncateText(stripHtml(item?.description), 260);
    if (!title) return;
    const dedupeKey = `${String(item?.source || '').toLowerCase()}|${title.toLowerCase()}`;
    if (seen.has(dedupeKey)) return;
    seen.add(dedupeKey);
    list.push({
      id: `daily_news::${(item?.source || 'news').toLowerCase()}::${title.slice(0, 48)}`,
      source: item?.source || 'daily_business_news',
      type: 'news',
      scope,
      published_at: item?.pubDate || new Date().toISOString(),
      text: truncateText(description ? `${title}. ${description}` : title, 300)
    });
  };

  const docs = [];
  const seen = new Set();
  for (const item of (feed || [])) {
    const title = truncateText(stripHtml(item?.title), 150);
    const description = truncateText(stripHtml(item?.description), 260);
    if (!title) continue;

    const blob = `${title} ${description}`.toLowerCase();
    if (scope !== 'MARKET' && symbolRegex && !symbolRegex.test(blob)) continue;
    if (scope === 'MARKET' && !marketRelevanceRegex.test(blob)) continue;

    pushItem(item, seen, docs);
  }

  if (scope === 'MARKET' && docs.length < 8) {
    for (const item of (feed || [])) {
      if (docs.length >= 18) break;
      pushItem(item, seen, docs);
    }
  }

  return docs;
}

/**
 * When live RSS returns nothing (firewall, timeouts), reuse recent rows from sentiment ingest.
 */
function getDbRecentNewsDocs(scope, limit = 28) {
  if (scope !== 'MARKET') return [];
  try {
    const cap = Math.min(80, Math.max(12, Number(limit || 28)));
    const rows = db.prepare(`
      SELECT headline, source, analyzed_at AS published_at, label, score, symbol
      FROM sentiment
      WHERE headline IS NOT NULL AND TRIM(headline) != ''
      ORDER BY datetime(analyzed_at) DESC
      LIMIT ?
    `).all(cap);

    const out = [];
    const seen = new Set();
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const h = normalizeText(row.headline);
      if (!h) continue;
      const dedupe = h.toLowerCase();
      if (seen.has(dedupe)) continue;
      seen.add(dedupe);

      out.push({
        id: `db_feed_news::MARKET::${i}::${String(row.published_at || '').slice(0, 16)}`,
        source: `sentiment_db:${row.source || 'ingest'}`,
        type: 'news',
        scope: 'MARKET',
        published_at: row.published_at,
        text: `DB-cached headline — **${row.symbol}** (${row.label || 'neutral'}, score ${formatNum(row.score, 3)}): ${truncateText(h, 300)}`
      });
      if (out.length >= cap) break;
    }
    return out;
  } catch {
    return [];
  }
}

async function retrieveRagDocs({ scope, question, topK = 10 }) {
  const questionTokens = tokenize(question);
  const wantsPeers = /(related|similar|peer|peers|comparable|like this|other stocks|other symbol|sector peer)/i.test(String(question || ''));

  const [csvDocs, psxPageDocs, historicalDocs, dailyNewsDocs] = await Promise.all([
    getCsvFallbackHistoricalDocs(scope),
    getPsxCompanyPageDocs(scope),
    Promise.resolve(getHistoricalDocs(scope)),
    getDailyNewsDocs(scope)
  ]);

  const dbNewsBackfill = scope === 'MARKET' ? getDbRecentNewsDocs(scope, 32) : [];

  const peerDocs = wantsPeers && scope !== 'MARKET'
    ? [...getRelatedSymbolDocs(scope), ...getLiquidPeerDocs(scope)]
    : [];

  let allDocs = [...csvDocs, ...psxPageDocs, ...historicalDocs, ...dailyNewsDocs, ...dbNewsBackfill, ...peerDocs];
  let sparseSymbolFallback = false;

  if (scope !== 'MARKET' && allDocs.length < 4) {
    sparseSymbolFallback = true;

    const marketHistoricalDocs = getHistoricalDocs('MARKET').slice(0, 10).map((d) => ({
      ...d,
      id: `${d.id}::market_fallback`
    }));

    const marketDailyNewsDocs = (await getDailyNewsDocs('MARKET')).slice(0, 12).map((d) => ({
      ...d,
      id: `${d.id}::market_fallback`
    }));

    const relatedDocs = getRelatedSymbolDocs(scope);

    allDocs = [
      ...allDocs,
      {
        id: `coverage::${scope}`,
        source: 'coverage_guardrail',
        type: 'historical',
        scope,
        published_at: new Date().toISOString(),
        text: `Direct evidence for ${scope} is limited right now, so additional market and related-symbol evidence is included for context.`
      },
      ...relatedDocs,
      ...marketHistoricalDocs,
      ...marketDailyNewsDocs
    ];
  }

  allDocs = dedupeDocs(allDocs);

  const ranked = allDocs
    .map((doc) => ({ ...doc, _score: scoreDoc(doc, questionTokens, scope, question) }))
    .filter((doc) => doc._score > 0.055
      || (doc.type === 'news' && scope === 'MARKET' && doc._score > 0.035)
      || (doc.type === 'news' && scope === 'MARKET' && String(doc.source || '').includes('sentiment_db')))
    .sort((a, b) => b._score - a._score);

  const target = Math.max(6, Number(topK || 10));
  const rankedEvidence = ranked.filter((d) => d.type === 'historical' || d.type === 'report');
  const rankedNews = ranked.filter((d) => d.type === 'news');

  let historicalTarget = scope === 'MARKET'
    ? Math.max(4, Math.ceil(target * 0.52))
    : Math.max(4, Math.ceil(target * 0.55));
  let newsTarget = Math.max(1, target - historicalTarget);

  if (scope === 'MARKET' && rankedNews.length >= 2) {
    const minNews = Math.min(rankedNews.length, Math.max(4, Math.round(target * 0.36)));
    newsTarget = Math.max(newsTarget, minNews);
    historicalTarget = Math.max(4, target - newsTarget);
  }

  const selected = [
    ...rankedNews.slice(0, newsTarget),
    ...rankedEvidence.slice(0, historicalTarget)
  ].slice(0, target);

  if (selected.length < target) {
    const selectedIds = new Set(selected.map((d) => d.id));
    for (const row of ranked) {
      if (selected.length >= target) break;
      if (selectedIds.has(row.id)) continue;
      selected.push(row);
      selectedIds.add(row.id);
    }
  }

  const fallback = selected.length
    ? selected
    : allDocs.slice(0, target).map((doc) => ({ ...doc, _score: 0 }));

  let docs = fallback.map(({ _score, ...doc }) => doc);

  if (scope === 'MARKET') {
    const newsHave = docs.filter((d) => d.type === 'news').length;
    if (newsHave < 3) {
      const seen = new Set(docs.map((d) => d.id));
      const extra = allDocs
        .filter((d) => d.type === 'news' && !seen.has(d.id))
        .sort((a, b) => String(b.published_at || '').localeCompare(String(a.published_at || '')))
        .slice(0, Math.max(0, 4 - newsHave));
      docs = [...docs, ...extra];
    }
  }

  return {
    docs,
    meta: {
      scope,
      sparse_symbol_fallback: sparseSymbolFallback,
      total_candidates: allDocs.length,
      used_chunks: docs.length,
      historical_chunks: docs.filter((d) => d.type === 'historical').length,
      report_chunks: docs.filter((d) => d.type === 'report').length,
      news_chunks: docs.filter((d) => d.type === 'news').length,
      refreshed_at: new Date().toISOString()
    }
  };
}

function buildRetrievedDocsBlock(docs) {
  if (!docs.length) return 'No relevant evidence was retrieved.';
  return docs.map((d, idx) => {
    const when = d.published_at ? `, date: ${d.published_at}` : '';
    return `[${idx + 1}] source: ${d.source}, type: ${d.type}${when}\n${d.text}`;
  }).join('\n\n');
}

function buildLocalStructuredFallback({ scope, question, docs }) {
  const deepMode = /(deep|detailed|comprehensive|full|totally|scenario|probability|confidence|risk|multi\s*timeframe)/i.test(String(question || ''));
  const wantsBuyIdeas = /(buy|recommend|recommended|best\s+stocks?|stock\s+picks?|entry)/i.test(String(question || ''));
  const asksHistory = /(history|historical|record|past|data)/i.test(String(question || ''));
  const asksRecommendation = /(recommendation|recommend|advice|buy|sell|hold|action)/i.test(String(question || ''));
  const asksRelated = /(related|similar|peer|peers|comparable|like this|other stocks|other symbol)/i.test(String(question || ''));

  let histLimit = deepMode ? 5 : 3;
  let newsLimit = deepMode ? 4 : 2;
  if (asksRelated) {
    histLimit += 2;
    newsLimit += 1;
  }

  const historicalRank = (d) => {
    const src = String(d?.source || '').toLowerCase();
    const typ = String(d?.type || '');
    if (src.includes('csv_staleness')) return 0;
    if (src.includes('csv_symbol_archival')) return 1;
    if (typ === 'report' && src.includes('psx_dps')) return 2;
    if (src.includes('csv_symbol_multi')) return 3;
    if (src.includes('csv_symbol_regime')) return 4;
    if (src.includes('csv_symbol_history_rollup') || src.includes('csv_symbol_latest')) return 5;
    if (src.includes('multi_timeframe')) return 6;
    if (src.includes('regime')) return 7;
    if (src.includes('liquid_peer') || src.includes('related_symbol')) return 8;
    if (src.includes('momentum')) return 9;
    if (src.includes('rollup') || src.includes('breadth')) return 10;
    return 12;
  };

  const historical = docs
    .filter((d) => d.type === 'historical')
    .sort((a, b) => historicalRank(a) - historicalRank(b))
    .slice(0, histLimit);

  const reportDocs = docs.filter((d) => d.type === 'report').slice(0, 3);

  const news = docs.filter((d) => d.type === 'news').slice(0, newsLimit);
  const candidateSymbols = Array.from(new Set(
    docs
      .filter((d) => d.type === 'historical' && String(d.source || '').includes('market_composite_strength_detail'))
      .map((d) => String(d.scope || '').toUpperCase())
      .filter((s) => isLikelyEquitySymbol(s))
  )).slice(0, 5);
  const hasCoverageGap = docs.some((d) => d.source === 'coverage_guardrail');

  const histBullets = historical.length
    ? historical.map((d) => `- ${truncateText(stripHtml(d.text), 220)}`).join('\n')
    : '- Direct historical records are limited for this query.';

  const reportBullets = reportDocs.length
    ? reportDocs.map((d) => `- ${truncateText(stripHtml(d.text), 240)}`).join('\n')
    : '';

  const newsBullets = news.length
    ? news.map((d) => `- ${truncateText(stripHtml(d.text), 200)}`).join('\n')
    : '- No strong same-day news signal matched this query.';

  const signalDoc = docs.find((d) => {
    const s = String(d.source || '');
    return s.includes('symbol_multi_timeframe_metrics') || s.includes('csv_symbol_multi_timeframe');
  });
  const regimeDoc = docs.find((d) => {
    const s = String(d.source || '');
    return s.includes('symbol_regime_inference') || s.includes('csv_symbol_regime');
  });
  const sentimentDoc = docs.find((d) => String(d.source || '').includes('sentiment_regime') || String(d.source || '').includes('sentiment_symbol_aggregate'));

  const extractMetric = (text, label) => {
    const m = String(text || '').match(new RegExp(`${label}\\s+(-?\\d+(?:\\.\\d+)?)%?`, 'i'));
    return m ? Number(m[1]) : null;
  };

  const ret20 = extractMetric(signalDoc?.text, '20-session return');
  const ret60 = extractMetric(signalDoc?.text, '60-session return');
  const ret5 = extractMetric(signalDoc?.text, '5-session return');
  const maSpread = extractMetric(signalDoc?.text, 'spread');
  const sentAvg = extractMetric(sentimentDoc?.text, 'avg score');
  const sentPos = extractMetric(sentimentDoc?.text, 'positive');
  const sentNeg = extractMetric(sentimentDoc?.text, 'negative');
  const vol20 = extractMetric(regimeDoc?.text, '20-session volatility');
  const volRatio = extractMetric(regimeDoc?.text, 'volume ratio vs 20-session average');
  const bullSignals = extractMetric(regimeDoc?.text, 'bullish signals');
  const bearSignals = extractMetric(regimeDoc?.text, 'bearish signals');
  const regimeMatch = String(regimeDoc?.text || '').match(/regime inference:\s*([^()]+?)\s*\(/i);
  const regimeLabel = regimeMatch ? normalizeText(regimeMatch[1]) : '';

  const dynamicRecommendation = [];
  if (scope !== 'MARKET' && Number.isFinite(ret20) && Number.isFinite(ret60)) {
    const trendScore = (Number.isFinite(ret5) ? ret5 * 0.2 : 0) + (ret20 * 0.45) + (ret60 * 0.35);
    const maHint = Number.isFinite(maSpread)
      ? `MA20/MA60 spread ${formatNum(maSpread)}%`
      : 'moving-average confirmation unavailable';

    const shortVsLongConflict = Number.isFinite(ret5) && Number.isFinite(ret60)
      && ((ret5 > 0 && ret60 < 0) || (ret5 < 0 && ret60 > 0));

    if (trendScore >= 2.2) {
      dynamicRecommendation.push(`- ${scope} momentum is constructive (5d/20d/60d: ${formatNum(ret5)}% / ${formatNum(ret20)}% / ${formatNum(ret60)}%, ${maHint}); favor pullback entries near support instead of breakout chasing.`);
    } else if (trendScore <= -2.2) {
      dynamicRecommendation.push(`- ${scope} trend remains fragile (5d/20d/60d: ${formatNum(ret5)}% / ${formatNum(ret20)}% / ${formatNum(ret60)}%, ${maHint}); prioritize downside protection until trend structure improves.`);
    } else if (shortVsLongConflict) {
      dynamicRecommendation.push(`- ${scope} is in a transition phase: short-term move (${formatNum(ret5)}%) conflicts with broader direction (${formatNum(ret60)}%); wait for 2-3 session confirmation before sizing up.`);
    } else {
      dynamicRecommendation.push(`- ${scope} is range/mixed across timeframes (5d/20d/60d: ${formatNum(ret5)}% / ${formatNum(ret20)}% / ${formatNum(ret60)}%); use staggered entries with predefined invalidation levels.`);
    }
  }

  if (scope !== 'MARKET' && regimeLabel) {
    const volHint = Number.isFinite(vol20) ? `${formatNum(vol20)}% daily volatility` : 'volatility not quantified';
    const flowHint = Number.isFinite(volRatio) ? `${formatNum(volRatio)}x volume vs 20-session average` : 'volume ratio unavailable';
    dynamicRecommendation.push(`- Regime check: **${regimeLabel}** with ${volHint} and ${flowHint}; demand stronger closing strength before aggressive positioning.`);
  }

  if (scope !== 'MARKET' && Number.isFinite(sentAvg)) {
    if (sentAvg > 0.08) {
      dynamicRecommendation.push(`- Sentiment backdrop is supportive (avg score ${formatNum(sentAvg, 3)}, pos/neg ${formatNum(sentPos, 0)}/${formatNum(sentNeg, 0)}); positive headlines can extend trend if price/volume confirm.`);
    } else if (sentAvg < -0.08) {
      dynamicRecommendation.push(`- Sentiment is weak (avg score ${formatNum(sentAvg, 3)}, pos/neg ${formatNum(sentPos, 0)}/${formatNum(sentNeg, 0)}); headline risk can accelerate drawdowns, so tighten stop discipline.`);
    } else {
      dynamicRecommendation.push(`- Sentiment is near-neutral (avg score ${formatNum(sentAvg, 3)}, pos/neg ${formatNum(sentPos, 0)}/${formatNum(sentNeg, 0)}); rely more on price structure than narrative.`);
    }
  }

  if (scope !== 'MARKET' && Number.isFinite(bullSignals) && Number.isFinite(bearSignals)) {
    const tilt = bullSignals > bearSignals ? 'bullish tilt' : (bearSignals > bullSignals ? 'bearish tilt' : 'balanced signal mix');
    dynamicRecommendation.push(`- Signal stack shows ${tilt} (${formatNum(bullSignals, 0)} bullish vs ${formatNum(bearSignals, 0)} bearish); align position size with conviction rather than forcing full allocation.`);
  }

  if (asksRelated && scope !== 'MARKET') {
    const peerLine = docs.find((d) => String(d.source || '').includes('liquid_peer'));
    const rel = docs.filter((d) => String(d.source || '').includes('related_symbol'));
    if (peerLine) {
      dynamicRecommendation.push(`- **Related / liquidity screen:** ${truncateText(stripHtml(peerLine.text), 220)}`);
    }
    if (rel.length) {
      dynamicRecommendation.push(`- **Prefix-cluster sentiment (weak relatedness):** ${rel.slice(0, 3).map((d) => truncateText(stripHtml(d.text), 100)).join(' | ')}`);
    }
    if (!peerLine && !rel.length) {
      dynamicRecommendation.push('- No dedicated peer list was retrieved; compare against sector leaders from PSX DPS or liquid index names manually.');
    }
  }

  if (scope === 'MARKET' && /(outlook|risk|full|today|macro|balanced)/i.test(String(question || ''))) {
    const breadthLatest = docs.find((d) => /market breadth|advancers.*decliners/i.test(String(d.text || '')));
    const breadthTrend = docs.find((d) => String(d.source || '').includes('breadth_trend'));
    const mktSent = docs.find((d) => /market sentiment regime|psx market sentiment/i.test(String(d.text || '')));
    if (breadthLatest) {
      dynamicRecommendation.push(`- **Participation / breadth:** ${truncateText(stripHtml(breadthLatest.text), 220)}`);
    }
    if (breadthTrend && breadthTrend !== breadthLatest) {
      dynamicRecommendation.push(`- **Breadth trend:** ${truncateText(stripHtml(breadthTrend.text), 210)}`);
    }
    if (mktSent) {
      dynamicRecommendation.push(`- **Sentiment vs headlines:** ${truncateText(stripHtml(mktSent.text), 200)}`);
    }
    if (news.length) {
      dynamicRecommendation.push('- **Catalyst watch:** treat “Latest news” bullets as event risk—size positions for possible gap risk on local/global headlines.');
    }
  }

  const recommendationBullets = dynamicRecommendation.length
    ? dynamicRecommendation
    : [
      '- Use multi-signal confirmation (trend + sentiment + fresh headlines) before acting.',
      '- If evidence is mixed, prefer smaller position sizing and staged entries.',
      '- Watch for headline-driven reversals that can quickly change short-term setups.'
    ];

  const lines = [
    '## Direct Answer',
    hasCoverageGap
      ? `For **${scope}**, direct symbol coverage is limited, so this answer blends available symbol data with broader market context to keep the analysis useful.`
      : `For **${scope}**, here is a context-grounded analysis for: "${normalizeText(question)}".`,
    ''
  ];

  if (!asksRecommendation || asksHistory) {
    lines.push('## Historical Data Evidence');
    if (reportBullets) {
      lines.push(
        '- **PSX DPS / fundamentals**',
        reportBullets.split('\n').map((line) => `  ${line}`).join('\n')
      );
    }
    lines.push(
      '- **CSV / price archive & trend signals**',
      histBullets.split('\n').map((line) => `  ${line}`).join('\n'),
      '- **Latest news signals**',
      newsBullets.split('\n').map((line) => `  ${line}`).join('\n'),
      ''
    );
  }

  if (!asksHistory || asksRecommendation) {
    lines.push(
      '## Recommendation',
      ...(wantsBuyIdeas && scope === 'MARKET' && candidateSymbols.length
        ? [`- Candidate watchlist from current composite leadership: **${candidateSymbols.join(', ')}**.`]
        : []),
      ...recommendationBullets,
      '- This is educational analysis, not financial advice.'
    );
  }

  return lines.join('\n');
}

function isDbMalformedError(err) {
  const msg = String(err?.message || err || '').toLowerCase();
  return msg.includes('database disk image is malformed')
    || msg.includes('database malformed')
    || msg.includes('sql logic error');
}

function buildDbRecoveryFallback({ scope, question, newsDocs = [] }) {
  const newsLines = newsDocs.slice(0, 2).map((d) => `- ${truncateText(stripHtml(d.text), 170)}`).join('\n')
    || '- Latest market headlines are temporarily unavailable.';

  return [
    '## Direct Answer',
    `For **${scope}**, here is a practical market view based on currently available evidence for: "${normalizeText(question)}".`,
    '',
    '## Historical Data Evidence',
    '- Broader historical coverage is temporarily limited in this response.',
    '- Latest market context:',
    newsLines.split('\n').map((line) => `  ${line}`).join('\n'),
    '',
    '## Recommendation',
    '- Prioritize risk-managed decisions and avoid over-committing on a single signal.',
    '- Re-check this setup as fresh market data updates to confirm continuation or reversal.',
    '- This is educational analysis, not financial advice.'
  ].join('\n');
}

function normalizeHeadings(rawText) {
  let text = String(rawText || '');
  text = text.replace(/^\s*#{1,3}\s*direct answer\s*[:\-]*\s*$/gim, '## Direct Answer');
  text = text.replace(/^\s*direct answer\s*[:\-]*\s*$/gim, '## Direct Answer');
  text = text.replace(/^\s*#{1,3}\s*historical data evidence\s*[:\-]*\s*$/gim, '## Historical Data Evidence');
  text = text.replace(/^\s*historical data evidence\s*[:\-]*\s*$/gim, '## Historical Data Evidence');
  text = text.replace(/^\s*#{1,3}\s*recommendation\s*[:\-]*\s*$/gim, '## Recommendation');
  text = text.replace(/^\s*recommendation\s*[:\-]*\s*$/gim, '## Recommendation');
  return text;
}

function extractSection(text, heading) {
  const escaped = String(heading).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const body = String(text || '');
  const patterns = [
    new RegExp(`(?:^|\\n)\\s*#{1,3}\\s*${escaped}\\s*\\n+\\s*([\\s\\S]*?)(?=\\n\\s*#{1,3}\\s+|$)`, 'i'),
    new RegExp(`(?:^|\\n)\\s*\\*\\*\\s*${escaped}\\s*\\*\\*\\s*\\n\\s*([\\s\\S]*?)(?=\\n\\s*#{1,3}\\s+|$)`, 'i'),
    new RegExp(`(?:^|\\n)\\s*#{1,3}\\s*${escaped}\\s*:\\s*\\n\\s*([\\s\\S]*?)(?=\\n\\s*#{1,3}\\s+|$)`, 'i'),
    new RegExp(`(?:^|\\n)\\s*#{1,3}\\s*${escaped}\\s*[:\\-]?\\s+([^\\n][\\s\\S]*?)(?=\\n\\s*#{1,3}\\s+|$)`, 'i')
  ];
  for (const rx of patterns) {
    const m = body.match(rx);
    if (m?.[1]?.trim()) return m[1].trim();
  }
  return '';
}

/** When headings are split across lines oddly, parse by ## blocks. */
function extractSectionsFlexible(text) {
  const raw = String(text || '').trim();
  const map = { direct: '', evidence: '', recommendation: '' };
  if (!raw) return map;
  const parts = raw.split(/\n(?=\s*#{1,3}\s+)/);
  for (const part of parts) {
    const m = part.match(/^\s*#{1,3}\s+(.+?)\s*\r?\n([\s\S]*)$/im);
    if (!m) continue;
    const title = normalizeText(m[1]).replace(/\*+/g, '').replace(/[:]+$/, '').toLowerCase();
    const body = m[2].trim();
    if (!body) continue;
    if (title.includes('direct answer')) map.direct = body;
    else if (title.includes('historical data evidence')) map.evidence = body;
    else if (title.includes('recommendation')) map.recommendation = body;
  }
  return map;
}

function normalizeBullets(sectionText) {
  let text = String(sectionText || '');
  text = text.replace(/^\s*\+\s*/gm, '- ');
  text = text.replace(/\s+\+\s+/g, '\n- ');
  const lines = text.split('\n').map((line) => line.trim()).filter(Boolean);
  if (!lines.length) return '';
  return lines.map((line) => {
    if (/^[-*]\s+/.test(line) || /^\d+\.\s+/.test(line)) return line;
    return `- ${line}`;
  }).join('\n');
}

function cleanRecommendationSection(sectionText) {
  const forbidden = /(degraded|database|integrity issue|repair|fallback mode|news-only)/i;
  const lines = normalizeBullets(sectionText)
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line && !forbidden.test(line));

  if (lines.length) return lines.join('\n');
  return [
    '- Prefer confirmation from trend, breadth, and sentiment before entry.',
    '- Use staged entries and strict risk limits when volatility is elevated.',
    '- This is educational analysis, not financial advice.'
  ].join('\n');
}

function enforceStructuredAnswer(answer, { scope, question, docs }) {
  let text = removeForbiddenPhrases(String(answer || ''));
  text = normalizeHeadings(text)
    .replace(/^\s*\+\s*/gm, '- ')
    .replace(/\s+\+\s+/g, '\n- ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  let direct = removeForbiddenPhrases(extractSection(text, 'Direct Answer'));
  let evidence = normalizeBullets(extractSection(text, 'Historical Data Evidence'));
  let recommendation = cleanRecommendationSection(extractSection(text, 'Recommendation'));

  if (!direct || !evidence) {
    const loose = extractSectionsFlexible(text);
    if (!direct && loose.direct) direct = removeForbiddenPhrases(loose.direct);
    if (!evidence && loose.evidence) evidence = normalizeBullets(loose.evidence);
    if ((!recommendation || recommendation.length < 12) && loose.recommendation) {
      recommendation = cleanRecommendationSection(loose.recommendation);
    }
  }

  if (!direct || !evidence) {
    return buildLocalStructuredFallback({ scope, question, docs });
  }

  return [
    '## Direct Answer',
    direct,
    '',
    '## Historical Data Evidence',
    evidence,
    '',
    '## Recommendation',
    recommendation
  ].join('\n');
}

export async function generateChatReply({ stock, question, history = [], groqApiKey, groqModel }) {
  const requestedScope = normalizeScope(stock);
  const inferredScope = requestedScope === 'MARKET' ? inferScopeFromQuestion(question, history) : null;
  const scope = inferredScope || requestedScope;
  const wantsDeepDive = /(deep|detailed|comprehensive|full|totally|scenario|probability|confidence|risk|multi\s*timeframe)/i.test(String(question || ''));
  const groqTemp = Number.isFinite(config.groqTemperature) ? config.groqTemperature : 0.6;
  const intent = await classifyUserIntent(question);
  if (intent === 'greeting') {
    return {
      answer: scope === 'MARKET'
        ? 'Hi! Ask me any PSX question and I will answer with structured historical + latest-news evidence.'
        : `Hi! Ask me anything about ${scope} and I will answer with structured historical + latest-news evidence.`,
      sentiment: 'neutral',
      source: 'rules',
      scope,
      sources: [],
      retrieval: { scope, used_chunks: 0, historical_chunks: 0, news_chunks: 0 }
    };
  }

  if (intent === 'off_topic') {
    return {
      answer: buildOffTopicReply({ scope, question }),
      sentiment: 'neutral',
      source: 'guardrail-off-topic',
      scope,
      sources: [],
      retrieval: { scope, used_chunks: 0, historical_chunks: 0, news_chunks: 0 }
    };
  }

  let retrievalPack;
  let docs = [];
  let sources = [];
  let docsBlock = '';
  let historyText = '';
  let sentiment = 'neutral';
  let prompt = '';

  try {
    retrievalPack = await retrieveRagDocs({ scope, question, topK: wantsDeepDive ? 14 : 10 });
    docs = retrievalPack.docs || [];
    docsBlock = buildRetrievedDocsBlock(docs);
    historyText = formatHistory(history);
    sentiment = classifySentiment(docsBlock);
    prompt = PROMPT_TEMPLATE
      .replace('{retrieved_docs}', docsBlock)
      .replace('{chat_history}', historyText)
      .replace('{user_query}', `${question}\n\nScope: ${scope}${wantsDeepDive ? '\n\nDepth mode: User requested a full / deep answer. Use every relevant chunk type (CSV, PSX DPS, DB metrics, news). Cite evidence indices [n]. Minimum ~280 words in body sections combined.' : ''}`);

    sources = docs.slice(0, 10).map((d) => ({
      source: d.source,
      type: d.type,
      published_at: d.published_at || null,
      text: d.text
    }));
  } catch (err) {
    if (!isDbMalformedError(err)) throw err;

    const csvDocs = await getCsvFallbackHistoricalDocs(scope).catch(() => []);
    const newsDocs = await getDailyNewsDocs(scope === 'MARKET' ? 'MARKET' : scope).catch(() => []);
    const fallbackDocs = [...csvDocs, ...(newsDocs || []).slice(0, 5)];
    const fallbackPrompt = PROMPT_TEMPLATE
      .replace('{retrieved_docs}', buildRetrievedDocsBlock(fallbackDocs))
      .replace('{chat_history}', formatHistory(history))
      .replace('{user_query}', `${question}\n\nScope: ${scope}\n\nNote: Build a confident answer from the provided evidence and keep recommendations actionable.`);

    const modelResult = await callPrimaryChatModel({
      prompt: fallbackPrompt,
      maxTokens: wantsDeepDive ? 900 : 560,
      temperature: groqTemp,
      groqApiKey,
      groqModel
    });
    let answer = String(modelResult?.answer || '').trim();

    if (!answer) {
      answer = csvDocs.length
        ? buildLocalStructuredFallback({ scope, question, docs: fallbackDocs })
        : buildDbRecoveryFallback({ scope, question, newsDocs });
    }

    answer = enforceStructuredAnswer(answer, { scope, question, docs: fallbackDocs });

    return {
      answer,
      sentiment: 'neutral',
      source: modelResult?.provider && modelResult.provider !== 'none'
        ? `${modelResult.provider}-db-recovery`
        : 'fallback-db-recovery',
      scope,
      sources: fallbackDocs.slice(0, 8).map((d) => ({
        source: d.source,
        type: d.type,
        published_at: d.published_at || null,
        text: d.text
      })),
      retrieval: {
        scope,
        degraded_mode: true,
        reason: 'database_malformed',
        used_chunks: Number(fallbackDocs.length || 0),
        historical_chunks: Number(csvDocs.length || 0),
        news_chunks: Number((newsDocs || []).slice(0, 5).length)
      },
      ...(modelResult?.error ? { error: modelResult.error } : {})
    };
  }

  const modelResult = await callPrimaryChatModel({
    prompt,
    maxTokens: wantsDeepDive ? 960 : 620,
    temperature: groqTemp,
    groqApiKey,
    groqModel
  });

  if (!modelResult?.answer) {
    const fallbackAnswer = enforceStructuredAnswer(
      buildLocalStructuredFallback({ scope, question, docs }),
      { scope, question, docs }
    );
    return {
      answer: fallbackAnswer,
      sentiment: classifySentiment(fallbackAnswer),
      source: 'fallback',
      scope,
      sources,
      retrieval: retrievalPack.meta,
      ...(modelResult?.error ? { error: modelResult.error } : {})
    };
  }

  try {
    const rawAnswer = String(modelResult.answer || '').trim() || 'No response generated.';
    const answer = enforceStructuredAnswer(rawAnswer, { scope, question, docs });
    return {
      answer,
      sentiment: classifySentiment(answer),
      source: modelResult.provider || 'model',
      scope,
      sources,
      retrieval: retrievalPack.meta
    };
  } catch (err) {
    const answer = enforceStructuredAnswer(
      buildLocalStructuredFallback({ scope, question, docs }),
      { scope, question, docs }
    );
    return {
      answer,
      sentiment: classifySentiment(answer),
      source: 'fallback-local',
      scope,
      sources,
      retrieval: retrievalPack.meta,
      error: String(err?.message || err)
    };
  }
}

export const __testables = {
  detectIntent,
  buildOffTopicReply
};
