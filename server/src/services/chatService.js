import axios from 'axios';
import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import { db } from '../lib/db.js';
import { config } from '../config.js';
import { getLatestBusinessNewsFeed } from './sentimentIngestionService.js';
import { scrapeCompanyPage } from './companyScraperService.js';

const PROMPT_TEMPLATE = `You are a financial analysis assistant for Pakistan Stock Exchange (PSX).

Your job is to produce clear, natural, and human-like analysis from the evidence only.

STRICT RULES:
- Use only the provided evidence. Never fabricate prices, percentages, dates, or metrics.
- Do not use markdown symbols like **, ##, or numbered template headings.
- Avoid fixed repetitive sections such as "Direct Answer" or "Conclusion" in every reply.
- Adapt tone and length to the user request style.
- Keep recommendations consistent with trend/risk evidence (avoid contradictions).
- Use bullet points only when they improve readability.
- Always include practical caution language when risk is elevated.
- End with: This is educational information, not financial advice.

Response style guidance:
{response_style}

Retrieved evidence:
{retrieved_docs}

Conversation history:
{chat_history}

User question:
{user_query}`;

const STOPWORDS = new Set([
  'a', 'an', 'the', 'is', 'are', 'was', 'were', 'be', 'to', 'for', 'of', 'in', 'on', 'at', 'from', 'and', 'or', 'with', 'about',
  'this', 'that', 'these', 'those', 'it', 'its', 'as', 'by', 'into', 'over', 'under', 'i', 'you', 'we', 'they', 'he', 'she', 'them',
  'my', 'your', 'our', 'their', 'what', 'which', 'how', 'why', 'when', 'where', 'who', 'should', 'would', 'could', 'can', 'may',
  'today', 'now', 'latest', 'current', 'tell', 'me', 'please'
]);

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

async function getCsvFallbackHistoricalDocs(scope) {
  const snapshot = await loadCsvMarketSnapshot();
  if (!snapshot) return [];

  const docs = [];
  if (scope === 'MARKET') {
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

  const row = snapshot.bySymbol.get(String(scope || '').toUpperCase());
  if (!row) return [];
  return [{
    id: `csv::${scope}::latest`,
    source: 'csv_symbol_latest',
    type: 'historical',
    scope,
    published_at: row.date,
    text: `${scope} latest CSV record (${row.date}): close ${formatNum(row.close)} PKR, change ${formatNum(row.change_pct)}%, volume ${formatNum(row.volume, 0)}.`
  }];
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

  if (!/(this\s+stock|this\s+stocks|that\s+stock|that\s+stocks|it\b)/i.test(q)) return null;

  const recent = Array.isArray(history)
    ? history
      .slice(-16)
      .filter((msg) => String(msg?.role || '').toLowerCase() === 'user')
      .reverse()
    : [];

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
    SELECT date, open, high, low, close, change_pct, volume
    FROM stocks
    WHERE symbol = ?
    ORDER BY date DESC
    LIMIT 252
  `).all(scope);

  if (!rows.length) return [];

  const latest = rows[0];
  const closeAt5 = rows[Math.min(4, rows.length - 1)]?.close;
  const closeAt20 = rows[Math.min(19, rows.length - 1)]?.close;
  const closeAt60 = rows[Math.min(59, rows.length - 1)]?.close;

  const ret5 = pctChange(latest?.close, closeAt5);
  const ret20 = pctChange(latest?.close, closeAt20);
  const ret60 = pctChange(latest?.close, closeAt60);

  const closes20 = rows.slice(0, 20).map((r) => Number(r.close)).filter(Number.isFinite);
  const closes60 = rows.slice(0, 60).map((r) => Number(r.close)).filter(Number.isFinite);
  const ma20 = average(closes20);
  const ma60 = average(closes60);
  const maSpreadPct = pctChange(ma20, ma60);

  const vol20 = stdDev(rows.slice(0, 20).map((r) => Number(r.change_pct)));
  const avgVol20 = average(rows.slice(0, 20).map((r) => Number(r.volume)));
  const avgVol5 = average(rows.slice(0, 5).map((r) => Number(r.volume)));
  const volRatio = Number.isFinite(Number(latest?.volume)) && Number.isFinite(avgVol20) && avgVol20 > 0
    ? Number(latest.volume) / avgVol20
    : null;

  // Support/Resistance levels
  const high20 = closes20.length ? Math.max(...closes20) : null;
  const low20 = closes20.length ? Math.min(...closes20) : null;
  const high60 = closes60.length ? Math.max(...closes60) : null;
  const low60 = closes60.length ? Math.min(...closes60) : null;

  // 52-week high/low
  const closes252 = rows.slice(0, 252).map((r) => Number(r.close)).filter(Number.isFinite);
  const yearHigh = closes252.length ? Math.max(...closes252) : null;
  const yearLow = closes252.length ? Math.min(...closes252) : null;

  // RSI calculation
  const changes14 = rows.slice(0, 14).map((r) => Number(r.change_pct)).filter(Number.isFinite);
  let rsiVal = null;
  if (changes14.length >= 14) {
    const gains = changes14.map((c) => Math.max(c, 0));
    const losses = changes14.map((c) => Math.abs(Math.min(c, 0)));
    const avgGain = average(gains);
    const avgLoss = average(losses);
    rsiVal = avgLoss === 0 ? 100 : 100 - (100 / (1 + avgGain / avgLoss));
  }

  // Momentum acceleration
  let momentumNote = '';
  if (Number.isFinite(ret5) && Number.isFinite(ret20)) {
    if (ret5 > ret20 && ret20 > 0) momentumNote = 'Momentum ACCELERATING — short-term outpacing medium-term.';
    else if (ret5 < ret20 && ret20 > 0) momentumNote = 'Momentum DECELERATING — short-term slowing despite positive medium-term.';
    else if (ret5 > 0 && ret20 < 0) momentumNote = 'Potential TREND REVERSAL — short-term recovery while medium-term still negative.';
    else if (ret5 < 0 && ret20 > 0) momentumNote = 'Short-term DIVERGENCE — recent weakness despite broader uptrend.';
  }

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

  // MA crossover signal
  const maCrossover = (Number.isFinite(ma20) && Number.isFinite(ma60))
    ? (ma20 > ma60 ? 'GOLDEN CROSS (20-MA above 60-MA, bullish)' : 'DEATH CROSS (20-MA below 60-MA, bearish)')
    : '';

  const docs = [
    {
      id: `hist::${scope}::multi_timeframe`,
      source: 'symbol_multi_timeframe_metrics',
      type: 'historical',
      scope,
      published_at: latest?.date,
      text: `${scope} multi-timeframe performance as of ${latest?.date}: Latest close ${formatNum(latest?.close)} PKR, last session change ${formatNum(latest?.change_pct)}%. 5-session return ${formatNum(ret5)}%, 20-session return ${formatNum(ret20)}%, 60-session return ${formatNum(ret60)}%. MA20 ${formatNum(ma20)} PKR vs MA60 ${formatNum(ma60)} PKR (spread ${formatNum(maSpreadPct)}%). ${maCrossover}. ${momentumNote}${rsiVal != null ? ` RSI(14): ${formatNum(rsiVal, 1)}${rsiVal > 70 ? ' (OVERBOUGHT)' : rsiVal < 30 ? ' (OVERSOLD)' : ' (neutral range)'}.` : ''}`
    },
    {
      id: `hist::${scope}::regime`,
      source: 'symbol_regime_inference',
      type: 'historical',
      scope,
      published_at: latest?.date,
      text: `${scope} current regime inference: ${regime.regime} (confidence ${regime.confidence}%, bullish signals ${regime.bullishSignals}, bearish signals ${regime.bearishSignals}), 20-session volatility ${formatNum(vol20)}%, volume ratio vs 20-session average ${formatNum(volRatio)}x.`
    },
    {
      id: `hist::${scope}::key_levels`,
      source: 'symbol_key_levels',
      type: 'historical',
      scope,
      published_at: latest?.date,
      text: `${scope} key technical levels: 20-day support ${formatNum(low20)} PKR, 20-day resistance ${formatNum(high20)} PKR. 60-day support ${formatNum(low60)} PKR, 60-day resistance ${formatNum(high60)} PKR.${yearHigh != null ? ` 52-week range: ${formatNum(yearLow)}-${formatNum(yearHigh)} PKR, currently ${formatNum(pctChange(latest?.close, yearHigh))}% from 52-week high.` : ''}`
    },
    {
      id: `hist::${scope}::volume_analysis`,
      source: 'symbol_volume_analysis',
      type: 'historical',
      scope,
      published_at: latest?.date,
      text: `${scope} volume analysis: Latest volume ${formatNum(latest?.volume, 0)}, 5-day avg ${formatNum(avgVol5, 0)}, 20-day avg ${formatNum(avgVol20, 0)}.${Number.isFinite(avgVol5) && Number.isFinite(avgVol20) && avgVol20 > 0 ? ` 5-day vs 20-day volume ratio: ${formatNum(avgVol5 / avgVol20)}x.${avgVol5 > avgVol20 * 1.5 ? ' VOLUME SURGE: rising interest/institutional activity.' : avgVol5 < avgVol20 * 0.6 ? ' Volume contracting: declining interest.' : ' Normal volume levels.'}` : ''}`
    }
  ];

  // Recent price action (last 5 sessions)
  if (rows.length >= 3) {
    const recentSessions = rows.slice(0, 5);
    const sessionDetails = recentSessions.map((r) =>
      `${r.date}: close ${formatNum(r.close)} PKR, change ${formatNum(r.change_pct)}%, vol ${formatNum(r.volume, 0)}`
    ).join('; ');

    // Streak analysis
    let streak = 0;
    let streakDir = null;
    for (const r of recentSessions) {
      const ch = Number(r.change_pct);
      if (!Number.isFinite(ch)) break;
      if (streakDir === null) { streakDir = ch > 0 ? 'up' : 'down'; streak = 1; }
      else if ((ch > 0 && streakDir === 'up') || (ch < 0 && streakDir === 'down')) streak += 1;
      else break;
    }
    const streakNote = streak >= 2 ? ` ${streak}-session ${streakDir} streak.` : '';

    docs.push({
      id: `hist::${scope}::price_action`,
      source: 'symbol_price_action_5d',
      type: 'historical',
      scope,
      published_at: latest?.date,
      text: `${scope} recent price action (last 5 sessions): ${sessionDetails}.${streakNote}`
    });
  }

  // Weekly summaries (last 4 weeks)
  if (rows.length >= 10) {
    const weekParts = [];
    for (let w = 0; w < 4; w++) {
      const start = w * 5;
      const end = Math.min(start + 5, rows.length);
      if (start >= rows.length) break;
      const weekRows = rows.slice(start, end);
      const weekCloses = weekRows.map((r) => Number(r.close)).filter(Number.isFinite);
      if (!weekCloses.length) continue;
      const weekReturn = pctChange(weekCloses[0], weekCloses[weekCloses.length - 1]);
      weekParts.push(`Week ${w + 1}: ${formatNum(weekReturn)}% (${weekRows[weekRows.length - 1]?.date} to ${weekRows[0]?.date})`);
    }
    if (weekParts.length >= 2) {
      docs.push({
        id: `hist::${scope}::weekly_summary`,
        source: 'symbol_weekly_performance',
        type: 'historical',
        scope,
        published_at: latest?.date,
        text: `${scope} weekly performance (most recent first): ${weekParts.join('; ')}.`
      });
    }
  }

  if (sent7?.total) {
    const sentTrend = Number.isFinite(Number(sent7.avg_score)) && Number.isFinite(Number(sent30?.avg_score))
      ? (Number(sent7.avg_score) > Number(sent30?.avg_score) + 0.05 ? ' Sentiment IMPROVING (7d > 30d).' :
         Number(sent7.avg_score) < Number(sent30?.avg_score) - 0.05 ? ' Sentiment DETERIORATING (7d < 30d).' :
         ' Sentiment STABLE.')
      : '';

    docs.push({
      id: `hist::${scope}::sentiment_7d_30d`,
      source: 'symbol_sentiment_regime',
      type: 'historical',
      scope,
      published_at: latest?.date,
      text: `${scope} sentiment analysis: last 7 days avg score ${formatNum(sent7.avg_score, 3)} (${sent7.positive_count || 0} positive, ${sent7.negative_count || 0} negative, ${sent7.neutral_count || 0} neutral), last 30 days avg score ${formatNum(sent30?.avg_score, 3)} from ${sent30?.total || 0} items.${sentTrend}`
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
  const q = String(question || '').trim().toLowerCase();
  if (!q) return 'unknown';
  if (/^(hi|hello|hey|salam|assalam|aoa)\b/.test(q)) return 'greeting';
  return 'unknown';
}

function isConciseQuestion(question) {
  const q = String(question || '').trim();
  if (!q) return true;
  const tokenCount = tokenize(q).length;
  const requestsDeep = /(deep|detailed|comprehensive|full|complete|breakdown|step by step|in detail|full analysis)/i.test(q);
  return tokenCount <= 10 && !requestsDeep;
}

function wantsStructuredOutput(question) {
  const q = String(question || '').trim();
  if (!q) return false;
  return /(complete|full|detailed|comprehensive|technical and fundamental|key levels|risk factors|actionable interpretation|format|formatted|heading|headings|bold)/i.test(q);
}

function buildResponseStyleInstruction(question, wantsDeepDive, structuredMode) {
  if (structuredMode) {
    return 'Use markdown structure with relevant headings for the context (not rigid repeated templates). Highlight key metrics in bold and keep bullet points short and readable.';
  }
  if (wantsDeepDive) {
    return 'Provide a detailed but readable response. Use short paragraphs, optional bullets for key levels/risks, and explain why the recommendation follows from the data.';
  }
  if (isConciseQuestion(question)) {
    return 'Keep it concise (about 4-8 lines). Answer directly, include only the most relevant numbers, and avoid unnecessary detail.';
  }
  return 'Use a balanced response (roughly 8-14 lines): direct answer, key supporting evidence, and clear risk-aware interpretation.';
}

function cleanupPlainTextAnswer(rawText) {
  let text = String(rawText || '').replace(/\r\n?/g, '\n');
  text = removeForbiddenPhrases(text)
    .replace(/\*\*/g, '')
    .replace(/^#{1,6}\s*/gm, '')
    .replace(/`+/g, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  // Break inline bullet-like segments into separate lines.
  text = text
    .replace(/\s+-\s+(?=[A-Z0-9])/g, '\n- ')
    .replace(/\s+•\s+/g, '\n- ')
    .replace(/^\*\s+/gm, '- ')
    .replace(/^•\s+/gm, '- ');

  // Remove repetitive hard template labels if they appear.
  text = text
    .replace(/^\s*(Direct Answer|Historical Data Evidence|Recommendation|Risk Factors|Actionable Interpretation|Conclusion)\s*:?\s*$/gim, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  return text;
}

function cleanupStructuredAnswer(rawText) {
  let text = normalizeStructuredMarkdown(String(rawText || ''));
  text = removeForbiddenPhrases(text)
    .replace(/^(#{2,3}\s+[^\n*]+)\*\*\s*$/gm, '$1')
    .replace(/^(#{2,3}\s+[^\n-]+?)\s+-\s+/gm, '$1\n- ')
    .replace(/\s+-\s+(?=(?:\d{1,3}-day|\d{1,3}-session|MA\d+|RSI|DEATH CROSS|Current|Risk|Practical|Latest|Volume|Sentiment|News|Key|Support|Resistance))/g, '\n- ')
    .replace(/`+/g, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  // If model didn't provide meaningful headings, map from fallback scaffold.
  if (!/##\s+/.test(text)) {
    text = normalizeStructuredMarkdown(buildLocalStructuredFallback({ scope: 'MARKET', question: '', docs: [] })) + '\n\n' + text;
  }

  return text;
}

function enforceRecommendationConsistency(text) {
  let out = String(text || '');
  const bearish = /(bearish|death cross|under pressure|downtrend|high downside risk)/i.test(out);
  const bullish = /(bullish|uptrend|strong momentum|breakout confirmation)/i.test(out);
  const buyWords = /\b(buy|accumulate|strong buy)\b/i.test(out);
  const sellWords = /\b(sell|reduce exposure|avoid long exposure)\b/i.test(out);

  if (bearish && buyWords && !bullish) {
    out += '\nRisk note: Trend signals are bearish, so avoid aggressive buying until clear reversal confirmation appears.';
  }
  if (bullish && sellWords && !bearish) {
    out += '\nRisk note: Trend signals are constructive, so full liquidation may be premature without a confirmed breakdown.';
  }
  return out;
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

  const exactPhraseBoost = text.includes(normalizeText(rawQuestion).toLowerCase()) ? 0.12 : 0;
  const typeBoost = doc.type === 'historical' ? 0.13 : 0.05;
  let intentBoost = 0;
  const recency = recencyBoost(doc?.published_at);
  const source = String(doc?.source || '').toLowerCase();
  if (/(strongest|best|top|momentum|trend|sentiment|outlook|historical)/.test(q) && doc.type === 'historical') intentBoost += 0.18;
  if (/(strongest|best|top|momentum|trend|sentiment|outlook|historical)/.test(q) && doc.type === 'news') intentBoost -= 0.05;
  if (/(risk|uncertainty|downside|threat|warning)/.test(q) && doc.type === 'news') intentBoost += 0.08;
  if (/(buy|recommend|entry|accumulate|pick)/.test(q) && source.includes('market_composite_strength_detail')) intentBoost += 0.22;
  if (/(buy|recommend|entry|accumulate|pick)/.test(q) && source.includes('market_composite_strength')) intentBoost += 0.16;
  if (/(deep|detailed|comprehensive|full|scenario|probability|confidence|multi|timeframe)/.test(q) && doc.type === 'historical') intentBoost += 0.16;
  if (/(current|today|latest|now|fresh)/.test(q)) intentBoost += recency * 0.5;
  if (/(news|headline|sentiment)/.test(q) && doc.type === 'news') intentBoost += 0.12;
  if (/(news|headline|sentiment)/.test(q) && source.includes('sentiment_regime')) intentBoost += 0.1;
  if (source.includes('multi_timeframe') || source.includes('regime') || source.includes('breadth_trend')) intentBoost += 0.08;

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
  const marketRelevanceRegex = /(psx|kse|stock|market|inflation|gdp|exports|rupee|interest|policy|bank|oil|gas|industry|economy|imf|fiscal|current account|trade|manufacturing)/i;

  const docs = [];
  const seen = new Set();
  for (const item of (feed || [])) {
    const title = truncateText(stripHtml(item?.title), 150);
    const description = truncateText(stripHtml(item?.description), 260);
    if (!title) continue;

    const blob = `${title} ${description}`.toLowerCase();
    if (scope !== 'MARKET' && symbolRegex && !symbolRegex.test(blob)) continue;
    if (scope === 'MARKET' && !marketRelevanceRegex.test(blob)) continue;

    const dedupeKey = `${String(item?.source || '').toLowerCase()}|${title.toLowerCase()}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);

    docs.push({
      id: `daily_news::${(item?.source || 'news').toLowerCase()}::${title.slice(0, 48)}`,
      source: item?.source || 'daily_business_news',
      type: 'news',
      scope,
      published_at: item?.pubDate || new Date().toISOString(),
      text: truncateText(description ? `${title}. ${description}` : title, 300)
    });
  }

  return docs;
}

// --- PSX Company Fundamentals (scraped from dps.psx.com.pk) ---
const _psxCompanyCache = new Map(); // symbol -> { ts, docs }
const PSX_COMPANY_CACHE_TTL = 6 * 60 * 60 * 1000; // 6 hours

async function getPsxCompanyDocs(scope) {
  if (!scope || scope === 'MARKET') return [];

  const now = Date.now();
  const cached = _psxCompanyCache.get(scope);
  if (cached && (now - cached.ts) < PSX_COMPANY_CACHE_TTL) return cached.docs;

  let data;
  try {
    data = await scrapeCompanyPage(scope);
  } catch (err) {
    console.warn(`PSX company scrape failed for ${scope}: ${err.message}`);
    return [];
  }
  if (!data) return [];

  const docs = [];
  const today = new Date().toISOString().slice(0, 10);

  // 1) Company profile & market snapshot
  const profileParts = [`${data.symbol} — ${data.company_name || 'N/A'}`];
  if (data.sector) profileParts.push(`Sector: ${data.sector}`);
  if (data.close != null) profileParts.push(`Close: PKR ${data.close}`);
  if (data.change != null && data.change_pct != null) profileParts.push(`Change: ${data.change >= 0 ? '+' : ''}${data.change} (${data.change_pct >= 0 ? '+' : ''}${data.change_pct}%)`);
  if (data.open != null) profileParts.push(`Open: PKR ${data.open}`);
  if (data.high != null && data.low != null) profileParts.push(`Day Range: PKR ${data.low} – ${data.high}`);
  if (data.volume != null) profileParts.push(`Volume: ${data.volume.toLocaleString()}`);
  if (data.year_range?.low != null) profileParts.push(`52-Week Range: PKR ${data.year_range.low} – ${data.year_range.high}`);
  if (data.pe_ratio != null) profileParts.push(`P/E Ratio (TTM): ${data.pe_ratio}`);
  if (data.year_change_pct != null) profileParts.push(`1-Year Change: ${data.year_change_pct}%`);
  if (data.ytd_change_pct != null) profileParts.push(`YTD Change: ${data.ytd_change_pct}%`);
  if (data.ldcp != null) profileParts.push(`LDCP: PKR ${data.ldcp}`);
  if (data.circuit_breaker?.low != null) profileParts.push(`Circuit Breaker: PKR ${data.circuit_breaker.low} – ${data.circuit_breaker.high}`);
  if (data.profile?.business_description) profileParts.push(`Business: ${data.profile.business_description}`);

  docs.push({
    id: `psx_company_profile::${scope}`,
    source: 'psx_company_profile',
    type: 'historical',
    scope,
    published_at: today,
    text: profileParts.join(' | ')
  });

  // 2) Financial series (revenue, net income, EPS across years)
  const finSeries = data.financial_series;
  if (finSeries && (finSeries.revenue?.length || finSeries.net_income?.length || finSeries.eps?.length)) {
    const fParts = [`${scope} Financial Summary`];
    if (finSeries.years?.length) fParts.push(`Years: ${finSeries.years.join(', ')}`);
    if (finSeries.revenue?.length) fParts.push(`Revenue (Rs M): ${finSeries.revenue.join(', ')}`);
    if (finSeries.net_income?.length) fParts.push(`Net Income (Rs M): ${finSeries.net_income.join(', ')}`);
    if (finSeries.eps?.length) fParts.push(`EPS (Rs): ${finSeries.eps.join(', ')}`);

    // Growth calculations
    if (finSeries.revenue?.length >= 2) {
      const latest = finSeries.revenue[finSeries.revenue.length - 1];
      const prev = finSeries.revenue[finSeries.revenue.length - 2];
      if (prev && prev !== 0) fParts.push(`Revenue Growth: ${((latest - prev) / Math.abs(prev) * 100).toFixed(1)}%`);
    }
    if (finSeries.eps?.length >= 2) {
      const latest = finSeries.eps[finSeries.eps.length - 1];
      const prev = finSeries.eps[finSeries.eps.length - 2];
      if (prev && prev !== 0) fParts.push(`EPS Growth: ${((latest - prev) / Math.abs(prev) * 100).toFixed(1)}%`);
    }

    docs.push({
      id: `psx_company_financials::${scope}`,
      source: 'psx_company_financials',
      type: 'historical',
      scope,
      published_at: today,
      text: fParts.join(' | ')
    });
  }

  _psxCompanyCache.set(scope, { ts: now, docs });
  return docs;
}

async function retrieveRagDocs({ scope, question, topK = 14 }) {
  const questionTokens = tokenize(question);
  const historicalDocs = getHistoricalDocs(scope);
  const [dailyNewsDocs, psxCompanyDocs] = await Promise.all([
    getDailyNewsDocs(scope),
    getPsxCompanyDocs(scope)
  ]);

  let allDocs = [...historicalDocs, ...dailyNewsDocs, ...psxCompanyDocs];
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
    .filter((doc) => doc._score > 0.05)
    .sort((a, b) => b._score - a._score);

  const target = Math.max(10, Number(topK || 14));
  const rankedHistorical = ranked.filter((d) => d.type === 'historical');
  const rankedNews = ranked.filter((d) => d.type === 'news');

  // Increase historical allocation for richer context
  const historicalTarget = scope === 'MARKET'
    ? Math.max(6, Math.ceil(target * 0.65))
    : Math.max(5, Math.ceil(target * 0.6));
  const newsTarget = Math.max(2, target - historicalTarget);

  const selected = [
    ...rankedHistorical.slice(0, historicalTarget),
    ...rankedNews.slice(0, newsTarget)
  ];

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

  const docs = fallback.map(({ _score, ...doc }) => doc);
  return {
    docs,
    meta: {
      scope,
      sparse_symbol_fallback: sparseSymbolFallback,
      total_candidates: allDocs.length,
      used_chunks: docs.length,
      historical_chunks: docs.filter((d) => d.type === 'historical' && !String(d.source || '').startsWith('psx_company_')).length,
      fundamentals_chunks: docs.filter((d) => String(d.source || '').startsWith('psx_company_')).length,
      news_chunks: docs.filter((d) => d.type === 'news').length,
      refreshed_at: new Date().toISOString()
    }
  };
}

function buildRetrievedDocsBlock(docs) {
  if (!docs.length) return 'No relevant evidence was retrieved.';

  // Separate PSX company fundamentals from other historical docs
  const fundamentals = docs.filter((d) => String(d.source || '').startsWith('psx_company_'));
  const historical = docs.filter((d) => d.type === 'historical' && !String(d.source || '').startsWith('psx_company_'));
  const news = docs.filter((d) => d.type === 'news');
  const other = docs.filter((d) => d.type !== 'historical' && d.type !== 'news');

  const sections = [];

  if (fundamentals.length) {
    sections.push('=== COMPANY FUNDAMENTALS (PSX Official) ===');
    fundamentals.forEach((d, idx) => {
      sections.push(`[F${idx + 1}] ${d.source}: ${d.text}`);
    });
  }

  if (historical.length) {
    sections.push('\n=== HISTORICAL & TECHNICAL DATA ===');
    historical.forEach((d, idx) => {
      const when = d.published_at ? ` (${d.published_at})` : '';
      sections.push(`[H${idx + 1}] ${d.source}${when}: ${d.text}`);
    });
  }

  if (news.length) {
    sections.push('\n=== NEWS & SENTIMENT ===');
    news.forEach((d, idx) => {
      const when = d.published_at ? ` (${d.published_at})` : '';
      sections.push(`[N${idx + 1}] ${d.source}${when}: ${d.text}`);
    });
  }

  if (other.length) {
    sections.push('\n=== ADDITIONAL CONTEXT ===');
    other.forEach((d, idx) => {
      sections.push(`[A${idx + 1}] ${d.source}: ${d.text}`);
    });
  }

  return sections.join('\n');
}

function buildLocalStructuredFallback({ scope, question, docs }) {
  const deepMode = /(deep|detailed|comprehensive|full|totally|scenario|probability|confidence|risk|multi\s*timeframe)/i.test(String(question || ''));
  const wantsBuyIdeas = /(buy|recommend|recommended|best\s+stocks?|stock\s+picks?|entry)/i.test(String(question || ''));
  const asksHistory = /(history|historical|record|past|data)/i.test(String(question || ''));
  const asksRecommendation = /(recommendation|recommend|advice|buy|sell|hold|action)/i.test(String(question || ''));
  
  const histLimit = deepMode ? 8 : 5;
  const newsLimit = deepMode ? 4 : 3;
  const historicalRank = (d) => {
    const src = String(d?.source || '').toLowerCase();
    if (src.includes('multi_timeframe')) return 1;
    if (src.includes('regime')) return 2;
    if (src.includes('momentum')) return 3;
    if (src.includes('rollup') || src.includes('breadth')) return 4;
    return 9;
  };

  const historical = docs
    .filter((d) => d.type === 'historical')
    .sort((a, b) => historicalRank(a) - historicalRank(b))
    .slice(0, histLimit);

  const news = docs.filter((d) => d.type === 'news').slice(0, newsLimit);
  const candidateSymbols = Array.from(new Set(
    docs
      .filter((d) => d.type === 'historical' && String(d.source || '').includes('market_composite_strength_detail'))
      .map((d) => String(d.scope || '').toUpperCase())
      .filter((s) => isLikelyEquitySymbol(s))
  )).slice(0, 5);
  const hasCoverageGap = docs.some((d) => d.source === 'coverage_guardrail');

  const histBullets = historical.length
    ? historical.map((d) => `- ${truncateText(stripHtml(d.text), 180)}`).join('\n')
    : '- Direct historical records are limited for this query.';

  const newsBullets = news.length
    ? news.map((d) => `- ${truncateText(stripHtml(d.text), 180)}`).join('\n')
    : '- No strong same-day news signal matched this query.';

  const signalDoc = docs.find((d) => String(d.source || '').includes('symbol_multi_timeframe_metrics'));
  const regimeDoc = docs.find((d) => String(d.source || '').includes('symbol_regime_inference'));
  const sentimentDoc = docs.find((d) => String(d.source || '').includes('sentiment_regime') || String(d.source || '').includes('sentiment_symbol_aggregate'));

  const extractMetric = (text, label) => {
    const m = String(text || '').match(new RegExp(`${label}\\s+(-?\\d+(?:\\.\\d+)?)%?`, 'i'));
    return m ? Number(m[1]) : null;
  };

  const ret20 = extractMetric(signalDoc?.text, '20-session return');
  const ret60 = extractMetric(signalDoc?.text, '60-session return');
  const sentAvg = extractMetric(sentimentDoc?.text, 'avg score');
  const regimeMatch = String(regimeDoc?.text || '').match(/regime inference:\s*([^()]+?)\s*\(/i);
  const regimeLabel = regimeMatch ? normalizeText(regimeMatch[1]) : '';

  const dynamicRecommendation = [];
  if (scope !== 'MARKET' && Number.isFinite(ret20) && Number.isFinite(ret60)) {
    if (ret20 > 0.8 && ret60 > 0.8) {
      dynamicRecommendation.push(`- ${scope} shows positive short+medium trend (${formatNum(ret20)}% / ${formatNum(ret60)}%); consider pullback entries instead of chasing spikes.`);
    } else if (ret20 < -0.8 && ret60 < -0.8) {
      dynamicRecommendation.push(`- ${scope} remains under pressure (${formatNum(ret20)}% / ${formatNum(ret60)}%); prioritize capital protection and wait for confirmed reversal structure.`);
    } else {
      dynamicRecommendation.push(`- ${scope} trend is mixed across timeframes (${formatNum(ret20)}% / ${formatNum(ret60)}%); prefer staged entries with tighter risk controls.`);
    }
  }

  if (scope !== 'MARKET' && regimeLabel) {
    dynamicRecommendation.push(`- Current regime appears **${regimeLabel}**; validate this view with next sessions' volume and close behavior.`);
  }

  if (scope !== 'MARKET' && Number.isFinite(sentAvg)) {
    if (sentAvg > 0.08) dynamicRecommendation.push(`- Sentiment backdrop is supportive (avg score ${formatNum(sentAvg, 3)}), which can reinforce bullish follow-through if price confirms.`);
    else if (sentAvg < -0.08) dynamicRecommendation.push(`- Sentiment is weak (avg score ${formatNum(sentAvg, 3)}), so headline shocks may amplify downside volatility.`);
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
    lines.push(
      '## Historical Data Evidence',
      '- **Historical / trend signals**',
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
  text = text.replace(/^\s*direct answer\s*[:\-]*\s*$/gim, '## Direct Answer');
  text = text.replace(/^\s*historical data evidence\s*[:\-]*\s*$/gim, '## Historical Data Evidence');
  text = text.replace(/^\s*recommendation\s*[:\-]*\s*$/gim, '## Recommendation');
  return text;
}

function extractSection(text, heading) {
  const rx = new RegExp(`##\\s+${heading}\\s*\\n([\\s\\S]*?)(?=\\n##\\s+|$)`, 'i');
  const m = String(text || '').match(rx);
  return m ? m[1].trim() : '';
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

function buildContextStructuredAnswer({ scope, question, docs }) {
  const historical = (docs || []).filter((d) => d.type === 'historical').slice(0, 7);
  const news = (docs || []).filter((d) => d.type === 'news').slice(0, 5);
  const q = String(question || '');
  const asksBuySell = /(buy|sell|hold|entry|exit|action|recommend)/i.test(q);

  const histBullets = historical.length
    ? historical.map((d) => `- ${truncateText(stripHtml(String(d.text || '')), 180)}`)
    : ['- Historical technical coverage is limited for this symbol in the current snapshot.'];

  const newsBullets = news.length
    ? news.map((d) => `- ${truncateText(stripHtml(String(d.text || '')), 170)}`)
    : ['- No strong fresh headline cluster was captured in the latest scan.'];

  const sentimentHint = (docs || []).map((d) => String(d.text || '')).join(' ').toLowerCase();
  const bearish = /(bearish|under pressure|death cross|downtrend|negative)/i.test(sentimentHint);
  const bullish = /(bullish|uptrend|positive momentum|breakout)/i.test(sentimentHint);

  const actionBullets = [];
  if (asksBuySell) {
    if (bearish && !bullish) {
      actionBullets.push('- Bias: **defensive / risk-first** until reversal confirmation appears.');
      actionBullets.push('- Prefer staged entries only near support with strict stop-loss discipline.');
    } else if (bullish && !bearish) {
      actionBullets.push('- Bias: **constructive** while trend structure and volume confirmation persist.');
      actionBullets.push('- Favor pullback entries instead of chasing extended intraday spikes.');
    } else {
      actionBullets.push('- Bias: **mixed signals**; avoid oversized positions and wait for cleaner confirmation.');
      actionBullets.push('- Manage exposure with phased entries and clear invalidation levels.');
    }
  } else {
    actionBullets.push('- Use this as a monitoring framework, then re-check after the next market session update.');
  }
  actionBullets.push('- This is educational information, not financial advice.');

  return [
    `## ${scope} technical and fundamental snapshot`,
    `Primary context for this query: **${normalizeText(q)}**.`,
    '',
    '## Technical and historical signals',
    ...histBullets,
    '',
    '## Fundamental and sentiment context',
    ...newsBullets,
    '',
    '## Risk and actionable interpretation',
    ...actionBullets
  ].join('\n');
}

function normalizeStructuredMarkdown(rawText) {
  let text = String(rawText || '').replace(/\r\n?/g, '\n').trim();
  if (!text) return text;

  text = text
    .replace(/^\*\*ummary\b/i, 'Summary')
    .replace(/([^\n])\s+(##\s+)/g, '$1\n$2')
    .replace(/([^\n])\s+(###\s+)/g, '$1\n$2')
    .replace(/^##\s+(Summary|Direct Answer|Historical Data Evidence|Historical & Technical Data|Technical Analysis|Fundamental Analysis|Sentiment Analysis|News & Sentiment|News and Sentiment|Actionable Interpretation|Risk Factors|Conclusion|Recommendation)\s+(?=[A-Z])/gim, '## $1\n')
  .replace(/^##\s+(Volume Analysis|Regime Inference|News and Sentiment Alignment|Multi-Timeframe Performance)\s+(?=[A-Z])/gim, '## $1\n')
    .replace(/^###\s+(Key Levels|Multi-Timeframe Metrics|Multi-Timeframe Signals|Volume Analysis|Current Regime|Sentiment Analysis|Headlines)\s+(?=[A-Z])/gim, '### $1\n')
    .replace(/((?:##|###)\s+[^\n]+?)\s+-\s+/g, '$1\n- ')
    .replace(/\s+(Technical Analysis|Fundamental Analysis|Sentiment Analysis|News and Sentiment|Headlines|Conclusion|Risk Factors|Actionable Interpretation|Disclaimer):/gi, '\n$1:')
  .replace(/\s+-\s+(?=[A-Z*])/g, '\n- ')
    .replace(/(:)\s+\*\s+/g, '$1\n- ')
    .replace(/([.!?])\s+\*\s+/g, '$1\n- ')
    .replace(/\s+-\s+\*\*/g, '\n- **')
    .replace(/\)\s+-\s+/g, ')\n- ')
    .replace(/\s+\*\s+/g, '\n- ')
    .replace(/^\*\s+/gm, '- ')
    .replace(/^•\s+/gm, '- ')
  .replace(/^-\s+(.+?)\*\*:\s*/gm, '- **$1:** ')
  .replace(/(^|\n)-\s+([^\n]*?):\*\*/g, '$1- **$2:**')
  .replace(/(^|\n)-\s+([^\n*]+?)\*\*(?=\s|$)/g, '$1- **$2**')
  .replace(/(^|\n)-\s+([^\n]+?)\*\*$/g, '$1- **$2**')
    .replace(/^\s*[-*]\s*[-*]+\s*/gm, '- ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  return normalizeHeadings(text);
}

function enforceStructuredAnswer(answer, { scope, question, docs, structuredMode = false }) {
  let text = structuredMode
    ? cleanupStructuredAnswer(answer)
    : cleanupPlainTextAnswer(answer);

  // If the answer is too short/weak, use local fallback then sanitize it.
  if (text.length < 120) {
    text = structuredMode
      ? cleanupStructuredAnswer(buildLocalStructuredFallback({ scope, question, docs }))
      : cleanupPlainTextAnswer(buildLocalStructuredFallback({ scope, question, docs }));
  }

  text = enforceRecommendationConsistency(text)
    .replace(/\n{3,}/g, '\n\n')
    .trim();

  if (structuredMode) {
    const malformedStructured =
      (!/^##\s+/m.test(text) && !/^###\s+/m.test(text))
      || /\*\*:/.test(text)
      || /^-\s+[A-Za-z][^\n]{0,60}\*\*\s*=+/m.test(text)
      || /={4,}/.test(text)
      || /^-\s+.*\*\*\s*$/m.test(text);
    if (malformedStructured) {
      text = buildContextStructuredAnswer({ scope, question, docs });
    }
  }

  return text;
}

export async function generateChatReply({ stock, question, history = [], groqApiKey, groqModel }) {
  const requestedScope = normalizeScope(stock);
  const inferredScope = requestedScope === 'MARKET' ? inferScopeFromQuestion(question, history) : null;
  const scope = inferredScope || requestedScope;
  const wantsDeepDive = /(deep|detailed|comprehensive|full|totally|scenario|probability|confidence|risk|multi\s*timeframe)/i.test(String(question || ''));
  const structuredMode = wantsStructuredOutput(question);
  const conciseMode = isConciseQuestion(question);
  const chatTemperature = structuredMode ? 0.52 : (wantsDeepDive ? 0.62 : (conciseMode ? 0.48 : 0.58));
  const chatMaxTokens = wantsDeepDive ? 2100 : (conciseMode ? 700 : 1300);
  const intent = detectIntent(question);
  if (intent === 'greeting') {
    const greetingText = String(question || '').trim().toLowerCase();
    const historyTurns = Array.isArray(history) ? history.length : 0;
    const baseGreeter = historyTurns >= 4
      ? 'Welcome back'
      : (historyTurns >= 2 ? 'Great to see you again' : 'Hi');
    const greeter = /assalam|salam|aoa/.test(greetingText)
      ? 'Wa Alaikum Assalam'
      : (/^hello\b/.test(greetingText)
          ? (historyTurns ? 'Hello again' : 'Hello')
          : (/^hey\b/.test(greetingText) ? 'Hey there' : baseGreeter));

    const starterPrompts = scope === 'MARKET'
      ? [
          'What is the PSX market outlook today with key risks?',
          'Which sectors and symbols look strongest right now?',
          'Summarize latest business news impact on PSX sentiment.'
        ]
      : [
          `Give me a full analysis for ${scope} with entry/exit levels.`,
          `What are the latest news and sentiment signals for ${scope}?`,
          `Assess risk, support/resistance and trade plan for ${scope}.`
        ];

    try {
      const greetingRetrieval = await retrieveRagDocs({
        scope,
        question: scope === 'MARKET'
          ? 'latest psx market snapshot with breadth momentum sentiment and business news'
          : `latest ${scope} historical trend technical levels fundamentals and news sentiment`,
        topK: 6
      });

      const highlights = greetingRetrieval.docs
        .slice(0, 3)
        .map((d) => `- ${truncateText(String(d.text || ''), 180)}`)
        .filter(Boolean);

      const answer = [
        `${greeter}! Ask me any PSX question and I will answer with structured historical + latest-news evidence.`,
        '',
        `Quick ${scope} snapshot:`,
        ...(highlights.length ? highlights : ['- I am ready, but no evidence chunks were available right now.']),
        '',
        'Try one of these:',
        ...starterPrompts.map((p) => `- ${p}`)
      ].join('\n');

      return {
        answer,
        sentiment: classifySentiment(answer),
        source: 'rules-greeting-context',
        scope,
        sources: greetingRetrieval.docs.slice(0, 6).map((d) => ({
          source: d.source,
          type: d.type,
          published_at: d.published_at || null,
          text: d.text
        })),
        retrieval: greetingRetrieval.meta
      };
    } catch {
      // If retrieval fails, keep a deterministic lightweight greeting fallback.
    }

    return {
      answer: scope === 'MARKET'
        ? `${greeter}! Ask me any PSX question and I will answer with structured historical + latest-news evidence. Try: "What is the PSX outlook today with key risks?"`
        : `${greeter}! Ask me anything about ${scope} and I will answer with structured historical + latest-news evidence. Try: "Give me full analysis for ${scope} with entry/exit levels."`,
      sentiment: 'neutral',
      source: 'rules',
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
  const responseStyle = buildResponseStyleInstruction(question, wantsDeepDive, structuredMode);

  try {
    retrievalPack = await retrieveRagDocs({ scope, question, topK: wantsDeepDive ? 18 : 14 });
    docs = retrievalPack.docs || [];
    docsBlock = buildRetrievedDocsBlock(docs);
    historyText = formatHistory(history);
    sentiment = classifySentiment(docsBlock);
    prompt = PROMPT_TEMPLATE
      .replace('{response_style}', responseStyle)
      .replace('{retrieved_docs}', docsBlock)
      .replace('{chat_history}', historyText)
      .replace('{user_query}', `${question}\n\nScope: ${scope}${wantsDeepDive ? '\n\nDepth mode: Deep dive requested. Include multi-timeframe signals, current regime, and sentiment/news alignment clearly.' : ''}`);

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
      .replace('{response_style}', responseStyle)
      .replace('{retrieved_docs}', buildRetrievedDocsBlock(fallbackDocs))
      .replace('{chat_history}', formatHistory(history))
      .replace('{user_query}', `${question}\n\nScope: ${scope}\n\nNote: Build a confident answer from the provided evidence and keep recommendations actionable.`);

    const fallbackSystemPrompt = structuredMode
      ? 'You are a PSX financial analyst. Return structured markdown with context-aware headings (for example: Market View, Technical Signals, Fundamentals & Sentiment, Risks, Action Plan). Use **bold** for key metrics. Keep bullets clear and avoid contradictions.'
      : 'You are a PSX financial analyst. Respond in clear plain text (no markdown symbols like ** or ##). Avoid repetitive template headings. Adapt depth to the question. Keep recommendations consistent with trend/risk evidence and avoid contradictions. Use bullets only if helpful.';

    let answer = '';
    if (groqApiKey) {
      try {
        const resp = await axios.post(
          'https://api.groq.com/openai/v1/chat/completions',
          {
            model: groqModel,
            messages: [
              {
                role: 'system',
                content: fallbackSystemPrompt
              },
              { role: 'user', content: fallbackPrompt }
            ],
            temperature: chatTemperature,
            max_tokens: chatMaxTokens
          },
          {
            headers: {
              Authorization: `Bearer ${groqApiKey}`,
              'Content-Type': 'application/json'
            },
            timeout: 45000
          }
        );
        answer = String(resp?.data?.choices?.[0]?.message?.content || '').trim();
      } catch {
        // fallback below
      }
    }

    if (!answer) {
      answer = csvDocs.length
        ? buildLocalStructuredFallback({ scope, question, docs: fallbackDocs })
        : buildDbRecoveryFallback({ scope, question, newsDocs });
    }

  answer = enforceStructuredAnswer(answer, { scope, question, docs: fallbackDocs, structuredMode });

    return {
      answer,
      sentiment: 'neutral',
      source: 'fallback-db-recovery',
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
      }
    };
  }

  if (!groqApiKey) {
    const fallbackAnswer = enforceStructuredAnswer(
      buildLocalStructuredFallback({ scope, question, docs }),
      { scope, question, docs, structuredMode }
    );
    return {
      answer: fallbackAnswer,
      sentiment: classifySentiment(fallbackAnswer),
      source: 'fallback',
      scope,
      sources,
      retrieval: retrievalPack.meta
    };
  }

  const mainSystemPrompt = structuredMode
    ? 'You are a PSX financial analysis assistant. Provide a well-structured markdown response with relevant headings chosen by context (not the same rigid template every time). Use **bold** for critical metrics (price, support/resistance, returns, RSI, sentiment score). Keep recommendations aligned with trend/risk evidence and avoid contradictions. End with educational disclaimer.'
    : 'You are a PSX financial analysis assistant. Produce natural, human-like plain-text responses (no markdown symbols). Vary structure by user question, avoid fixed repetitive headings, and keep wording concise unless a full analysis is requested. Use concrete evidence values and keep recommendations logically consistent with trend and risk.';

  try {
    const resp = await axios.post(
      'https://api.groq.com/openai/v1/chat/completions',
      {
        model: groqModel,
        messages: [
          {
            role: 'system',
            content: mainSystemPrompt
          },
          { role: 'user', content: prompt }
        ],
        temperature: chatTemperature,
        max_tokens: chatMaxTokens
      },
      {
        headers: {
          Authorization: `Bearer ${groqApiKey}`,
          'Content-Type': 'application/json'
        },
        timeout: 60000
      }
    );

    const rawAnswer = resp.data?.choices?.[0]?.message?.content?.trim() || 'No response generated.';
  const answer = enforceStructuredAnswer(rawAnswer, { scope, question, docs, structuredMode });
    return {
      answer,
      sentiment: classifySentiment(answer),
      source: 'groq',
      scope,
      sources,
      retrieval: retrievalPack.meta
    };
  } catch (err) {
    const answer = enforceStructuredAnswer(
      buildLocalStructuredFallback({ scope, question, docs }),
      { scope, question, docs, structuredMode }
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
