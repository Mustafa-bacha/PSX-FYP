import { db } from '../lib/db.js';
import { sma, ema, bollinger } from '../lib/indicators.js';
import { getMarketPerformers } from './marketPerformersService.js';
import { scrapeCompanyPage } from './companyScraperService.js';
import { scrapeTickerAnalystsPage } from './tickerAnalystsScraperService.js';
import { getPythonModelPrediction } from './modelBridgeService.js';
import { buildFocusCandidates, getFocusSymbols, mapToBaseSymbol } from './focusSymbolsService.js';

const overviewCache = new Map();
const OVERVIEW_TTL_MS = 2 * 60 * 1000;
const stocksListCache = new Map();
const STOCKS_LIST_TTL_MS = 45 * 1000;

function round2(v) {
  return v == null || Number.isNaN(v) ? null : Number(v.toFixed(2));
}

function avg(values) {
  if (!values.length) return 0;
  return values.reduce((s, n) => s + n, 0) / values.length;
}

function stdDev(values) {
  if (values.length < 2) return 0;
  const m = avg(values);
  const variance = values.reduce((s, n) => s + ((n - m) ** 2), 0) / values.length;
  return Math.sqrt(variance);
}

function clamp(num, min, max) {
  return Math.min(max, Math.max(min, num));
}

function safeOne(sql, params = []) {
  try {
    return db.prepare(sql).get(...params);
  } catch {
    return null;
  }
}

function safeAll(sql, params = []) {
  try {
    return db.prepare(sql).all(...params);
  } catch {
    return [];
  }
}

function getCompanyFromFocus(symbol) {
  const focus = getFocusSymbols();
  const profileMap = new Map((focus.profiles || []).map((p) => [String(p.symbol || '').toUpperCase(), p.company || '']));
  const sym = String(symbol || '').toUpperCase();
  return profileMap.get(sym) || profileMap.get(mapToBaseSymbol(sym)) || '';
}

function normalizeFinancialHighlights(fundamentals = []) {
  return (fundamentals || [])
    .filter((row) => row && row.label && row.value)
    .map((row) => ({
      key: String(row.label || '').toLowerCase().replace(/[^a-z0-9]+/g, '_'),
      label: row.label,
      value: row.value,
      rating: row.rating || null,
      numeric: Number.isFinite(Number(row.numeric)) ? Number(row.numeric) : null
    }));
}

function toChartSeries(label, years, values) {
  const cleanValues = (values || []).map((v) => Number(v)).filter((v) => Number.isFinite(v));
  if (!cleanValues.length) return null;

  const tailYears = (years || []).slice(-cleanValues.length);
  const labels = tailYears.length === cleanValues.length
    ? tailYears
    : cleanValues.map((_, i) => `FY ${i + 1}`);

  return {
    label,
    points: cleanValues.map((value, idx) => ({
      period: labels[idx],
      value: round2(value)
    }))
  };
}

async function resolveExternalOverview(symbol, { refresh = false } = {}) {
  const ticker = String(symbol || '').toUpperCase().trim();
  if (!ticker) return null;

  const cached = overviewCache.get(ticker);
  if (!refresh && cached && (Date.now() - cached.at) <= OVERVIEW_TTL_MS) {
    return cached.payload;
  }

  const candidates = [ticker, ticker.includes('-') ? ticker.split('-')[0] : null].filter(Boolean);
  let psx = null;
  let ta = null;

  for (const candidate of candidates) {
    if (!psx) {
      try {
        psx = await scrapeCompanyPage(candidate);
      } catch {
        // try next candidate
      }
    }
    if (!ta) {
      try {
        ta = await scrapeTickerAnalystsPage(candidate);
      } catch {
        // try next candidate
      }
    }
    if (psx && ta) break;
  }

  const payload = { psx, ta };
  overviewCache.set(ticker, { at: Date.now(), payload });
  return payload;
}

export function listStocks(limit = 600) {
  const maxRows = Math.max(1, Number(limit || 600));
  const cacheKey = String(maxRows);
  const cached = stocksListCache.get(cacheKey);
  if (cached && (Date.now() - cached.at) <= STOCKS_LIST_TTL_MS) {
    return cached.payload;
  }

  const rows = db.prepare(`
    SELECT s.symbol, s.close, s.change_pct, s.date
    FROM stocks s
    INNER JOIN (
      SELECT symbol, MAX(date) AS max_date
      FROM stocks
      GROUP BY symbol
    ) latest ON latest.symbol = s.symbol AND latest.max_date = s.date
    ORDER BY s.symbol ASC
  `).all();

  const latestBySymbol = new Map(rows.map((r) => [String(r.symbol || '').toUpperCase(), r]));
  const focus = getFocusSymbols();
  const focusSymbols = (focus.symbols || []).slice(0, maxRows);

  const snapshots = focusSymbols.map((symbol) => {
    const r = latestBySymbol.get(String(symbol || '').toUpperCase());
    return {
      symbol,
      close: round2(r?.close),
      change_pct: round2(r?.change_pct),
      as_of: r?.date || null
    };
  });

  const payload = {
    count: snapshots.length,
    symbols: snapshots.map((r) => r.symbol),
    snapshots
  };

  stocksListCache.set(cacheKey, { at: Date.now(), payload });
  return payload;
}

export function getStockHistory(symbol, days = 365) {
  const rows = db.prepare(`
    SELECT symbol, date, open, high, low, close, volume, change, change_pct
    FROM stocks
    WHERE symbol = ?
    ORDER BY date DESC
    LIMIT ?
  `).all(symbol.toUpperCase(), Math.max(1, Number(days || 365)));

  if (!rows.length) return null;

  const history = [...rows].reverse();
  const closes = history.map((r) => Number(r.close || 0));

  const ma7 = sma(closes, 7);
  const ma20 = sma(closes, 20);
  const ma50 = sma(closes, 50);
  const ema10 = ema(closes, 10);
  const ema20 = ema(closes, 20);
  const ema50 = ema(closes, 50);
  const bb = bollinger(closes, 20, 2);

  const data = history.map((r, i) => ({
    date: r.date,
    open: round2(r.open),
    high: round2(r.high),
    low: round2(r.low),
    close: round2(r.close),
    volume: r.volume == null ? null : Number(r.volume),
    ma_7: round2(ma7[i]),
    ma_20: round2(ma20[i]),
    ma_50: round2(ma50[i]),
    ema_10: round2(ema10[i]),
    ema_20: round2(ema20[i]),
    ema_50: round2(ema50[i]),
    bb_mid: round2(bb.mid[i]),
    bb_upper: round2(bb.upper[i]),
    bb_lower: round2(bb.lower[i]),
    change_pct: round2(r.change_pct)
  }));

  const latest = data[data.length - 1];
  return { symbol: symbol.toUpperCase(), data, latest };
}

export function getStockInsights(symbol) {
  const ticker = String(symbol || '').toUpperCase().trim();

  const recentRows = db.prepare(`
    SELECT date, close, change_pct
    FROM stocks
    WHERE symbol = ?
    ORDER BY date DESC
    LIMIT 120
  `).all(ticker);

  if (!recentRows.length) return null;

  const latest = recentRows[0];
  const changes = recentRows
    .slice(0, 30)
    .map((r) => Number(r.change_pct))
    .filter((v) => Number.isFinite(v));

  const driftPct = avg(changes);
  const volPct = stdDev(changes);
  const currentPrice = Number(latest.close || 0);
  const predictedPrice = currentPrice * (1 + (driftPct / 100));
  const confidence = clamp(1 - (volPct / 10), 0.35, 0.95);

  const heuristicPrediction = {
    predicted_price: round2(predictedPrice),
    current_price: round2(currentPrice),
    predicted_direction: predictedPrice >= currentPrice ? 'UP' : 'DOWN',
    confidence: Number(confidence.toFixed(4)),
    mae: round2(Math.abs(currentPrice * (volPct / 100) * 0.6)),
    rmse: round2(Math.abs(currentPrice * (volPct / 100) * 0.8)),
    direction_accuracy: Number(clamp(0.5 + (Math.abs(driftPct) / (volPct + 1.5)), 0.5, 0.9).toFixed(4)),
    source: 'heuristic'
  };

  const modelPrediction = getPythonModelPrediction(ticker);
  const prediction = modelPrediction || heuristicPrediction;

  const sentimentRow = safeOne(`
    SELECT AVG(score) AS average_score,
           SUM(CASE WHEN label='positive' THEN 1 ELSE 0 END) AS positive_count,
           SUM(CASE WHEN label='negative' THEN 1 ELSE 0 END) AS negative_count,
           SUM(CASE WHEN label='neutral' THEN 1 ELSE 0 END) AS neutral_count,
           SUM(CASE WHEN source='psx_report' THEN 1 ELSE 0 END) AS psx_report_count,
           MAX(analyzed_at) AS analyzed_at
    FROM sentiment
    WHERE symbol = ?
  `, [ticker]) || {};

  const recentHeadlines = safeAll(`
    SELECT headline, label, score, source, analyzed_at
    FROM sentiment
    WHERE symbol = ?
    ORDER BY analyzed_at DESC
    LIMIT 8
  `, [ticker]);

  const averageScore = sentimentRow.average_score == null
    ? 0
    : Number(sentimentRow.average_score);

  return {
    symbol: ticker,
    prediction,
    sentiment: {
      average_score: Number(averageScore.toFixed(4)),
      positive_count: Number(sentimentRow.positive_count || 0),
      negative_count: Number(sentimentRow.negative_count || 0),
      neutral_count: Number(sentimentRow.neutral_count || 0),
      psx_report_count: Number(sentimentRow.psx_report_count || 0),
      analyzed_at: sentimentRow.analyzed_at || null,
      recent_headlines: recentHeadlines.map((h) => ({
        headline: h.headline,
        label: h.label || 'neutral',
        score: round2(Number(h.score || 0)),
        source: h.source || 'news',
        analyzed_at: h.analyzed_at || null
      }))
    }
  };
}

export async function getStockOverview(symbol, { refresh = false } = {}) {
  const ticker = String(symbol || '').toUpperCase().trim();
  if (!ticker) return null;

  const history = getStockHistory(ticker, 365);
  if (!history) return null;

  const external = await resolveExternalOverview(ticker, { refresh });
  const psx = external?.psx || null;
  const ta = external?.ta || null;

  const latest = history.latest || {};
  const focusCompany = getCompanyFromFocus(ticker);
  const companyName = ta?.company_name || psx?.company_name || focusCompany || ticker;
  const industry = ta?.sector || psx?.sector || null;

  const financialHighlights = normalizeFinancialHighlights(ta?.fundamentals || []);

  const years = psx?.financial_series?.years || [];
  const chartCandidates = [
    toChartSeries('Total Revenue', years, psx?.financial_series?.revenue || []),
    toChartSeries('Net Income', years, psx?.financial_series?.net_income || []),
    toChartSeries('Earnings Per Share (EPS)', years, psx?.financial_series?.eps || [])
  ].filter(Boolean);

  return {
    symbol: ticker,
    company_name: companyName,
    industry,
    profile: {
      business_description: psx?.profile?.business_description || null,
      website: psx?.website || null,
      fiscal_year_end: psx?.fiscal_year_end || null
    },
    market: {
      close: ta?.market?.close ?? psx?.close ?? latest.close ?? null,
      prev_close: ta?.market?.prev_close ?? psx?.ldcp ?? null,
      open: ta?.market?.open ?? psx?.open ?? latest.open ?? null,
      high: psx?.high ?? latest.high ?? null,
      low: psx?.low ?? latest.low ?? null,
      volume: ta?.market?.volume ?? psx?.volume ?? latest.volume ?? null,
      day_change: ta?.market?.day_change ?? psx?.change ?? latest.change ?? null,
      day_change_pct: ta?.market?.day_change_pct ?? psx?.change_pct ?? latest.change_pct ?? null,
      ytd_return_pct: ta?.market?.ytd_return_pct ?? psx?.ytd_change_pct ?? null,
      day_range: ta?.market?.day_range ?? psx?.day_range ?? null,
      year_52_range: ta?.market?.year_52_range ?? psx?.year_range ?? null,
      as_of: psx?.as_of || null,
      source: {
        price: ta ? 'ticker_analysts' : (psx ? 'psx_company_page' : 'db'),
        stats: psx ? 'psx_company_page' : (ta ? 'ticker_analysts' : 'db')
      }
    },
    financial_highlights: financialHighlights,
    financial_charts: chartCandidates,
    fetched_at: new Date().toISOString()
  };
}

// Refresh latest prices for a list of symbols by scraping performers data (best-effort).
// This will insert or replace rows into the `stocks` table for today's date using
// the data we can find on the performers page. Returns a summary of updated symbols.
export async function refreshPricesForSymbols(symbols = []) {
  if (!Array.isArray(symbols) || symbols.length === 0) return { updated: 0, symbols: [] };

  // normalize symbols to uppercase set
  const wanted = new Set(buildFocusCandidates(symbols));
  if (!wanted.size) return { updated: 0, symbols: [] };

  const payload = await getMarketPerformers(true).catch(() => null);
  const bySymbolFromPerformers = new Map();
  if (payload?.performers) {
    const allRows = [];
    ['top_active_stocks', 'top_advancers', 'top_decliners'].forEach((k) => {
      const rows = payload.performers[k] || [];
      rows.forEach((r) => allRows.push(r));
    });
    allRows.forEach((r) => {
      if (!r || !r.symbol) return;
      bySymbolFromPerformers.set(String(r.symbol).toUpperCase(), r);
    });
  }

  const updatedSymbols = [];
  const skippedSymbols = [];
  const insertSql = db.prepare(`
    INSERT OR REPLACE INTO stocks (
      symbol, ldcp, open, high, low, close, change, change_pct, volume, date, timestamp
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, date('now'), datetime('now')
    )
  `);

  for (const s of Array.from(wanted)) {
    let row = null;

    // Primary source: company page for full per-symbol detail
    const companyCandidates = [s, s.includes('-') ? s.split('-')[0] : null].filter(Boolean);
    for (const candidate of companyCandidates) {
      try {
        const detail = await scrapeCompanyPage(candidate);
        if (detail?.close != null) {
          row = {
            symbol: s,
            ldcp: detail.ldcp ?? null,
            open: detail.open ?? null,
            high: detail.high ?? null,
            low: detail.low ?? null,
            close: detail.close ?? null,
            change: detail.change ?? null,
            change_pct: detail.change_pct ?? null,
            volume: detail.volume ?? null
          };
          break;
        }
      } catch {
        // try next candidate
      }

      if (!row) {
        try {
          const ta = await scrapeTickerAnalystsPage(candidate);
          if (ta?.market?.close != null) {
            row = {
              symbol: s,
              ldcp: ta.market.prev_close ?? null,
              open: ta.market.open ?? null,
              high: ta.market.day_range?.high ?? null,
              low: ta.market.day_range?.low ?? null,
              close: ta.market.close ?? null,
              change: ta.market.day_change ?? null,
              change_pct: ta.market.day_change_pct ?? null,
              volume: ta.market.volume ?? null
            };
            break;
          }
        } catch {
          // fallback to performers
        }
      }
    }

    // Fallback source: performers page (limited subset)
    if (!row) {
      const perf = bySymbolFromPerformers.get(s);
      if (perf) {
        row = {
          symbol: s,
          ldcp: null,
          open: null,
          high: null,
          low: null,
          close: perf.price ?? null,
          change: perf.change ?? null,
          change_pct: perf.change_pct ?? null,
          volume: perf.volume ?? null
        };
      }
    }

    if (!row) {
      skippedSymbols.push(s);
      continue;
    }

    try {
      insertSql.run(
        s,
        row.ldcp ?? null,
        row.open ?? null,
        row.high ?? null,
        row.low ?? null,
        row.close ?? null,
        row.change ?? null,
        row.change_pct ?? null,
        row.volume ?? null
      );
      updatedSymbols.push(s);
    } catch (err) {
      skippedSymbols.push(s);
      continue;
    }
  }

  return {
    updated: updatedSymbols.length,
    symbols: updatedSymbols,
    skipped: skippedSymbols.length,
    skipped_symbols: skippedSymbols
  };
}
