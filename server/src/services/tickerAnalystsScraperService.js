import axios from 'axios';
import * as cheerio from 'cheerio';

const URL_TEMPLATE = 'https://www.tickeranalysts.com/stocks/';

function normalizeWhitespace(text) {
  return String(text || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseNumberSafe(text) {
  const raw = String(text || '').trim();
  if (!raw) return null;
  const match = raw.replace(/,/g, '').match(/[-+]?\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
}

function parseMagnitude(text) {
  const raw = normalizeWhitespace(text);
  if (!raw) return null;
  const m = raw.match(/([-+]?\d+(?:\.\d+)?)\s*([KMB])?/i);
  if (!m) return null;
  const base = Number(m[1]);
  if (!Number.isFinite(base)) return null;
  const unit = String(m[2] || '').toUpperCase();
  if (unit === 'K') return base * 1_000;
  if (unit === 'M') return base * 1_000_000;
  if (unit === 'B') return base * 1_000_000_000;
  return base;
}

function extractRegex(text, regex) {
  const m = text.match(regex);
  return m || null;
}

function parseRange(text, label) {
  const rgx = new RegExp(`${label}\\s*([-+]?\\d+(?:\\.\\d+)?)\\s+([-+]?\\d+(?:\\.\\d+)?)`, 'i');
  const m = extractRegex(text, rgx);
  if (!m) return null;
  return {
    low: Number(m[1]),
    high: Number(m[2])
  };
}

function parseSummaryMetric(text, label) {
  const rgx = new RegExp(`${label}\\s*(?:[✓✗~]\\s*)?(Good|Bad|Average)?\\s*(Rs\\.?\\s*[0-9.,]+\\s*[KMB]?|₨\\s*[0-9.,]+\\s*[KMB]?|[-+]?\\d+(?:\\.\\d+)?%|[-+]?\\d+(?:\\.\\d+)?x)`, 'i');
  const m = extractRegex(text, rgx);
  if (!m) return null;

  return {
    label,
    rating: m[1] ? String(m[1]).toLowerCase() : null,
    value: normalizeWhitespace(m[2]),
    numeric: parseMagnitude(m[2]) ?? parseNumberSafe(m[2])
  };
}

export async function scrapeTickerAnalystsPage(symbol) {
  const upperSymbol = String(symbol || '').toUpperCase().trim();
  if (!upperSymbol) throw new Error('Symbol is required');

  const res = await axios.get(`${URL_TEMPLATE}${upperSymbol}`, {
    timeout: 30000,
    headers: { 'User-Agent': 'Mozilla/5.0' }
  });

  const html = String(res.data || '');
  const $ = cheerio.load(html);
  const text = normalizeWhitespace($('body').text());

  const h1Text = normalizeWhitespace($('h1').first().text()) || normalizeWhitespace($('h2').first().text());
  const nameMatch = h1Text.match(/^(.+?)\s*\(([A-Z0-9-]+)\)\s*-/i);
  const tickerSectorMatch = text.match(/Ticker:\s*([A-Z0-9-]+)\s*•\s*Sector:\s*([^\n]+?)\s*(?:Open in|Overview|Income Statement|Balance Sheet|Cash Flow|Activities)/i);

  const prevCloseMatch = extractRegex(text, /Prev\s*Close\s*([-+]?\d+(?:\.\d+)?)/i);
  const openMatch = extractRegex(text, /Open\s*([-+]?\d+(?:\.\d+)?)/i);
  const dayChangeMatch = extractRegex(text, /Day\s*Change\s*([-+]?\d+(?:\.\d+)?)\s*([-+]?\d+(?:\.\d+)?)%/i);
  const volumeMatch = extractRegex(text, /Volume\s*([0-9]+(?:\.[0-9]+)?\s*[KMB]?)/i);
  const ytdMatch = extractRegex(text, /YTD\s*Return\s*[↗↑]?\s*([-+]?\d+(?:\.\d+)?)%/i);

  const fundamentals = [
    parseSummaryMetric(text, 'Revenue'),
    parseSummaryMetric(text, 'Net Profit'),
    parseSummaryMetric(text, 'EPS'),
    parseSummaryMetric(text, 'ROE'),
    parseSummaryMetric(text, 'P/E'),
    parseSummaryMetric(text, 'Dividend Yield')
  ].filter(Boolean);

  return {
    source: 'ticker_analysts',
    fetched_at: new Date().toISOString(),
    symbol: (tickerSectorMatch?.[1] || nameMatch?.[2] || upperSymbol).toUpperCase(),
    company_name: normalizeWhitespace(nameMatch?.[1] || '') || null,
    sector: normalizeWhitespace(tickerSectorMatch?.[2] || '') || null,
    market: {
      prev_close: prevCloseMatch ? Number(prevCloseMatch[1]) : null,
      open: openMatch ? Number(openMatch[1]) : null,
      close: parseNumberSafe(h1Text),
      day_change: dayChangeMatch ? Number(dayChangeMatch[1]) : null,
      day_change_pct: dayChangeMatch ? Number(dayChangeMatch[2]) : null,
      volume: volumeMatch ? parseMagnitude(volumeMatch[1]) : null,
      ytd_return_pct: ytdMatch ? Number(ytdMatch[1]) : null,
      day_range: parseRange(text, 'DAY RANGE'),
      year_52_range: parseRange(text, '52W RANGE')
    },
    fundamentals
  };
}
