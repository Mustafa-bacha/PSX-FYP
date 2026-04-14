import axios from 'axios';
import * as cheerio from 'cheerio';

const URL_TEMPLATE = 'https://dps.psx.com.pk/company/';

function parseFloatSafe(text) {
  const match = String(text ?? '').replaceAll(',', '').match(/[-+]?\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
}

function parseChange(text) {
  const nums = String(text ?? '').replaceAll(',', '').match(/[-+]?\d+(?:\.\d+)?/g) || [];
  return {
    change: nums[0] ? Number(nums[0]) : null,
    change_pct: nums[1] ? Number(nums[1]) : null
  };
}

function parseRange(text) {
  const nums = String(text ?? '').replaceAll(',', '').match(/\d+(?:\.\d+)?/g) || [];
  return {
    low: nums[0] ? Number(nums[0]) : null,
    high: nums[1] ? Number(nums[1]) : null
  };
}

function normalizeLabel(text) {
  return String(text || '')
    .replace(/[\^*]/g, '')
    .trim()
    .toUpperCase();
}

function normalizeWhitespace(text) {
  return String(text || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractFinancialYears(text) {
  const years = Array.from(new Set((String(text || '').match(/FY\s*20\d{2}/gi) || []).map((y) => y.toUpperCase())));
  return years.slice(-5);
}

function extractSeriesByLabel(text, label, maxItems = 5) {
  const normalized = normalizeWhitespace(text);
  const idx = normalized.toUpperCase().indexOf(label.toUpperCase());
  if (idx < 0) return [];

  const chunk = normalized.slice(idx, idx + 220);
  const matches = chunk.match(/\(?[-+]?\d[\d,]*(?:\.\d+)?\)?/g) || [];
  return matches
    .map((n) => parseFloatSafe(n))
    .filter((n) => Number.isFinite(n))
    .slice(0, maxItems);
}

export async function scrapeCompanyPage(symbol) {
  const upperSymbol = String(symbol).toUpperCase().trim();
  if (!upperSymbol) throw new Error('Symbol is required');

  const res = await axios.get(`${URL_TEMPLATE}${upperSymbol}`, {
    timeout: 30000,
    headers: { 'User-Agent': 'Mozilla/5.0' }
  });
  const $ = cheerio.load(res.data);
  const pageText = normalizeWhitespace($('body').text());

  const data = {};
  const quoteDetail = $('.company__quote');

  data.symbol = upperSymbol;
  data.company_name = quoteDetail.find('.quote__name').first().clone().children().remove().end().text().trim();
  data.sector = quoteDetail.find('.quote__sector').first().text().trim();

  const priceText = quoteDetail.find('.quote__close').first().text();
  const changeText = [
    quoteDetail.find('.change__value').first().text(),
    quoteDetail.find('.change__percent').first().text()
  ].join(' ').trim();
  data.close = parseFloatSafe(priceText);
  const { change, change_pct } = parseChange(changeText);
  data.change = change;
  data.change_pct = change_pct;

  const asOfText = quoteDetail.find('.chart__timing').first().text().trim();
  data.as_of = asOfText || null;

  const stats = {};
  $('.stats_item').each((_, el) => {
    const label = normalizeLabel($(el).find('.stats_label').first().text());
    const value = $(el).find('.stats_value').first().clone().children().remove().end().text().trim();
    if (label) stats[label] = value;
  });

  data.open = parseFloatSafe(stats.OPEN);
  data.high = parseFloatSafe(stats.HIGH);
  data.low = parseFloatSafe(stats.LOW);
  data.volume = parseFloatSafe(stats.VOLUME);

  data.circuit_breaker = parseRange(stats['CIRCUIT BREAKER']);
  data.day_range = parseRange(stats['DAY RANGE']);
  data.year_range = parseRange(stats['52-WEEK RANGE']);

  data.ldcp = parseFloatSafe(stats.LDCP);
  data.var = parseFloatSafe(stats.VAR);
  data.haircut = parseFloatSafe(stats.HAIRCUT);
  data.pe_ratio = parseFloatSafe(stats['P/E RATIO (TTM)']);
  data.year_change_pct = parseFloatSafe(stats['1-YEAR CHANGE']);
  data.ytd_change_pct = parseFloatSafe(stats['YTD CHANGE']);

  const businessDescHeading = $('h2, h3, h4').filter((_, el) => normalizeLabel($(el).text()) === 'BUSINESS DESCRIPTION').first();
  const businessDesc = businessDescHeading.length
    ? normalizeWhitespace(businessDescHeading.parent().find('p').first().text())
    : null;

  const years = extractFinancialYears(pageText);
  const salesSeries = extractSeriesByLabel(pageText, 'Sales');
  const netIncomeSeries = extractSeriesByLabel(pageText, 'Profit after Taxation');
  const epsSeries = extractSeriesByLabel(pageText, 'EPS');

  const inferredYears = years.length
    ? years.slice(-Math.max(salesSeries.length, netIncomeSeries.length, epsSeries.length))
    : [];

  return {
    source: 'psx_company_page',
    fetched_at: new Date().toISOString(),
    profile: {
      business_description: businessDesc || null
    },
    financial_series: {
      years: inferredYears,
      revenue: salesSeries,
      net_income: netIncomeSeries,
      eps: epsSeries
    },
    ...data
  };
}
