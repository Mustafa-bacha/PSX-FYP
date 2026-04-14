import fs from 'node:fs';
import path from 'node:path';
import axios from 'axios';
import * as cheerio from 'cheerio';
import { config } from '../config.js';

const URL = 'https://dps.psx.com.pk/performers';
const TITLE_MAP = {
  'TOP ACTIVE STOCKS': 'top_active_stocks',
  'TOP ADVANCERS': 'top_advancers',
  'TOP DECLINERS': 'top_decliners'
};

function parseFloatSafe(text) {
  const match = String(text ?? '').replaceAll(',', '').match(/[-+]?\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
}

function parseIntSafe(text) {
  const match = String(text ?? '').replaceAll(',', '').match(/\d+/);
  return match ? Number(match[0]) : null;
}

function parseChange(text) {
  const nums = String(text ?? '').replaceAll(',', '').match(/[-+]?\d+(?:\.\d+)?/g) || [];
  return {
    change: nums[0] ? Number(nums[0]) : null,
    change_pct: nums[1] ? Number(nums[1]) : null
  };
}

function readCache() {
  try {
    if (!fs.existsSync(config.performersCachePath)) return null;
    return JSON.parse(fs.readFileSync(config.performersCachePath, 'utf-8'));
  } catch {
    return null;
  }
}

function writeCache(payload) {
  fs.mkdirSync(path.dirname(config.performersCachePath), { recursive: true });
  fs.writeFileSync(config.performersCachePath, JSON.stringify(payload), 'utf-8');
}

function cacheFresh(payload) {
  if (!payload?.fetched_at) return false;
  const ts = new Date(payload.fetched_at).getTime();
  if (!Number.isFinite(ts)) return false;
  return Date.now() - ts <= config.performersCacheMinutes * 60 * 1000;
}

function cacheComplete(payload) {
  const p = payload?.performers || {};
  return [
    p.top_active_stocks,
    p.top_advancers,
    p.top_decliners
  ].every((rows) => Array.isArray(rows) && rows.length > 0);
}

async function scrapePerformers() {
  const res = await axios.get(URL, { timeout: 30000, headers: { 'User-Agent': 'Mozilla/5.0' } });
  const $ = cheerio.load(res.data);
  const performers = {
    top_active_stocks: [],
    top_advancers: [],
    top_decliners: []
  };

  const parseRowsFromTable = (table) => {
    const rows = [];
    table.find('tbody tr').each((__, row) => {
      const cols = $(row).find('td');
      if (cols.length < 4) return;

      const symbol = $(cols[0]).find('strong').first().text().trim().toUpperCase()
        || $(cols[0]).text().replace(/\s+/g, ' ').trim().toUpperCase();
      const price = parseFloatSafe($(cols[1]).text());
      const changeText = $(cols[2]).text().replace(/\s+/g, ' ').trim();
      const { change, change_pct } = parseChange(changeText);
      const volume = parseIntSafe($(cols[3]).text());

      rows.push({ symbol, price, change, change_pct, volume, raw_change: changeText });
    });
    return rows.slice(0, 10);
  };

  // Primary strategy: use table order (DPS markup can be malformed around wrappers/headings).
  const orderedKeys = ['top_active_stocks', 'top_advancers', 'top_decliners'];
  const dataTables = $('table.tbl').toArray()
    .map((el) => $(el))
    .filter((table) => table.find('tbody tr').length > 0);

  orderedKeys.forEach((key, idx) => {
    if (!performers[key]?.length && dataTables[idx]) {
      performers[key] = parseRowsFromTable(dataTables[idx]);
    }
  });

  // Fallback strategy: heading-based lookup (kept for compatibility when DOM is well-structured).
  $('h3.marketPerf__heading').each((_, el) => {
    const title = $(el).text().replace(/\s+/g, ' ').trim().toUpperCase();
    const key = TITLE_MAP[title];
    if (!key) return;
    if (performers[key]?.length) return;

    const table = $(el).nextAll('table').first();
    performers[key] = parseRowsFromTable(table);
  });

  return {
    source: 'psx',
    url: URL,
    fetched_at: new Date().toISOString(),
    cache_status: 'miss',
    performers
  };
}

export async function getMarketPerformers(forceRefresh = false) {
  const cached = readCache();
  if (!forceRefresh && cached && cacheFresh(cached) && cacheComplete(cached)) {
    return { ...cached, cache_status: 'hit' };
  }

  try {
    const payload = await scrapePerformers();
    writeCache(payload);
    return payload;
  } catch (err) {
    if (cached) {
      return { ...cached, cache_status: 'stale_fallback', warning: String(err?.message || err) };
    }
    throw err;
  }
}
