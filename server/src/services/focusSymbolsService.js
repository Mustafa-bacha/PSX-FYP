import fs from 'node:fs';
import path from 'node:path';
import xlsx from 'xlsx';
import { config } from '../config.js';

function normalizeSymbol(sym) {
  return String(sym || '')
    .trim()
    .toUpperCase()
    .replace(/\s+/g, '');
}

function normalizeRowSymbol(sym) {
  const s = normalizeSymbol(sym);
  if (!s) return null;
  return s;
}

function collectSymbolsFromSheet(sheet) {
  const rows = xlsx.utils.sheet_to_json(sheet, { defval: null });
  if (!rows.length) return { symbols: [], profiles: [] };

  const first = rows[0] || {};
  const keys = Object.keys(first);
  const symbolKey = keys.find((k) => String(k).trim().toUpperCase() === 'SYMBOL') || keys[0];
  const companyKey = keys.find((k) => String(k).trim().toUpperCase() === 'COMPANY') || null;

  const out = [];
  const profiles = [];
  for (const row of rows) {
    const symbol = normalizeRowSymbol(row[symbolKey]);
    if (symbol) {
      out.push(symbol);
      profiles.push({
        symbol,
        company: String(companyKey ? (row[companyKey] || '') : '').trim()
      });
    }
  }

  const symbols = Array.from(new Set(out));
  const dedupProfiles = [];
  const seen = new Set();
  for (const p of profiles) {
    if (seen.has(p.symbol)) continue;
    seen.add(p.symbol);
    dedupProfiles.push(p);
  }

  return { symbols, profiles: dedupProfiles };
}

function readFromWorkbook(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    return { symbols: [], source: 'missing', path: filePath || null };
  }

  const wb = xlsx.readFile(filePath, { cellDates: false });
  const all = [];
  const profiles = [];
  for (const name of wb.SheetNames) {
    const sheet = wb.Sheets[name];
    const payload = collectSymbolsFromSheet(sheet);
    all.push(...payload.symbols);
    profiles.push(...payload.profiles);
  }

  const symbols = Array.from(new Set(all));
  const profilesBySymbol = new Map();
  for (const p of profiles) {
    if (!profilesBySymbol.has(p.symbol)) profilesBySymbol.set(p.symbol, p.company || '');
  }

  return {
    symbols,
    profiles: symbols.map((s) => ({ symbol: s, company: profilesBySymbol.get(s) || '' })),
    source: 'xlsx',
    path: filePath,
    sheets: wb.SheetNames,
    count: symbols.length
  };
}

let cache = { at: 0, payload: null };

export function getFocusSymbols({ force = false } = {}) {
  const now = Date.now();
  if (!force && cache.payload && now - cache.at < 60_000) {
    return cache.payload;
  }

  const payload = readFromWorkbook(config.focusSymbolsXlsxPath);
  cache = { at: now, payload };
  return payload;
}

export function mapToBaseSymbol(symbol) {
  const sym = normalizeSymbol(symbol);
  if (!sym) return '';
  return sym.includes('-') ? sym.split('-')[0] : sym;
}

export function buildFocusCandidates(inputSymbols = []) {
  const set = new Set();
  for (const raw of inputSymbols) {
    const s = normalizeSymbol(raw);
    if (!s) continue;
    set.add(s);
    set.add(mapToBaseSymbol(s));
  }
  return Array.from(set).filter(Boolean);
}
