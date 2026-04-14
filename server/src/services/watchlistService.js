import { db } from '../lib/db.js';

function normalizeSymbol(symbol) {
  return String(symbol || '').trim().toUpperCase();
}

function symbolExists(symbol) {
  const row = db.prepare('SELECT 1 AS ok FROM stocks WHERE symbol = ? LIMIT 1').get(symbol);
  return !!row;
}

export function listUserWatchlist(userId) {
  return db.prepare(`
    SELECT stock_symbol, created_at
    FROM user_watchlist
    WHERE user_id = ?
    ORDER BY created_at DESC
  `).all(Number(userId)).map((r) => ({
    symbol: r.stock_symbol,
    created_at: r.created_at
  }));
}

export function addUserWatchlistSymbol(userId, symbol) {
  const ticker = normalizeSymbol(symbol);
  if (!ticker) throw new Error('Symbol is required');
  if (!symbolExists(ticker)) throw new Error('Symbol not found');

  db.prepare(`
    INSERT OR IGNORE INTO user_watchlist (user_id, stock_symbol)
    VALUES (?, ?)
  `).run(Number(userId), ticker);

  return ticker;
}

export function removeUserWatchlistSymbol(userId, symbol) {
  const ticker = normalizeSymbol(symbol);
  if (!ticker) throw new Error('Symbol is required');

  db.prepare(`
    DELETE FROM user_watchlist
    WHERE user_id = ? AND stock_symbol = ?
  `).run(Number(userId), ticker);

  return ticker;
}
