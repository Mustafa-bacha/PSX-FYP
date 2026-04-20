const API_BASE = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:5001/api';

async function api(path, { method = 'GET', body, token } = {}) {
  const headers = {};
  if (body != null) headers['Content-Type'] = 'application/json';
  if (token) headers.Authorization = `Bearer ${token}`;

  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers,
    body: body == null ? undefined : JSON.stringify(body)
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `Request failed: ${res.status}`);
  return data;
}

export const apiClient = {
  health: () => api('/health'),
  stocks: () => api('/stocks'),
  stock: (symbol, days = 365, refresh = false) => api(`/stock/${encodeURIComponent(symbol)}?days=${days}${refresh ? '&refresh=true' : ''}`),
  marketSummary: (days = 365, refresh = false, indexCode = 'KSE100') => api(`/market-summary?index=${encodeURIComponent(indexCode)}&days=${Math.max(7, Number(days || 365))}${refresh ? '&refresh=true' : ''}`),
  stockInsights: (symbol) => api(`/stock/${encodeURIComponent(symbol)}/insights`),
  stockOverview: (symbol, refresh = false) => api(`/stock/${encodeURIComponent(symbol)}/overview${refresh ? '?refresh=true' : ''}`),
  performers: (refresh = false) => api(`/market-performers${refresh ? '?refresh=true' : ''}`),
  dailyNews: (limit = 8, refresh = false) => api(`/news/daily?limit=${Math.max(1, Number(limit || 8))}${refresh ? '&refresh=true' : ''}`),
  symbolNews: (symbol, limit = 12, refresh = false) => api(`/news/symbol/${encodeURIComponent(symbol)}?limit=${Math.max(1, Number(limit || 12))}${refresh ? '&refresh=true' : ''}`),
  refreshPrices: (symbols = []) => api('/refresh/prices', { method: 'POST', body: { symbols } }),
  syncStatus: () => api('/sync/status'),
  syncHistorical: (startDate = null) => api('/sync/historical', {
    method: 'POST',
    body: startDate ? { start_date: startDate } : {}
  }),
  focusSymbols: () => api('/focus-symbols'),
  warmupModels: (limit = 50) => api('/models/warmup', { method: 'POST', body: { limit } }),
  appendAndRetrainModels: ({
    max_symbols = 120,
    min_rows = 120,
    epochs = 20,
    variant = 'lstm'
  } = {}) => api('/models/append-retrain', {
    method: 'POST',
    body: {
      max_symbols,
      min_rows,
      epochs,
      variant
    }
  }),
  focusRefreshStatus: () => api('/refresh/focus/status'),
  focusRefreshStart: (intervalSec = null, batchSize = null) => api('/refresh/focus/start', {
    method: 'POST',
    body: {
      ...(intervalSec != null ? { interval_sec: intervalSec } : {}),
      ...(batchSize != null ? { batch_size: batchSize } : {})
    }
  }),
  focusRefreshStop: () => api('/refresh/focus/stop', { method: 'POST' }),
  focusRefreshRunNow: () => api('/refresh/focus/run-now', { method: 'POST' }),
  sentimentStatus: () => api('/sentiment/status'),
  sentimentStart: (intervalSec = null, batchSize = null) => api('/sentiment/start', {
    method: 'POST',
    body: {
      ...(intervalSec != null ? { interval_sec: intervalSec } : {}),
      ...(batchSize != null ? { batch_size: batchSize } : {})
    }
  }),
  sentimentStop: () => api('/sentiment/stop', { method: 'POST' }),
  sentimentRunNow: () => api('/sentiment/run-now', { method: 'POST' }),
  sentimentRunFull: () => api('/sentiment/run-full', { method: 'POST' }),
  chat: ({ stock, question, history = [], token }) => api('/chat', {
    method: 'POST',
    token,
    body: { stock, question, history }
  }),

  signup: ({ email, password, full_name, date_of_birth }) => api('/auth/signup', {
    method: 'POST',
    body: { email, password, full_name, date_of_birth }
  }),
  login: ({ email, password }) => api('/auth/login', {
    method: 'POST',
    body: { email, username: email, password }
  }),
  me: (token) => api('/auth/me', { token }),
  watchlist: (token) => api('/user/watchlist', { token }),
  addWatchlist: (symbol, token) => api('/user/watchlist', {
    method: 'POST',
    token,
    body: { symbol }
  }),
  removeWatchlist: (symbol, token) => api(`/user/watchlist/${encodeURIComponent(symbol)}`, {
    method: 'DELETE',
    token
  }),
  simPortfolio: (token) => api('/user/sim/portfolio', { token }),
  simTrades: (token, limit = 30) => api(`/user/sim/trades?limit=${limit}`, { token }),
  simOrders: (token, limit = 30, status = null) => api(`/user/sim/orders?limit=${limit}${status ? `&status=${encodeURIComponent(status)}` : ''}`, { token }),
  simPlaceOrder: ({ symbol, side, order_type = 'MARKET', quantity, limit_price = null, token }) => api('/user/sim/orders', {
    method: 'POST',
    token,
    body: {
      symbol,
      side,
      order_type,
      quantity,
      ...(limit_price != null ? { limit_price } : {})
    }
  }),
  simCancelOrder: (orderId, token) => api(`/user/sim/orders/${encodeURIComponent(orderId)}`, {
    method: 'DELETE',
    token
  }),
  simLeaderboard: (token, limit = 20) => api(`/sim/leaderboard?limit=${limit}`, { token }),
  simBuy: (symbol, quantity, token) => api('/user/sim/buy', {
    method: 'POST',
    token,
    body: { symbol, quantity }
  }),
  simSell: (symbol, quantity, token) => api('/user/sim/sell', {
    method: 'POST',
    token,
    body: { symbol, quantity }
  }),
  simReset: (token) => api('/user/sim/reset', {
    method: 'POST',
    token
  }),
  chatHistory: (symbol, token, limit = 20) => api(`/user/chat-history/${encodeURIComponent(symbol || 'MARKET')}?limit=${limit}`, { token }),
  clearChatHistory: (symbol, token) => api(`/user/chat-history/${encodeURIComponent(symbol || 'MARKET')}`, {
    method: 'DELETE',
    token
  }),
  googleStartUrl: () => `${API_BASE}/auth/google/start`
};
