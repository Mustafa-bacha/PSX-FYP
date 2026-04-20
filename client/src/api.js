import { apiClient } from './api/client.js';

const TOKEN_KEYS = ['token', 'psx_auth_token'];

export function getToken() {
  for (const key of TOKEN_KEYS) {
    const value = localStorage.getItem(key);
    if (value) return value;
  }
  return '';
}

const enc = (s) => encodeURIComponent(s || '');

function saveToken(token) {
  if (!token) {
    TOKEN_KEYS.forEach((k) => localStorage.removeItem(k));
    return;
  }
  TOKEN_KEYS.forEach((k) => localStorage.setItem(k, token));
}

function normalizeDirection(direction) {
  const val = String(direction || '').toUpperCase();
  if (val === 'UP') return 'Up';
  if (val === 'DOWN') return 'Down';
  return 'Neutral';
}

function normalizeConfidence(confidence) {
  const n = Number(confidence ?? 0);
  return n <= 1 ? Math.round(n * 100) : Math.round(n);
}

function sentimentLabel(score) {
  const n = Number(score || 0);
  if (n > 0.08) return 'Positive';
  if (n < -0.08) return 'Negative';
  return 'Neutral';
}

function toPlainTextSummary(value) {
  const input = String(value || '');
  if (!input) return '';

  return input
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<\s*br\s*\/?>/gi, '\n')
    .replace(/<\/p\s*>/gi, '\n\n')
    .replace(/<\/div\s*>/gi, '\n')
    .replace(/<\/li\s*>/gi, '\n')
    .replace(/<li[^>]*>/gi, '• ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/\r/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n[ \t]+/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function mapHeadline(item = {}, fallbackSymbol = '') {
  const score = Number(item.score ?? 0);
  const ts = item.time || item.analyzed_at || item.pubDate || item.published_at || '';
  return {
    symbol: item.symbol || fallbackSymbol,
    sentiment: item.label ? `${item.label[0]}${item.label.slice(1).toLowerCase()}` : sentimentLabel(score),
    score: Number.isFinite(score) ? score.toFixed(2) : '0.00',
    headline: item.headline || item.title || 'Untitled',
    summary: toPlainTextSummary(item.summary || item.description || ''),
    source: item.source || 'news',
    time: ts,
    link: item.link || item.url || ''
  };
}

export const auth = {
  async login(email, password) {
    const payload = await apiClient.login({ email, password });
    const token = payload?.token || payload?.access_token || '';
    if (token) saveToken(token);
    return {
      user: payload?.user || { email },
      access_token: token
    };
  },
  async register(email, password, profile = {}) {
    return apiClient.signup({
      email,
      password,
      full_name: String(profile?.full_name || '').trim(),
      date_of_birth: profile?.date_of_birth || null,
    });
  },
  googleStartUrl() {
    return apiClient.googleStartUrl();
  },
};

export const stocks = {
  async list(q = '', limit = 5000) {
    const payload = await apiClient.stocks();
    const snapshots = Array.isArray(payload?.snapshots) ? payload.snapshots : [];
    const term = String(q || '').trim().toLowerCase();
    const rows = snapshots.map((s) => ({
      symbol: s.symbol,
      name: s.name || s.symbol,
      close: Number.isFinite(Number(s.close)) ? Number(s.close) : null,
      change_pct: Number.isFinite(Number(s.change_pct)) ? Number(s.change_pct) : null,
    }));
    const filtered = term
      ? rows.filter((r) => r.symbol.toLowerCase().includes(term) || (r.name || '').toLowerCase().includes(term))
      : rows;
    const max = Number(limit);
    return { stocks: Number.isFinite(max) && max > 0 ? filtered.slice(0, max) : filtered };
  },

  async get(symbol) {
    const payload = await apiClient.stock(symbol, 365);
    return {
      symbol: payload?.symbol || String(symbol || '').toUpperCase(),
      name: payload?.company_name || payload?.name || String(symbol || '').toUpperCase(),
      industry: payload?.industry || null,
    };
  },

  async overview(symbol, refresh = false) {
    const payload = await apiClient.stockOverview(symbol, refresh);
    return {
      symbol: payload?.symbol || String(symbol || '').toUpperCase(),
      company_name: payload?.company_name || String(symbol || '').toUpperCase(),
      industry: payload?.industry || null,
      market: payload?.market || {},
      profile: payload?.profile || {},
      financial_highlights: Array.isArray(payload?.financial_highlights) ? payload.financial_highlights : [],
      financial_charts: Array.isArray(payload?.financial_charts) ? payload.financial_charts : [],
      fetched_at: payload?.fetched_at || null,
    };
  },

  async refreshSymbol(symbol) {
    return apiClient.refreshPrices([String(symbol || '').toUpperCase()]);
  },

  async prediction(symbol) {
    const detail = await apiClient.stockInsights(symbol);
    const p = detail?.prediction || {};
    return {
      direction: normalizeDirection(p.predicted_direction || p.direction),
      confidence: normalizeConfidence(p.confidence),
      predicted_price: p.predicted_price,
      current_price: p.current_price,
    };
  },

  async sentiment(symbol) {
    const detail = await apiClient.stockInsights(symbol);
    const s = detail?.sentiment || {};
    const score = Number(s.average_score || 0);
    return {
      sentiment: sentimentLabel(score),
      score: Number(score.toFixed(2)),
    };
  },

  async recommendation(symbol) {
    const detail = await apiClient.stockInsights(symbol);
    const prediction = detail?.prediction || {};
    const sentiment = detail?.sentiment || {};
    const explicit = detail?.recommendation || detail?.action || '';

    const predictedDirection = normalizeDirection(prediction.predicted_direction || prediction.direction);
    const sentimentScore = Number(sentiment.average_score || 0);

    let recommendation = explicit;
    if (!recommendation) {
      if (predictedDirection === 'Up' && sentimentScore >= 0) recommendation = 'Buy';
      else if (predictedDirection === 'Down' && sentimentScore <= 0) recommendation = 'Sell';
      else recommendation = 'Hold';
    }

    return {
      recommendation,
      confidence: normalizeConfidence(prediction.confidence),
      prediction_direction: predictedDirection,
      reasoning: detail?.reasoning || 'Based on model direction and sentiment balance.'
    };
  },

  async chart(symbol, days = 30, refresh = false) {
    const payload = await apiClient.stock(symbol, days, refresh);
    return { data: Array.isArray(payload?.data) ? payload.data : [] };
  },

  async news(symbol, limit = 5, refresh = false) {
    const payload = await apiClient.symbolNews(symbol, limit, refresh);
    const items = Array.isArray(payload?.items) ? payload.items : [];
    return { news: items.slice(0, limit).map((h) => mapHeadline(h, symbol)) };
  },

  async sentimentFeed(limit = 30, refresh = false) {
    const payload = await apiClient.dailyNews(limit, refresh);
    const items = Array.isArray(payload?.items) ? payload.items : [];
    return { news: items.map((item) => mapHeadline(item, item.symbol || 'MARKET')) };
  },
};

export const watchlist = {
  async get() {
    const token = getToken();
    const payload = await apiClient.watchlist(token);
    const items = Array.isArray(payload?.items) ? payload.items : [];
    return { watchlist: items.map((i) => ({ symbol: i.symbol, name: i.symbol })) };
  },
  async add(symbol) {
    const token = getToken();
    const payload = await apiClient.addWatchlist(enc(symbol), token);
    const items = Array.isArray(payload?.items) ? payload.items : [];
    return { watchlist: items.map((i) => ({ symbol: i.symbol, name: i.symbol })) };
  },
  async remove(symbol) {
    const token = getToken();
    const payload = await apiClient.removeWatchlist(enc(symbol), token);
    const items = Array.isArray(payload?.items) ? payload.items : [];
    return { watchlist: items.map((i) => ({ symbol: i.symbol, name: i.symbol })) };
  },
};

export const chatbot = {
  async ask(query, { stock = 'MARKET', history = [] } = {}) {
    let questionText = query;
    if (typeof questionText !== 'string') {
      if (questionText && typeof questionText === 'object') {
        questionText = questionText.text ?? questionText.content ?? questionText.message ?? '';
      }
      questionText = String(questionText ?? '').trim();
    }
    const token = getToken() || undefined;
    const payload = await apiClient.chat({
      stock,
      question: questionText,
      history,
      token,
    });
    return {
      answer: payload?.answer || 'No answer generated.',
      sentiment: payload?.sentiment || 'neutral',
      scope: payload?.scope || String(stock || 'MARKET').toUpperCase(),
      retrieval: payload?.retrieval || null,
      sources: Array.isArray(payload?.sources) ? payload.sources : [],
    };
  },
};

export const market = {
  async summary(days = 365, refresh = false, indexCode = 'KSE100') {
    const payload = await apiClient.marketSummary(days, refresh, indexCode);
    return {
      index_value: payload?.index_value ?? null,
      change: payload?.change ?? null,
      change_percent: payload?.change_percent ?? null,
      volume: payload?.volume ?? null,
      high: payload?.high ?? null,
      low: payload?.low ?? null,
      previous_close: payload?.previous_close ?? null,
      day_range: payload?.day_range || null,
      as_of: payload?.as_of || null,
      history: Array.isArray(payload?.history) ? payload.history : [],
      source: payload?.source || 'dps.psx.com.pk',
      cache_status: payload?.cache_status || null,
      fetched_at: payload?.fetched_at || null,
      date: payload?.date || null,
      index_code: payload?.index_code || indexCode,
      warning: payload?.warning || null,
      message: payload?.message || null
    };
  },
};
