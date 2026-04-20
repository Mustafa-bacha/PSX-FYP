import { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { stocks as stocksApi } from '../api.js';
import { cardClass } from '../lib/constants.js';

const CACHE_KEY_PREFIX = 'sentiment_news_cache_';
const CACHE_EXPIRY_MS = 10 * 60 * 1000;

function SentimentBadge({ sentiment, score }) {
  const colors =
    sentiment === 'Positive'
      ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/40'
      : sentiment === 'Negative'
        ? 'bg-red-500/20 text-red-400 border-red-500/40'
        : 'bg-slate-500/20 text-slate-400 border-slate-500/40';
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-xs font-medium border ${colors}`}>
      {sentiment} <span className="opacity-80">({score})</span>
    </span>
  );
}

function getCacheKey(symbol) {
  return `${CACHE_KEY_PREFIX}${symbol}`;
}

function getCachedNews(symbol) {
  try {
    const cacheKey = getCacheKey(symbol);
    const cached = localStorage.getItem(cacheKey);
    if (!cached) return null;

    const { data, timestamp } = JSON.parse(cached);
    const now = Date.now();
    if (now - timestamp < CACHE_EXPIRY_MS) {
      return { data, timestamp };
    }
    localStorage.removeItem(cacheKey);
    return null;
  } catch {
    return null;
  }
}

function setCachedNews(symbol, newsData) {
  try {
    const cacheKey = getCacheKey(symbol);
    const cacheValue = {
      data: newsData,
      timestamp: Date.now(),
    };
    localStorage.setItem(cacheKey, JSON.stringify(cacheValue));
  } catch {
    // ignore
  }
}

export default function SentimentAnalysis() {
  const initialCached = getCachedNews('all');
  const [stocks, setStocks] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState('all');
  const [news, setNews] = useState(() => initialCached?.data || []);
  const [lastUpdatedAt, setLastUpdatedAt] = useState(() => initialCached?.timestamp || null);
  const [loading, setLoading] = useState(() => !initialCached);
  const [error, setError] = useState('');
  const [loadingStocks, setLoadingStocks] = useState(true);

  const loadStocks = useCallback(async () => {
    setLoadingStocks(true);
    try {
      const res = await stocksApi.list();
      setStocks(res.stocks || []);
      if (!res.stocks?.length) setSelectedSymbol('all');
    } catch {
      setStocks([]);
    } finally {
      setLoadingStocks(false);
    }
  }, []);

  const loadNews = useCallback(async (forceRefresh = false) => {
    if (!forceRefresh) {
      const cachedNews = getCachedNews(selectedSymbol);
      if (cachedNews) {
        setNews(cachedNews.data || []);
        setLastUpdatedAt(cachedNews.timestamp || null);
        setLoading(false);
        setError('');
        return;
      }
    }

    setLoading(true);
    setError('');
    try {
      let res;
      if (selectedSymbol === 'all') {
        res = await stocksApi.sentimentFeed(40, forceRefresh);
      } else {
        res = await stocksApi.news(selectedSymbol, 20);
      }
      const newsData = res.news || [];
      setNews(newsData);
      setCachedNews(selectedSymbol, newsData);
      setLastUpdatedAt(Date.now());
    } catch (e) {
      setError(e.error || e.message || 'Failed to load news');
      setNews([]);
    } finally {
      setLoading(false);
    }
  }, [selectedSymbol]);

  useEffect(() => {
    loadStocks();
  }, [loadStocks]);

  useEffect(() => {
    loadNews();
  }, [loadNews]);

  return (
    <div className="max-w-7xl mx-auto px-4 py-8">
      <section className={`${cardClass} p-5 border-slate-600/50`}>
        <h1 className="text-2xl font-bold text-white mb-2">Daily Pakistan Business News</h1>
        <p className="text-slate-400 text-sm mb-4">
          Freshly scraped business headlines from Pakistan sources with quick sentiment context.
        </p>

        <div className="flex flex-wrap items-center gap-3">
          <span className="text-slate-400 text-sm">
            Updated:{' '}
            <span className="text-slate-200">
              {lastUpdatedAt ? new Date(lastUpdatedAt).toLocaleString() : '—'}
            </span>
          </span>

          <label className="text-slate-400 text-sm">Feed:</label>
          <select
            value={selectedSymbol}
            onChange={(e) => setSelectedSymbol(e.target.value)}
            className="bg-slate-800 border border-slate-600 rounded-lg px-3 py-2 text-slate-200 text-sm focus:ring-2 focus:ring-brand-500 focus:border-brand-500"
            disabled={loadingStocks}
          >
            <option value="all">All</option>
            {(stocks || []).map((s) => (
              <option key={s.symbol} value={s.symbol}>
                {s.symbol} – {s.name}
              </option>
            ))}
          </select>

          <button
            type="button"
            onClick={() => loadNews(true)}
            disabled={loading}
            className="bg-slate-700/80 hover:bg-slate-600 border border-slate-500/60 disabled:opacity-50 text-white px-4 py-2 rounded-lg text-sm font-medium"
          >
            {loading ? 'Loading…' : 'Refresh'}
          </button>
        </div>

        {error && (
          <div className="mt-4 p-4 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">
            {error}
          </div>
        )}
      </section>

      {loading ? (
        <p className="text-slate-500 py-8">Loading news…</p>
      ) : news.length === 0 ? (
        <div className={`${cardClass} p-5 mt-4 border-slate-600/50`}>
          <p className="text-slate-500 text-sm">No news items for this selection.</p>
        </div>
      ) : (
        <section className="mt-4 grid grid-cols-1 xl:grid-cols-2 gap-4">
          {news.map((item, i) => (
            <NewsCard key={`${item.symbol || 'all'}-${i}`} item={item} showSymbol={selectedSymbol === 'all'} />
          ))}
        </section>
      )}
    </div>
  );
}

function NewsCard({ item, showSymbol }) {
  const sentiment = String(item.sentiment || 'Neutral');
  const score = Number(item.score);

  return (
    <article className={`${cardClass} border-slate-600/50 p-4 h-full flex flex-col`}>
      <div className="flex items-center justify-between gap-3 mb-3">
        <div className="flex flex-wrap items-center gap-2">
          {showSymbol && item.symbol && (
            <Link to={`/dashboard/${item.symbol}`} className="text-brand-400 hover:underline font-mono text-sm">
              {item.symbol}
            </Link>
          )}
          <SentimentBadge sentiment={sentiment} score={Number.isFinite(score) ? score.toFixed(2) : item.score} />
        </div>
        <span className="text-slate-400 text-xs font-medium truncate max-w-[140px] text-right">{item.source || 'Unknown source'}</span>
      </div>

      <h3 className="text-slate-100 text-xl font-bold leading-tight mb-2">
        {item.headline || 'Untitled headline'}
      </h3>

      {item.summary && (
        <p className="text-slate-300 text-sm leading-6 mb-4 flex-1 whitespace-pre-line">{item.summary}</p>
      )}

      <div className="mt-auto flex items-center justify-between gap-3">
        <span className="text-slate-500 text-xs">
          {item.time || 'Latest update'}
        </span>
      </div>
    </article>
  );
}
