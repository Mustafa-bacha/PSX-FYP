import { useState, useEffect, useCallback } from 'react';
import { useParams, Link } from 'react-router-dom';
import { stocks as stocksApi, watchlist as watchlistApi } from '../api.js';
import { useAuth } from '../context/AuthContext.jsx';
import { getPredictionColor, getRecommendationColor } from '../lib/utils.js';
import { cardClass } from '../lib/constants.js';
import StockChart from '../components/StockChart.jsx';
import StockSearch from '../components/StockSearch.jsx';

function formatPrice(value) {
  const n = Number(value);
  return Number.isFinite(n) ? `PKR ${n.toFixed(2)}` : '—';
}

function formatVolume(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '—';
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(2)}B`;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return `${n.toFixed(0)}`;
}

export default function StockDetail() {
  const { symbol: routeSymbol } = useParams();
  const symbol = String(routeSymbol || 'ACPL').toUpperCase();
  const { isAuthenticated } = useAuth();
  const [stock, setStock] = useState(null);
  const [prediction, setPrediction] = useState(null);
  const [sentiment, setSentiment] = useState(null);
  const [recommendation, setRecommendation] = useState(null);
  const [chartData, setChartData] = useState([]);
  const [news, setNews] = useState([]);
  const [overview, setOverview] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [refreshing, setRefreshing] = useState(false);
  const [watchlistMsg, setWatchlistMsg] = useState('');
  const [watchlistAdding, setWatchlistAdding] = useState(false);
  const [selectedDays, setSelectedDays] = useState(365);
  const [timelineMenuOpen, setTimelineMenuOpen] = useState(false);
  const [indicators, setIndicators] = useState({
    sma: true,
    ema: true,
    bollinger: false,
    rsi: false,
    macd: false,
    volume: true,
    atr: false,
  });

  const timelineOptions = [
    { label: '1W', days: 7 },
    { label: '1M', days: 30 },
    { label: '3M', days: 90 },
    { label: '6M', days: 180 },
    { label: 'YTD', days: 300 },
    { label: '1Y', days: 365 },
    { label: '2Y', days: 730 },
    { label: '5Y', days: 1825 },
  ];

  const load = useCallback(async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    else setLoading(true);
    setError('');
    try {
      if (isRefresh) {
        await stocksApi.refreshSymbol(symbol).catch(() => null);
      }

      const [s, p, sent, rec, ch, n, ov] = await Promise.all([
        stocksApi.get(symbol),
        stocksApi.prediction(symbol).catch(() => null),
        stocksApi.sentiment(symbol).catch(() => null),
        stocksApi.recommendation(symbol).catch(() => null),
  stocksApi.chart(symbol, selectedDays, isRefresh).catch(() => ({ data: [] })),
        stocksApi.news(symbol, 5).catch(() => ({ news: [] })),
        stocksApi.overview(symbol, isRefresh).catch(() => null),
      ]);

      const mergedStock = {
        ...s,
        name: ov?.company_name || s?.name || String(symbol || '').toUpperCase(),
        industry: ov?.industry || s?.industry || null,
      };

      setStock(mergedStock);
      setPrediction(p);
      setSentiment(sent);
      setRecommendation(rec);
      setChartData(ch.data || []);
      setNews(n.news || []);
      setOverview(ov);
    } catch (e) {
      setError(e.error || e.message || 'Failed to load stock data');
      setStock(null);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [symbol, selectedDays]);

  useEffect(() => {
    load();
  }, [load]);

  if (loading && !stock) {
    return (
      <div className="max-w-5xl mx-auto px-4 py-8">
        <p className="text-slate-400">Loading...</p>
      </div>
    );
  }
  if (error && !stock) {
    return (
      <div className="max-w-5xl mx-auto px-4 py-8">
        <p className="text-red-400">{error}</p>
        <Link to="/" className="text-brand-400 hover:underline mt-2 inline-block">Back to home</Link>
      </div>
    );
  }

  const recColor = getRecommendationColor(recommendation?.recommendation);
  const predColor = getPredictionColor(prediction?.direction);
  const predictionConfidence = Math.max(0, Math.min(100, Number(prediction?.confidence || 0)));
  const predictionFillClass = prediction?.direction === 'Up'
    ? 'bg-green-400'
    : prediction?.direction === 'Down'
      ? 'bg-red-400'
      : 'bg-slate-400';
  const market = overview?.market || {};
  const quickStats = [
    { label: 'Close', value: formatPrice(market.close) },
    { label: 'Open', value: formatPrice(market.open) },
    { label: 'High', value: formatPrice(market.high) },
    { label: 'Low', value: formatPrice(market.low) },
    { label: 'Volume', value: formatVolume(market.volume) },
    {
      label: 'Day Change',
      value: Number.isFinite(Number(market.day_change)) && Number.isFinite(Number(market.day_change_pct))
        ? `${Number(market.day_change) >= 0 ? '+' : ''}${Number(market.day_change).toFixed(2)} (${Number(market.day_change_pct) >= 0 ? '+' : ''}${Number(market.day_change_pct).toFixed(2)}%)`
        : '—'
    },
  ];
  const compactNews = (news || []).slice(0, 4);

  return (
    <div className="max-w-6xl mx-auto px-4 py-8">
      <div className="flex items-center justify-between mb-6">
        <div>
          <Link to="/" className="text-slate-400 hover:text-white text-sm mb-1 inline-block">← Back to search</Link>
          <h1 className="text-2xl font-bold text-white">{stock?.name || stock?.symbol}</h1>
          <p className="text-slate-400 text-sm mt-1">
            Symbol: <span className="font-mono text-brand-300">{stock?.symbol}</span>
            {stock?.industry ? ` • Industry: ${stock.industry}` : ''}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {isAuthenticated && (
            <button
              onClick={async () => {
                setWatchlistMsg('');
                setWatchlistAdding(true);
                try {
                  await watchlistApi.add(symbol);
                  setWatchlistMsg('Added to watchlist');
                } catch (e) {
                  const msg = e.status === 401
                    ? 'Please log in to use watchlist'
                    : (e.error || e.msg || e.detail || 'Failed to add');
                  setWatchlistMsg(msg);
                } finally {
                  setWatchlistAdding(false);
                }
              }}
              disabled={watchlistAdding}
              className="bg-brand-600 hover:bg-brand-500 text-white px-3 py-2 rounded-lg text-sm font-medium disabled:opacity-50"
            >
              {watchlistAdding ? 'Adding…' : 'Add to watchlist'}
            </button>
          )}
          <button
            onClick={() => load(true)}
            disabled={refreshing}
            className="bg-surface-800 border border-slate-600 text-slate-300 px-4 py-2 rounded-lg text-sm hover:bg-slate-700 disabled:opacity-50"
          >
            {refreshing ? 'Refreshing...' : 'Refresh data'}
          </button>
        </div>
      </div>
      {watchlistMsg && <p className={watchlistMsg.startsWith('Added') ? 'text-green-400 text-sm mt-1 mb-4' : 'text-red-400 text-sm mt-1 mb-4'}>{watchlistMsg}</p>}

      <div className="grid lg:grid-cols-[190px_minmax(0,1fr)_260px] gap-5 items-start">
        <aside className="lg:sticky lg:top-24">
          <div className={`${cardClass} p-3 border-slate-600/50`}>
            <h3 className="text-slate-200 text-sm font-semibold mb-3">Analyze another stock</h3>
            <StockSearch
              placeholder="Search symbol..."
              alwaysShowList
              listHeightClass="max-h-[calc(100vh-260px)]"
            />
          </div>
        </aside>

        <div className="space-y-6">
          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
            <div className={`${cardClass} p-3 min-h-[112px] shadow-lg hover:shadow-xl transition-shadow duration-300`}>
              <h3 className="text-slate-400 text-xs font-medium mb-2 uppercase tracking-wider">Prediction</h3>
              {prediction ? (
                <>
                  <p className={`text-2xl font-bold leading-none ${predColor} mb-2`}>{prediction.direction}</p>
                  <div className="flex items-center gap-2">
                    <div className="flex-1 bg-slate-700/50 rounded-full h-2 overflow-hidden">
                      <div
                        className={`h-full ${predictionFillClass} transition-all duration-500`}
                        style={{ width: `${predictionConfidence}%` }}
                      ></div>
                    </div>
                    <span className="text-slate-400 text-sm font-medium">{predictionConfidence}%</span>
                  </div>
                </>
              ) : (
                <p className="text-slate-500">Unavailable</p>
              )}
            </div>

            <div className={`${cardClass} p-3 min-h-[112px] shadow-lg hover:shadow-xl transition-shadow duration-300`}>
              <h3 className="text-slate-400 text-xs font-medium mb-2 uppercase tracking-wider">Recommendation</h3>
              {recommendation && recommendation.recommendation !== 'Unavailable' ? (
                <>
                  <p className={`text-2xl font-bold leading-none ${recColor} mb-1`}>{recommendation.recommendation}</p>
                  <p className="text-slate-400 text-xs leading-5 max-h-8 overflow-hidden">{recommendation.reasoning}</p>
                  <p className="text-slate-500 text-[11px] mt-1">Confidence: {recommendation.confidence}%</p>
                </>
              ) : (
                <p className="text-slate-500">Unavailable</p>
              )}
            </div>

            <div className={`${cardClass} p-3 min-h-[112px] shadow-lg hover:shadow-xl transition-shadow duration-300`}>
              <h3 className="text-slate-400 text-xs font-medium mb-2 uppercase tracking-wider">Sentiment Analysis</h3>
              {sentiment ? (
                <>
                  <p className="text-2xl font-bold leading-none text-white mb-1">{sentiment.sentiment}</p>
                  <p className="text-slate-400 text-xs">
                    <span className="text-slate-500">Score: </span>
                    <span className="text-white font-semibold">{sentiment.score}</span>
                  </p>
                </>
              ) : (
                <p className="text-slate-500">Unavailable</p>
              )}
            </div>
          </div>

          <div>
            <h2 className="text-lg font-semibold text-slate-200 mb-3">Price Chart</h2>
            <div className={`${cardClass} p-6 shadow-xl`}>
              <div className="flex items-start justify-between gap-4 mb-4">
                <div className="flex flex-wrap gap-2">
                {[
                  ['sma', 'SMA'],
                  ['ema', 'EMA'],
                  ['bollinger', 'Bollinger'],
                  ['rsi', 'RSI'],
                  ['macd', 'MACD'],
                  ['volume', 'Volume'],
                  ['atr', 'ATR'],
                ].map(([key, label]) => {
                  const isOn = Boolean(indicators[key]);
                  return (
                    <button
                      key={key}
                      type="button"
                      onClick={() => setIndicators((prev) => ({ ...prev, [key]: !prev[key] }))}
                      className={`px-3 py-1.5 rounded-lg text-xs font-semibold border transition-colors ${
                        isOn
                          ? 'bg-emerald-600/20 border-emerald-500 text-emerald-300'
                          : 'bg-slate-900/30 border-slate-700 text-slate-300 hover:border-slate-500'
                      }`}
                    >
                      {label}
                    </button>
                  );
                })}
                </div>

                <div className="relative shrink-0">
                  <button
                    type="button"
                    onClick={() => setTimelineMenuOpen((v) => !v)}
                    className="px-3 py-2 rounded-lg text-xs font-semibold border border-slate-700 bg-slate-900/30 text-slate-200 hover:border-slate-500 min-w-[108px] text-left"
                  >
                    Timeline: {timelineOptions.find((t) => t.days === selectedDays)?.label || 'Custom'}
                  </button>

                  {timelineMenuOpen && (
                    <div className="absolute right-0 mt-2 w-40 max-h-48 overflow-y-auto rounded-lg border border-slate-700 bg-slate-900/95 backdrop-blur-sm shadow-xl z-20">
                      {timelineOptions.map((t) => {
                        const active = selectedDays === t.days;
                        return (
                          <button
                            key={t.days}
                            type="button"
                            onClick={() => {
                              setSelectedDays(t.days);
                              setTimelineMenuOpen(false);
                            }}
                            className={`w-full text-left px-3 py-2 text-xs font-semibold border-b border-slate-800/80 last:border-b-0 ${
                              active
                                ? 'text-brand-200 bg-brand-600/20'
                                : 'text-slate-300 hover:bg-slate-800/70'
                            }`}
                          >
                            {t.label}
                          </button>
                        );
                      })}
                    </div>
                  )}
                </div>
              </div>

              <StockChart data={chartData} height={400} indicators={indicators} />
            </div>
          </div>

          {overview?.profile?.business_description && (
            <div className={`${cardClass} p-5`}>
              <h3 className="text-slate-200 font-semibold mb-2">Company Profile</h3>
              <p className="text-slate-300 text-sm leading-6">{overview.profile.business_description}</p>
            </div>
          )}

        </div>

        <aside className="space-y-4">
          <h2 className="text-lg font-semibold text-slate-200 mb-3">News & sentiment</h2>
          <div className={`${cardClass} divide-y divide-slate-700/50 max-h-[240px] lg:min-h-[380px] lg:max-h-[380px] overflow-y-auto`}>
            {compactNews.length === 0 ? (
              <p className="p-4 text-slate-500 text-sm">No news items. News is fetched from Google News RSS.</p>
            ) : (
              compactNews.map((item, i) => (
                <div key={i} className="p-4">
                  <p className="text-slate-200 text-sm font-medium">{item.headline}</p>
                  {item.summary && <p className="text-slate-400 text-xs mt-1">{item.summary}</p>}
                  <p className="text-slate-500 text-xs mt-1">
                    {item.sentiment} (score: {item.score}) · {item.source}
                  </p>
                </div>
              ))
            )}
          </div>
          <p className="mt-4 text-slate-500 text-xs">
            For detailed Q&A, use the <Link to="/chat" className="text-brand-400 hover:underline">Chat</Link> tab.
          </p>

          <div className={`${cardClass} p-5 shadow-xl`}>
            <h3 className="text-slate-200 font-semibold mb-3">Market Snapshot</h3>
            <div className="space-y-3">
              {quickStats.map((item) => (
                <div key={item.label} className="flex items-center justify-between border-b border-slate-700/40 pb-2 last:border-b-0 last:pb-0">
                  <span className="text-slate-400 text-sm">{item.label}</span>
                  <span className="text-slate-100 font-semibold text-sm text-right">{item.value}</span>
                </div>
              ))}
            </div>
            {(market.day_range?.low != null || market.year_52_range?.low != null) && (
              <div className="mt-4 space-y-2 text-xs text-slate-400">
                <p>
                  Day Range: {market.day_range?.low != null ? market.day_range.low : '—'} — {market.day_range?.high != null ? market.day_range.high : '—'}
                </p>
                <p>
                  52W Range: {market.year_52_range?.low != null ? market.year_52_range.low : '—'} — {market.year_52_range?.high != null ? market.year_52_range.high : '—'}
                </p>
              </div>
            )}
          </div>
        </aside>

      </div>

      {Array.isArray(overview?.financial_highlights) && overview.financial_highlights.length > 0 && (
        <div className={`${cardClass} p-5 mt-6`}>
          <h2 className="text-lg font-semibold text-slate-200 mb-3">Fundamental Highlights</h2>
          <div className="grid sm:grid-cols-2 lg:grid-cols-6 gap-3">
            {overview.financial_highlights.map((item) => (
              <div key={item.key} className="rounded-lg border border-slate-700/60 bg-slate-900/30 p-3">
                <p className="text-slate-400 text-xs uppercase tracking-wide">{item.label}</p>
                <p className="text-slate-100 font-bold text-lg mt-1">{item.value}</p>
                {item.rating && (
                  <p className="text-xs mt-1 text-brand-300">{item.rating[0].toUpperCase() + item.rating.slice(1)}</p>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
