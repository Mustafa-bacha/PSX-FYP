import { useState, useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../context/AuthContext.jsx';
import { market } from '../api.js';
import { apiClient } from '../api/client.js';
import { cardClass } from '../lib/constants.js';
import StockSearch from '../components/StockSearch.jsx';
import StockChart from '../components/StockChart.jsx';
import { MarketPerformers } from '../components/MarketPerformers.jsx';

export default function Home() {
  const { isAuthenticated } = useAuth();
  const [marketData, setMarketData] = useState(null);
  const [marketLoading, setMarketLoading] = useState(true);
  const [selectedDays, setSelectedDays] = useState(365);
  const [timelineMenuOpen, setTimelineMenuOpen] = useState(false);
  const [performersData, setPerformersData] = useState(null);
  const [performersLoading, setPerformersLoading] = useState(false);
  const [performersError, setPerformersError] = useState('');
  const performersMountedRef = useRef(true);

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

  useEffect(() => {
    let cancelled = false;
    setMarketLoading(true);
    market
      .summary(selectedDays)
      .then((data) => {
        if (!cancelled) setMarketData(data);
      })
      .catch(() => {
        if (!cancelled) setMarketData(null);
      })
      .finally(() => {
        if (!cancelled) setMarketLoading(false);
      });
    return () => { cancelled = true; };
  }, [selectedDays]);

  const loadPerformers = async (refresh = false) => {
    setPerformersLoading(true);
    setPerformersError('');
    try {
      const payload = await apiClient.performers(refresh);
      if (!performersMountedRef.current) return;
      setPerformersData(payload);
    } catch (err) {
      if (!performersMountedRef.current) return;
      setPerformersError(err?.message || 'Failed to load market performers');
      setPerformersData(null);
    } finally {
      if (performersMountedRef.current) setPerformersLoading(false);
    }
  };

  useEffect(() => {
    performersMountedRef.current = true;
    loadPerformers(false);

    const timer = setInterval(() => {
      if (!document.hidden) loadPerformers(false);
    }, 5 * 60 * 1000);

    return () => {
      performersMountedRef.current = false;
      clearInterval(timer);
    };
  }, []);

  const indexValue = marketData?.index_value;
  const changeValue = marketData?.change;
  const changePercent = marketData?.change_percent;
  const volume = marketData?.volume;
  const history = marketData?.history || [];
  const isUp = changePercent != null && changePercent >= 0;
  const dateStr = marketData?.as_of || marketData?.date;
  const formattedVolume = volume != null
    ? (volume >= 1e9
      ? `${(volume / 1e9).toFixed(2)}B`
      : volume >= 1e6
        ? `${(volume / 1e6).toFixed(2)}M`
        : volume.toLocaleString())
    : '—';
  const performersMeta = [
    performersData?.as_of ? `As of ${performersData.as_of}` : null,
    performersData?.fetched_at ? `Fetched ${new Date(performersData.fetched_at).toLocaleString()}` : null,
    performersData?.cache_status ? `Cache: ${performersData.cache_status}` : null,
    'Auto-refresh: 5 min'
  ].filter(Boolean).join(' • ');

  return (
    <div className="max-w-7xl mx-auto px-4 py-10 lg:py-12">
      <section className="max-w-4xl mx-auto text-center mb-10">
        <h1 className="text-4xl lg:text-6xl font-bold text-white mb-4 bg-gradient-to-r from-white via-brand-100 to-brand-400 bg-clip-text text-transparent leading-tight">
          AI-Powered Decision Support for <span className="text-brand-400">PSX</span>
        </h1>
        <p className="text-slate-300 text-base lg:text-lg mb-3">
          Stock predictions, sentiment analysis, and Buy/Sell/Hold recommendations for the Pakistan Stock Exchange.
        </p>
        <p className="text-slate-500 text-sm">Find stocks quickly on the left and see the live market pulse on the right.</p>
      </section>

      <section className="lg:grid lg:grid-cols-[300px_minmax(0,1fr)] lg:gap-8 mb-10 items-start">
        <aside className="mb-6 lg:mb-0 lg:sticky lg:top-24">
          <div className={`${cardClass} p-6 border-slate-600/50 shadow-lg lg:h-[560px] flex flex-col min-h-0 overflow-hidden`}>
            <h3 className="text-xl font-semibold text-slate-200 mb-4">Search PSX stocks</h3>
            <div className="flex-1 min-h-0">
              <StockSearch
                placeholder="e.g. OGDC, MCB, LUCK..."
                alwaysShowList
                fitContainer
              />
            </div>
          </div>
        </aside>

        <div className="w-full lg:max-w-4xl">
          <section className={`${cardClass} p-6 lg:p-8 border-slate-600/50 shadow-xl`}>
            <h2 className="text-2xl font-bold text-white mb-6 flex items-center gap-2">
              <span className="w-1 h-6 bg-gradient-to-b from-brand-400 to-brand-600 rounded-full"></span>
              KSE-100 Market Summary
            </h2>
            {marketLoading ? (
              <p className="text-slate-500">Loading market data...</p>
            ) : marketData?.message && !marketData?.index_value ? (
              <p className="text-slate-500">{marketData.message}</p>
            ) : (
              <>
                {marketData?.warning && (
                  <p className="text-amber-300/80 text-sm mb-3">{marketData.warning}</p>
                )}
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 lg:gap-4 mb-6">
                  <div className="rounded-xl border border-slate-700/70 bg-slate-900/25 p-3">
                    <p className="text-slate-400 text-xs uppercase tracking-wider">Index</p>
                    <p className="text-2xl font-bold text-white mt-0.5">
                      {indexValue != null ? indexValue.toLocaleString() : '—'}
                    </p>
                    {dateStr && <p className="text-slate-500 text-xs mt-1">As of {dateStr}</p>}
                  </div>
                  <div className="rounded-xl border border-slate-700/70 bg-slate-900/25 p-3">
                    <p className="text-slate-400 text-xs uppercase tracking-wider">Change</p>
                    <p className={`text-xl font-semibold mt-0.5 ${isUp ? 'text-green-400' : 'text-red-400'}`}>
                      {changePercent != null ? `${isUp ? '+' : ''}${changePercent.toFixed(2)}%` : '—'}
                    </p>
                    {changeValue != null && (
                      <p className={`text-xs mt-1 ${isUp ? 'text-green-400/80' : 'text-red-400/80'}`}>
                        {changeValue >= 0 ? '+' : ''}{Number(changeValue).toFixed(2)}
                      </p>
                    )}
                  </div>
                  <div className="rounded-xl border border-slate-700/70 bg-slate-900/25 p-3">
                    <p className="text-slate-400 text-xs uppercase tracking-wider">Direction</p>
                    <p className={`text-lg font-medium mt-0.5 ${isUp ? 'text-green-400' : 'text-red-400'}`}>
                      {changePercent != null ? (isUp ? 'Up' : 'Down') : '—'}
                    </p>
                  </div>
                  <div className="rounded-xl border border-slate-700/70 bg-slate-900/25 p-3">
                    <p className="text-slate-400 text-xs uppercase tracking-wider">Volume</p>
                    <p className="text-xl font-semibold text-white mt-0.5">{formattedVolume}</p>
                  </div>
                </div>

                <h3 className="text-slate-300 font-medium mb-3">KSE-100 Historical Trend</h3>
                {history.length > 0 ? (
                  <div className="space-y-3">
                    <div className="flex items-start justify-end gap-4">
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

                    <div className="h-[320px]">
                    <StockChart
                      data={history}
                      height={320}
                      indicators={{
                        sma: true,
                        ema: true,
                        bollinger: false,
                        rsi: false,
                        macd: false,
                        volume: false,
                        atr: false,
                      }}
                    />
                    </div>
                  </div>
                ) : (
                  <p className="text-slate-500 text-sm">Historical chart data not available yet.</p>
                )}
              </>
            )}
          </section>
        </div>
      </section>

      <section className="mb-10">
        {performersError ? (
          <p className="text-red-300 text-sm mb-3">{performersError}</p>
        ) : null}

        <MarketPerformers
          data={performersData?.performers}
          loading={performersLoading}
          onRefresh={() => loadPerformers(true)}
          meta={performersMeta || (performersLoading ? 'Loading...' : '')}
        />
      </section>

      {!isAuthenticated && (
        <div className="text-center">
          <p className="text-slate-400 mb-4">Create an account to save watchlists and access all features.</p>
          <Link to="/signup" className="inline-block bg-brand-600 hover:bg-brand-500 text-white px-6 py-2.5 rounded-lg font-medium">
            Get started
          </Link>
        </div>
      )}
    </div>
  );
}
