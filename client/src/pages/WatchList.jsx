import { useState, useEffect, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { watchlist as watchlistApi, stocks } from '../api.js';
import { cardClass } from '../lib/constants.js';
import StockSearch from '../components/StockSearch.jsx';
import StockChart from '../components/StockChart.jsx';

export default function WatchList() {
  const [watchlist, setWatchlist] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [stockData, setStockData] = useState({});

  const load = async () => {
    try {
      const data = await watchlistApi.get();
      setWatchlist(data.watchlist || []);
      setError('');
    } catch (e) {
      setError(e.status === 401 ? 'Please log in to see your watchlist' : 'Could not load watchlist');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  useEffect(() => {
    if (!watchlist.length) {
      setStockData({});
      return;
    }
    let cancelled = false;
    const map = {};
    Promise.all(
      watchlist.map(async (item) => {
        const sym = item.symbol;
        try {
          const [chartRes, recRes] = await Promise.all([
            stocks.chart(sym, 30),
            stocks.recommendation(sym).catch(() => null),
          ]);
          if (cancelled) return;
          map[sym] = {
            chart: chartRes?.data || [],
            recommendation: recRes?.recommendation,
            prediction: recRes?.prediction_direction,
            confidence: recRes?.confidence,
          };
        } catch {
          if (!cancelled) map[sym] = { chart: [], recommendation: null, prediction: null };
        }
      })
    ).then(() => {
      if (!cancelled) setStockData((prev) => ({ ...prev, ...map }));
    });
    return () => { cancelled = true; };
  }, [watchlist]);

  const kpis = useMemo(() => {
    const recs = Object.values(stockData).map((d) => d.recommendation).filter(Boolean);
    return {
      total: watchlist.length,
      buy: recs.filter((r) => r === 'Buy').length,
      hold: recs.filter((r) => r === 'Hold').length,
      sell: recs.filter((r) => r === 'Sell').length,
    };
  }, [watchlist.length, stockData]);

  const addToWatchlist = async (symbol) => {
    setError('');
    try {
      const data = await watchlistApi.add(symbol);
      setWatchlist(data.watchlist || []);
    } catch (e) {
      setError(e.status === 401 ? 'Please log in to add to watchlist' : (e.error || e.msg || e.detail || 'Failed to add'));
    }
  };

  const remove = async (symbol) => {
    setError('');
    try {
      const data = await watchlistApi.remove(symbol);
      setWatchlist(data.watchlist || []);
    } catch {
      setError('Failed to remove');
    }
  };

  return (
    <div className="max-w-5xl mx-auto px-4 py-8">
      <h1 className="text-2xl font-bold text-white mb-2">Watch List</h1>
      <p className="text-slate-400 text-sm mb-6">Manage your watchlist, KPIs, and quick-access charts.</p>

      <div className="mb-6">
        <label className="block text-slate-400 text-sm mb-2">Add stock to watchlist</label>
        <StockSearch onSelect={addToWatchlist} placeholder="Search KSE-100 and select to add..." />
      </div>

      {error && <p className="text-red-400 text-sm mb-4">{error}</p>}

      {!loading && watchlist.length > 0 && (
        <section className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-8">
          <div className={`${cardClass} p-4 border-slate-600/50`}>
            <p className="text-slate-400 text-xs uppercase tracking-wider">Stocks</p>
            <p className="text-2xl font-bold text-white mt-0.5">{kpis.total}</p>
          </div>
          <div className={`${cardClass} p-4 border-green-500/30`}>
            <p className="text-slate-400 text-xs uppercase tracking-wider">Buy</p>
            <p className="text-2xl font-bold text-green-400 mt-0.5">{kpis.buy}</p>
          </div>
          <div className={`${cardClass} p-4 border-amber-500/30`}>
            <p className="text-slate-400 text-xs uppercase tracking-wider">Hold</p>
            <p className="text-2xl font-bold text-amber-400 mt-0.5">{kpis.hold}</p>
          </div>
          <div className={`${cardClass} p-4 border-red-500/30`}>
            <p className="text-slate-400 text-xs uppercase tracking-wider">Sell</p>
            <p className="text-2xl font-bold text-red-400 mt-0.5">{kpis.sell}</p>
          </div>
        </section>
      )}

      <section>
        <h2 className="text-lg font-semibold text-slate-200 mb-3">Your watchlist</h2>
        {loading ? (
          <p className="text-slate-500">Loading...</p>
        ) : watchlist.length === 0 ? (
          <p className="text-slate-500">No stocks in watchlist. Search above to add.</p>
        ) : (
          <div className="space-y-4">
            {watchlist.map((item) => {
              const data = stockData[item.symbol] || {};
              const rec = data.recommendation;
              const recColor =
                rec === 'Buy' ? 'bg-green-500/20 text-green-400 border-green-500/40' :
                rec === 'Sell' ? 'bg-red-500/20 text-red-400 border-red-500/40' :
                'bg-amber-500/20 text-amber-400 border-amber-500/40';
              return (
                <div
                  key={item.symbol}
                  className={`${cardClass} border-slate-600/50 overflow-hidden`}
                >
                  <div className="flex flex-col sm:flex-row sm:items-center gap-3 p-4">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <Link
                          to={`/dashboard/${item.symbol}`}
                          className="font-mono text-brand-400 hover:underline text-lg"
                        >
                          {item.symbol}
                        </Link>
                        {rec && (
                          <span className={`px-2 py-0.5 rounded text-xs font-medium border ${recColor}`}>
                            {rec}
                          </span>
                        )}
                        {data.prediction && (
                          <span className="text-slate-500 text-xs">
                            {data.prediction}
                            {data.confidence != null && ` ${data.confidence}%`}
                          </span>
                        )}
                      </div>
                      <span className="text-slate-400 text-sm block mt-0.5 truncate">{item.name}</span>
                    </div>
                    <button
                      onClick={() => remove(item.symbol)}
                      className="text-slate-400 hover:text-red-400 text-sm shrink-0"
                    >
                      Remove
                    </button>
                  </div>
                  {data.chart && data.chart.length > 0 && (
                    <div className="px-4 pb-4 pt-0">
                      <div className="h-[200px] -mx-1 rounded-lg overflow-hidden">
                        <StockChart data={data.chart} height={200} />
                      </div>
                      <Link
                        to={`/dashboard/${item.symbol}`}
                        className="text-brand-400 hover:text-brand-300 hover:underline text-sm mt-3 inline-block font-medium transition-colors"
                      >
                        View full chart & details →
                      </Link>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </section>
    </div>
  );
}
