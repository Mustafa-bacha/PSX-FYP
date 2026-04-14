import React, { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';

function fmt(v) {
  if (v == null || Number.isNaN(Number(v))) return '—';
  return Number(v).toLocaleString();
}

function FlashCell({ value, className = '', children }) {
  const [flashClass, setFlashClass] = useState('');
  const prevValue = useRef(value);

  useEffect(() => {
    if (prevValue.current !== undefined && prevValue.current !== value) {
      if (Number(value) > Number(prevValue.current)) {
        setFlashClass('flash-up');
      } else if (Number(value) < Number(prevValue.current)) {
        setFlashClass('flash-down');
      } else {
        setFlashClass('');
      }
      prevValue.current = value;
      
      const timer = setTimeout(() => setFlashClass(''), 1000);
      return () => clearTimeout(timer);
    }
    prevValue.current = value;
  }, [value]);

  return <td className={`${className} ${flashClass}`}>{children || value}</td>;
}

export function MarketPerformers({ data, onRefresh, loading, meta }) {
  const navigate = useNavigate();

  const renderTable = (title, rows = []) => (
    <div className="rounded-2xl border border-slate-700/70 bg-slate-900/25 p-4 md:p-5 shadow-lg">
      <h3 className="text-sm font-semibold tracking-wide text-slate-100 uppercase mb-3">{title}</h3>
      <div className="overflow-x-auto rounded-xl border border-slate-700/60">
        <table className="min-w-full text-sm">
          <thead className="bg-slate-800/80">
            <tr>
              <th className="px-3 py-2 text-left text-slate-300 font-semibold">Symbol</th>
              <th className="px-3 py-2 text-right text-slate-300 font-semibold">Price</th>
              <th className="px-3 py-2 text-right text-slate-300 font-semibold">Change</th>
              <th className="px-3 py-2 text-right text-slate-300 font-semibold">Volume</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {rows.length ? rows.map((r) => {
              const isPos = (r.change || 0) >= 0;
              return (
                <tr key={`${title}-${r.symbol}`} className="bg-slate-900/20 hover:bg-slate-800/30 transition-colors">
                  <td className="px-3 py-2 font-semibold">
                    <button
                      type="button"
                      onClick={() => navigate(`/dashboard/${encodeURIComponent(String(r.symbol || '').toUpperCase())}`)}
                      className="font-mono text-brand-300 hover:text-brand-200 hover:underline underline-offset-2"
                      title={`Open ${r.symbol} in dashboard`}
                    >
                      {r.symbol}
                    </button>
                  </td>
                  <FlashCell value={r.price} className="px-3 py-2 text-right text-slate-100">
                    {fmt(Number(r.price).toFixed ? Number(r.price).toFixed(2) : r.price)}
                  </FlashCell>
                  <td className={`px-3 py-2 text-right font-medium ${isPos ? 'text-green-400' : 'text-red-400'}`}>
                    {r.raw_change || `${r.change} (${r.change_pct}%)`}
                  </td>
                  <td className="px-3 py-2 text-right text-slate-200">{fmt(r.volume)}</td>
                </tr>
              );
            }) : (
              <tr>
                <td colSpan={4} className="px-3 py-6 text-center text-slate-400">No data</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );

  return (
    <section className="rounded-2xl border border-slate-700/70 bg-slate-900/20 p-5 md:p-6 shadow-xl">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between mb-4">
        <h2 className="text-2xl font-bold text-white flex items-center gap-2">
          <span className="w-1 h-6 bg-gradient-to-b from-brand-400 to-brand-600 rounded-full"></span>
          Market Performers
        </h2>
        <div className="flex flex-col items-start md:items-end gap-2">
          {meta ? <span className="text-xs text-slate-400">{meta}</span> : null}
          <button
            className="px-3 py-2 rounded-lg text-xs font-semibold border border-slate-700 bg-slate-900/30 text-slate-200 hover:border-slate-500 disabled:opacity-60 disabled:cursor-not-allowed"
            onClick={onRefresh}
            disabled={loading}
          >
            {loading ? 'Refreshing...' : 'Refresh'}
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4 md:gap-5">
        {renderTable('TOP ACTIVE STOCKS', data?.top_active_stocks)}
        {renderTable('TOP ADVANCERS', data?.top_advancers)}
        {renderTable('TOP DECLINERS', data?.top_decliners)}
      </div>
    </section>
  );
}
