import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { stocks } from '../api.js';

export default function StockSearch({
  placeholder = 'Search stocks...',
  onSelect,
  alwaysShowList = false,
  listHeightClass = 'max-h-80',
  fitContainer = false,
  showDefaultWhenEmpty = true,
  defaultListLimit = 80,
  queryLimit = 5000,
}) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [defaultStocks, setDefaultStocks] = useState([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await stocks.list('', queryLimit);
        if (cancelled) return;
        setDefaultStocks((data.stocks || []).slice(0, Math.max(1, Number(defaultListLimit || 80))));
      } catch {
        if (!cancelled) setDefaultStocks([]);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [defaultListLimit, queryLimit]);

  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      return;
    }
    const t = setTimeout(async () => {
      setLoading(true);
      try {
        const data = await stocks.list(query, queryLimit);
        setResults(data.stocks || []);
      } catch {
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, 200);
    return () => clearTimeout(t);
  }, [query, queryLimit]);

  const handleSelect = (symbol) => {
    setQuery('');
    setResults([]);
    setOpen(false);
    if (onSelect) onSelect(symbol);
    else navigate(`/dashboard/${symbol}`);
  };

  const visibleStocks = query.trim() ? results : (showDefaultWhenEmpty ? defaultStocks : []);
  const showList = alwaysShowList || open;
  const containerClass = fitContainer ? 'relative flex flex-col h-full min-h-0' : 'relative';
  const inputWrapClass = fitContainer ? 'shrink-0' : '';
  const listClass = alwaysShowList
    ? `relative mt-3 ${fitContainer ? 'flex-1 min-h-0' : listHeightClass}`
    : `absolute top-full left-0 right-0 mt-1 ${listHeightClass} z-50`;
  const footerClass = fitContainer ? 'text-slate-500 text-xs mt-2 shrink-0' : 'text-slate-500 text-xs mt-1';

  return (
    <div className={containerClass}>
      <div className={inputWrapClass}>
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => setOpen(true)}
          onBlur={() => {
            if (alwaysShowList) return;
            setTimeout(() => setOpen(false), 120);
          }}
          placeholder={placeholder}
          className="w-full bg-surface-800 border border-slate-600 rounded-xl px-4 py-3 text-white placeholder-slate-500 focus:ring-2 focus:ring-brand-500 focus:border-transparent"
        />
      </div>
      {showList && visibleStocks.length > 0 && (
        <ul className={`${listClass} bg-surface-800 border border-slate-600 rounded-xl shadow-xl overflow-y-auto`}>
          {visibleStocks.map((s) => (
            <li key={s.symbol}>
              <button
                type="button"
                onClick={() => handleSelect(s.symbol)}
                className="w-full text-left px-4 py-2.5 hover:bg-slate-700/50 flex justify-between items-center"
              >
                <span className="font-mono text-brand-400">{s.symbol}</span>
                <span className="text-right ml-2">
                  <span className="text-slate-200 text-xs font-semibold block">
                    {Number.isFinite(Number(s.close)) ? `PKR ${Number(s.close).toFixed(2)}` : '—'}
                  </span>
                  <span
                    className={`text-[11px] block ${
                      Number(s.change_pct) > 0
                        ? 'text-green-400'
                        : Number(s.change_pct) < 0
                          ? 'text-red-400'
                          : 'text-slate-300'
                    }`}
                  >
                    {Number.isFinite(Number(s.change_pct))
                      ? `${Number(s.change_pct) > 0 ? '+' : ''}${Number(s.change_pct).toFixed(2)}%`
                      : '0.00%'}
                  </span>
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
      {loading && query && <p className="text-slate-500 text-sm mt-1">Searching...</p>}
      {!query && showList && showDefaultWhenEmpty && defaultStocks.length > 0 && (
        <p className={footerClass}>Showing top PSX stocks — scroll for more.</p>
      )}
    </div>
  );
}
