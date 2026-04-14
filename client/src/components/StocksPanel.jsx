import React, { useMemo, useState } from 'react';

export function StocksPanel({ stocks, selected, onSelect, watchlistSet = new Set(), onToggleWatchlist }) {
  const [query, setQuery] = useState('');

  const filteredStocks = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return stocks;
    return stocks.filter((s) => String(s.symbol || '').toLowerCase().includes(q));
  }, [stocks, query]);

  const fmtPct = (v) => {
    if (v == null || Number.isNaN(Number(v))) return '—';
    const n = Number(v);
    return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`;
  };

  return (
    <aside className="ad-card stocks-panel">
      <div className="ad-card-header">
        <h3 className="ad-h3">Symbols</h3>
        <input
          type="text"
          className="ad-input"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search symbol..."
          aria-label="Search symbol"
        />
      </div>
      <div className="ad-list stocks-list">
        {filteredStocks.map((s) => (
          <div key={s.symbol} className={`ad-list-item stocks-list-item ${selected === s.symbol ? 'ad-list-item-active' : ''}`}>
            <button className="ad-btn ad-btn-ghost stocks-select-btn" onClick={() => onSelect(s.symbol)}>
              <strong className="stocks-symbol">{s.symbol}</strong>
              <small className="stocks-secondary">
                <b className="stocks-price">PKR {s.close ?? '—'}</b>
                <em className={Number(s.change_pct || 0) >= 0 ? 'ad-text-success' : 'ad-text-danger'}>{fmtPct(s.change_pct)}</em>
              </small>
            </button>

            {onToggleWatchlist ? (
              <button
                className={`ad-btn ad-btn-icon stocks-star-btn ${watchlistSet.has(s.symbol) ? 'ad-text-primary' : ''}`}
                onClick={() => onToggleWatchlist(s.symbol)}
                title={watchlistSet.has(s.symbol) ? 'Remove from watchlist' : 'Add to watchlist'}
              >
                {watchlistSet.has(s.symbol) ? '★' : '☆'}
              </button>
            ) : null}
          </div>
        ))}
      </div>
    </aside>
  );
}
