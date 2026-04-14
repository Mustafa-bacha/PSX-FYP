import React, { useEffect, useMemo, useState } from 'react';
import { apiClient } from '../api/client.js';
import { StocksPanel } from './StocksPanel.jsx';
import { PaperTradingPanel } from './PaperTradingPanel.jsx';

export function PaperTradingPage({ token }) {
  const [stocks, setStocks] = useState([]);
  const [selected, setSelected] = useState('');
  const [watchlist, setWatchlist] = useState([]);
  const [showWatchlistOnly, setShowWatchlistOnly] = useState(false);
  const [error, setError] = useState('');

  const watchlistSet = useMemo(() => new Set(watchlist), [watchlist]);

  const loadStocks = async () => {
    const data = await apiClient.stocks();
    setStocks(data.snapshots || []);
  };

  const loadWatchlist = async () => {
    const data = await apiClient.watchlist(token);
    const symbols = (data.items || []).map((x) => x.symbol);
    setWatchlist(symbols);
    return symbols;
  };

  useEffect(() => {
    if (!token) return;
    let mounted = true;

    (async () => {
      setError('');
      try {
        const [_, wl] = await Promise.all([loadStocks(), loadWatchlist()]);
        if (!mounted) return;
        if (!selected && wl.length) setSelected(wl[0]);
      } catch (err) {
        if (mounted) setError(err.message || 'Failed to load simulator data');
      }
    })();

    const timer = setInterval(() => {
      if (!document.hidden) loadStocks().catch(() => {});
    }, 2 * 60 * 1000);

    return () => {
      mounted = false;
      clearInterval(timer);
    };
  }, [token]);

  const visibleStocks = useMemo(() => {
    if (!showWatchlistOnly) return stocks;
    return stocks.filter((s) => watchlistSet.has(s.symbol));
  }, [stocks, showWatchlistOnly, watchlistSet]);

  useEffect(() => {
    if (!visibleStocks.length) {
      setSelected('');
      return;
    }

    if (!selected || !visibleStocks.some((s) => s.symbol === selected)) {
      setSelected(visibleStocks[0].symbol);
    }
  }, [visibleStocks, selected]);

  const toggleWatch = async (symbol) => {
    const ticker = String(symbol || '').toUpperCase();
    setError('');
    try {
      if (watchlistSet.has(ticker)) {
        const data = await apiClient.removeWatchlist(ticker, token);
        setWatchlist((data.items || []).map((x) => x.symbol));
      } else {
        const data = await apiClient.addWatchlist(ticker, token);
        setWatchlist((data.items || []).map((x) => x.symbol));
      }
    } catch (err) {
      setError(err.message || 'Watchlist update failed');
    }
  };

  return (
    <section className="page-stack">
      <div className="card dashboard-toolbar">
        <div>
          <h2>Paper Trading Simulator</h2>
          <p className="muted-line">Trade in a risk-free environment with virtual cash, positions, and P/L analytics.</p>
        </div>
        <div className="toolbar">
          <button
            className={`toggle-btn ${showWatchlistOnly ? 'active' : ''}`}
            onClick={() => setShowWatchlistOnly((v) => !v)}
          >
            {showWatchlistOnly ? 'Showing Watchlist Only' : 'Show Watchlist Only'}
          </button>
        </div>
      </div>

      {error ? <div className="error-box">{error}</div> : null}

      <div className="layout">
        <StocksPanel
          stocks={visibleStocks}
          selected={selected}
          onSelect={setSelected}
          watchlistSet={watchlistSet}
          onToggleWatchlist={toggleWatch}
        />

        <section className="content">
          <PaperTradingPanel symbol={selected} token={token} />
        </section>
      </div>
    </section>
  );
}
