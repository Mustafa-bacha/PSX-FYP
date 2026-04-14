import React, { useEffect, useMemo, useState } from 'react';
import { apiClient } from '../api/client.js';
import { StocksPanel } from './StocksPanel.jsx';
import { StockDashboard } from './StockDashboard.jsx';

export function DashboardPage({ token }) {
  const [stocks, setStocks] = useState([]);
  const [selected, setSelected] = useState('');
  const [watchlist, setWatchlist] = useState([]);
  const [showWatchlistOnly, setShowWatchlistOnly] = useState(false);
  const [error, setError] = useState('');
  const [syncing, setSyncing] = useState(false);
  const [latestDbDate, setLatestDbDate] = useState('');
  const [focusCount, setFocusCount] = useState(0);
  const [warmingModels, setWarmingModels] = useState(false);
  const [runningAppendRetrain, setRunningAppendRetrain] = useState(false);
  const [appendRetrainSummary, setAppendRetrainSummary] = useState(null);
  const [focusRefreshStatus, setFocusRefreshStatus] = useState(null);
  const [runningBatch, setRunningBatch] = useState(false);
  const [sentimentStatus, setSentimentStatus] = useState(null);
  const [runningSentiment, setRunningSentiment] = useState(false);
  const [runningSentimentFull, setRunningSentimentFull] = useState(false);

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

  const loadSyncStatus = async () => {
    const data = await apiClient.syncStatus();
    setLatestDbDate(data.latest_date || '');
  };

  const loadFocusSymbols = async () => {
    const data = await apiClient.focusSymbols();
    setFocusCount(Number(data.count || 0));
  };

  const loadFocusRefreshStatus = async () => {
    const data = await apiClient.focusRefreshStatus();
    setFocusRefreshStatus(data || null);
  };

  const loadSentimentStatus = async () => {
    const data = await apiClient.sentimentStatus();
    setSentimentStatus(data || null);
  };

  useEffect(() => {
    if (!token) return;
    let mounted = true;

    (async () => {
      setError('');
      try {
        const [_, wl] = await Promise.all([
          loadStocks(),
          loadWatchlist(),
          loadSyncStatus(),
          loadFocusSymbols(),
          loadFocusRefreshStatus(),
          loadSentimentStatus()
        ]);
        if (!mounted) return;

        if (!selected) {
          if (wl.length) setSelected(wl[0]);
        }
      } catch (err) {
        if (mounted) setError(err.message || 'Failed to load dashboard data');
      }
    })();

    const timer = setInterval(() => {
      if (!document.hidden) {
        loadStocks().catch(() => {});
        loadFocusRefreshStatus().catch(() => {});
        loadSentimentStatus().catch(() => {});
      }
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

  const handleSyncHistorical = async () => {
    setSyncing(true);
    setError('');
    try {
      await apiClient.syncHistorical();
      await Promise.all([loadStocks(), loadSyncStatus()]);
    } catch (err) {
      setError(err.message || 'Historical sync failed');
    } finally {
      setSyncing(false);
    }
  };

  const handleWarmupModels = async () => {
    setWarmingModels(true);
    setError('');
    try {
      await apiClient.warmupModels(60);
    } catch (err) {
      setError(err.message || 'Model warmup failed');
    } finally {
      setWarmingModels(false);
    }
  };

  const handleAppendAndRetrain = async () => {
    setRunningAppendRetrain(true);
    setError('');
    try {
      const data = await apiClient.appendAndRetrainModels({
        max_symbols: 120,
        min_rows: 120,
        epochs: 20,
        variant: 'lstm'
      });
      setAppendRetrainSummary({
        inserted_rows: Number(data?.inserted_rows || 0),
        changed_symbols_count: Number(data?.changed_symbols_count || 0),
        selected_count: Number(data?.selected_count || 0),
        trained_count: Number(data?.trained_count || 0),
        failed_count: Number(data?.failed_count || 0),
        fixed_csv_path: String(data?.fixed_csv_path || '')
      });
      await Promise.all([loadStocks(), loadSyncStatus()]);
    } catch (err) {
      setError(err.message || 'Append + quick retrain failed');
    } finally {
      setRunningAppendRetrain(false);
    }
  };

  const handleFocusRefreshStart = async () => {
    setError('');
    try {
      const data = await apiClient.focusRefreshStart();
      setFocusRefreshStatus(data || null);
    } catch (err) {
      setError(err.message || 'Failed to start focus refresh scheduler');
    }
  };

  const handleFocusRefreshStop = async () => {
    setError('');
    try {
      const data = await apiClient.focusRefreshStop();
      setFocusRefreshStatus(data || null);
    } catch (err) {
      setError(err.message || 'Failed to stop focus refresh scheduler');
    }
  };

  const handleFocusRefreshRunNow = async () => {
    setRunningBatch(true);
    setError('');
    try {
      const data = await apiClient.focusRefreshRunNow();
      setFocusRefreshStatus(data.status || null);
      await loadStocks();
    } catch (err) {
      setError(err.message || 'Failed to run focus refresh batch');
    } finally {
      setRunningBatch(false);
    }
  };

  const handleSentimentRunNow = async () => {
    setRunningSentiment(true);
    setError('');
    try {
      const data = await apiClient.sentimentRunNow();
      setSentimentStatus(data.status || null);
    } catch (err) {
      setError(err.message || 'Failed to run sentiment batch');
    } finally {
      setRunningSentiment(false);
    }
  };

  const handleSentimentRunFull = async () => {
    setRunningSentimentFull(true);
    setError('');
    try {
      const data = await apiClient.sentimentRunFull();
      setSentimentStatus(data.status || null);
    } catch (err) {
      setError(err.message || 'Failed to run full sentiment cycle');
    } finally {
      setRunningSentimentFull(false);
    }
  };

  const handleSentimentStartStop = async () => {
    setError('');
    try {
      const data = sentimentStatus?.running
        ? await apiClient.sentimentStop()
        : await apiClient.sentimentStart();
      setSentimentStatus(data || null);
    } catch (err) {
      setError(err.message || 'Failed to toggle sentiment scheduler');
    }
  };

  return (
  <section className="ad-section dashboard-shell">
      <div className="ad-card">
        <div>
          <h2 className="ad-h2">Analysis Dashboard</h2>
          <p className="ad-p">Manage your personal watchlist and run deep stock analysis.</p>
        </div>
        <div className="dashboard-actions">
          <div className="dashboard-actions-main">
            <button
              className={`ad-btn ${showWatchlistOnly ? 'ad-btn-primary' : ''}`}
              onClick={() => setShowWatchlistOnly((v) => !v)}
            >
              {showWatchlistOnly ? 'Showing Watchlist Only' : 'Show Watchlist Only'}
            </button>
            <button className="ad-btn" onClick={handleSyncHistorical} disabled={syncing}>
              {syncing ? 'Syncing…' : 'Sync to Today'}
            </button>
            <button className="ad-btn" onClick={handleWarmupModels} disabled={warmingModels}>
              {warmingModels ? 'Warming…' : 'Warmup Models'}
            </button>
            <button className="ad-btn" onClick={handleFocusRefreshRunNow} disabled={runningBatch}>
              {runningBatch ? 'Running batch…' : 'Run Focus Batch Now'}
            </button>
          </div>

          <details className="dashboard-actions-more">
            <summary className="ad-btn">More Actions</summary>
            <div className="dashboard-actions-menu">
              <button className="ad-btn" onClick={handleAppendAndRetrain} disabled={runningAppendRetrain}>
                {runningAppendRetrain ? 'Appending + retraining…' : 'Append + Quick Retrain'}
              </button>
              <button
                className="ad-btn"
                onClick={focusRefreshStatus?.running ? handleFocusRefreshStop : handleFocusRefreshStart}
              >
                {focusRefreshStatus?.running ? 'Stop Auto Focus Refresh' : 'Start Auto Focus Refresh'}
              </button>
              <button className="ad-btn" onClick={handleSentimentRunNow} disabled={runningSentiment}>
                {runningSentiment ? 'Running sentiment…' : 'Run Sentiment Batch'}
              </button>
              <button className="ad-btn" onClick={handleSentimentRunFull} disabled={runningSentimentFull}>
                {runningSentimentFull ? 'Running full sentiment…' : 'Run Full Sentiment (549)'}
              </button>
              <button className="ad-btn" onClick={handleSentimentStartStop}>
                {sentimentStatus?.running ? 'Stop Sentiment Auto' : 'Start Sentiment Auto'}
              </button>
            </div>
          </details>
        </div>
      </div>

      {error ? <div className="ad-alert ad-alert-danger">{error}</div> : null}

      <div className="ad-grid ad-grid-cols-1 md:ad-grid-cols-4 ad-gap-4 dashboard-grid">
        <div className="ad-col-span-1 dashboard-left">
          <StocksPanel
            stocks={visibleStocks}
            selected={selected}
            onSelect={setSelected}
            watchlistSet={watchlistSet}
            onToggleWatchlist={toggleWatch}
          />
        </div>
  <section className="ad-col-span-1 md:ad-col-span-3 dashboard-right">
          <StockDashboard symbol={selected} token={token} />
          <div className="ad-card">
            <h4 className="ad-h4">Your Watchlist ({watchlist.length})</h4>
            <div className="ad-flex ad-gap-2 ad-flex-wrap">
              {watchlist.length ? watchlist.map((sym) => (
                <button key={sym} className={`ad-btn ad-btn-sm ${selected === sym ? 'ad-btn-primary' : ''}`} onClick={() => setSelected(sym)}>
                  {sym}
                </button>
              )) : <span className="ad-p">No favorite stocks yet. Click ☆ in the symbols list to add.</span>}
            </div>
          </div>
        </section>
      </div>
    </section>
  );
}
