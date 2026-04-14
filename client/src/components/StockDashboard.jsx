import React, { useEffect, useMemo, useState } from 'react';
import { ResponsiveContainer, ComposedChart, Area, Line, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';
import { apiClient } from '../api/client.js';
const RANGES = [
  { label: '1M', days: 30 },
  { label: '3M', days: 90 },
  { label: '6M', days: 180 },
  { label: '1Y', days: 365 },
  { label: 'All', days: 5000 }
];

const OVERLAYS = [
  { key: 'ma_7', label: 'MA7' },
  { key: 'ma_20', label: 'MA20' },
  { key: 'ma_50', label: 'MA50' },
  { key: 'ema_10', label: 'EMA10' },
  { key: 'ema_20', label: 'EMA20' },
  { key: 'ema_50', label: 'EMA50' },
  { key: 'bb_upper', label: 'Bollinger' }
];

function fmtMoney(v) {
  return v == null || Number.isNaN(Number(v)) ? '—' : Number(v).toFixed(2);
}

function fmtPct(v) {
  if (v == null || Number.isNaN(Number(v))) return '—';
  const n = Number(v);
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`;
}

function StockLineChart({ rows, overlays }) {
  const data = useMemo(() => {
    return rows.map((r) => ({
      ...r,
      close: Number(r.close || 0),
      ma_7: r.ma_7 ? Number(r.ma_7) : null,
      ma_20: r.ma_20 ? Number(r.ma_20) : null,
      ma_50: r.ma_50 ? Number(r.ma_50) : null,
      ema_10: r.ema_10 ? Number(r.ema_10) : null,
      ema_20: r.ema_20 ? Number(r.ema_20) : null,
      ema_50: r.ema_50 ? Number(r.ema_50) : null,
      bb_upper: r.bb_upper ? Number(r.bb_upper) : null,
      bb_lower: r.bb_lower ? Number(r.bb_lower) : null,
    }));
  }, [rows]);

  if (!rows.length) {
    return <div className="chart-empty">No chart data available.</div>;
  }

  const formatYAxis = (tickItem) => {
    return tickItem >= 1000 ? `${(tickItem / 1000).toFixed(1)}k` : tickItem.toFixed(2);
  };

  return (
    <div className="chart-wrap" style={{ width: '100%', height: 350, marginTop: '20px' }}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="colorClose" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#0d9488" stopOpacity={0.4}/>
              <stop offset="95%" stopColor="#0d9488" stopOpacity={0}/>
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
          <XAxis 
            dataKey="date" 
            axisLine={false} 
            tickLine={false} 
            tick={{ fill: '#64748b', fontSize: 12 }} 
            minTickGap={50} 
          />
          <YAxis 
            domain={['auto', 'auto']} 
            orientation="right" 
            axisLine={false} 
            tickLine={false} 
            tick={{ fill: '#64748b', fontSize: 12 }} 
            tickFormatter={formatYAxis} 
            width={50}
          />
          <Tooltip 
            contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px', color: '#f8fafc', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
            itemStyle={{ color: '#e2e8f0', fontSize: '13px', paddingTop: '4px' }}
            labelStyle={{ color: '#94a3b8', marginBottom: '4px', fontWeight: 600, fontSize: '14px' }}
          />
          <Area type="monotone" dataKey="close" name="Close" stroke="#0d9488" strokeWidth={2.5} fillOpacity={1} fill="url(#colorClose)" />
          
          {overlays.ma_7 && <Line type="monotone" dataKey="ma_7" name="MA 7" stroke="#3b82f6" strokeWidth={1.5} dot={false} isAnimationActive={false} />}
          {overlays.ma_20 && <Line type="monotone" dataKey="ma_20" name="MA 20" stroke="#f59e0b" strokeWidth={1.5} dot={false} isAnimationActive={false} />}
          {overlays.ma_50 && <Line type="monotone" dataKey="ma_50" name="MA 50" stroke="#8b5cf6" strokeWidth={1.5} dot={false} isAnimationActive={false} />}
          {overlays.ema_10 && <Line type="monotone" dataKey="ema_10" name="EMA 10" stroke="#ef4444" strokeWidth={1.5} dot={false} isAnimationActive={false} />}
          {overlays.ema_20 && <Line type="monotone" dataKey="ema_20" name="EMA 20" stroke="#ec4899" strokeWidth={1.5} dot={false} isAnimationActive={false} />}
          {overlays.ema_50 && <Line type="monotone" dataKey="ema_50" name="EMA 50" stroke="#14b8a6" strokeWidth={1.5} dot={false} isAnimationActive={false} />}
          {overlays.bb_upper && (
            <>
              <Line type="monotone" dataKey="bb_upper" name="Bollinger Upper" stroke="#64748b" strokeWidth={1.2} strokeDasharray="4 4" dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="bb_lower" name="Bollinger Lower" stroke="#64748b" strokeWidth={1.2} strokeDasharray="4 4" dot={false} isAnimationActive={false} />
            </>
          )}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

function SentimentGauge({ score = 0 }) {
  const normalized = Math.max(-1, Math.min(1, Number(score || 0)));
  const positivePct = ((normalized + 1) / 2) * 100;

  return (
    <div className="sent-gauge" aria-label="sentiment gauge">
      <div className="sent-gauge-track">
        <div className="sent-gauge-fill" style={{ width: `${positivePct}%` }} />
      </div>
      <div className="sent-gauge-labels">
        <span>Negative</span>
        <span>Positive</span>
      </div>
    </div>
  );
}

export function StockDashboard({ symbol, token = '' }) {
  const [days, setDays] = useState(365);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [history, setHistory] = useState([]);
  const [latest, setLatest] = useState(null);
  const [insights, setInsights] = useState(null);
  const [overlays, setOverlays] = useState({
    ma_7: true,
    ma_20: true,
    ma_50: false,
    ema_10: false,
    ema_20: false,
    ema_50: false,
    bb_upper: false
  });

  useEffect(() => {
    if (!symbol) return;
    let active = true;

    (async () => {
      setLoading(true);
      setError('');
      try {
        const [stock, detail] = await Promise.all([
          apiClient.stock(symbol, days),
          apiClient.stockInsights(symbol)
        ]);

        if (!active) return;
        setHistory(stock.data || []);
        setLatest(stock.latest || null);
        setInsights(detail || null);
      } catch (err) {
        if (active) setError(err.message || 'Failed to load stock dashboard');
      } finally {
        if (active) setLoading(false);
      }
    })();

    return () => {
      active = false;
    };
  }, [symbol, days]);

  // Add Refresh button to trigger refreshPrices API
  const handleRefresh = async () => {
    try {
      setLoading(true);
      setError('');
      await apiClient.refreshPrices([symbol]);
      const [stock, detail] = await Promise.all([
        apiClient.stock(symbol, days),
        apiClient.stockInsights(symbol)
      ]);
      setHistory(stock.data || []);
      setLatest(stock.latest || null);
      setInsights(detail || null);
    } catch (err) {
      setError(err.message || 'Failed to refresh prices');
    } finally {
      setLoading(false);
    }
  };

  // Poll for updates every 60 seconds while the dashboard is open
  useEffect(() => {
    if (!symbol) return;
    const id = setInterval(async () => {
      try {
        const stock = await apiClient.stock(symbol, days);
        setHistory(stock.data || []);
        setLatest(stock.latest || null);
      } catch (err) {
        // ignore transient polling errors
      }
    }, 60 * 1000);

    return () => clearInterval(id);
  }, [symbol, days]);

  const changeClass = Number(latest?.change_pct || 0) >= 0 ? 'pos' : 'neg';
  const confidence = Number(insights?.prediction?.confidence || 0) * 100;
  const sentiment = insights?.sentiment || {
    average_score: 0,
    positive_count: 0,
    negative_count: 0,
    neutral_count: 0,
    recent_headlines: []
  };
  const totalSent = Math.max(1, (sentiment.positive_count || 0) + (sentiment.negative_count || 0) + (sentiment.neutral_count || 0));

  return (
    <section className="detail-layout">
      <article className="card stock-main">
        {error && <div className="error-box">{error}</div>}
        <header className="main-head">
          <div>
            <h2>{symbol || '--'}</h2>
            <p className="muted-line">Last refreshed: {latest?.date || '--'}</p>
            <div className="price">PKR {fmtMoney(latest?.close)}</div>
          </div>
          <span className={`badge ${changeClass}`}>{fmtPct(latest?.change_pct)}</span>
        </header>

        <div className="toolbar">
          {OVERLAYS.map((item) => (
            <button
              key={item.key}
              className={`toggle-btn ${overlays[item.key] ? 'active' : ''}`}
              onClick={() => setOverlays((prev) => ({ ...prev, [item.key]: !prev[item.key] }))}
            >
              {item.label}
            </button>
          ))}
        </div>

        <div className="toolbar">
          {RANGES.map((r) => (
            <button key={r.days} className={`toggle-btn ${days === r.days ? 'active' : ''}`} onClick={() => setDays(r.days)}>
              {r.label}
            </button>
          ))}
        </div>

        <div className="toolbar">
          <button className="btn" onClick={handleRefresh}>
            Refresh
          </button>
        </div>

        {loading ? <p className="muted-line">Loading chart...</p> : <StockLineChart rows={history} overlays={overlays} />}

        <h3>Prediction</h3>
        <div className="pred-grid">
          <div><span className="label">Predicted Price</span><strong>{fmtMoney(insights?.prediction?.predicted_price)}</strong></div>
          <div><span className="label">Current Price</span><strong>{fmtMoney(insights?.prediction?.current_price)}</strong></div>
          <div><span className="label">Direction</span><strong>{insights?.prediction?.predicted_direction === 'UP' ? '↑ UP' : '↓ DOWN'}</strong></div>
        </div>

        <div className="confidence-wrap">
          <span>Confidence {confidence.toFixed(1)}%</span>
          <div className="confidence-bar"><div style={{ width: `${Math.max(0, Math.min(100, confidence))}%` }} /></div>
        </div>

        <div className="metrics-grid">
          <div className="metric"><span>MAE</span><strong>{fmtMoney(insights?.prediction?.mae)}</strong></div>
          <div className="metric"><span>RMSE</span><strong>{fmtMoney(insights?.prediction?.rmse)}</strong></div>
          <div className="metric"><span>Direction Accuracy</span><strong>{(Number(insights?.prediction?.direction_accuracy || 0) * 100).toFixed(2)}%</strong></div>
        </div>

      </article>

      <aside className="card stock-right">
        <h3>Sentiment</h3>
        <SentimentGauge score={sentiment.average_score} />
        <div className="sentiment-score">Score: {Number(sentiment.average_score || 0).toFixed(2)}</div>
        <p className="muted-line">PSX reports analyzed: {sentiment.psx_report_count || 0}</p>

        <div className="breakdown">
          <div className="bar-row pos">
            <label>Positive</label>
            <i><span style={{ width: `${((sentiment.positive_count || 0) / totalSent) * 100}%` }} /></i>
            <em>{sentiment.positive_count || 0}</em>
          </div>
          <div className="bar-row neg">
            <label>Negative</label>
            <i><span style={{ width: `${((sentiment.negative_count || 0) / totalSent) * 100}%` }} /></i>
            <em>{sentiment.negative_count || 0}</em>
          </div>
          <div className="bar-row neu">
            <label>Neutral</label>
            <i><span style={{ width: `${((sentiment.neutral_count || 0) / totalSent) * 100}%` }} /></i>
            <em>{sentiment.neutral_count || 0}</em>
          </div>
        </div>

        <h4>Recent Headlines</h4>
        <ul className="headlines">
          {(sentiment.recent_headlines || []).length ? (sentiment.recent_headlines || []).map((item, idx) => (
            <li key={`${item.headline}-${idx}`}>
              <span>{item.headline}</span>
              <small>{(item.source || 'news').replaceAll('_', ' ')}</small>
              <b className={item.label || 'neutral'}>{item.label || 'neutral'} ({Number(item.score || 0).toFixed(2)})</b>
            </li>
          )) : <li><span>No sentiment headlines found for this symbol yet.</span></li>}
        </ul>

      </aside>
    </section>
  );
}
