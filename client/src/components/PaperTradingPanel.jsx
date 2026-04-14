import React, { useEffect, useMemo, useState } from 'react';
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip } from 'recharts';
import { apiClient } from '../api/client.js';

function fmtMoney(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return '—';
  return n.toFixed(2);
}

function pnlClass(v) {
  return Number(v || 0) >= 0 ? 'pos' : 'neg';
}

export function PaperTradingPanel({ symbol, token }) {
  const [portfolio, setPortfolio] = useState(null);
  const [leaderboard, setLeaderboard] = useState([]);
  const [quantity, setQuantity] = useState(100);
  const [side, setSide] = useState('BUY');
  const [orderType, setOrderType] = useState('MARKET');
  const [limitPrice, setLimitPrice] = useState('');
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');

  const account = portfolio?.account || {
    initial_cash: 0,
    cash_balance: 0,
    equity: 0,
    market_value: 0,
    realized_pnl: 0,
    unrealized_pnl: 0,
    total_pnl: 0
  };

  const positions = useMemo(() => portfolio?.positions || [], [portfolio]);
  const trades = useMemo(() => portfolio?.trades || [], [portfolio]);
  const openOrders = useMemo(() => portfolio?.open_orders || [], [portfolio]);
  const equityHistory = useMemo(() => portfolio?.equity_history || [], [portfolio]);

  const loadPortfolio = async () => {
    setLoading(true);
    setError('');
    try {
      const data = await apiClient.simPortfolio(token);
      setPortfolio(data || null);
    } catch (err) {
      setError(err.message || 'Failed to load simulation portfolio');
    } finally {
      setLoading(false);
    }
  };

  const loadLeaderboard = async () => {
    try {
      const data = await apiClient.simLeaderboard(token, 10);
      setLeaderboard(data?.leaderboard || []);
    } catch {
      setLeaderboard([]);
    }
  };

  useEffect(() => {
    if (!token) return;
    Promise.all([loadPortfolio(), loadLeaderboard()]).catch(() => {});

    const id = setInterval(() => {
      if (!document.hidden) {
        loadPortfolio().catch(() => {});
        loadLeaderboard().catch(() => {});
      }
    }, 60 * 1000);

    return () => clearInterval(id);
  }, [token]);

  const executeTrade = async () => {
    const qty = Number(quantity);
    if (!symbol) {
      setError('Select a symbol first.');
      return;
    }
    if (!Number.isFinite(qty) || qty <= 0) {
      setError('Quantity must be a positive number.');
      return;
    }

    const lim = Number(limitPrice);
    if (orderType === 'LIMIT' && (!Number.isFinite(lim) || lim <= 0)) {
      setError('Limit price must be a positive number.');
      return;
    }

    setSubmitting(true);
    setError('');
    try {
      await apiClient.simPlaceOrder({
        symbol,
        side,
        order_type: orderType,
        quantity: qty,
        ...(orderType === 'LIMIT' ? { limit_price: lim } : {}),
        token
      });
      await loadPortfolio();
      await loadLeaderboard();
    } catch (err) {
      setError(err.message || `${side} ${orderType} order failed`);
    } finally {
      setSubmitting(false);
    }
  };

  const cancelOrder = async (orderId) => {
    setSubmitting(true);
    setError('');
    try {
      await apiClient.simCancelOrder(orderId, token);
      await loadPortfolio();
    } catch (err) {
      setError(err.message || 'Failed to cancel order');
    } finally {
      setSubmitting(false);
    }
  };

  const handleReset = async () => {
    setSubmitting(true);
    setError('');
    try {
      await apiClient.simReset(token);
      await loadPortfolio();
    } catch (err) {
      setError(err.message || 'Failed to reset simulation');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="sim-panel">
      <div className="card dashboard-toolbar" style={{ padding: '12px 20px', marginBottom: '0' }}>
        <div>
          <h3>Paper Trading Simulator</h3>
          <p className="muted-line">Practice buy/sell in a risk-free environment with virtual cash.</p>
        </div>
        <div className="actions">
          <span className="meta" style={{ marginRight: '10px' }}>Selected: <b>{symbol || '—'}</b></span>
          <button className="toggle-btn" onClick={loadPortfolio} disabled={loading || submitting}>Refresh</button>
          <button className="toggle-btn" onClick={handleReset} disabled={submitting}>Reset Simulation</button>
        </div>
      </div>

      {error ? <div className="error-box" style={{ marginBottom: 0 }}>{error}</div> : null}

      <div className="sim-stats">
        <div><span className="label">Cash</span><strong>PKR {fmtMoney(account.cash_balance)}</strong></div>
        <div><span className="label">Equity</span><strong>PKR {fmtMoney(account.equity)}</strong></div>
        <div><span className="label">Market Value</span><strong>PKR {fmtMoney(account.market_value)}</strong></div>
        <div><span className="label">Realized P/L</span><strong className={pnlClass(account.realized_pnl)}>{fmtMoney(account.realized_pnl)}</strong></div>
        <div><span className="label">Unrealized P/L</span><strong className={pnlClass(account.unrealized_pnl)}>{fmtMoney(account.unrealized_pnl)}</strong></div>
        <div><span className="label">Total P/L</span><strong className={pnlClass(account.total_pnl)}>{fmtMoney(account.total_pnl)}</strong></div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'minmax(320px, 1fr) 2fr', gap: '16px', flex: 1, minHeight: 0 }}>
        
        {/* Left Column: Trade & Chart */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px', minHeight: 0 }}>
          <div className="sim-trade-box">
            <div style={{ display: 'flex', gap: '8px', width: '100%' }}>
              <select id="sim-side" value={side} onChange={(e) => setSide(e.target.value)} style={{ flex: 1 }}>
                <option value="BUY">BUY</option>
                <option value="SELL">SELL</option>
              </select>
              <select id="sim-otype" value={orderType} onChange={(e) => setOrderType(e.target.value)} style={{ flex: 1 }}>
                <option value="MARKET">MARKET</option>
                <option value="LIMIT">LIMIT</option>
              </select>
            </div>
            
            <div style={{ display: 'flex', gap: '8px', width: '100%', alignItems: 'center' }}>
              <input type="number" min="1" step="1" value={quantity} onChange={(e) => setQuantity(e.target.value)} placeholder="Qty" style={{ width: '80px' }} />
              {orderType === 'LIMIT' ? (
                <input type="number" min="0" step="0.01" value={limitPrice} onChange={(e) => setLimitPrice(e.target.value)} placeholder="Limit Price" style={{ flex: 1 }} />
              ) : null}
            </div>

            <button className="toggle-btn" onClick={executeTrade} disabled={submitting} style={{ width: '100%', justifyContent: 'center' }}>
              Place {side} Order
            </button>
          </div>

          <div className="card" style={{ flex: 1, display: 'flex', flexDirection: 'column', padding: '16px', minHeight: 0 }}>
            <h4 style={{ margin: '0 0 12px' }}>Equity Curve</h4>
            <div style={{ flex: 1, display: 'flex', alignItems: 'flex-end', minHeight: 0 }}>
              {equityHistory.length ? (
                <div style={{ width: '100%', height: '100%', minHeight: '120px' }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={equityHistory} margin={{ top: 5, right: 0, left: 0, bottom: 0 }}>
                      <defs>
                        <linearGradient id="colorEquity" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#14b8a6" stopOpacity={0.5}/>
                          <stop offset="95%" stopColor="#14b8a6" stopOpacity={0}/>
                        </linearGradient>
                      </defs>
                      <Tooltip
                        contentStyle={{ backgroundColor: '#ffffff', border: '1px solid #e2e8f0', borderRadius: '8px', color: '#0f172a', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                        itemStyle={{ color: '#0d9488', fontWeight: 600 }}
                        formatter={(value) => [fmtMoney(value), 'Equity']}
                        labelFormatter={(label) => `Record ${label}`}
                      />
                      <Area type="monotone" dataKey="equity" stroke="#14b8a6" strokeWidth={2} fillOpacity={1} fill="url(#colorEquity)" />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              ) : <p className="muted-line" style={{ margin: 'auto' }}>No equity history yet.</p>}
            </div>
          </div>
        </div>

        {/* Right Column: Tables Grid */}
        <div className="sim-tables-grid">
          <div className="sim-table-wrap">
            <h4>Open Positions</h4>
            <div className="sim-table-inner">
              <table>
                <thead>
                  <tr><th>Symbol</th><th>Qty</th><th>Avg Cost</th><th>Unrealized</th></tr>
                </thead>
                <tbody>
                  {positions.length ? positions.map((p) => (
                    <tr key={p.symbol}>
                      <td>{p.symbol}</td>
                      <td>{Number(p.quantity || 0).toFixed(0)}</td>
                      <td>{fmtMoney(p.avg_cost)}</td>
                      <td className={pnlClass(p.unrealized_pnl)}>{fmtMoney(p.unrealized_pnl)}</td>
                    </tr>
                  )) : <tr><td colSpan={4}>No open positions.</td></tr>}
                </tbody>
              </table>
            </div>
          </div>

          <div className="sim-table-wrap">
            <h4>Open Orders</h4>
            <div className="sim-table-inner">
              <table>
                <thead>
                  <tr><th>Side/Type</th><th>Symbol</th><th>Qty</th><th>Action</th></tr>
                </thead>
                <tbody>
                  {openOrders.length ? openOrders.map((o) => (
                    <tr key={o.id}>
                      <td className={o.side === 'BUY' ? 'pos' : 'neg'}>{o.side} {o.order_type}</td>
                      <td>{o.symbol}</td>
                      <td>{Number(o.quantity || 0).toFixed(0)}</td>
                      <td><button className="toggle-btn" onClick={() => cancelOrder(o.id)} disabled={submitting}>Cancel</button></td>
                    </tr>
                  )) : <tr><td colSpan={4}>No pending orders.</td></tr>}
                </tbody>
              </table>
            </div>
          </div>

          <div className="sim-table-wrap">
            <h4>Recent Trades</h4>
            <div className="sim-table-inner">
              <table>
                <thead>
                  <tr><th>Side</th><th>Symbol</th><th>Price</th><th>Realized</th></tr>
                </thead>
                <tbody>
                  {trades.length ? trades.map((t) => (
                    <tr key={t.id}>
                      <td className={t.side === 'BUY' ? 'pos' : 'neg'}>{t.side}</td>
                      <td>{t.symbol}</td>
                      <td>{fmtMoney(t.price)}</td>
                      <td className={pnlClass(t.realized_pnl)}>{fmtMoney(t.realized_pnl)}</td>
                    </tr>
                  )) : <tr><td colSpan={4}>No trades yet.</td></tr>}
                </tbody>
              </table>
            </div>
          </div>

          <div className="sim-table-wrap">
            <h4>Leaderboard</h4>
            <div className="sim-table-inner">
              <table>
                <thead>
                  <tr><th>#</th><th>User</th><th>Equity</th></tr>
                </thead>
                <tbody>
                  {leaderboard.length ? leaderboard.map((row) => (
                    <tr key={row.user_id}>
                      <td>{row.rank}</td>
                      <td>{row.name}</td>
                      <td>{fmtMoney(row.equity)}</td>
                    </tr>
                  )) : <tr><td colSpan={3}>No leaderboard data yet.</td></tr>}
                </tbody>
              </table>
            </div>
          </div>
        </div>

      </div>
    </div>
  );
}
