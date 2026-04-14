import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiClient } from '../api/client.js';
import { useAuth } from '../context/AuthContext.jsx';
import { cardClass } from '../lib/constants.js';
import StockSearch from '../components/StockSearch.jsx';

function formatMoney(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return '—';
  return `PKR ${n.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

function formatQty(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return '0';
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function pnlClass(value) {
  return Number(value || 0) >= 0 ? 'text-green-400' : 'text-red-400';
}

function SideBadge({ side }) {
  const isBuy = String(side || '').toUpperCase() === 'BUY';
  return (
    <span className={`px-2 py-0.5 rounded border text-xs font-semibold ${isBuy ? 'text-green-300 border-green-500/40 bg-green-500/20' : 'text-red-300 border-red-500/40 bg-red-500/20'}`}>
      {isBuy ? 'BUY' : 'SELL'}
    </span>
  );
}

function SummaryCard({ label, value, accentClass = 'text-white', subtitle }) {
  return (
    <div className={`${cardClass} p-4 border-slate-700/60`}>
      <p className="text-slate-400 text-xs uppercase tracking-wider">{label}</p>
      <p className={`text-xl font-bold mt-1 ${accentClass}`}>{value}</p>
      {subtitle ? <p className="text-xs text-slate-500 mt-1">{subtitle}</p> : null}
    </div>
  );
}

function TableWrapper({ title, children }) {
  return (
    <section className={`${cardClass} border-slate-700/60 overflow-hidden`}>
      <div className="px-4 py-3 border-b border-slate-700/60">
        <h3 className="text-slate-100 font-semibold">{title}</h3>
      </div>
      <div className="overflow-x-auto">{children}</div>
    </section>
  );
}

export default function Simulator() {
  const { token } = useAuth();
  const [portfolio, setPortfolio] = useState(null);
  const [trades, setTrades] = useState([]);
  const [orders, setOrders] = useState([]);
  const [leaderboard, setLeaderboard] = useState([]);
  const [stocks, setStocks] = useState([]);

  const [symbol, setSymbol] = useState('');
  const [side, setSide] = useState('BUY');
  const [orderType, setOrderType] = useState('MARKET');
  const [quantity, setQuantity] = useState(100);
  const [limitPrice, setLimitPrice] = useState('');

  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');

  const loadSimulatorData = useCallback(async (silent = false) => {
    if (!token) return;
    if (!silent) setLoading(true);

    try {
      const [portfolioRes, tradesRes, ordersRes, leaderboardRes, stocksRes] = await Promise.all([
        apiClient.simPortfolio(token),
        apiClient.simTrades(token, 80),
        apiClient.simOrders(token, 80),
        apiClient.simLeaderboard(token, 10),
        apiClient.stocks(),
      ]);

      setPortfolio(portfolioRes || null);
      setTrades(Array.isArray(tradesRes?.trades) ? tradesRes.trades : []);
      setOrders(Array.isArray(ordersRes?.orders) ? ordersRes.orders : []);
      setLeaderboard(Array.isArray(leaderboardRes?.leaderboard) ? leaderboardRes.leaderboard : []);
      setStocks(Array.isArray(stocksRes?.snapshots) ? stocksRes.snapshots : []);

      setSymbol((prev) => {
        if (prev) return prev;
        const firstFromPos = portfolioRes?.positions?.[0]?.symbol;
        const firstFromStocks = stocksRes?.snapshots?.[0]?.symbol;
        return firstFromPos || firstFromStocks || '';
      });

      setError('');
    } catch (err) {
      setError(err.message || 'Failed to load simulator data');
    } finally {
      if (!silent) setLoading(false);
    }
  }, [token]);

  useEffect(() => {
    loadSimulatorData();
  }, [loadSimulatorData]);

  useEffect(() => {
    if (!token) return undefined;
    const id = setInterval(() => {
      if (!document.hidden) loadSimulatorData(true).catch(() => {});
    }, 60 * 1000);
    return () => clearInterval(id);
  }, [loadSimulatorData, token]);

  const account = portfolio?.account || {
    initial_cash: 0,
    cash_balance: 0,
    equity: 0,
    market_value: 0,
    realized_pnl: 0,
    unrealized_pnl: 0,
    total_pnl: 0,
  };

  const positions = useMemo(() => portfolio?.positions || [], [portfolio]);

  const totalExposure = useMemo(() => {
    return positions.reduce((sum, p) => sum + Math.abs(Number(p.market_value || 0)), 0);
  }, [positions]);

  const largestPositionPct = useMemo(() => {
    const eq = Number(account.equity || 0);
    if (!eq) return 0;
    const largest = positions.reduce((mx, p) => Math.max(mx, Math.abs(Number(p.market_value || 0))), 0);
    return (largest / eq) * 100;
  }, [account.equity, positions]);

  const selectedSnapshot = useMemo(() => {
    return stocks.find((s) => String(s.symbol || '').toUpperCase() === String(symbol || '').toUpperCase()) || null;
  }, [stocks, symbol]);

  const estimatedOrderValue = useMemo(() => {
    const qty = Number(quantity || 0);
    if (!Number.isFinite(qty) || qty <= 0) return null;

    const px = orderType === 'LIMIT'
      ? Number(limitPrice || 0)
      : Number(selectedSnapshot?.close || 0);

    if (!Number.isFinite(px) || px <= 0) return null;
    return qty * px;
  }, [quantity, limitPrice, orderType, selectedSnapshot?.close]);

  const submitOrder = async () => {
    const qty = Number(quantity);
    const lim = Number(limitPrice);

    if (!symbol) {
      setError('Please select a stock symbol first.');
      return;
    }
    if (!Number.isFinite(qty) || qty <= 0) {
      setError('Quantity must be a positive number.');
      return;
    }
    if (orderType === 'LIMIT' && (!Number.isFinite(lim) || lim <= 0)) {
      setError('Limit price must be a positive number.');
      return;
    }

    setSubmitting(true);
    setError('');
    setNotice('');

    try {
      await apiClient.simPlaceOrder({
        symbol: String(symbol).toUpperCase(),
        side,
        order_type: orderType,
        quantity: qty,
        ...(orderType === 'LIMIT' ? { limit_price: lim } : {}),
        token,
      });
      setNotice(`${side} ${orderType} order placed for ${symbol}.`);
      await loadSimulatorData(true);
    } catch (err) {
      setError(err.message || 'Order submission failed');
    } finally {
      setSubmitting(false);
    }
  };

  const cancelOrder = async (orderId) => {
    setSubmitting(true);
    setError('');
    setNotice('');
    try {
      await apiClient.simCancelOrder(orderId, token);
      setNotice('Order cancelled successfully.');
      await loadSimulatorData(true);
    } catch (err) {
      setError(err.message || 'Failed to cancel order');
    } finally {
      setSubmitting(false);
    }
  };

  const resetSimulation = async () => {
    setSubmitting(true);
    setError('');
    setNotice('');
    try {
      await apiClient.simReset(token);
      setNotice('Simulation reset complete.');
      await loadSimulatorData(true);
    } catch (err) {
      setError(err.message || 'Failed to reset simulation');
    } finally {
      setSubmitting(false);
    }
  };

  const pendingOrders = orders.filter((o) => String(o.status || '').toUpperCase() === 'PENDING');

  return (
    <div className="max-w-7xl mx-auto px-4 py-8 space-y-5">
      <section className={`${cardClass} p-5 border-slate-600/50`}>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-white">Simulator</h1>
            <p className="text-slate-400 text-sm mt-1">Advanced paper trading in real-time with portfolio analytics, live orders, and P/L tracking.</p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => loadSimulatorData()}
              disabled={loading || submitting}
              className="px-3 py-2 rounded-lg border border-slate-600 text-slate-200 hover:bg-slate-700/60 text-sm disabled:opacity-50"
            >
              Refresh
            </button>
            <button
              type="button"
              onClick={resetSimulation}
              disabled={submitting}
              className="px-3 py-2 rounded-lg border border-red-500/40 text-red-300 hover:bg-red-500/10 text-sm disabled:opacity-50"
            >
              Reset
            </button>
          </div>
        </div>

        {error ? <p className="mt-3 text-sm text-red-400">{error}</p> : null}
        {!error && notice ? <p className="mt-3 text-sm text-green-400">{notice}</p> : null}
      </section>

      <section className="grid sm:grid-cols-2 xl:grid-cols-4 gap-3">
        <SummaryCard label="Cash" value={formatMoney(account.cash_balance)} />
        <SummaryCard label="Equity" value={formatMoney(account.equity)} />
        <SummaryCard label="Market Value" value={formatMoney(account.market_value)} />
        <SummaryCard label="Total P/L" value={formatMoney(account.total_pnl)} accentClass={pnlClass(account.total_pnl)} subtitle={`Realized: ${formatMoney(account.realized_pnl)} · Unrealized: ${formatMoney(account.unrealized_pnl)}`} />
      </section>

      <section className="grid lg:grid-cols-[360px_minmax(0,1fr)] gap-4 items-start">
        <div>
          <div className={`${cardClass} p-4 border-slate-700/60 space-y-3`}>
            <h2 className="text-slate-100 font-semibold">Place Order</h2>

            <div>
              <label className="text-xs text-slate-400 block mb-1">Symbol</label>
              <StockSearch
                placeholder="Search symbol..."
                onSelect={(ticker) => setSymbol(ticker)}
              />
              {symbol ? <p className="text-xs text-slate-500 mt-1">Selected: <span className="text-brand-300 font-mono">{symbol}</span></p> : null}
            </div>

            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="text-xs text-slate-400 block mb-1">Side</label>
                <select
                  value={side}
                  onChange={(e) => setSide(e.target.value)}
                  className="w-full bg-slate-900/40 border border-slate-600 rounded-lg px-3 py-2 text-slate-100"
                >
                  <option value="BUY">BUY</option>
                  <option value="SELL">SELL</option>
                </select>
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Order Type</label>
                <select
                  value={orderType}
                  onChange={(e) => setOrderType(e.target.value)}
                  className="w-full bg-slate-900/40 border border-slate-600 rounded-lg px-3 py-2 text-slate-100"
                >
                  <option value="MARKET">MARKET</option>
                  <option value="LIMIT">LIMIT</option>
                </select>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="text-xs text-slate-400 block mb-1">Quantity</label>
                <input
                  type="number"
                  min="1"
                  value={quantity}
                  onChange={(e) => setQuantity(e.target.value)}
                  className="w-full bg-slate-900/40 border border-slate-600 rounded-lg px-3 py-2 text-slate-100"
                />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Limit Price</label>
                <input
                  type="number"
                  min="0"
                  step="0.01"
                  value={limitPrice}
                  onChange={(e) => setLimitPrice(e.target.value)}
                  disabled={orderType !== 'LIMIT'}
                  placeholder={orderType === 'LIMIT' ? 'Enter price' : 'Market'}
                  className="w-full bg-slate-900/40 border border-slate-600 rounded-lg px-3 py-2 text-slate-100 disabled:opacity-50"
                />
              </div>
            </div>

            <div className="flex items-center gap-2">
              {[10, 50, 100, 500].map((preset) => (
                <button
                  key={preset}
                  type="button"
                  onClick={() => setQuantity(preset)}
                  className="px-2.5 py-1.5 text-xs rounded border border-slate-600 text-slate-300 hover:bg-slate-700/50"
                >
                  {preset}
                </button>
              ))}
            </div>

            <div className="rounded-lg border border-slate-700/60 bg-slate-900/30 p-3 space-y-1 text-xs">
              <p className="text-slate-400">Estimated Order Value</p>
              <p className="text-slate-100 font-semibold">{estimatedOrderValue != null ? formatMoney(estimatedOrderValue) : '—'}</p>
              <p className="text-slate-500">Exposure: {formatMoney(totalExposure)} · Largest Position: {largestPositionPct.toFixed(2)}%</p>
            </div>

            <button
              type="button"
              onClick={submitOrder}
              disabled={submitting || loading}
              className="w-full py-2.5 rounded-lg bg-brand-600 hover:bg-brand-500 text-white font-semibold disabled:opacity-50"
            >
              {submitting ? 'Processing…' : `Place ${side} ${orderType}`}
            </button>
          </div>
        </div>

        <div className="space-y-4">
          <TableWrapper title="Open Positions">
            <table className="w-full text-sm">
              <thead className="bg-slate-900/40 text-slate-400">
                <tr>
                  <th className="text-left px-4 py-2">Symbol</th>
                  <th className="text-right px-4 py-2">Qty</th>
                  <th className="text-right px-4 py-2">Avg Cost</th>
                  <th className="text-right px-4 py-2">Mkt Value</th>
                  <th className="text-right px-4 py-2">Unrealized</th>
                </tr>
              </thead>
              <tbody>
                {positions.length ? positions.map((p) => (
                  <tr key={p.symbol} className="border-t border-slate-700/50">
                    <td className="px-4 py-2 font-mono text-brand-300">{p.symbol}</td>
                    <td className="px-4 py-2 text-right text-slate-200">{formatQty(p.quantity)}</td>
                    <td className="px-4 py-2 text-right text-slate-300">{formatMoney(p.avg_cost)}</td>
                    <td className="px-4 py-2 text-right text-slate-300">{formatMoney(p.market_value)}</td>
                    <td className={`px-4 py-2 text-right font-semibold ${pnlClass(p.unrealized_pnl)}`}>{formatMoney(p.unrealized_pnl)}</td>
                  </tr>
                )) : (
                  <tr><td className="px-4 py-3 text-slate-500" colSpan={5}>No open positions.</td></tr>
                )}
              </tbody>
            </table>
          </TableWrapper>

          <TableWrapper title="Open Orders">
            <table className="w-full text-sm">
              <thead className="bg-slate-900/40 text-slate-400">
                <tr>
                  <th className="text-left px-4 py-2">Order</th>
                  <th className="text-left px-4 py-2">Symbol</th>
                  <th className="text-right px-4 py-2">Qty</th>
                  <th className="text-right px-4 py-2">Limit</th>
                  <th className="text-right px-4 py-2">Action</th>
                </tr>
              </thead>
              <tbody>
                {pendingOrders.length ? pendingOrders.map((o) => (
                  <tr key={o.id} className="border-t border-slate-700/50">
                    <td className="px-4 py-2"><SideBadge side={o.side} /> <span className="text-slate-400 text-xs ml-1">{o.order_type}</span></td>
                    <td className="px-4 py-2 font-mono text-brand-300">{o.symbol}</td>
                    <td className="px-4 py-2 text-right text-slate-200">{formatQty(o.quantity)}</td>
                    <td className="px-4 py-2 text-right text-slate-300">{o.limit_price ? formatMoney(o.limit_price) : '—'}</td>
                    <td className="px-4 py-2 text-right">
                      <button
                        type="button"
                        onClick={() => cancelOrder(o.id)}
                        disabled={submitting}
                        className="px-2.5 py-1.5 rounded border border-red-500/40 text-red-300 hover:bg-red-500/10 text-xs disabled:opacity-50"
                      >
                        Cancel
                      </button>
                    </td>
                  </tr>
                )) : (
                  <tr><td className="px-4 py-3 text-slate-500" colSpan={5}>No pending orders.</td></tr>
                )}
              </tbody>
            </table>
          </TableWrapper>

          <section className="grid xl:grid-cols-2 gap-4">
            <TableWrapper title="Recent Trades">
              <table className="w-full text-sm">
                <thead className="bg-slate-900/40 text-slate-400">
                  <tr>
                    <th className="text-left px-4 py-2">Side</th>
                    <th className="text-left px-4 py-2">Symbol</th>
                    <th className="text-right px-4 py-2">Price</th>
                    <th className="text-right px-4 py-2">Realized</th>
                  </tr>
                </thead>
                <tbody>
                  {trades.length ? trades.slice(0, 20).map((t) => (
                    <tr key={t.id} className="border-t border-slate-700/50">
                      <td className="px-4 py-2"><SideBadge side={t.side} /></td>
                      <td className="px-4 py-2 font-mono text-brand-300">{t.symbol}</td>
                      <td className="px-4 py-2 text-right text-slate-200">{formatMoney(t.price)}</td>
                      <td className={`px-4 py-2 text-right font-semibold ${pnlClass(t.realized_pnl)}`}>{formatMoney(t.realized_pnl)}</td>
                    </tr>
                  )) : (
                    <tr><td className="px-4 py-3 text-slate-500" colSpan={4}>No trades yet.</td></tr>
                  )}
                </tbody>
              </table>
            </TableWrapper>

            <TableWrapper title="Top Traders Leaderboard">
              <table className="w-full text-sm">
                <thead className="bg-slate-900/40 text-slate-400">
                  <tr>
                    <th className="text-left px-4 py-2">Rank</th>
                    <th className="text-left px-4 py-2">Trader</th>
                    <th className="text-right px-4 py-2">Equity</th>
                  </tr>
                </thead>
                <tbody>
                  {leaderboard.length ? leaderboard.map((row) => (
                    <tr key={row.user_id} className="border-t border-slate-700/50">
                      <td className="px-4 py-2 text-slate-200">#{row.rank}</td>
                      <td className="px-4 py-2 text-slate-200">{row.name}</td>
                      <td className="px-4 py-2 text-right font-semibold text-brand-300">{formatMoney(row.equity)}</td>
                    </tr>
                  )) : (
                    <tr><td className="px-4 py-3 text-slate-500" colSpan={3}>No leaderboard data yet.</td></tr>
                  )}
                </tbody>
              </table>
            </TableWrapper>
          </section>
        </div>
      </section>

      {loading ? <p className="text-slate-500 text-sm">Loading simulator data…</p> : null}
    </div>
  );
}
