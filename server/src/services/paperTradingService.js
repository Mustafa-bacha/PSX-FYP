import { db } from '../lib/db.js';

const DEFAULT_INITIAL_CASH = 1_000_000;

function normalizeSymbol(symbol) {
  return String(symbol || '').trim().toUpperCase();
}

function normalizeQty(quantity) {
  const q = Number(quantity);
  if (!Number.isFinite(q) || q <= 0) return null;
  return q;
}

function normalizeOrderType(value) {
  const t = String(value || 'MARKET').trim().toUpperCase();
  return t === 'LIMIT' ? 'LIMIT' : 'MARKET';
}

function normalizeSide(value) {
  const s = String(value || '').trim().toUpperCase();
  if (s !== 'BUY' && s !== 'SELL') return null;
  return s;
}

function getLatestPrice(symbol) {
  return db.prepare(`
    SELECT symbol, close, date
    FROM stocks
    WHERE symbol = ?
    ORDER BY date DESC
    LIMIT 1
  `).get(symbol);
}

function ensureAccount(userId) {
  let account = db.prepare(`
    SELECT user_id, initial_cash, cash_balance, created_at, updated_at
    FROM user_sim_accounts
    WHERE user_id = ?
  `).get(userId);

  if (!account) {
    db.prepare(`
      INSERT INTO user_sim_accounts (user_id, initial_cash, cash_balance)
      VALUES (?, ?, ?)
    `).run(userId, DEFAULT_INITIAL_CASH, DEFAULT_INITIAL_CASH);

    account = db.prepare(`
      SELECT user_id, initial_cash, cash_balance, created_at, updated_at
      FROM user_sim_accounts
      WHERE user_id = ?
    `).get(userId);
  }

  return account;
}

function getPosition(userId, symbol) {
  return db.prepare(`
    SELECT id, user_id, symbol, quantity, avg_cost, realized_pnl, updated_at
    FROM user_sim_positions
    WHERE user_id = ? AND symbol = ?
  `).get(userId, symbol);
}

function upsertPosition({ userId, symbol, quantity, avgCost, realizedPnl }) {
  db.prepare(`
    INSERT INTO user_sim_positions (user_id, symbol, quantity, avg_cost, realized_pnl, updated_at)
    VALUES (?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(user_id, symbol)
    DO UPDATE SET
      quantity = excluded.quantity,
      avg_cost = excluded.avg_cost,
      realized_pnl = excluded.realized_pnl,
      updated_at = datetime('now')
  `).run(userId, symbol, quantity, avgCost, realizedPnl);
}

function updateCash(userId, nextCash) {
  db.prepare(`
    UPDATE user_sim_accounts
    SET cash_balance = ?, updated_at = datetime('now')
    WHERE user_id = ?
  `).run(nextCash, userId);
}

function addTrade({ userId, symbol, side, quantity, price, notional, realizedPnl = 0 }) {
  db.prepare(`
    INSERT INTO user_sim_trades (user_id, symbol, side, quantity, price, notional, realized_pnl)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(userId, symbol, side, quantity, price, notional, realizedPnl);
}

function addOrder({
  userId,
  symbol,
  side,
  orderType,
  quantity,
  limitPrice = null,
  status = 'PENDING',
  filledPrice = null,
  note = null
}) {
  const result = db.prepare(`
    INSERT INTO user_sim_orders (
      user_id, symbol, side, order_type, quantity, limit_price,
      status, filled_price, note, created_at, updated_at, filled_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), CASE WHEN ?='FILLED' THEN datetime('now') ELSE NULL END)
  `).run(
    userId,
    symbol,
    side,
    orderType,
    quantity,
    limitPrice,
    status,
    filledPrice,
    note,
    status
  );
  return Number(result.lastInsertRowid || 0);
}

function updateOrderStatus(orderId, { status, filledPrice = null, note = null }) {
  db.prepare(`
    UPDATE user_sim_orders
    SET status = ?,
        filled_price = COALESCE(?, filled_price),
        note = COALESCE(?, note),
        updated_at = datetime('now'),
        filled_at = CASE WHEN ?='FILLED' THEN datetime('now') ELSE filled_at END
    WHERE id = ?
  `).run(status, filledPrice, note, status, orderId);
}

function listPendingOrders(userId) {
  return db.prepare(`
    SELECT id, user_id, symbol, side, order_type, quantity, limit_price, status, created_at
    FROM user_sim_orders
    WHERE user_id = ? AND status = 'PENDING'
    ORDER BY id ASC
  `).all(userId);
}

function maybeRecordEquitySnapshot(userId, equity) {
  const recent = db.prepare(`
    SELECT id
    FROM user_sim_equity_snapshots
    WHERE user_id = ?
      AND (julianday('now') - julianday(created_at)) * 1440 < 10
    ORDER BY id DESC
    LIMIT 1
  `).get(userId);

  if (recent) return;

  db.prepare(`
    INSERT INTO user_sim_equity_snapshots (user_id, equity)
    VALUES (?, ?)
  `).run(userId, equity);
}

function executeImmediateTrade(userId, symbol, side, quantity, priceInput = null) {
  const latest = getLatestPrice(symbol);
  if (!latest || latest.close == null) throw new Error(`No live price found for ${symbol}`);

  const price = Number(priceInput ?? latest.close);
  if (!Number.isFinite(price) || price <= 0) throw new Error(`Invalid execution price for ${symbol}`);
  const notional = price * quantity;

  const tx = db.transaction(() => {
    const account = ensureAccount(userId);
    const cash = Number(account.cash_balance || 0);
    const pos = getPosition(userId, symbol);
    const prevQty = Number(pos?.quantity || 0);
    const prevAvg = Number(pos?.avg_cost || 0);
    const prevRealized = Number(pos?.realized_pnl || 0);

    if (side === 'BUY') {
      if (cash < notional) {
        throw new Error(`Insufficient virtual cash. Required ${notional.toFixed(2)}, available ${cash.toFixed(2)}`);
      }

      const nextQty = prevQty + quantity;
      const nextAvg = nextQty > 0 ? ((prevQty * prevAvg) + (quantity * price)) / nextQty : 0;

      upsertPosition({ userId, symbol, quantity: nextQty, avgCost: nextAvg, realizedPnl: prevRealized });
      updateCash(userId, cash - notional);
      addTrade({ userId, symbol, side: 'BUY', quantity, price, notional, realizedPnl: 0 });
      return { price, notional, realized_pnl: 0 };
    }

    if (!pos || prevQty <= 0) throw new Error(`No open simulated position to sell for ${symbol}`);
    if (quantity > prevQty) throw new Error(`Cannot sell ${quantity}. Available quantity is ${prevQty}`);

    const realizedDelta = (price - prevAvg) * quantity;
    const nextQty = prevQty - quantity;
    const nextAvg = nextQty > 0 ? prevAvg : 0;
    const nextRealized = prevRealized + realizedDelta;

    upsertPosition({ userId, symbol, quantity: nextQty, avgCost: nextAvg, realizedPnl: nextRealized });
    updateCash(userId, cash + notional);
    addTrade({ userId, symbol, side: 'SELL', quantity, price, notional, realizedPnl: realizedDelta });
    return { price, notional, realized_pnl: realizedDelta };
  });

  return tx();
}

function shouldFillLimit(order, latestPrice) {
  const side = String(order.side || '').toUpperCase();
  const limit = Number(order.limit_price || 0);
  if (!Number.isFinite(limit) || limit <= 0) return false;
  if (side === 'BUY') return latestPrice <= limit;
  if (side === 'SELL') return latestPrice >= limit;
  return false;
}

function processPendingOrdersForUser(userId) {
  const orders = listPendingOrders(userId);
  let filled = 0;
  let rejected = 0;

  for (const order of orders) {
    const symbol = normalizeSymbol(order.symbol);
    const latest = getLatestPrice(symbol);
    const marketPrice = Number(latest?.close || 0);
    if (!latest || !Number.isFinite(marketPrice) || marketPrice <= 0) continue;
    if (!shouldFillLimit(order, marketPrice)) continue;

    try {
      const limitPrice = Number(order.limit_price || 0);
      const executionPrice = Number.isFinite(limitPrice) && limitPrice > 0 ? limitPrice : marketPrice;
      executeImmediateTrade(userId, symbol, String(order.side || 'BUY').toUpperCase(), Number(order.quantity || 0), executionPrice);
      updateOrderStatus(order.id, {
        status: 'FILLED',
        filledPrice: executionPrice,
        note: `Filled at ${executionPrice.toFixed(2)} (${String(order.order_type || 'LIMIT').toUpperCase()})`
      });
      filled += 1;
    } catch (err) {
      updateOrderStatus(order.id, {
        status: 'REJECTED',
        note: String(err?.message || err)
      });
      rejected += 1;
    }
  }

  return { checked: orders.length, filled, rejected };
}

export function buySimulatedAsset(userId, symbolInput, quantityInput) {
  const symbol = normalizeSymbol(symbolInput);
  const quantity = normalizeQty(quantityInput);
  if (!symbol) throw new Error('symbol is required');
  if (!quantity) throw new Error('quantity must be a positive number');
  const trade = executeImmediateTrade(userId, symbol, 'BUY', quantity);
  addOrder({ userId, symbol, side: 'BUY', orderType: 'MARKET', quantity, status: 'FILLED', filledPrice: trade?.price ?? null, note: 'Executed immediately' });
  return getSimulationPortfolio(userId);
}

export function sellSimulatedAsset(userId, symbolInput, quantityInput) {
  const symbol = normalizeSymbol(symbolInput);
  const quantity = normalizeQty(quantityInput);
  if (!symbol) throw new Error('symbol is required');
  if (!quantity) throw new Error('quantity must be a positive number');

  const trade = executeImmediateTrade(userId, symbol, 'SELL', quantity);
  addOrder({ userId, symbol, side: 'SELL', orderType: 'MARKET', quantity, status: 'FILLED', filledPrice: trade?.price ?? null, note: 'Executed immediately' });
  return getSimulationPortfolio(userId);
}

export function placeSimulatedOrder(userId, payload = {}) {
  const symbol = normalizeSymbol(payload.symbol);
  const side = normalizeSide(payload.side);
  const orderType = normalizeOrderType(payload.order_type);
  const quantity = normalizeQty(payload.quantity);
  const limitPrice = payload.limit_price == null ? null : Number(payload.limit_price);

  if (!symbol) throw new Error('symbol is required');
  if (!side) throw new Error('side must be BUY or SELL');
  if (!quantity) throw new Error('quantity must be a positive number');

  if (orderType === 'MARKET') {
    const trade = executeImmediateTrade(userId, symbol, side, quantity);
    const orderId = addOrder({ userId, symbol, side, orderType, quantity, status: 'FILLED', filledPrice: trade?.price ?? null, note: 'Executed immediately' });
    return { order_id: orderId, status: 'FILLED', ...getSimulationPortfolio(userId) };
  }

  if (!Number.isFinite(limitPrice) || limitPrice <= 0) {
    throw new Error('limit_price must be a positive number for LIMIT order');
  }

  const orderId = addOrder({
    userId,
    symbol,
    side,
    orderType,
    quantity,
    limitPrice,
    status: 'PENDING',
    note: `Waiting for limit ${limitPrice}`
  });

  processPendingOrdersForUser(userId);
  return { order_id: orderId, status: 'PENDING', ...getSimulationPortfolio(userId) };
}

export function listSimulationOrders(userId, limit = 30, status = null) {
  ensureAccount(userId);
  processPendingOrdersForUser(userId);
  const whereStatus = status ? 'AND status = ?' : '';
  const args = status
    ? [userId, String(status).toUpperCase(), Math.max(1, Number(limit || 30))]
    : [userId, Math.max(1, Number(limit || 30))];

  return db.prepare(`
    SELECT id, symbol, side, order_type, quantity, limit_price, status, filled_price, note, created_at, updated_at, filled_at
    FROM user_sim_orders
    WHERE user_id = ? ${whereStatus}
    ORDER BY id DESC
    LIMIT ?
  `).all(...args);
}

export function cancelSimulationOrder(userId, orderIdInput) {
  const orderId = Number(orderIdInput || 0);
  if (!Number.isFinite(orderId) || orderId <= 0) throw new Error('valid order_id is required');

  const order = db.prepare(`
    SELECT id, status
    FROM user_sim_orders
    WHERE id = ? AND user_id = ?
  `).get(orderId, userId);

  if (!order) throw new Error('order not found');
  if (order.status !== 'PENDING') throw new Error('only pending orders can be cancelled');

  updateOrderStatus(orderId, { status: 'CANCELLED', note: 'Cancelled by user' });
  return { order_id: orderId, status: 'CANCELLED' };
}

export function listSimulationTrades(userId, limit = 30) {
  ensureAccount(userId);
  return db.prepare(`
    SELECT id, symbol, side, quantity, price, notional, realized_pnl, created_at
    FROM user_sim_trades
    WHERE user_id = ?
    ORDER BY id DESC
    LIMIT ?
  `).all(userId, Math.max(1, Number(limit || 30)));
}

export function resetSimulationPortfolio(userId) {
  const tx = db.transaction(() => {
    ensureAccount(userId);
    db.prepare('DELETE FROM user_sim_positions WHERE user_id = ?').run(userId);
    db.prepare('DELETE FROM user_sim_trades WHERE user_id = ?').run(userId);
    db.prepare('DELETE FROM user_sim_orders WHERE user_id = ?').run(userId);
    db.prepare('DELETE FROM user_sim_equity_snapshots WHERE user_id = ?').run(userId);
    db.prepare(`
      UPDATE user_sim_accounts
      SET cash_balance = initial_cash,
          updated_at = datetime('now')
      WHERE user_id = ?
    `).run(userId);
  });

  tx();
  return getSimulationPortfolio(userId);
}

export function getSimulationPortfolio(userId) {
  processPendingOrdersForUser(userId);
  const account = ensureAccount(userId);

  const positions = db.prepare(`
    SELECT symbol, quantity, avg_cost, realized_pnl, updated_at
    FROM user_sim_positions
    WHERE user_id = ? AND quantity > 0
    ORDER BY symbol ASC
  `).all(userId);

  const positionRows = positions.map((p) => {
    const latest = getLatestPrice(p.symbol);
    const marketPrice = Number(latest?.close || 0);
    const quantity = Number(p.quantity || 0);
    const avgCost = Number(p.avg_cost || 0);
    const marketValue = marketPrice * quantity;
    const costValue = avgCost * quantity;
    const unrealizedPnl = marketValue - costValue;

    return {
      symbol: p.symbol,
      quantity,
      avg_cost: avgCost,
      market_price: marketPrice,
      market_value: marketValue,
      unrealized_pnl: unrealizedPnl,
      realized_pnl: Number(p.realized_pnl || 0),
      last_price_date: latest?.date || null
    };
  });

  const realized = db.prepare(`
    SELECT COALESCE(SUM(realized_pnl), 0) AS realized
    FROM user_sim_positions
    WHERE user_id = ?
  `).get(userId);

  const realizedPnl = Number(realized?.realized || 0);
  const unrealizedPnl = positionRows.reduce((s, p) => s + Number(p.unrealized_pnl || 0), 0);
  const marketValue = positionRows.reduce((s, p) => s + Number(p.market_value || 0), 0);
  const cash = Number(account.cash_balance || 0);
  const equity = cash + marketValue;

  maybeRecordEquitySnapshot(userId, equity);

  const equityHistory = db.prepare(`
    SELECT created_at, equity
    FROM user_sim_equity_snapshots
    WHERE user_id = ?
    ORDER BY id DESC
    LIMIT 40
  `).all(userId).reverse();

  const openOrders = db.prepare(`
    SELECT id, symbol, side, order_type, quantity, limit_price, status, created_at
    FROM user_sim_orders
    WHERE user_id = ? AND status = 'PENDING'
    ORDER BY id DESC
    LIMIT 20
  `).all(userId);

  return {
    account: {
      initial_cash: Number(account.initial_cash || DEFAULT_INITIAL_CASH),
      cash_balance: cash,
      equity,
      market_value: marketValue,
      realized_pnl: realizedPnl,
      unrealized_pnl: unrealizedPnl,
      total_pnl: realizedPnl + unrealizedPnl
    },
    positions: positionRows,
    trades: listSimulationTrades(userId, 20),
    open_orders: openOrders,
    equity_history: equityHistory
  };
}

export function getSimulationLeaderboard(limit = 20) {
  const users = db.prepare(`
    SELECT id, email, full_name
    FROM users
    ORDER BY id ASC
  `).all();

  const rows = users.map((u) => {
    const p = getSimulationPortfolio(u.id);
    return {
      user_id: u.id,
      name: u.full_name || u.email,
      email: u.email,
      equity: Number(p.account?.equity || 0),
      total_pnl: Number(p.account?.total_pnl || 0),
      realized_pnl: Number(p.account?.realized_pnl || 0),
      unrealized_pnl: Number(p.account?.unrealized_pnl || 0)
    };
  }).sort((a, b) => b.total_pnl - a.total_pnl)
    .slice(0, Math.max(1, Number(limit || 20)))
    .map((x, i) => ({ rank: i + 1, ...x }));

  return rows;
}
