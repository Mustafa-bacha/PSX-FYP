import { Router } from 'express';
import { getStockHistory, getStockInsights, getStockOverview, listStocks, refreshPricesForSymbols } from '../services/stocksService.js';
import { getMarketPerformers } from '../services/marketPerformersService.js';
import { generateChatReply } from '../services/chatService.js';
import { config } from '../config.js';
import { optionalAuth, requireAuth } from '../middleware/authMiddleware.js';
import { db } from '../lib/db.js';
import { addUserChatMessage, clearUserChatHistory, listUserChatHistory } from '../services/userChatService.js';
import { getLatestDbDate, runStartupHistoricalSync, syncMissingHistoricalData } from '../services/historicalSyncService.js';
import { getFocusSymbols } from '../services/focusSymbolsService.js';
import { warmupModelsForFocus } from '../services/modelWarmupService.js';
import { runIncrementalAppendAndRetrain } from '../services/modelIncrementalService.js';
import {
  getFocusRefreshStatus,
  runFocusRefreshBatchNow,
  startFocusRefreshScheduler,
  stopFocusRefreshScheduler
} from '../services/focusRefreshSchedulerService.js';
import {
  getSentimentSchedulerStatus,
  runSentimentFullCycleNow,
  runSentimentNow,
  startSentimentScheduler,
  stopSentimentScheduler
} from '../services/sentimentSchedulerService.js';
import { getMarketSummary } from '../services/marketSummaryService.js';
import { getMarketSummarySchedulerStatus } from '../services/marketSummarySchedulerService.js';
import { getLatestBusinessNewsFeed } from '../services/sentimentIngestionService.js';
import {
  addUserWatchlistSymbol,
  listUserWatchlist,
  removeUserWatchlistSymbol
} from '../services/watchlistService.js';
import {
  buySimulatedAsset,
  cancelSimulationOrder,
  getSimulationPortfolio,
  getSimulationLeaderboard,
  listSimulationOrders,
  listSimulationTrades,
  placeSimulatedOrder,
  resetSimulationPortfolio,
  sellSimulatedAsset
} from '../services/paperTradingService.js';

export const stocksRouter = Router();

stocksRouter.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'express-stock-api', ts: new Date().toISOString() });
});

stocksRouter.get('/stocks', (req, res) => {
  const limit = Number(req.query.limit || 600);
  res.json(listStocks(limit));
});

stocksRouter.get('/stock/:symbol', (req, res) => {
  const days = Number(req.query.days || 365);
  const payload = getStockHistory(req.params.symbol, days);
  if (!payload) return res.status(404).json({ error: 'Symbol not found' });
  return res.json(payload);
});

stocksRouter.get('/stock/:symbol/insights', (req, res) => {
  const payload = getStockInsights(req.params.symbol);
  if (!payload) return res.status(404).json({ error: 'Symbol not found' });
  return res.json(payload);
});

stocksRouter.get('/stock/:symbol/overview', async (req, res) => {
  try {
    const refresh = ['1', 'true', 'yes'].includes(String(req.query.refresh || '').toLowerCase());
    const payload = await getStockOverview(req.params.symbol, { refresh });
    if (!payload) return res.status(404).json({ error: 'Symbol not found' });
    return res.json(payload);
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.get('/market-performers', async (req, res) => {
  try {
    const force = ['1', 'true', 'yes'].includes(String(req.query.refresh || '').toLowerCase());
    const payload = await getMarketPerformers(force);
    res.json(payload);
  } catch (err) {
    res.status(502).json({ error: String(err?.message || err) });
  }
});

stocksRouter.get('/market-summary', async (req, res) => {
  try {
    const indexCode = String(req.query.index || 'KSE100').toUpperCase();
    const days = Math.max(7, Number(req.query.days || 365));
    const forceRefresh = ['1', 'true', 'yes'].includes(String(req.query.refresh || '').toLowerCase());
    const payload = await getMarketSummary({ indexCode, days, forceRefresh });
    return res.json(payload);
  } catch (err) {
    return res.status(502).json({ error: String(err?.message || err) });
  }
});

stocksRouter.get('/market-summary/status', (req, res) => {
  return res.json({ ok: true, ...getMarketSummarySchedulerStatus() });
});

stocksRouter.get('/news/daily', async (req, res) => {
  try {
    const limit = Math.min(24, Math.max(1, Number(req.query.limit || 8)));
    const forceRefresh = ['1', 'true', 'yes'].includes(String(req.query.refresh || '').toLowerCase());

    if (forceRefresh) {
      const feed = await getLatestBusinessNewsFeed({ limit: Math.max(limit * 3, 18), forceRefresh: true });

      const findLatestSentiment = db.prepare(`
        SELECT label, score, analyzed_at
        FROM sentiment
        WHERE source = ?
          AND LOWER(TRIM(headline)) = LOWER(TRIM(?))
        ORDER BY datetime(analyzed_at) DESC
        LIMIT 1
      `);

      const seenRefreshed = new Set();
      const refreshedItems = [];
      for (const row of feed) {
        const headline = String(row.title || '').trim();
        if (!headline) continue;

        const dedupeKey = `${String(row.source || '').toLowerCase()}|${headline.toLowerCase()}`;
        if (seenRefreshed.has(dedupeKey)) continue;
        seenRefreshed.add(dedupeKey);

        const sentiment = findLatestSentiment.get(String(row.source || ''), headline) || {};
        refreshedItems.push({
          headline,
          summary: String(row.description || '').trim(),
          source: row.source,
          label: String(sentiment.label || 'neutral').toLowerCase(),
          score: Number(sentiment.score || 0),
          analyzed_at: sentiment.analyzed_at || row.pubDate || new Date().toISOString()
        });

        if (refreshedItems.length >= limit) break;
      }

      return res.json({ ok: true, count: refreshedItems.length, refreshed: true, items: refreshedItems });
    }

    const rows = db.prepare(`
      SELECT headline, source, label, score, analyzed_at
      FROM sentiment
      WHERE source IN ('dawn_business', 'business_recorder', 'tribune_business', 'profit_pakistantoday')
        AND headline IS NOT NULL
        AND TRIM(headline) <> ''
        AND LOWER(TRIM(headline)) NOT LIKE 'daily sentiment scan%'
      ORDER BY datetime(analyzed_at) DESC
      LIMIT 220
    `).all();

    const seen = new Set();
    const items = [];
    for (const row of rows) {
      const key = String(row.headline || '').toLowerCase().trim();
      if (!key || seen.has(key)) continue;
      seen.add(key);
      items.push({
        headline: row.headline,
        summary: '',
        source: row.source,
        label: row.label || 'neutral',
        score: Number(row.score || 0),
        analyzed_at: row.analyzed_at || null
      });
      if (items.length >= limit) break;
    }

    return res.json({ ok: true, count: items.length, refreshed: false, items });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.post('/chat', optionalAuth, async (req, res) => {
  const isMalformedDb = (err) => String(err?.message || err || '').toLowerCase().includes('database disk image is malformed');
  try {
    const { stock, question, history } = req.body || {};
    if (!question) {
      return res.status(400).json({ error: 'question is required' });
    }

    const scope = String(stock || 'MARKET').toUpperCase();

    let userHistory = [];
    if (req.user) {
      try {
        userHistory = listUserChatHistory(req.user.id, scope, 12);
      } catch (err) {
        if (!isMalformedDb(err)) throw err;
        userHistory = [];
      }
    }

    const mergedHistory = req.user
      ? userHistory
      : (Array.isArray(history) ? history : []);

    const payload = await generateChatReply({
      stock: scope,
      question,
      history: mergedHistory,
      groqApiKey: config.groqApiKey,
      groqModel: config.groqModel
    });

    if (req.user) {
      try {
        addUserChatMessage(req.user.id, scope, 'user', question);
        addUserChatMessage(req.user.id, scope, 'assistant', payload.answer);
      } catch (err) {
        if (!isMalformedDb(err)) throw err;
      }
    }

    return res.json(payload);
  } catch (err) {
    if (isMalformedDb(err)) {
      const scope = String(req.body?.stock || 'MARKET').toUpperCase();
      return res.json({
        answer: [
          '## Direct Answer',
          `For **${scope}**, here is a concise market take based on currently available evidence.`,
          '',
          '## Historical Data Evidence',
          '- Full historical coverage is temporarily limited in this response path.',
          '- Current output focuses on the strongest available market context.',
          '',
          '## Recommendation',
          '- Prefer confirmation from price action and fresh headlines before acting.',
          '- Use staged entries and disciplined risk limits when signals are mixed.',
          '- This is educational analysis, not financial advice.'
        ].join('\n'),
        sentiment: 'neutral',
        source: 'fallback-db-recovery-route',
        scope,
        sources: [],
        retrieval: {
          scope,
          degraded_mode: true,
          reason: 'database_malformed',
          used_chunks: 0,
          historical_chunks: 0,
          news_chunks: 0
        }
      });
    }
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.get('/user/chat-history/:symbol', requireAuth, (req, res) => {
  const symbol = String(req.params.symbol || '').toUpperCase();
  const limit = Number(req.query.limit || 20);
  const messages = listUserChatHistory(req.user.id, symbol, limit);
  return res.json({ symbol, count: messages.length, messages });
});

stocksRouter.delete('/user/chat-history/:symbol', requireAuth, (req, res) => {
  const symbol = String(req.params.symbol || 'MARKET').toUpperCase();
  const cleared = clearUserChatHistory(req.user.id, symbol);
  return res.json({ symbol, cleared });
});

stocksRouter.get('/user/watchlist', requireAuth, (req, res) => {
  const items = listUserWatchlist(req.user.id);
  return res.json({ count: items.length, items });
});

stocksRouter.get('/user/sim/portfolio', requireAuth, (req, res) => {
  try {
    const payload = getSimulationPortfolio(req.user.id);
    return res.json({ ok: true, ...payload });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.get('/user/sim/trades', requireAuth, (req, res) => {
  try {
    const limit = Number(req.query.limit || 30);
    const trades = listSimulationTrades(req.user.id, limit);
    return res.json({ ok: true, count: trades.length, trades });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.get('/user/sim/orders', requireAuth, (req, res) => {
  try {
    const limit = Number(req.query.limit || 30);
    const status = req.query.status ? String(req.query.status) : null;
    const orders = listSimulationOrders(req.user.id, limit, status);
    return res.json({ ok: true, count: orders.length, orders });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.post('/user/sim/orders', requireAuth, (req, res) => {
  try {
    const payload = placeSimulatedOrder(req.user.id, req.body || {});
    return res.status(201).json({ ok: true, ...payload });
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg.includes('required') || msg.includes('positive number') || msg.includes('BUY or SELL')) return res.status(400).json({ error: msg });
    if (msg.includes('No live price found')) return res.status(404).json({ error: msg });
    if (msg.includes('Insufficient') || msg.includes('Cannot sell') || msg.includes('No open simulated position')) return res.status(409).json({ error: msg });
    return res.status(500).json({ error: msg });
  }
});

stocksRouter.delete('/user/sim/orders/:id', requireAuth, (req, res) => {
  try {
    const payload = cancelSimulationOrder(req.user.id, req.params.id);
    return res.json({ ok: true, ...payload });
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg.includes('required') || msg.includes('found') || msg.includes('pending')) return res.status(409).json({ error: msg });
    return res.status(500).json({ error: msg });
  }
});

stocksRouter.get('/sim/leaderboard', requireAuth, (req, res) => {
  try {
    const limit = Number(req.query.limit || 20);
    const rows = getSimulationLeaderboard(limit);
    return res.json({ ok: true, count: rows.length, leaderboard: rows });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.post('/user/sim/buy', requireAuth, (req, res) => {
  try {
    const payload = buySimulatedAsset(req.user.id, req.body?.symbol, req.body?.quantity);
    return res.status(201).json({ ok: true, action: 'BUY', ...payload });
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg.includes('required') || msg.includes('positive number')) return res.status(400).json({ error: msg });
    if (msg.includes('No live price found')) return res.status(404).json({ error: msg });
    if (msg.includes('Insufficient')) return res.status(409).json({ error: msg });
    return res.status(500).json({ error: msg });
  }
});

stocksRouter.post('/user/sim/sell', requireAuth, (req, res) => {
  try {
    const payload = sellSimulatedAsset(req.user.id, req.body?.symbol, req.body?.quantity);
    return res.status(201).json({ ok: true, action: 'SELL', ...payload });
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg.includes('required') || msg.includes('positive number')) return res.status(400).json({ error: msg });
    if (msg.includes('No live price found')) return res.status(404).json({ error: msg });
    if (msg.includes('No open simulated position') || msg.includes('Cannot sell')) return res.status(409).json({ error: msg });
    return res.status(500).json({ error: msg });
  }
});

stocksRouter.post('/user/sim/reset', requireAuth, (req, res) => {
  try {
    const payload = resetSimulationPortfolio(req.user.id);
    return res.json({ ok: true, action: 'RESET', ...payload });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

// Refresh latest prices for symbols (best-effort). Body: { symbols: ["ABC", "DEF"] }
stocksRouter.post('/refresh/prices', async (req, res) => {
  try {
    let symbols = Array.isArray(req.body?.symbols) ? req.body.symbols : [];
    if (!symbols.length) {
      const focus = getFocusSymbols();
      symbols = (focus.symbols || []).slice(0, Math.max(1, config.focusRefreshLimit));
    }
    if (!symbols.length) return res.status(400).json({ error: 'symbols array required in body and no focus symbols found' });
    const payload = await refreshPricesForSymbols(symbols);
    return res.json({ ok: true, ...payload });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.get('/focus-symbols', (req, res) => {
  const payload = getFocusSymbols();
  return res.json({
    ok: true,
    ...payload,
    sample: (payload.symbols || []).slice(0, 20)
  });
});

stocksRouter.post('/models/warmup', (req, res) => {
  try {
    const limit = Number(req.body?.limit || config.startupModelWarmupLimit || 50);
    const payload = warmupModelsForFocus(limit);
    return res.json({ ok: true, ...payload });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.post('/models/append-retrain', (req, res) => {
  try {
    const payload = runIncrementalAppendAndRetrain({
      maxSymbols: Number(req.body?.max_symbols || 120),
      minRows: Number(req.body?.min_rows || 120),
      epochs: Number(req.body?.epochs || 20),
      variant: String(req.body?.variant || 'lstm')
    });
    return res.json({ ok: true, ...payload });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.get('/refresh/focus/status', (req, res) => {
  return res.json({ ok: true, ...getFocusRefreshStatus() });
});

stocksRouter.post('/refresh/focus/start', (req, res) => {
  try {
    const status = startFocusRefreshScheduler({
      intervalSec: Number(req.body?.interval_sec || 0) || undefined,
      batchSize: Number(req.body?.batch_size || 0) || undefined
    });
    return res.json({ ok: true, ...status });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.post('/refresh/focus/stop', (req, res) => {
  try {
    const status = stopFocusRefreshScheduler();
    return res.json({ ok: true, ...status });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.post('/refresh/focus/run-now', async (req, res) => {
  try {
    const result = await runFocusRefreshBatchNow();
    return res.json({ ok: true, ...result, status: getFocusRefreshStatus() });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err), status: getFocusRefreshStatus() });
  }
});

stocksRouter.get('/sentiment/status', (req, res) => {
  return res.json({ ok: true, ...getSentimentSchedulerStatus() });
});

stocksRouter.post('/sentiment/start', (req, res) => {
  try {
    const status = startSentimentScheduler({
      intervalSec: Number(req.body?.interval_sec || 0) || undefined,
      batchSize: Number(req.body?.batch_size || 0) || undefined
    });
    return res.json({ ok: true, ...status });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.post('/sentiment/stop', (req, res) => {
  try {
    const status = stopSentimentScheduler();
    return res.json({ ok: true, ...status });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.post('/sentiment/run-now', async (req, res) => {
  try {
    const result = await runSentimentNow();
    return res.json({ ok: true, result, status: getSentimentSchedulerStatus() });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err), status: getSentimentSchedulerStatus() });
  }
});

stocksRouter.post('/sentiment/run-full', async (req, res) => {
  try {
    const result = await runSentimentFullCycleNow();
    return res.json({ ok: true, result, status: getSentimentSchedulerStatus() });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err), status: getSentimentSchedulerStatus() });
  }
});

stocksRouter.get('/sync/status', (req, res) => {
  return res.json({
    latest_date: getLatestDbDate(),
    now: new Date().toISOString()
  });
});

stocksRouter.post('/sync/historical', async (req, res) => {
  try {
    const mode = String(req.body?.mode || 'incremental').toLowerCase();
    if (mode === 'startup') {
      const summary = await runStartupHistoricalSync();
      return res.json({ ok: true, mode, ...summary });
    }

    const summary = await syncMissingHistoricalData({
      startDate: req.body?.start_date || null
    });
    return res.json({ ok: true, mode, ...summary });
  } catch (err) {
    return res.status(500).json({ error: String(err?.message || err) });
  }
});

stocksRouter.post('/user/watchlist', requireAuth, (req, res) => {
  try {
    const symbol = addUserWatchlistSymbol(req.user.id, req.body?.symbol);
    const items = listUserWatchlist(req.user.id);
    return res.status(201).json({ symbol, count: items.length, items });
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg.includes('not found')) return res.status(404).json({ error: msg });
    if (msg.includes('required')) return res.status(400).json({ error: msg });
    return res.status(500).json({ error: msg });
  }
});

stocksRouter.delete('/user/watchlist/:symbol', requireAuth, (req, res) => {
  try {
    const symbol = removeUserWatchlistSymbol(req.user.id, req.params.symbol);
    const items = listUserWatchlist(req.user.id);
    return res.json({ symbol, count: items.length, items });
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg.includes('required')) return res.status(400).json({ error: msg });
    return res.status(500).json({ error: msg });
  }
});
