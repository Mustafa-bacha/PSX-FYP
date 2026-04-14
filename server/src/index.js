import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import { config } from './config.js';
import { stocksRouter } from './routes/stocksRoutes.js';
import { authRouter } from './routes/authRoutes.js';
import { initDb } from './lib/db.js';
import { runStartupHistoricalSync } from './services/historicalSyncService.js';
import { warmupModelsForFocus } from './services/modelWarmupService.js';
import { autoStartFocusRefreshScheduler, getFocusRefreshStatus } from './services/focusRefreshSchedulerService.js';
import { autoStartSentimentScheduler, getSentimentSchedulerStatus } from './services/sentimentSchedulerService.js';
import { autoStartMarketSummaryScheduler, getMarketSummarySchedulerStatus } from './services/marketSummarySchedulerService.js';

const app = express();

initDb();

app.use(cors({ origin: config.corsOrigins }));
app.use(express.json({ limit: '1mb' }));
app.use(morgan('dev'));

app.use('/api/auth', authRouter);
app.use('/api', stocksRouter);

app.get('/', (req, res) => {
  res.json({
    app: 'psx-platform-express',
    docs: {
      health: '/api/health',
      auth: '/api/auth/*',
      stocks: '/api/stocks',
      stock: '/api/stock/:symbol?days=365',
      performers: '/api/market-performers'
    }
  });
});

app.listen(config.port, () => {
  // eslint-disable-next-line no-console
  console.log(`Express API running on http://127.0.0.1:${config.port}`);

  if (config.startupHistoricalSync) {
    runStartupHistoricalSync()
      .then((summary) => {
        // eslint-disable-next-line no-console
        console.log('[startup-sync] done', JSON.stringify(summary));
      })
      .catch((err) => {
        // eslint-disable-next-line no-console
        console.error('[startup-sync] failed', String(err?.message || err));
      });
  }

  if (config.startupModelWarmup) {
    try {
      const payload = warmupModelsForFocus(config.startupModelWarmupLimit);
      // eslint-disable-next-line no-console
      console.log('[startup-model-warmup] done', JSON.stringify({
        count: payload.count,
        ok_count: payload.ok_count,
        failed_count: payload.failed_count
      }));
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('[startup-model-warmup] failed', String(err?.message || err));
    }
  }

  try {
    autoStartFocusRefreshScheduler();
    // eslint-disable-next-line no-console
    console.log('[focus-refresh-scheduler]', JSON.stringify(getFocusRefreshStatus()));
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[focus-refresh-scheduler] failed', String(err?.message || err));
  }

  try {
    autoStartSentimentScheduler();
    // eslint-disable-next-line no-console
    console.log('[sentiment-scheduler]', JSON.stringify(getSentimentSchedulerStatus()));
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[sentiment-scheduler] failed', String(err?.message || err));
  }

  try {
    autoStartMarketSummaryScheduler();
    // eslint-disable-next-line no-console
    console.log('[market-summary-scheduler]', JSON.stringify(getMarketSummarySchedulerStatus()));
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[market-summary-scheduler] failed', String(err?.message || err));
  }
});
