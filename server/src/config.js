import path from 'node:path';
import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, '..', '..');

dotenv.config({ path: path.resolve(ROOT, '.env') });

function normalizePath(value, fallback) {
  if (!value) return fallback;
  let raw = String(value).trim();
  if (raw.startsWith('sqlite:///')) {
    raw = raw.replace('sqlite:///', '/');
  }
  return path.isAbsolute(raw) ? raw : path.resolve(ROOT, raw);
}

export const config = {
  rootDir: ROOT,
  port: Number(process.env.SERVER_PORT || 5001),
  corsOrigins: (process.env.CORS_ORIGINS || 'http://localhost:5173,http://127.0.0.1:5173').split(',').map((v) => v.trim()),
  frontendUrl: process.env.FRONTEND_URL || 'http://127.0.0.1:5173',
  dbPath: normalizePath(process.env.DB_PATH || process.env.DATABASE_URL, path.resolve(ROOT, 'data/psx_platform.db')),
  performersCachePath: normalizePath(process.env.MARKET_PERFORMERS_CACHE_PATH, path.resolve(ROOT, 'server/cache/market_performers.json')),
  performersCacheMinutes: Number(process.env.MARKET_PERFORMERS_CACHE_MINUTES || 30),
  groqApiKey: process.env.GROQ_API_KEY || '',
  groqApiKeys: (() => {
    const inline = String(process.env.GROQ_API_KEYS || '');
    const parsed = inline
      .split(',')
      .map((v) => v.trim())
      .filter(Boolean);
    const single = String(process.env.GROQ_API_KEY || '').trim();
    return Array.from(new Set([...(single ? [single] : []), ...parsed]));
  })(),
  groqModel: process.env.GROQ_MODEL || 'llama-3.1-8b-instant',
  mainChatProvider: process.env.MAIN_CHAT_PROVIDER || 'groq',
  geminiApiKey: process.env.GEMINI_API_KEY || '',
  geminiChatModel: process.env.GEMINI_CHAT_MODEL || 'gemini-2.0-flash',
  geminiChatTimeoutMs: Number(process.env.GEMINI_CHAT_TIMEOUT_MS || 60000),
  geminiIntentModel: process.env.GEMINI_INTENT_MODEL || 'gemini-2.0-flash-lite',
  enableGeminiIntentGuard: !['0', 'false', 'no'].includes(String(process.env.ENABLE_GEMINI_INTENT_GUARD || 'false').toLowerCase()),
  geminiIntentTimeoutMs: Number(process.env.GEMINI_INTENT_TIMEOUT_MS || 3500),
  /** RAG chat creativity; override with GROQ_TEMPERATURE (0–2). */
  groqTemperature: (() => {
    const raw = process.env.GROQ_TEMPERATURE;
    const t = raw === undefined || raw === '' ? 0.6 : Number(raw);
    return Number.isFinite(t) ? Math.min(2, Math.max(0, t)) : 0.6;
  })(),
  jwtSecret: process.env.JWT_SECRET_KEY || process.env.SECRET_KEY || 'change-me-jwt-secret',
  jwtExpiresHours: Number(process.env.JWT_ACCESS_TOKEN_EXPIRES_HOURS || 12),
  startupHistoricalSync: !['0', 'false', 'no'].includes(String(process.env.STARTUP_HISTORICAL_SYNC || 'true').toLowerCase()),
  focusSymbolsXlsxPath: normalizePath(process.env.FOCUS_SYMBOLS_XLSX_PATH, path.resolve(ROOT, '..', 'Stocks and symbols.xlsx')),
  focusRefreshLimit: Number(process.env.FOCUS_REFRESH_LIMIT || 200),
  focusAutoRefreshEnabled: !['0', 'false', 'no'].includes(String(process.env.FOCUS_AUTO_REFRESH_ENABLED || 'true').toLowerCase()),
  focusAutoRefreshIntervalSec: Number(process.env.FOCUS_AUTO_REFRESH_INTERVAL_SEC || 300),
  focusAutoRefreshBatchSize: Number(process.env.FOCUS_AUTO_REFRESH_BATCH_SIZE || 80),
  sentimentAutoEnabled: !['0', 'false', 'no'].includes(String(process.env.SENTIMENT_AUTO_ENABLED || 'true').toLowerCase()),
  sentimentIntervalSec: Number(process.env.SENTIMENT_INTERVAL_SEC || 1800),
  sentimentBatchSize: Number(process.env.SENTIMENT_BATCH_SIZE || 40),
  sentimentNewsCacheMinutes: Number(process.env.SENTIMENT_NEWS_CACHE_MINUTES || 20),
  sentimentReportsCacheMinutes: Number(process.env.SENTIMENT_REPORTS_CACHE_MINUTES || 1440),
  startupModelWarmup: !['0', 'false', 'no'].includes(String(process.env.STARTUP_MODEL_WARMUP || 'false').toLowerCase()),
  startupModelWarmupLimit: Number(process.env.STARTUP_MODEL_WARMUP_LIMIT || 50),
  marketSummaryIndexCode: String(process.env.MARKET_SUMMARY_INDEX_CODE || 'KSE100').toUpperCase(),
  marketSummaryCacheMinutes: Number(process.env.MARKET_SUMMARY_CACHE_MINUTES || 5),
  marketSummaryAutoRefreshEnabled: !['0', 'false', 'no'].includes(String(process.env.MARKET_SUMMARY_AUTO_REFRESH_ENABLED || 'true').toLowerCase()),
  marketSummaryRefreshIntervalSec: Number(process.env.MARKET_SUMMARY_REFRESH_INTERVAL_SEC || 300),
  marketSummaryWarmupDays: Number(process.env.MARKET_SUMMARY_WARMUP_DAYS || 365),
  incrementalCsvPath: normalizePath(
    process.env.INCREMENTAL_CSV_PATH,
    path.resolve(ROOT, 'data', 'new_psx_historical_.csv')
  )
};
