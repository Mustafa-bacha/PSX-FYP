import { config } from '../config.js';
import { getMarketSummary } from './marketSummaryService.js';

const state = {
  running: false,
  timer: null,
  intervalSec: Math.max(60, Number(config.marketSummaryRefreshIntervalSec || 300)),
  runs: 0,
  lastStartedAt: null,
  lastFinishedAt: null,
  lastResult: null,
  lastError: null
};

export function getMarketSummarySchedulerStatus() {
  return {
    running: state.running,
    interval_sec: state.intervalSec,
    runs: state.runs,
    last_started_at: state.lastStartedAt,
    last_finished_at: state.lastFinishedAt,
    last_result: state.lastResult,
    last_error: state.lastError
  };
}

export async function runMarketSummaryRefreshNow() {
  state.lastStartedAt = new Date().toISOString();
  state.lastError = null;

  try {
    const payload = await getMarketSummary({
      indexCode: config.marketSummaryIndexCode,
      days: config.marketSummaryWarmupDays,
      forceRefresh: true
    });

    state.runs += 1;
    state.lastResult = {
      index_code: payload.index_code,
      cache_status: payload.cache_status,
      fetched_at: payload.fetched_at,
      index_value: payload.index_value,
      change_percent: payload.change_percent
    };
    state.lastFinishedAt = new Date().toISOString();
    return state.lastResult;
  } catch (err) {
    state.lastError = String(err?.message || err);
    state.lastFinishedAt = new Date().toISOString();
    throw err;
  }
}

function schedule() {
  if (state.timer) clearInterval(state.timer);
  state.timer = setInterval(() => {
    runMarketSummaryRefreshNow().catch(() => {});
  }, Math.max(60, state.intervalSec) * 1000);
}

export function startMarketSummaryScheduler({ intervalSec } = {}) {
  if (Number.isFinite(Number(intervalSec)) && Number(intervalSec) > 0) {
    state.intervalSec = Math.max(60, Number(intervalSec));
  }

  state.running = true;
  schedule();

  // Warm once on start to avoid first-hit latency.
  runMarketSummaryRefreshNow().catch(() => {});

  return getMarketSummarySchedulerStatus();
}

export function stopMarketSummaryScheduler() {
  state.running = false;
  if (state.timer) {
    clearInterval(state.timer);
    state.timer = null;
  }
  return getMarketSummarySchedulerStatus();
}

export function autoStartMarketSummaryScheduler() {
  if (!config.marketSummaryAutoRefreshEnabled) return getMarketSummarySchedulerStatus();
  return startMarketSummaryScheduler({ intervalSec: config.marketSummaryRefreshIntervalSec });
}
