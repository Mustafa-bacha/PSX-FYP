import { config } from '../config.js';
import { getFocusSymbols } from './focusSymbolsService.js';
import { refreshPricesForSymbols } from './stocksService.js';

const state = {
  running: false,
  timer: null,
  intervalSec: Math.max(30, Number(config.focusAutoRefreshIntervalSec || 300)),
  batchSize: Math.max(10, Number(config.focusAutoRefreshBatchSize || 80)),
  cursor: 0,
  lastStartedAt: null,
  lastFinishedAt: null,
  lastDurationMs: null,
  lastResult: null,
  runs: 0,
  totalUpdated: 0,
  totalSkipped: 0,
  lastError: null
};

function nextBatch(symbols, batchSize) {
  if (!symbols.length) return [];
  const start = state.cursor % symbols.length;
  const end = start + batchSize;

  let batch;
  if (end <= symbols.length) {
    batch = symbols.slice(start, end);
  } else {
    batch = [...symbols.slice(start), ...symbols.slice(0, end - symbols.length)];
  }

  state.cursor = end % symbols.length;
  return batch;
}

export function getFocusRefreshStatus() {
  const focus = getFocusSymbols();
  return {
    running: state.running,
    interval_sec: state.intervalSec,
    batch_size: state.batchSize,
    focus_count: Number(focus.count || 0),
    cursor: state.cursor,
    runs: state.runs,
    total_updated: state.totalUpdated,
    total_skipped: state.totalSkipped,
    last_started_at: state.lastStartedAt,
    last_finished_at: state.lastFinishedAt,
    last_duration_ms: state.lastDurationMs,
    last_result: state.lastResult,
    last_error: state.lastError
  };
}

export async function runFocusRefreshBatchNow() {
  const focus = getFocusSymbols();
  const symbols = focus.symbols || [];
  if (!symbols.length) {
    state.lastError = 'No focus symbols found';
    return { updated: 0, skipped: 0, symbols: [], skipped_symbols: [], note: 'no_focus_symbols' };
  }

  const batch = nextBatch(symbols, state.batchSize);
  const started = Date.now();
  state.lastStartedAt = new Date(started).toISOString();
  state.lastError = null;

  try {
    const result = await refreshPricesForSymbols(batch);
    const finished = Date.now();

    state.lastFinishedAt = new Date(finished).toISOString();
    state.lastDurationMs = finished - started;
    state.lastResult = {
      batch_count: batch.length,
      ...result
    };
    state.runs += 1;
    state.totalUpdated += Number(result.updated || 0);
    state.totalSkipped += Number(result.skipped || 0);

    return state.lastResult;
  } catch (err) {
    const finished = Date.now();
    state.lastFinishedAt = new Date(finished).toISOString();
    state.lastDurationMs = finished - started;
    state.lastError = String(err?.message || err);
    throw err;
  }
}

function schedule() {
  if (state.timer) clearInterval(state.timer);
  state.timer = setInterval(() => {
    runFocusRefreshBatchNow().catch(() => {});
  }, Math.max(30, state.intervalSec) * 1000);
}

export function startFocusRefreshScheduler({ intervalSec, batchSize } = {}) {
  if (Number.isFinite(Number(intervalSec)) && Number(intervalSec) > 0) {
    state.intervalSec = Math.max(30, Number(intervalSec));
  }
  if (Number.isFinite(Number(batchSize)) && Number(batchSize) > 0) {
    state.batchSize = Math.max(10, Number(batchSize));
  }

  state.running = true;
  schedule();
  return getFocusRefreshStatus();
}

export function stopFocusRefreshScheduler() {
  state.running = false;
  if (state.timer) {
    clearInterval(state.timer);
    state.timer = null;
  }
  return getFocusRefreshStatus();
}

export function autoStartFocusRefreshScheduler() {
  if (!config.focusAutoRefreshEnabled) return getFocusRefreshStatus();
  return startFocusRefreshScheduler({
    intervalSec: config.focusAutoRefreshIntervalSec,
    batchSize: config.focusAutoRefreshBatchSize
  });
}
