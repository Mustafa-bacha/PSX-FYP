import { config } from '../config.js';
import { runSentimentBatch, runSentimentForAllSymbolsOnce } from './sentimentIngestionService.js';

const state = {
	running: false,
	timer: null,
	cursor: 0,
	intervalSec: Math.max(300, Number(config.sentimentIntervalSec || 1800)),
	batchSize: Math.max(10, Number(config.sentimentBatchSize || 40)),
	runs: 0,
	totalInserted: 0,
	lastStartedAt: null,
	lastFinishedAt: null,
	lastResult: null,
	lastError: null
};

export function getSentimentSchedulerStatus() {
	return {
		running: state.running,
		interval_sec: state.intervalSec,
		batch_size: state.batchSize,
		cursor: state.cursor,
		runs: state.runs,
		total_inserted: state.totalInserted,
		last_started_at: state.lastStartedAt,
		last_finished_at: state.lastFinishedAt,
		last_result: state.lastResult,
		last_error: state.lastError
	};
}

export async function runSentimentNow() {
	const started = new Date().toISOString();
	state.lastStartedAt = started;
	state.lastError = null;

	try {
		const result = await runSentimentBatch({
			startIndex: state.cursor,
			batchSize: state.batchSize
		});

		state.cursor = Number(result.next_index || 0);
		state.runs += 1;
		state.totalInserted += Number(result.inserted_total || 0);
		state.lastResult = result;
		state.lastFinishedAt = new Date().toISOString();
		return result;
	} catch (err) {
		state.lastError = String(err?.message || err);
		state.lastFinishedAt = new Date().toISOString();
		throw err;
	}
}

function schedule() {
	if (state.timer) clearInterval(state.timer);
	state.timer = setInterval(() => {
		runSentimentNow().catch(() => {});
	}, Math.max(300, state.intervalSec) * 1000);
}

export function startSentimentScheduler({ intervalSec, batchSize } = {}) {
	if (Number.isFinite(Number(intervalSec)) && Number(intervalSec) > 0) {
		state.intervalSec = Math.max(300, Number(intervalSec));
	}
	if (Number.isFinite(Number(batchSize)) && Number(batchSize) > 0) {
		state.batchSize = Math.max(10, Number(batchSize));
	}

	state.running = true;
	schedule();
	return getSentimentSchedulerStatus();
}

export function stopSentimentScheduler() {
	state.running = false;
	if (state.timer) {
		clearInterval(state.timer);
		state.timer = null;
	}
	return getSentimentSchedulerStatus();
}

export function autoStartSentimentScheduler() {
	if (!config.sentimentAutoEnabled) return getSentimentSchedulerStatus();
	return startSentimentScheduler({
		intervalSec: config.sentimentIntervalSec,
		batchSize: config.sentimentBatchSize
	});
}

export async function runSentimentFullCycleNow() {
	const started = new Date().toISOString();
	state.lastStartedAt = started;
	state.lastError = null;
	try {
		const result = await runSentimentForAllSymbolsOnce();
		state.runs += Number(result.cycles || 1);
		state.totalInserted += Number(result.inserted_total || 0);
		state.lastResult = { mode: 'full_cycle', ...result };
		state.lastFinishedAt = new Date().toISOString();
		state.cursor = 0;
		return state.lastResult;
	} catch (err) {
		state.lastError = String(err?.message || err);
		state.lastFinishedAt = new Date().toISOString();
		throw err;
	}
}

