import axios from 'axios';
import * as cheerio from 'cheerio';
import { db } from '../lib/db.js';
import { config } from '../config.js';
import { getFocusSymbols } from './focusSymbolsService.js';
import { classifyHeadlinesWithPython } from './sentimentModelBridgeService.js';

const NEWS_SOURCES = [
	{ name: 'dawn_business', url: 'https://www.dawn.com/feeds/business' },
	{ name: 'business_recorder', url: 'https://www.brecorder.com/feeds/latest-news' },
	{ name: 'tribune_business', url: 'https://tribune.com.pk/feed/business' },
	{ name: 'profit_pakistantoday', url: 'https://profit.pakistantoday.com.pk/feed/' }
];

const POS_WORDS = ['gain', 'growth', 'profit', 'surge', 'up', 'strong', 'record', 'bullish', 'improve', 'beat'];
const NEG_WORDS = ['loss', 'decline', 'drop', 'down', 'weak', 'risk', 'cut', 'bearish', 'fall', 'miss'];

const newsCache = { at: 0, items: [] };
const reportsCache = new Map();

const insertSentiment = db.prepare(`
	INSERT INTO sentiment (symbol, score, label, source, headline, analyzed_at)
	VALUES (?, ?, ?, ?, ?, datetime('now'))
`);

const dedupeToday = db.prepare(`
	SELECT id FROM sentiment
	WHERE symbol = ? AND source = ? AND headline = ? AND date(analyzed_at) = date('now')
	LIMIT 1
`);

function normalizeText(text) {
	return String(text || '').replace(/\s+/g, ' ').trim();
}

function scoreTextHeuristic(text) {
	const t = normalizeText(text).toLowerCase();
	if (!t) return 0;
	let pos = 0;
	let neg = 0;
	for (const w of POS_WORDS) if (t.includes(w)) pos += 1;
	for (const w of NEG_WORDS) if (t.includes(w)) neg += 1;
	const raw = pos - neg;
	return Math.max(-1, Math.min(1, raw / 4));
}

function labelFromScoreHeuristic(score) {
	if (score > 0.12) return 'positive';
	if (score < -0.12) return 'negative';
	return 'neutral';
}

async function fetchRss(source) {
	try {
		const res = await axios.get(source.url, {
			timeout: 25000,
			headers: { 'User-Agent': 'Mozilla/5.0' }
		});

		const $ = cheerio.load(res.data, { xmlMode: true });
		const items = [];
		$('item').each((_, node) => {
			const title = normalizeText($(node).find('title').first().text());
			const link = normalizeText($(node).find('link').first().text());
			const description = normalizeText($(node).find('description').first().text());
			const pubDate = normalizeText($(node).find('pubDate').first().text());
			if (!title) return;
			items.push({ title, link, description, pubDate, source: source.name });
		});
		return items.slice(0, 120);
	} catch {
		return [];
	}
}

async function getNewsPool(forceRefresh = false) {
	const maxAge = Math.max(1, Number(config.sentimentNewsCacheMinutes || 20)) * 60 * 1000;
	if (!forceRefresh && newsCache.items.length && (Date.now() - newsCache.at) <= maxAge) {
		return newsCache.items;
	}

	const all = [];
	for (const src of NEWS_SOURCES) {
		const items = await fetchRss(src);
		all.push(...items);
	}

	const seen = new Set();
	const deduped = [];
	for (const item of all) {
		const key = `${item.source}|${item.title}`.toLowerCase();
		if (seen.has(key)) continue;
		seen.add(key);
		deduped.push(item);
	}

	newsCache.items = deduped;
	newsCache.at = Date.now();
	return deduped;
}

export async function getLatestBusinessNewsFeed({ limit = 24, forceRefresh = false } = {}) {
	const normalizedLimit = Math.min(120, Math.max(1, Number(limit || 24)));
	const pool = await getNewsPool(Boolean(forceRefresh));

	return pool.slice(0, normalizedLimit).map((item) => ({
		title: normalizeText(item.title),
		description: normalizeText(item.description),
		pubDate: normalizeText(item.pubDate),
		source: normalizeText(item.source)
	}));
}

async function fetchPsxReportHeadlines(symbol) {
	const ttlMs = Math.max(5, Number(config.sentimentReportsCacheMinutes || 1440)) * 60 * 1000;
	const hit = reportsCache.get(symbol);
	if (hit && (Date.now() - hit.at) <= ttlMs) return hit.rows;

	const candidates = [symbol, symbol.includes('-') ? symbol.split('-')[0] : null].filter(Boolean);
	for (const c of candidates) {
		try {
			const res = await axios.get(`https://dps.psx.com.pk/company/${encodeURIComponent(c)}`, {
				timeout: 25000,
				headers: { 'User-Agent': 'Mozilla/5.0' }
			});
			const $ = cheerio.load(res.data);
			const rows = [];
			$('a').each((_, a) => {
				const text = normalizeText($(a).text());
				const href = normalizeText($(a).attr('href'));
				const combined = `${text} ${href}`.toLowerCase();
				if (!text) return;
				if (!/(report|result|financial|statement|notice|announcement|dividend)/i.test(combined)) return;
				rows.push({
					title: text,
					link: href,
					source: 'psx_report'
				});
			});

			const unique = [];
			const seen = new Set();
			for (const r of rows) {
				const key = `${r.title}|${r.link}`.toLowerCase();
				if (seen.has(key)) continue;
				seen.add(key);
				unique.push(r);
			}

			const out = unique.slice(0, 8);
			reportsCache.set(symbol, { at: Date.now(), rows: out });
			return out;
		} catch {
			// try next candidate
		}
	}

	reportsCache.set(symbol, { at: Date.now(), rows: [] });
	return [];
}

function symbolMatchesItem(symbol, company, item) {
	const text = `${item.title} ${item.description || ''}`.toLowerCase();
	const sym = String(symbol || '').toLowerCase();
	if (new RegExp(`(^|[^a-z0-9])${sym}([^a-z0-9]|$)`, 'i').test(text)) return true;

	const companyName = normalizeText(company).toLowerCase();
	if (!companyName) return false;

	const terms = companyName
		.replace(/\b(limited|ltd|company|co|pakistan|industries|mills|bank)\b/g, ' ')
		.split(/\s+/)
		.filter((w) => w.length >= 4)
		.slice(0, 3);
	if (!terms.length) return false;

	return terms.some((t) => text.includes(t));
}

function storeSentimentRow({ symbol, score, label, source, headline }) {
	const cleanHeadline = normalizeText(headline).slice(0, 500);
	if (!cleanHeadline) return false;
	const exists = dedupeToday.get(symbol, source, cleanHeadline);
	if (exists) return false;
	insertSentiment.run(symbol, score, label, source, cleanHeadline);
	return true;
}

async function collectSentimentCandidatesForSymbol(symbol, company = '') {
	const newsPool = await getNewsPool();
	const reports = await fetchPsxReportHeadlines(symbol);

	const symbolNews = newsPool
		.filter((item) => symbolMatchesItem(symbol, company, item))
		.slice(0, 8);

	const candidates = [];
	for (const item of symbolNews) {
		candidates.push({
			symbol,
			company,
			source: item.source,
			headline: item.title,
			body: item.description || ''
		});
	}

	for (const rep of reports) {
		candidates.push({
			symbol,
			company,
			source: 'psx_report',
			headline: rep.title,
			body: ''
		});
	}

	return {
		symbol,
		company,
		matched_news: symbolNews.length,
		matched_reports: reports.length,
		candidates
	};
}

export async function ingestSentimentForSymbol(symbol, company = '', opts = {}) {
	const forceDailyCheckpoint = Boolean(opts.forceDailyCheckpoint);
	const checkpointTag = String(opts.checkpointTag || '').trim();
	const payload = await collectSentimentCandidatesForSymbol(symbol, company);

	let inserted = 0;
	const texts = payload.candidates.map((c) => `${c.headline} ${c.body || ''}`.trim());
	const modelResults = classifyHeadlinesWithPython(texts);

	payload.candidates.forEach((item, idx) => {
		const scored = modelResults[idx] || {};
		const score = Number.isFinite(Number(scored.score))
			? Number(scored.score)
			: scoreTextHeuristic(`${item.headline} ${item.body || ''}`);
		const label = ['positive', 'negative', 'neutral'].includes(String(scored.label || '').toLowerCase())
			? String(scored.label).toLowerCase()
			: labelFromScoreHeuristic(score);

		if (storeSentimentRow({
			symbol,
			score,
			label,
			source: item.source,
			headline: item.headline
		})) inserted += 1;
	});

		// Guarantee at least one daily sentiment checkpoint per symbol.
		if (inserted === 0 || forceDailyCheckpoint) {
			const fallbackTitle = checkpointTag
				? `Daily sentiment scan for ${symbol} [${checkpointTag}]`
				: `Daily sentiment scan for ${symbol}: no material headlines captured.`;
		if (storeSentimentRow({
			symbol,
			score: 0,
			label: 'neutral',
			source: 'daily_scan',
			headline: fallbackTitle
		})) inserted += 1;
	}

	return {
		symbol,
		company,
		inserted,
			matched_news: payload.matched_news,
			matched_reports: payload.matched_reports
	};
}

export async function runSentimentBatch({ startIndex = 0, batchSize = 40 } = {}) {
	const focus = getFocusSymbols();
	const profiles = focus.profiles || [];
	const total = profiles.length;
	if (!total) {
		return { total_symbols: 0, processed: 0, start_index: 0, next_index: 0, results: [] };
	}

	const safeStart = Math.max(0, Number(startIndex || 0)) % total;
	const size = Math.max(1, Number(batchSize || 40));

	const selected = [];
	for (let i = 0; i < Math.min(size, total); i += 1) {
		selected.push(profiles[(safeStart + i) % total]);
	}

	const results = [];
	for (const p of selected) {
			// sequential fetch for source-friendliness + request pacing
			// eslint-disable-next-line no-await-in-loop
			const row = await ingestSentimentForSymbol(p.symbol, p.company || '');
		results.push(row);
	}

	return {
		total_symbols: total,
		processed: selected.length,
		start_index: safeStart,
		next_index: (safeStart + selected.length) % total,
		inserted_total: results.reduce((s, r) => s + Number(r.inserted || 0), 0),
		results
	};
}

	export async function runSentimentForAllSymbolsOnce() {
		const focus = getFocusSymbols();
		const total = Number(focus.count || 0);
		if (!total) {
			return { total_symbols: 0, processed_symbols: 0, inserted_total: 0, cycles: 0 };
		}

		const batch = Math.max(1, Number(config.sentimentBatchSize || 40));
		let index = 0;
		let processed = 0;
		let inserted = 0;
		let cycles = 0;
			const checkpointTag = new Date().toISOString().slice(0, 19).replace('T', ' ');

			const profiles = focus.profiles || [];
			while (processed < total) {
				const selected = [];
				for (let i = 0; i < Math.min(batch, total - processed); i += 1) {
					selected.push(profiles[(index + i) % total]);
				}

				let batchInserted = 0;
				for (const p of selected) {
					// eslint-disable-next-line no-await-in-loop
					const row = await ingestSentimentForSymbol(p.symbol, p.company || '', {
						forceDailyCheckpoint: true,
						checkpointTag
					});
					batchInserted += Number(row.inserted || 0);
				}

				index = (index + selected.length) % total;
				processed += selected.length;
				inserted += batchInserted;
				cycles += 1;
				if (!selected.length) break;
			}

		return {
			total_symbols: total,
			processed_symbols: Math.min(processed, total),
			inserted_total: inserted,
			cycles
		};
	}

