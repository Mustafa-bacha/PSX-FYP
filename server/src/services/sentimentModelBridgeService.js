import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { config } from '../config.js';

function fallbackResults(size) {
  return Array.from({ length: size }, () => ({ label: 'neutral', score: 0 }));
}

export function classifyHeadlinesWithPython(headlines = []) {
  const list = Array.isArray(headlines) ? headlines.map((h) => String(h || '').trim()) : [];
  if (!list.length) return [];

  const pythonBin = process.env.PYTHON_BIN || 'python3';
  const backendDir = path.resolve(config.rootDir, 'backend');
  const script = path.resolve(backendDir, 'run_sentiment_batch.py');

  const run = spawnSync(
    pythonBin,
    [script],
    {
      cwd: backendDir,
      input: JSON.stringify({ headlines: list }),
      encoding: 'utf-8',
      timeout: Number(process.env.SENTIMENT_BRIDGE_TIMEOUT_MS || 120000)
    }
  );

  if (run.error || run.status !== 0) {
    return fallbackResults(list.length);
  }

  let parsed = null;
  try {
    const raw = String(run.stdout || '').trim();
    const lines = raw.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
    for (let i = lines.length - 1; i >= 0; i -= 1) {
      try {
        parsed = JSON.parse(lines[i]);
        break;
      } catch {
        // keep scanning upward for JSON payload line
      }
    }
    if (!parsed && raw) {
      parsed = JSON.parse(raw);
    }
  } catch {
    return fallbackResults(list.length);
  }

  if (!parsed?.ok || !Array.isArray(parsed.results)) {
    return fallbackResults(list.length);
  }

  const out = parsed.results.map((r) => {
    const score = Number(r?.score);
    return {
      label: ['positive', 'negative', 'neutral'].includes(String(r?.label || '').toLowerCase())
        ? String(r.label).toLowerCase()
        : 'neutral',
      score: Number.isFinite(score) ? Math.max(-1, Math.min(1, score)) : 0
    };
  });

  if (out.length < list.length) {
    return [...out, ...fallbackResults(list.length - out.length)];
  }

  return out.slice(0, list.length);
}
