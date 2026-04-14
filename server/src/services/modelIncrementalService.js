import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { config } from '../config.js';

function parseJsonFromStdout(stdout) {
  const raw = String(stdout || '').trim();
  if (!raw) return null;

  const lines = raw.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    try {
      return JSON.parse(lines[i]);
    } catch {
      // continue searching
    }
  }

  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function runIncrementalAppendAndRetrain({
  maxSymbols = 120,
  minRows = 120,
  epochs = 20,
  variant = 'lstm'
} = {}) {
  const pythonBin = process.env.PYTHON_BIN || 'python3';
  const backendDir = path.resolve(config.rootDir, 'backend');
  const script = path.resolve(backendDir, 'run_incremental_update.py');

  const args = [
    script,
    '--csv', config.incrementalCsvPath,
    '--max-symbols', String(Math.max(1, Number(maxSymbols || 120))),
    '--min-rows', String(Math.max(1, Number(minRows || 120))),
    '--epochs', String(Math.max(1, Number(epochs || 20))),
    '--variant', String(variant || 'lstm').toLowerCase()
  ];

  const run = spawnSync(
    pythonBin,
    args,
    {
      cwd: backendDir,
      encoding: 'utf-8',
      timeout: Number(process.env.MODEL_INCREMENTAL_TIMEOUT_MS || 15 * 60 * 1000)
    }
  );

  if (run.error) {
    throw new Error(`Incremental runner failed: ${String(run.error.message || run.error)}`);
  }
  if (run.status !== 0) {
    const stderr = String(run.stderr || '').trim();
    const stdout = String(run.stdout || '').trim();
    throw new Error(stderr || stdout || `Incremental runner exited with status ${run.status}`);
  }

  const parsed = parseJsonFromStdout(run.stdout);
  if (!parsed || typeof parsed !== 'object') {
    throw new Error('Incremental runner returned non-JSON output');
  }

  return {
    ...parsed,
    fixed_csv_path: config.incrementalCsvPath
  };
}
