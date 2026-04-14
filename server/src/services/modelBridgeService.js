import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { config } from '../config.js';

function toNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function getPythonModelPrediction(symbol) {
  const sym = String(symbol || '').trim().toUpperCase();
  if (!sym) return null;

  const pythonBin = process.env.PYTHON_BIN || 'python3';
  const backendDir = path.resolve(config.rootDir, 'backend');
  const script = path.resolve(backendDir, 'run_model_prediction.py');

  const run = spawnSync(
    pythonBin,
    [script, sym],
    {
      cwd: backendDir,
      encoding: 'utf-8',
      timeout: Number(process.env.MODEL_BRIDGE_TIMEOUT_MS || 60000)
    }
  );

  if (run.error || run.status !== 0) {
    return null;
  }

  let parsed = null;
  const raw = String(run.stdout || '').trim();
  const lines = raw.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    try {
      parsed = JSON.parse(lines[i]);
      break;
    } catch {
      // continue scanning upward for a JSON line
    }
  }
  if (!parsed && raw) {
    try {
      parsed = JSON.parse(raw);
    } catch {
      return null;
    }
  }

  if (!parsed?.ok || !parsed?.prediction) return null;

  const p = parsed.prediction;
  return {
    predicted_price: toNumber(p.predicted_price),
    current_price: toNumber(p.current_price),
    predicted_direction: String(p.predicted_direction || '').toUpperCase() === 'UP' ? 'UP' : 'DOWN',
    confidence: toNumber(p.confidence, 0.5),
    mae: toNumber(p.mae),
    rmse: toNumber(p.rmse),
    direction_accuracy: toNumber(p.direction_accuracy, 0.5),
    source: 'python_model',
    trained_on_demand: Boolean(parsed.trained)
  };
}
