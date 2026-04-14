export function sma(values, period) {
  const out = [];
  for (let i = 0; i < values.length; i += 1) {
    const start = Math.max(0, i - period + 1);
    const window = values.slice(start, i + 1);
    const avg = window.reduce((a, b) => a + b, 0) / window.length;
    out.push(avg);
  }
  return out;
}

export function ema(values, period) {
  const out = [];
  const k = 2 / (period + 1);
  for (let i = 0; i < values.length; i += 1) {
    if (i === 0) out.push(values[0]);
    else out.push(values[i] * k + out[i - 1] * (1 - k));
  }
  return out;
}

export function bollinger(values, period = 20, mult = 2) {
  const mid = sma(values, period);
  const upper = [];
  const lower = [];
  for (let i = 0; i < values.length; i += 1) {
    const start = Math.max(0, i - period + 1);
    const window = values.slice(start, i + 1);
    const mean = mid[i];
    const variance = window.reduce((acc, v) => acc + (v - mean) ** 2, 0) / window.length;
    const std = Math.sqrt(variance);
    upper.push(mean + mult * std);
    lower.push(mean - mult * std);
  }
  return { mid, upper, lower };
}
