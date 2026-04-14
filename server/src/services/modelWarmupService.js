import { getPythonModelPrediction } from './modelBridgeService.js';
import { getFocusSymbols } from './focusSymbolsService.js';

export function warmupModelsForFocus(limit = 50) {
  const payload = getFocusSymbols();
  const symbols = (payload.symbols || []).slice(0, Math.max(1, Number(limit || 50)));

  const results = [];
  for (const sym of symbols) {
    const pred = getPythonModelPrediction(sym);
    results.push({
      symbol: sym,
      ok: Boolean(pred),
      source: pred?.source || null,
      trained_on_demand: Boolean(pred?.trained_on_demand)
    });
  }

  return {
    count: symbols.length,
    ok_count: results.filter((r) => r.ok).length,
    failed_count: results.filter((r) => !r.ok).length,
    results
  };
}
