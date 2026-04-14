from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from config import CSV_PATH, DB_PATH, REPORTS_DIR
from database import ingest_csv, init_db


def _apply_deep_env(args: argparse.Namespace) -> None:
    os.environ["PREDICT_LOOKBACK_WINDOW"] = str(args.lookback)
    os.environ["PREDICT_LSTM_HIDDEN_SIZE"] = str(args.hidden_size)
    os.environ["PREDICT_LSTM_NUM_LAYERS"] = str(args.layers)
    os.environ["PREDICT_LSTM_DROPOUT"] = str(args.dropout)
    os.environ["PREDICT_LSTM_EPOCHS"] = str(args.epochs)
    os.environ["PREDICT_LSTM_LR"] = str(args.lr)
    os.environ["PREDICT_LSTM_BATCH"] = str(args.batch_size)
    os.environ["PREDICT_LSTM_PATIENCE"] = str(args.patience)
    os.environ["PREDICT_DIRECTION_WEIGHT"] = str(args.direction_weight)
    os.environ["PREDICT_TRAIN_SEED"] = str(args.seed)
    os.environ["PREDICT_MODEL_VARIANT"] = str(args.base_variant)


def _all_symbol_counts() -> list[tuple[str, int, str, str]]:
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            """
            SELECT symbol, COUNT(*) AS cnt, MIN(date) AS min_date, MAX(date) AS max_date
            FROM stocks
            GROUP BY symbol
            ORDER BY symbol ASC
            """
        ).fetchall()
        return [(str(r[0]).strip().upper(), int(r[1]), str(r[2]), str(r[3])) for r in rows]
    finally:
        conn.close()


def _eligible_symbols(min_rows: int) -> list[str]:
    return [sym for sym, cnt, _, _ in _all_symbol_counts() if cnt >= int(min_rows)]


def _dataset_profile(min_rows: int) -> dict[str, Any]:
    rows = _all_symbol_counts()
    if not rows:
        return {
            "total_symbols": 0,
            "eligible_symbols": 0,
            "skipped_low_rows": 0,
            "total_rows": 0,
            "median_rows_per_symbol": 0.0,
            "min_rows_per_symbol": 0,
            "max_rows_per_symbol": 0,
            "row_threshold": int(min_rows),
            "sample_low_row_symbols": [],
        }

    counts = [cnt for _, cnt, _, _ in rows]
    counts_sorted = sorted(counts)
    n = len(counts_sorted)
    if n % 2 == 1:
        median = float(counts_sorted[n // 2])
    else:
        median = float((counts_sorted[(n // 2) - 1] + counts_sorted[n // 2]) / 2.0)

    low_rows = [(sym, cnt) for sym, cnt, _, _ in rows if cnt < int(min_rows)]
    return {
        "total_symbols": len(rows),
        "eligible_symbols": sum(1 for _, cnt, _, _ in rows if cnt >= int(min_rows)),
        "skipped_low_rows": len(low_rows),
        "total_rows": int(sum(counts)),
        "median_rows_per_symbol": round(median, 2),
        "min_rows_per_symbol": int(min(counts)),
        "max_rows_per_symbol": int(max(counts)),
        "row_threshold": int(min_rows),
        "sample_low_row_symbols": low_rows[:20],
    }


def _summary_report(results: dict[str, dict]) -> dict[str, float | int]:
    valid = [v for v in results.values() if "error" not in v]
    failed = [k for k, v in results.items() if "error" in v]

    if not valid:
        return {
            "trained": 0,
            "failed": len(failed),
            "avg_direction_accuracy": 0.0,
            "avg_rmse": 0.0,
            "avg_mae": 0.0,
            "symbols_over_target_accuracy": 0,
        }

    return {
        "trained": len(valid),
        "failed": len(failed),
        "avg_direction_accuracy": round(
            sum(float(v.get("direction_accuracy", 0.0)) for v in valid) / len(valid), 4
        ),
        "avg_rmse": round(sum(float(v.get("rmse", 0.0)) for v in valid) / len(valid), 4),
        "avg_mae": round(sum(float(v.get("mae", 0.0)) for v in valid) / len(valid), 4),
        "symbols_over_target_accuracy": sum(
            1 for v in valid if float(v.get("direction_accuracy", 0.0)) >= 0.8
        ),
    }


def _write_report(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_existing_results(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        existing = raw.get("results", {})
        return existing if isinstance(existing, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _parse_variants(raw: str) -> list[str]:
    items = [v.strip().lower() for v in str(raw or "").split(",") if v.strip()]
    allowed = {"lstm", "bilstm", "gru"}
    variants = [v for v in items if v in allowed]
    return variants if variants else ["bilstm", "gru"]


def _attempt_overrides(args: argparse.Namespace) -> list[dict[str, Any]]:
    base = {
        "lookback": args.lookback,
        "hidden_size": args.hidden_size,
        "num_layers": args.layers,
        "dropout": args.dropout,
        "max_epochs": args.epochs,
        "learning_rate": args.lr,
        "batch_size": args.batch_size,
        "patience": args.patience,
        "class_loss_weight": args.direction_weight,
        "seed": args.seed,
        "model_variant": args.base_variant,
    }
    attempts = [base]
    if not args.boost:
        return attempts

    for variant in _parse_variants(args.boost_variants):
        attempts.append(
            {
                **base,
                "model_variant": variant,
                "hidden_size": max(args.hidden_size, args.boost_hidden_size),
                "num_layers": max(args.layers, args.boost_layers),
                "max_epochs": max(args.epochs, args.boost_epochs),
                "patience": max(args.patience, args.boost_patience),
                "batch_size": max(args.batch_size, args.boost_batch_size),
                "learning_rate": args.boost_lr,
            }
        )
    return attempts


def _train_symbol_with_boost(symbol: str, args: argparse.Namespace, train_model: Any) -> dict[str, Any]:
    attempts = _attempt_overrides(args)
    best_metrics: dict[str, Any] | None = None
    best_overrides: dict[str, Any] | None = None
    history: list[dict[str, Any]] = []

    for idx, overrides in enumerate(attempts, start=1):
        variant = str(overrides.get("model_variant", "lstm"))
        try:
            metrics = train_model(symbol, overrides=overrides)
            accuracy = float(metrics.get("direction_accuracy", 0.0))
            history.append(
                {
                    "attempt": idx,
                    "variant": variant,
                    "direction_accuracy": round(accuracy, 6),
                    "rmse": float(metrics.get("rmse", 0.0)),
                    "mae": float(metrics.get("mae", 0.0)),
                }
            )

            if best_metrics is None or accuracy > float(best_metrics.get("direction_accuracy", -1.0)):
                best_metrics = metrics
                best_overrides = dict(overrides)

            if accuracy >= float(args.target_accuracy):
                break
        except Exception as exc:  # noqa: BLE001
            history.append({"attempt": idx, "variant": variant, "error": str(exc)})

    if best_metrics is None or best_overrides is None:
        raise ValueError(f"All training attempts failed for {symbol}")

    # Ensure persisted artifacts correspond to best attempt.
    last_success = next((h for h in reversed(history) if "error" not in h), None)
    if (
        last_success is None
        or str(last_success.get("variant", "")) != str(best_overrides.get("model_variant", "lstm"))
        or abs(
            float(last_success.get("direction_accuracy", -1.0))
            - float(best_metrics.get("direction_accuracy", -1.0))
        )
        > 1e-9
    ):
        best_metrics = train_model(symbol, overrides=best_overrides)

    return {
        **best_metrics,
        "attempts_run": len(history),
        "target_accuracy": float(args.target_accuracy),
        "hit_target": float(best_metrics.get("direction_accuracy", 0.0)) >= float(args.target_accuracy),
        "selected_variant": str(best_overrides.get("model_variant", "lstm")),
        "attempt_history": history,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deep-training runner for the PSX hybrid sequence predictor.",
    )
    parser.add_argument("--symbol", default="ALL", help="Single symbol or ALL (default: ALL)")
    parser.add_argument("--csv", default="", help="CSV path for ingestion before training")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip CSV ingestion step")
    parser.add_argument("--min-rows", type=int, default=300, help="Minimum rows per symbol")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of symbols for a partial run")

    parser.add_argument("--lookback", type=int, default=45)
    parser.add_argument("--hidden-size", type=int, default=128)
    parser.add_argument("--layers", type=int, default=2)
    parser.add_argument("--dropout", type=float, default=0.25)
    parser.add_argument("--epochs", type=int, default=80)
    parser.add_argument("--patience", type=int, default=14)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--lr", type=float, default=0.0007)
    parser.add_argument("--direction-weight", type=float, default=0.65)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--base-variant", default="lstm", choices=["lstm", "bilstm", "gru"])
    parser.add_argument("--target-accuracy", type=float, default=0.8)
    parser.add_argument("--boost", action="store_true", help="Enable heavy retry attempts")
    parser.add_argument("--boost-variants", default="bilstm,gru")
    parser.add_argument("--boost-hidden-size", type=int, default=192)
    parser.add_argument("--boost-layers", type=int, default=3)
    parser.add_argument("--boost-epochs", type=int, default=140)
    parser.add_argument("--boost-patience", type=int, default=22)
    parser.add_argument("--boost-batch-size", type=int, default=96)
    parser.add_argument("--boost-lr", type=float, default=0.0005)
    parser.add_argument(
        "--report-path",
        default="",
        help="Optional path to write JSON progress/results report",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from an existing report file by skipping completed symbols",
    )
    parser.add_argument(
        "--save-every",
        type=int,
        default=5,
        help="Persist report every N symbols in bulk mode",
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()

    init_db()

    csv_path = Path(args.csv).resolve() if args.csv else CSV_PATH
    inserted = 0
    if not args.skip_ingest:
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        inserted = ingest_csv(csv_path)

    _apply_deep_env(args)

    # Import only after env vars are set so model config picks up deep-training values.
    from models.stock_predictor import train_model

    started = time.perf_counter()
    symbol = str(args.symbol).strip().upper()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_path = (
        Path(args.report_path).resolve()
        if args.report_path
        else (REPORTS_DIR / f"hybrid_train_{symbol}_{timestamp}.json")
    )
    data_profile = _dataset_profile(args.min_rows)

    if symbol != "ALL":
        metrics = _train_symbol_with_boost(symbol, args, train_model)
        elapsed = round(time.perf_counter() - started, 2)
        payload = {
            "ok": True,
            "interrupted": False,
            "mode": "single",
            "symbol": symbol,
            "csv_ingested": str(csv_path),
            "rows_inserted": inserted,
            "elapsed_seconds": elapsed,
            "data_profile": data_profile,
            "metrics": metrics,
            "report_path": str(report_path),
        }
        _write_report(report_path, payload)
        print(json.dumps(payload, indent=2))
        return 0

    symbols = _eligible_symbols(args.min_rows)
    if args.limit > 0:
        symbols = symbols[: args.limit]

    results: dict[str, dict] = _load_existing_results(report_path) if args.resume else {}
    symbols_done = set(results.keys())
    symbols_to_run = [sym for sym in symbols if sym not in symbols_done]

    interrupted = False
    processed_now = 0
    for sym in symbols_to_run:
        try:
            results[sym] = _train_symbol_with_boost(sym, args, train_model)
        except KeyboardInterrupt:
            interrupted = True
            results[sym] = {"error": "interrupted_by_user"}
            break
        except Exception as exc:  # noqa: BLE001
            results[sym] = {"error": str(exc)}

        processed_now += 1
        if args.save_every > 0 and processed_now % args.save_every == 0:
            interim_elapsed = round(time.perf_counter() - started, 2)
            interim_payload = {
                "ok": True,
                "interrupted": False,
                "mode": "bulk",
                "symbols_requested": len(symbols),
                "symbols_previously_done": len(symbols_done),
                "symbols_processed_in_run": processed_now,
                "csv_ingested": str(csv_path),
                "rows_inserted": inserted,
                "elapsed_seconds": interim_elapsed,
                "data_profile": data_profile,
                "summary": _summary_report(results),
                "results": results,
                "report_path": str(report_path),
            }
            _write_report(report_path, interim_payload)

    elapsed = round(time.perf_counter() - started, 2)
    summary = _summary_report(results)

    payload = {
        "ok": True,
        "interrupted": interrupted,
        "mode": "bulk",
        "symbols_requested": len(symbols),
        "symbols_previously_done": len(symbols_done),
        "symbols_processed_in_run": processed_now,
        "symbols_remaining": max(0, len(symbols_to_run) - processed_now),
        "csv_ingested": str(csv_path),
        "rows_inserted": inserted,
        "elapsed_seconds": elapsed,
        "data_profile": data_profile,
        "summary": summary,
        "results": results,
        "report_path": str(report_path),
    }
    _write_report(report_path, payload)
    print(json.dumps(payload, indent=2))
    return 130 if interrupted else 0


if __name__ == "__main__":
    raise SystemExit(main())
