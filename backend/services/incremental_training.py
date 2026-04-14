from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from config import DB_PATH, CSV_PATH
from database import ingest_csv_with_symbols
from models.stock_predictor import train_model


def _changed_symbols_with_enough_rows(symbols: list[str], min_rows: int) -> list[str]:
    if not symbols:
        return []

    conn = sqlite3.connect(DB_PATH)
    try:
        placeholders = ",".join(["?"] * len(symbols))
        rows = conn.execute(
            f"""
            SELECT symbol, COUNT(*) AS cnt
            FROM stocks
            WHERE symbol IN ({placeholders})
            GROUP BY symbol
            """,
            tuple(symbols),
        ).fetchall()
    finally:
        conn.close()

    counts = {str(r[0]).strip().upper(): int(r[1]) for r in rows}
    return [s for s in symbols if counts.get(s, 0) >= int(min_rows)]


def _quick_overrides(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    p = dict(payload or {})
    return {
        "lookback": int(p.get("lookback", 30)),
        "hidden_size": int(p.get("hidden_size", 96)),
        "num_layers": int(p.get("num_layers", 2)),
        "dropout": float(p.get("dropout", 0.2)),
        "max_epochs": int(p.get("max_epochs", 20)),
        "learning_rate": float(p.get("learning_rate", 0.0009)),
        "batch_size": int(p.get("batch_size", 64)),
        "patience": int(p.get("patience", 6)),
        "class_loss_weight": float(p.get("class_loss_weight", 0.6)),
        "seed": int(p.get("seed", 42)),
        "model_variant": str(p.get("model_variant", "lstm")),
    }


def append_and_quick_retrain(
    csv_path: str | Path | None = None,
    max_symbols: int = 120,
    min_rows: int = 120,
    quick_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source_csv = Path(csv_path).resolve() if csv_path else CSV_PATH
    inserted_rows, changed_symbols = ingest_csv_with_symbols(source_csv)

    changed_symbols = [s.strip().upper() for s in changed_symbols if str(s).strip()]
    eligible_symbols = _changed_symbols_with_enough_rows(changed_symbols, min_rows=min_rows)
    target_symbols = eligible_symbols[: max(1, int(max_symbols))] if eligible_symbols else []

    overrides = _quick_overrides(quick_overrides)

    trained: dict[str, dict[str, Any]] = {}
    failed: dict[str, str] = {}
    for sym in target_symbols:
        try:
            trained[sym] = train_model(sym, overrides=overrides)
        except Exception as exc:  # noqa: BLE001
            failed[sym] = str(exc)

    return {
        "csv_path": str(source_csv),
        "inserted_rows": int(inserted_rows),
        "changed_symbols": changed_symbols,
        "changed_symbols_count": len(changed_symbols),
        "eligible_symbols": eligible_symbols,
        "eligible_symbols_count": len(eligible_symbols),
        "symbols_selected_for_retrain": target_symbols,
        "selected_count": len(target_symbols),
        "trained_count": len(trained),
        "failed_count": len(failed),
        "failed_symbols": failed,
        "trained_metrics": trained,
        "quick_overrides": overrides,
        "status": "ok",
    }
