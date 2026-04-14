from __future__ import annotations

import sqlite3
from pathlib import Path

from config import CSV_PATH, DB_PATH
from database import count_stock_rows, count_symbols, ingest_csv, init_db
from models.stock_predictor import train_model


def _top_symbols_by_volume(limit: int = 20) -> list[str]:
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            """
            SELECT symbol, AVG(volume) AS avg_volume
            FROM stocks
            GROUP BY symbol
            ORDER BY avg_volume DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def initialize_system(csv_path: Path | None = None) -> dict[str, int]:
    init_db()

    csv_to_use = csv_path or CSV_PATH
    inserted = ingest_csv(csv_to_use)

    trained_count = 0
    for symbol in _top_symbols_by_volume(limit=20):
        try:
            train_model(symbol)
            trained_count += 1
        except Exception as exc:  # noqa: BLE001
            print(f"Skipping training for {symbol}: {exc}")

    summary = {
        "rows_loaded": count_stock_rows(),
        "symbols_loaded": count_symbols(),
        "new_rows_inserted": inserted,
        "models_trained": trained_count,
    }
    return summary


def main() -> None:
    summary = initialize_system()
    print(
        "Init complete: "
        f"rows={summary['rows_loaded']}, "
        f"symbols={summary['symbols_loaded']}, "
        f"new_rows={summary['new_rows_inserted']}, "
        f"models_trained={summary['models_trained']}"
    )


if __name__ == "__main__":
    main()
