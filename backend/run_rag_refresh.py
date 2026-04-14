from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from config import CSV_PATH


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download PSX history, optionally retrain, and rebuild RAG index.",
    )
    parser.add_argument(
        "--download-psx-history",
        action="store_true",
        help="Fetch full historical records per symbol from PSX sources before refresh.",
    )
    parser.add_argument(
        "--symbols-limit",
        type=int,
        default=0,
        help="Optional symbol limit for downloader (0 means all discovered symbols).",
    )
    parser.add_argument(
        "--history-output",
        default=str(CSV_PATH),
        help="Output CSV path for downloaded historical records.",
    )
    parser.add_argument(
        "--download-timeout-sec",
        type=int,
        default=10,
        help="Per-request timeout in seconds for PSX history download.",
    )
    parser.add_argument(
        "--train-mode",
        choices=["none", "quick"],
        default="none",
        help="Optional model training step after download/ingestion.",
    )
    parser.add_argument("--max-symbols", type=int, default=120)
    parser.add_argument("--min-rows", type=int, default=120)
    return parser


def main() -> int:
    args = build_parser().parse_args()

    history_output = args.history_output
    download_stats = None
    train_stats = None

    try:
        from services.incremental_training import append_and_quick_retrain
        from services.psx_history_downloader import download_psx_history_all
    except ModuleNotFoundError as exc:
        missing = str(exc)
        payload = {
            "ok": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": f"Missing Python dependency: {missing}",
            "hint": "Use the project venv and install requirements: /home/sertv2cs/Desktop/Mustafa/hassan/.venv/bin/python -m pip install -r requirements.txt",
        }
        print(json.dumps(payload, indent=2))
        return 2

    if args.download_psx_history:
        dl = download_psx_history_all(
            output_csv=history_output,
            symbols_limit=(args.symbols_limit if args.symbols_limit and args.symbols_limit > 0 else None),
            timeout_sec=max(3, int(args.download_timeout_sec)),
        )
        download_stats = {
            "symbols_total": dl.symbols_total,
            "symbols_ok": dl.symbols_ok,
            "symbols_failed": dl.symbols_failed,
            "rows_downloaded": dl.rows_downloaded,
            "output_csv": dl.output_csv,
            "failed_symbols_preview": dl.failed_symbols[:25],
            "fallback_symbols_preview": dl.fallback_symbols[:25],
        }

    if args.train_mode == "quick":
        train_stats = append_and_quick_retrain(
            csv_path=history_output,
            max_symbols=args.max_symbols,
            min_rows=args.min_rows,
        )

    try:
        from rag_chatbot.rag_pipeline import StockRAGPipeline
    except ModuleNotFoundError as exc:
        missing = str(exc)
        payload = {
            "ok": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": f"Missing Python dependency: {missing}",
            "hint": "Use the project venv and install requirements: /home/sertv2cs/Desktop/Mustafa/hassan/.venv/bin/python -m pip install -r requirements.txt",
        }
        print(json.dumps(payload, indent=2))
        return 2

    pipeline = StockRAGPipeline()
    stats = pipeline.reindex()
    payload = {
        "ok": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "indexed_chunks": int(stats.get("indexed_chunks", 0)),
        "stock_count": int(stats.get("stock_count", 0)),
        "download": download_stats,
        "training": {
            "mode": args.train_mode,
            "result": train_stats,
        },
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
