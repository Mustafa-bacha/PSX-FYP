from __future__ import annotations

import argparse
import json
from pathlib import Path

from services.incremental_training import append_and_quick_retrain


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append latest market data and quick-retrain impacted symbols.",
    )
    parser.add_argument("--csv", default="", help="CSV path to ingest (default: config CSV_PATH)")
    parser.add_argument("--max-symbols", type=int, default=120)
    parser.add_argument("--min-rows", type=int, default=120)

    parser.add_argument("--lookback", type=int, default=30)
    parser.add_argument("--hidden-size", type=int, default=96)
    parser.add_argument("--layers", type=int, default=2)
    parser.add_argument("--dropout", type=float, default=0.2)
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--lr", type=float, default=0.0009)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--patience", type=int, default=6)
    parser.add_argument("--direction-weight", type=float, default=0.6)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--variant", default="lstm", choices=["lstm", "bilstm", "gru"])

    return parser


def main() -> int:
    args = build_parser().parse_args()
    csv_path = Path(args.csv).resolve() if args.csv else None

    payload = append_and_quick_retrain(
        csv_path=csv_path,
        max_symbols=args.max_symbols,
        min_rows=args.min_rows,
        quick_overrides={
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
            "model_variant": args.variant,
        },
    )

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
