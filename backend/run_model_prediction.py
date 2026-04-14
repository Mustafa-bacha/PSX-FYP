from __future__ import annotations

import json
import sys

from models.stock_predictor import predict_next_day, train_model


def main() -> int:
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "symbol argument required"}))
        return 2

    symbol = str(sys.argv[1]).strip().upper()
    if not symbol:
        print(json.dumps({"ok": False, "error": "symbol is empty"}))
        return 2

    try:
        payload = predict_next_day(symbol)
        print(json.dumps({"ok": True, "trained": False, "prediction": payload}))
        return 0
    except FileNotFoundError:
        # Auto-train-on-miss, then predict again.
        try:
            train_model(symbol)
            payload = predict_next_day(symbol)
            print(json.dumps({"ok": True, "trained": True, "prediction": payload}))
            return 0
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"ok": False, "error": str(exc), "symbol": symbol}))
            return 1
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": str(exc), "symbol": symbol}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
