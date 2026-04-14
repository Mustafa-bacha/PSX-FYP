from __future__ import annotations

import json
import sys

from models.sentiment_analyzer import analyze_text


def main() -> int:
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw or "{}")
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": f"invalid input json: {exc}"}))
        return 2

    headlines = payload.get("headlines") or []
    if not isinstance(headlines, list):
        print(json.dumps({"ok": False, "error": "headlines must be a list"}))
        return 2

    results = []
    for text in headlines:
        try:
            out = analyze_text(str(text or ""))
            results.append(
                {
                    "label": str(out.get("label", "neutral")).lower(),
                    "score": float(out.get("score", 0.0)),
                }
            )
        except Exception as exc:  # noqa: BLE001
            results.append({"label": "neutral", "score": 0.0, "error": str(exc)})

    print(json.dumps({"ok": True, "count": len(results), "results": results}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
