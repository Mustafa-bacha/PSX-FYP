from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any

from transformers import pipeline

from config import DB_PATH


@lru_cache(maxsize=1)
def _get_sentiment_pipeline():
    device = -1
    use_gpu = os.getenv("FINBERT_USE_GPU", "false").strip().lower() == "true"
    if use_gpu:
        try:
            import torch  # local import so CPU-only environments still work

            if torch.cuda.is_available():
                device = 0
        except Exception:  # noqa: BLE001
            device = -1

    return pipeline(
        "text-classification",
        model="ProsusAI/finbert",
        return_all_scores=True,
        device=device,
    )


def analyze_text(text: str) -> dict[str, Any]:
    if not text or not text.strip():
        return {"label": "neutral", "score": 0.0}

    sentiment_pipe = _get_sentiment_pipeline()
    output = sentiment_pipe(text[:512])[0]

    scores = {entry["label"].lower(): float(entry["score"]) for entry in output}
    positive = scores.get("positive", 0.0)
    negative = scores.get("negative", 0.0)
    neutral = scores.get("neutral", 0.0)

    final_score = max(-1.0, min(1.0, positive - negative))

    if final_score > 0.1:
        label = "positive"
    elif final_score < -0.1:
        label = "negative"
    else:
        label = "neutral" if neutral >= max(positive, negative) else ("positive" if positive >= negative else "negative")

    return {
        "label": label,
        "score": round(final_score, 4),
    }


def get_symbol_sentiment(symbol: str) -> dict[str, Any]:
    symbol = symbol.strip().upper()
    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat(sep=" ")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT symbol, score, label, source, headline, analyzed_at
            FROM sentiment
            WHERE symbol = ? AND analyzed_at >= ?
            ORDER BY analyzed_at DESC
            """,
            (symbol, cutoff),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {
            "symbol": symbol,
            "average_score": 0.0,
            "label": "neutral",
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": 0,
            "recent_headlines": [],
        }

    scores = [float(r["score"]) for r in rows if r["score"] is not None]
    avg = sum(scores) / len(scores) if scores else 0.0

    pos = sum(1 for r in rows if (r["label"] or "").lower() == "positive")
    neg = sum(1 for r in rows if (r["label"] or "").lower() == "negative")
    neu = sum(1 for r in rows if (r["label"] or "").lower() == "neutral")

    if avg > 0.1:
        label = "positive"
    elif avg < -0.1:
        label = "negative"
    else:
        label = "neutral"

    recent = [
        {
            "headline": r["headline"],
            "label": (r["label"] or "neutral").lower(),
            "score": round(float(r["score"] or 0.0), 4),
            "source": (r["source"] or "news").lower(),
        }
        for r in rows[:5]
    ]

    psx_report_count = sum(
        1
        for r in rows
        if "psx" in (r["source"] or "").lower() and "report" in (r["source"] or "").lower()
    )

    return {
        "symbol": symbol,
        "average_score": round(float(avg), 4),
        "label": label,
        "positive_count": pos,
        "negative_count": neg,
        "neutral_count": neu,
        "psx_report_count": psx_report_count,
        "recent_headlines": recent,
    }
