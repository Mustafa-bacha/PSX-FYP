from __future__ import annotations

import csv
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from typing import Any

from .config import DATA_DIR, RAG_DOCS_DIR, ROOT_DIR


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _to_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isfinite(out):
        return out
    return None


def _fmt_num(value: Any, digits: int = 2) -> str:
    num = _to_float(value)
    if num is None:
        return "—"
    return f"{num:.{digits}f}"


def _pct_change(latest: Any, base: Any) -> float | None:
    last_v = _to_float(latest)
    base_v = _to_float(base)
    if last_v is None or base_v is None or base_v == 0:
        return None
    return ((last_v / base_v) - 1.0) * 100.0


def _trend_word(pct: float | None) -> str:
    if pct is None:
        return "no data"
    if pct > 5:
        return "strongly bullish"
    if pct > 2:
        return "moderately bullish"
    if pct > 0.5:
        return "slightly bullish"
    if pct > -0.5:
        return "flat/sideways"
    if pct > -2:
        return "slightly bearish"
    if pct > -5:
        return "moderately bearish"
    return "strongly bearish"


def _volatility_label(vol: float | None) -> str:
    if vol is None:
        return "unknown volatility"
    if vol < 1.5:
        return "low volatility"
    if vol < 3.0:
        return "moderate volatility"
    if vol < 5.0:
        return "high volatility"
    return "very high volatility"


def _volume_context(current_vol: float | None, avg_vol: float | None) -> str:
    if current_vol is None or avg_vol is None or avg_vol == 0:
        return "volume data unavailable"
    ratio = current_vol / avg_vol
    if ratio > 2.0:
        return f"volume surge ({ratio:.1f}x average) — strong institutional interest"
    if ratio > 1.3:
        return f"above-average volume ({ratio:.1f}x) — increased activity"
    if ratio > 0.7:
        return f"normal volume ({ratio:.1f}x average)"
    return f"below-average volume ({ratio:.1f}x average) — low interest/thin trading"


# ---------------------------------------------------------------------------
# Detailed per-symbol document builders (from SQLite)
# ---------------------------------------------------------------------------

def _build_detailed_profile_doc(symbol: str, row: sqlite3.Row) -> dict[str, Any]:
    """Build a comprehensive historical profile document with analytical narrative."""
    avg_close = _to_float(row["avg_close"])
    min_close = _to_float(row["min_close"])
    max_close = _to_float(row["max_close"])
    avg_change = _to_float(row["avg_change_pct"])
    avg_vol = _to_float(row["avg_volume"])
    avg_sent = _to_float(row["avg_sentiment"])

    # Price range analysis
    price_range_pct = None
    if min_close and max_close and min_close > 0:
        price_range_pct = ((max_close - min_close) / min_close) * 100

    # Sentiment interpretation
    sent_desc = "no sentiment data available"
    if avg_sent is not None:
        if avg_sent > 0.1:
            sent_desc = f"overall positive sentiment (avg score {_fmt_num(avg_sent, 3)}), market generally views this stock favorably"
        elif avg_sent > 0.02:
            sent_desc = f"mildly positive sentiment (avg score {_fmt_num(avg_sent, 3)}), slightly above neutral"
        elif avg_sent > -0.02:
            sent_desc = f"neutral sentiment (avg score {_fmt_num(avg_sent, 3)}), no strong directional bias from news"
        elif avg_sent > -0.1:
            sent_desc = f"mildly negative sentiment (avg score {_fmt_num(avg_sent, 3)}), some cautious outlook"
        else:
            sent_desc = f"negative sentiment (avg score {_fmt_num(avg_sent, 3)}), market has concerns about this stock"

    text = (
        f"{symbol} comprehensive historical profile: "
        f"Tracked for {int(row['rows_count'] or 0)} sessions from {row['first_date']} to {row['latest_date']}. "
        f"Average closing price {_fmt_num(avg_close)} PKR with a historical range of "
        f"{_fmt_num(min_close)} to {_fmt_num(max_close)} PKR"
    )
    if price_range_pct is not None:
        text += f" (total range spread {_fmt_num(price_range_pct)}%)"
    text += f". "

    text += (
        f"Average daily price change is {_fmt_num(avg_change)}% — "
        f"this indicates the stock {'has a slight upward bias historically' if (avg_change or 0) > 0.05 else 'tends to be range-bound on average' if abs(avg_change or 0) <= 0.05 else 'has a slight downward tendency historically'}. "
        f"Average daily volume is {_fmt_num(avg_vol, 0)} shares. "
        f"Sentiment analysis: {sent_desc}."
    )

    return {
        "id": f"db::profile::{symbol}",
        "stock": symbol,
        "text": text,
        "source": "psx_platform.db",
        "doc_type": "historical",
        "published_at": row["latest_date"],
    }


def _build_trend_doc(symbol: str, rows: list[sqlite3.Row]) -> dict[str, Any] | None:
    """Build a multi-timeframe trend analysis document with actionable insights."""
    if not rows:
        return None

    latest = rows[0]
    latest_close = _to_float(latest["close"])
    latest_vol = _to_float(latest["volume"])
    latest_change = _to_float(latest["change_pct"])

    # Multi-timeframe returns
    close_5 = _to_float(rows[min(4, len(rows) - 1)]["close"])
    close_20 = _to_float(rows[min(19, len(rows) - 1)]["close"])
    close_60 = _to_float(rows[min(59, len(rows) - 1)]["close"])

    ret_5 = _pct_change(latest_close, close_5)
    ret_20 = _pct_change(latest_close, close_20)
    ret_60 = _pct_change(latest_close, close_60)

    # Calculate simple moving averages
    closes = [_to_float(r["close"]) for r in rows]
    closes = [c for c in closes if c is not None]

    ma20 = sum(closes[:20]) / min(20, len(closes)) if closes else None
    ma60 = sum(closes[:60]) / min(60, len(closes)) if len(closes) >= 20 else None

    # Calculate volatility (std dev of daily changes)
    changes = [_to_float(r["change_pct"]) for r in rows[:20]]
    changes = [c for c in changes if c is not None]
    volatility = None
    if len(changes) >= 5:
        mean_ch = sum(changes) / len(changes)
        volatility = math.sqrt(sum((c - mean_ch) ** 2 for c in changes) / len(changes))

    # Volume trend
    volumes = [_to_float(r["volume"]) for r in rows[:20]]
    volumes = [v for v in volumes if v is not None]
    avg_vol_20 = sum(volumes) / len(volumes) if volumes else None

    # Calculate recent high/low
    recent_highs = closes[:20] if closes else []
    recent_high = max(recent_highs) if recent_highs else None
    recent_low = min(recent_highs) if recent_highs else None

    # Build narrative
    text = f"{symbol} multi-timeframe trend analysis as of {latest['date']}: "
    text += f"Latest close {_fmt_num(latest_close)} PKR, last session change {_fmt_num(latest_change)}%. "

    # Timeframe analysis
    text += f"Short-term (5-session): {_trend_word(ret_5)} with {_fmt_num(ret_5)}% return. "
    text += f"Medium-term (20-session): {_trend_word(ret_20)} with {_fmt_num(ret_20)}% return. "
    text += f"Long-term (60-session): {_trend_word(ret_60)} with {_fmt_num(ret_60)}% return. "

    # Moving average context
    if ma20 and latest_close:
        ma20_pos = "above" if latest_close > ma20 else "below"
        ma20_dist = ((latest_close / ma20) - 1) * 100
        text += f"Price is {ma20_pos} 20-day MA ({_fmt_num(ma20)}) by {_fmt_num(abs(ma20_dist))}%. "
    if ma60 and latest_close:
        ma60_pos = "above" if latest_close > ma60 else "below"
        text += f"Price is {ma60_pos} 60-day MA ({_fmt_num(ma60)}). "
    if ma20 and ma60:
        if ma20 > ma60:
            text += "20-day MA above 60-day MA signals bullish momentum. "
        else:
            text += "20-day MA below 60-day MA signals bearish pressure. "

    # Volatility
    text += f"Recent volatility: {_volatility_label(volatility)} ({_fmt_num(volatility)}% daily std dev). "

    # Volume analysis
    text += f"Latest volume {_fmt_num(latest_vol, 0)}, {_volume_context(latest_vol, avg_vol_20)}. "

    # Support/Resistance levels
    if recent_high and recent_low:
        text += f"20-session trading range: {_fmt_num(recent_low)} (support) to {_fmt_num(recent_high)} (resistance). "
        if latest_close:
            range_pos = ((latest_close - recent_low) / (recent_high - recent_low) * 100) if recent_high != recent_low else 50
            text += f"Current price sits at {_fmt_num(range_pos)}% of this range. "

    # Momentum summary
    signals_bull = sum([
        1 if (ret_5 or 0) > 0 else 0,
        1 if (ret_20 or 0) > 0 else 0,
        1 if (ret_60 or 0) > 0 else 0,
        1 if ma20 and ma60 and ma20 > ma60 else 0,
        1 if latest_close and ma20 and latest_close > ma20 else 0,
    ])
    signals_bear = 5 - signals_bull

    if signals_bull >= 4:
        text += f"Overall momentum: STRONG BULLISH ({signals_bull}/5 bullish signals). "
    elif signals_bull >= 3:
        text += f"Overall momentum: MODERATELY BULLISH ({signals_bull}/5 bullish signals). "
    elif signals_bull >= 2:
        text += f"Overall momentum: MIXED/TRANSITIONAL ({signals_bull}/5 bullish signals). "
    else:
        text += f"Overall momentum: BEARISH ({signals_bear}/5 bearish signals). "

    return {
        "id": f"db::trend::{symbol}",
        "stock": symbol,
        "text": text,
        "source": "psx_platform.db",
        "doc_type": "trend",
        "published_at": latest["date"],
    }


def _build_price_action_doc(symbol: str, rows: list[sqlite3.Row]) -> dict[str, Any] | None:
    """Build a detailed price action narrative for the last 5 sessions."""
    if len(rows) < 2:
        return None

    sessions = rows[:5]
    text = f"{symbol} recent price action (last {len(sessions)} sessions): "

    for i, row in enumerate(sessions):
        close = _to_float(row["close"])
        change = _to_float(row["change_pct"])
        vol = _to_float(row["volume"])
        direction = "gained" if (change or 0) > 0 else "declined" if (change or 0) < 0 else "unchanged"
        text += f"On {row['date']}: closed at {_fmt_num(close)} PKR, {direction} {_fmt_num(abs(change or 0))}%, volume {_fmt_num(vol, 0)}. "

    # Streak analysis
    streak = 0
    streak_dir = None
    for row in sessions:
        ch = _to_float(row["change_pct"])
        if ch is None:
            break
        if streak_dir is None:
            streak_dir = "up" if ch > 0 else "down"
            streak = 1
        elif (ch > 0 and streak_dir == "up") or (ch < 0 and streak_dir == "down"):
            streak += 1
        else:
            break

    if streak >= 2:
        text += f"The stock is on a {streak}-session {streak_dir} streak. "

    return {
        "id": f"db::priceaction::{symbol}",
        "stock": symbol,
        "text": text,
        "source": "psx_platform.db",
        "doc_type": "price_action",
        "published_at": sessions[0]["date"],
    }


# ---------------------------------------------------------------------------
# File-based document loaders
# ---------------------------------------------------------------------------

def _load_txt_docs() -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for path in sorted(RAG_DOCS_DIR.glob("*.txt")):
        text = path.read_text(encoding="utf-8").strip()
        if text:
            docs.append(
                {
                    "id": f"txt::{path.stem}",
                    "stock": path.stem.split("_")[0].upper(),
                    "text": text,
                    "source": str(path.name),
                    "doc_type": "report",
                    "published_at": None,
                }
            )
    return docs


def _load_csv_docs() -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for path in sorted(RAG_DOCS_DIR.glob("*.csv")):
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row_idx, row in enumerate(reader):
                stock = str(row.get("symbol", "")).strip().upper() or "GENERAL"
                summary_bits = [f"{k}: {v}" for k, v in row.items() if str(v).strip()]
                text = " | ".join(summary_bits)
                if text:
                    docs.append(
                        {
                            "id": f"csv::{path.stem}::{row_idx}",
                            "stock": stock,
                            "text": text,
                            "source": str(path.name),
                            "doc_type": "report",
                            "published_at": None,
                        }
                    )
    return docs


def _load_reports_docs() -> list[dict[str, Any]]:
    reports_dir = DATA_DIR / "reports"
    if not reports_dir.exists():
        return []

    docs: list[dict[str, Any]] = []
    for symbol_dir in sorted(reports_dir.iterdir()):
        if not symbol_dir.is_dir():
            continue
        symbol = symbol_dir.name.strip().upper()
        if not symbol:
            continue

        for path in sorted(symbol_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue

            if not isinstance(payload, dict):
                continue

            notes: list[str] = []
            metrics = payload.get("metrics")
            if isinstance(metrics, dict):
                for key, value in metrics.items():
                    if value is None:
                        continue
                    notes.append(f"{key}: {value}")

            if isinstance(payload.get("summary"), str) and payload["summary"].strip():
                notes.append(payload["summary"].strip())

            if isinstance(payload.get("commentary"), str) and payload["commentary"].strip():
                notes.append(payload["commentary"].strip())

            if not notes:
                continue

            docs.append(
                {
                    "id": f"report::{symbol}::{path.stem}",
                    "stock": symbol,
                    "text": " | ".join(notes),
                    "source": f"reports/{symbol}/{path.name}",
                    "doc_type": "report",
                    "published_at": None,
                }
            )
    return docs


# ---------------------------------------------------------------------------
# Database document loaders — comprehensive
# ---------------------------------------------------------------------------

def _load_existing_psx_data() -> list[dict[str, Any]]:
    db_path = DATA_DIR / "psx_platform.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT s.symbol,
                   MAX(s.date) AS latest_date,
                   MIN(s.date) AS first_date,
                   COUNT(*) AS rows_count,
                   AVG(s.close) AS avg_close,
                   MIN(s.close) AS min_close,
                   MAX(s.close) AS max_close,
                   AVG(s.change_pct) AS avg_change_pct,
                   AVG(s.volume) AS avg_volume,
                   MAX(se.analyzed_at) AS latest_sentiment_at,
                   AVG(se.score) AS avg_sentiment
            FROM stocks s
            LEFT JOIN sentiment se ON se.symbol = s.symbol
            GROUP BY s.symbol
            ORDER BY s.symbol ASC
            LIMIT 5000
            """
        ).fetchall()

        docs = []
        for row in rows:
            symbol = str(row["symbol"]).upper()

            # 1. Detailed historical profile
            docs.append(_build_detailed_profile_doc(symbol, row))

            # 2. Multi-timeframe trend analysis
            trend_rows = conn.execute(
                """
                SELECT date, close, change_pct, volume
                FROM stocks
                WHERE symbol = ?
                ORDER BY date DESC
                LIMIT 90
                """,
                (symbol,),
            ).fetchall()

            trend_doc = _build_trend_doc(symbol, trend_rows)
            if trend_doc is not None:
                docs.append(trend_doc)

            # 3. Recent price action detail
            price_doc = _build_price_action_doc(symbol, trend_rows)
            if price_doc is not None:
                docs.append(price_doc)

        return docs
    finally:
        conn.close()


def _load_sentiment_news_docs(limit_rows: int = 8000, per_symbol_headlines: int = 8) -> list[dict[str, Any]]:
    """Load sentiment aggregates and individual headlines with analytical context."""
    db_path = DATA_DIR / "psx_platform.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        aggregate_rows = conn.execute(
            """
            SELECT symbol,
                   COUNT(*) AS total,
                   AVG(score) AS avg_score,
                   SUM(CASE WHEN label='positive' THEN 1 ELSE 0 END) AS positive_count,
                   SUM(CASE WHEN label='negative' THEN 1 ELSE 0 END) AS negative_count,
                   SUM(CASE WHEN label='neutral' THEN 1 ELSE 0 END) AS neutral_count,
                   MAX(analyzed_at) AS latest_at
            FROM sentiment
            WHERE analyzed_at >= datetime('now', '-60 day')
            GROUP BY symbol
            HAVING total >= 3
            ORDER BY total DESC, latest_at DESC
            LIMIT 5000
            """
        ).fetchall()

        docs: list[dict[str, Any]] = []
        for row in aggregate_rows:
            symbol = str(row["symbol"] or "").strip().upper() or "MARKET"
            total = int(row["total"] or 0)
            pos = int(row["positive_count"] or 0)
            neg = int(row["negative_count"] or 0)
            neu = int(row["neutral_count"] or 0)
            avg_sc = _to_float(row["avg_score"])

            # Calculate sentiment ratio and interpretation
            pos_ratio = (pos / total * 100) if total > 0 else 0
            neg_ratio = (neg / total * 100) if total > 0 else 0

            interpretation = "neutral/mixed — no strong directional sentiment from news"
            if pos_ratio > 60:
                interpretation = "predominantly positive news flow — market narrative is optimistic"
            elif pos_ratio > 45 and neg_ratio < 25:
                interpretation = "leaning positive — more good news than bad, but not overwhelmingly so"
            elif neg_ratio > 60:
                interpretation = "predominantly negative news flow — market narrative shows concern"
            elif neg_ratio > 45 and pos_ratio < 25:
                interpretation = "leaning negative — cautious market narrative"

            text = (
                f"{symbol} sentiment analysis (last 60 days): "
                f"Average sentiment score {_fmt_num(avg_sc, 3)} from {total} news items analyzed. "
                f"Breakdown: {pos} positive ({_fmt_num(pos_ratio)}%), "
                f"{neg} negative ({_fmt_num(neg_ratio)}%), {neu} neutral. "
                f"Interpretation: {interpretation}. "
                f"Last analyzed: {row['latest_at']}."
            )

            docs.append(
                {
                    "id": f"sent::agg::{symbol}",
                    "stock": symbol,
                    "source": "sentiment",
                    "doc_type": "sentiment",
                    "published_at": row["latest_at"],
                    "text": text,
                }
            )

        # Individual headlines with more context
        headline_rows = conn.execute(
            """
            SELECT symbol, headline, label, score, source, analyzed_at
            FROM sentiment
            WHERE analyzed_at >= datetime('now', '-45 day')
              AND headline IS NOT NULL
              AND TRIM(headline) <> ''
            ORDER BY datetime(analyzed_at) DESC
            LIMIT ?
            """,
            (int(limit_rows),),
        ).fetchall()

        seen_per_symbol: dict[str, int] = {}
        seen_texts: set[str] = set()
        for row in headline_rows:
            symbol = str(row["symbol"] or "").strip().upper() or "MARKET"
            if seen_per_symbol.get(symbol, 0) >= per_symbol_headlines:
                continue
            headline = str(row["headline"] or "").strip()
            if not headline:
                continue
            key = f"{symbol}|{headline.lower()}"
            if key in seen_texts:
                continue
            seen_texts.add(key)
            seen_per_symbol[symbol] = seen_per_symbol.get(symbol, 0) + 1

            label = str(row["label"] or "neutral")
            score = _to_float(row["score"])
            impact = "neutral impact"
            if score is not None:
                if score > 0.3:
                    impact = "strong positive signal"
                elif score > 0.1:
                    impact = "mildly positive signal"
                elif score < -0.3:
                    impact = "strong negative signal"
                elif score < -0.1:
                    impact = "mildly negative signal"

            docs.append(
                {
                    "id": f"sent::news::{symbol}::{hashlib.md5(key.encode('utf-8')).hexdigest()[:10]}",
                    "stock": symbol,
                    "source": str(row["source"] or "sentiment"),
                    "doc_type": "news",
                    "published_at": row["analyzed_at"],
                    "text": (
                        f"{symbol} news ({row['analyzed_at']}): \"{headline}\" — "
                        f"Classified as {label} (score {_fmt_num(score, 3)}), {impact}."
                    ),
                }
            )

        return docs
    finally:
        conn.close()


def _fallback_mock_docs() -> list[dict[str, Any]]:
    return [
        {
            "id": "mock::ENGRO",
            "stock": "ENGRO",
            "source": "mock",
            "doc_type": "report",
            "published_at": None,
            "text": "ENGRO operates across fertilizers, energy, and food sectors in Pakistan. Analysts highlight stable demand but note commodity and policy risks.",
        },
        {
            "id": "mock::HBL",
            "stock": "HBL",
            "source": "mock",
            "doc_type": "report",
            "published_at": None,
            "text": "HBL is a large commercial bank with diversified revenue. Positive sentiment comes from digital growth; risk factors include interest-rate volatility and credit quality.",
        },
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_documents() -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    docs.extend(_load_txt_docs())
    docs.extend(_load_csv_docs())
    docs.extend(_load_reports_docs())
    docs.extend(_load_existing_psx_data())
    docs.extend(_load_sentiment_news_docs())

    if not docs:
        docs = _fallback_mock_docs()

    dedup: dict[str, dict[str, Any]] = {}
    text_seen: set[str] = set()
    for doc in docs:
        text = str(doc.get("text") or "").strip()
        if not text:
            continue
        text_key = hashlib.sha256(text.lower().encode("utf-8")).hexdigest()
        if text_key in text_seen:
            continue
        text_seen.add(text_key)
        dedup[str(doc.get("id"))] = doc
    return list(dedup.values())


def save_mock_dataset_if_missing() -> None:
    RAG_DOCS_DIR.mkdir(parents=True, exist_ok=True)

    news_path = RAG_DOCS_DIR / "ENGRO_news.txt"
    if not news_path.exists():
        news_path.write_text(
            "ENGRO posted resilient earnings with steady fertilizer demand. Some analysts remain cautious on import costs and currency pressure.",
            encoding="utf-8",
        )

    csv_path = RAG_DOCS_DIR / "stock_summaries.csv"
    if not csv_path.exists():
        csv_path.write_text(
            "symbol,company,summary,sentiment_hint\n"
            "ENGRO,Engro Corporation,Strong diversified business with recurring demand,positive\n"
            "HBL,Habib Bank Limited,Large banking franchise with digital expansion,neutral\n"
            "TRG,TRG Pakistan,Technology exposure with higher volatility,negative\n",
            encoding="utf-8",
        )
