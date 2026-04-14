"""Comprehensive data ingestion for the PSX RAG pipeline.

Builds deeply contextual documents from:
1. SQLite historical data — per-stock profiles, trends, price action, weekly/monthly summaries
2. PSX company pages — fundamentals, announcements, dividends from dps.psx.com.pk
3. Sentiment/news database — aggregated + individual headlines
4. Market-wide breadth — index-level analysis, sector performance, advancers/decliners
5. File-based documents — txt, csv, json reports
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import sqlite3
from datetime import datetime
from typing import Any

import requests
from bs4 import BeautifulSoup

from .config import DATA_DIR, RAG_DOCS_DIR

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

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


def _rsi(changes: list[float], period: int = 14) -> float | None:
    """Calculate RSI from a list of daily changes (most recent first)."""
    if len(changes) < period:
        return None
    gains = [max(c, 0) for c in changes[:period]]
    losses = [abs(min(c, 0)) for c in changes[:period]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


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

    price_range_pct = None
    if min_close and max_close and min_close > 0:
        price_range_pct = ((max_close - min_close) / min_close) * 100

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

    close_5 = _to_float(rows[min(4, len(rows) - 1)]["close"])
    close_20 = _to_float(rows[min(19, len(rows) - 1)]["close"])
    close_60 = _to_float(rows[min(59, len(rows) - 1)]["close"])

    ret_5 = _pct_change(latest_close, close_5)
    ret_20 = _pct_change(latest_close, close_20)
    ret_60 = _pct_change(latest_close, close_60)

    closes = [_to_float(r["close"]) for r in rows]
    closes = [c for c in closes if c is not None]

    ma20 = sum(closes[:20]) / min(20, len(closes)) if closes else None
    ma60 = sum(closes[:60]) / min(60, len(closes)) if len(closes) >= 20 else None

    changes = [_to_float(r["change_pct"]) for r in rows[:20]]
    changes = [c for c in changes if c is not None]
    volatility = None
    if len(changes) >= 5:
        mean_ch = sum(changes) / len(changes)
        volatility = math.sqrt(sum((c - mean_ch) ** 2 for c in changes) / len(changes))

    # RSI calculation
    all_changes = [_to_float(r["change_pct"]) for r in rows[:30]]
    all_changes = [c for c in all_changes if c is not None]
    rsi_val = _rsi(all_changes)

    volumes = [_to_float(r["volume"]) for r in rows[:20]]
    volumes = [v for v in volumes if v is not None]
    avg_vol_20 = sum(volumes) / len(volumes) if volumes else None

    recent_highs = closes[:20] if closes else []
    recent_high = max(recent_highs) if recent_highs else None
    recent_low = min(recent_highs) if recent_highs else None

    # 52-week high/low
    closes_252 = closes[:252] if len(closes) >= 50 else closes
    year_high = max(closes_252) if closes_252 else None
    year_low = min(closes_252) if closes_252 else None

    text = f"{symbol} multi-timeframe trend analysis as of {latest['date']}: "
    text += f"Latest close {_fmt_num(latest_close)} PKR, last session change {_fmt_num(latest_change)}%. "

    text += f"Short-term (5-session): {_trend_word(ret_5)} with {_fmt_num(ret_5)}% return. "
    text += f"Medium-term (20-session): {_trend_word(ret_20)} with {_fmt_num(ret_20)}% return. "
    text += f"Long-term (60-session): {_trend_word(ret_60)} with {_fmt_num(ret_60)}% return. "

    # Momentum acceleration
    if ret_5 is not None and ret_20 is not None:
        if ret_5 > ret_20 > 0:
            text += "Momentum is ACCELERATING — short-term gains outpacing medium-term, indicating strengthening trend. "
        elif ret_5 < ret_20 and ret_20 > 0:
            text += "Momentum is DECELERATING — short-term slowing despite positive medium-term, possible pullback forming. "
        elif ret_5 < 0 and ret_20 > 0:
            text += "Short-term DIVERGENCE from medium-term trend — recent weakness despite broader uptrend, watch for reversal or continuation. "
        elif ret_5 > 0 and ret_20 < 0:
            text += "Potential TREND REVERSAL forming — short-term recovery while medium-term still negative. "

    if ma20 and latest_close:
        ma20_pos = "above" if latest_close > ma20 else "below"
        ma20_dist = ((latest_close / ma20) - 1) * 100
        text += f"Price is {ma20_pos} 20-day MA ({_fmt_num(ma20)}) by {_fmt_num(abs(ma20_dist))}%. "
    if ma60 and latest_close:
        ma60_pos = "above" if latest_close > ma60 else "below"
        text += f"Price is {ma60_pos} 60-day MA ({_fmt_num(ma60)}). "
    if ma20 and ma60:
        if ma20 > ma60:
            text += "GOLDEN CROSS: 20-day MA above 60-day MA signals bullish momentum. "
        else:
            text += "DEATH CROSS: 20-day MA below 60-day MA signals bearish pressure. "

    # RSI
    if rsi_val is not None:
        rsi_label = "OVERBOUGHT (>70)" if rsi_val > 70 else "OVERSOLD (<30)" if rsi_val < 30 else "NEUTRAL range"
        text += f"RSI(14): {_fmt_num(rsi_val, 1)} — {rsi_label}. "

    text += f"Recent volatility: {_volatility_label(volatility)} ({_fmt_num(volatility)}% daily std dev). "
    text += f"Latest volume {_fmt_num(latest_vol, 0)}, {_volume_context(latest_vol, avg_vol_20)}. "

    if recent_high and recent_low:
        text += f"20-session trading range: {_fmt_num(recent_low)} (support) to {_fmt_num(recent_high)} (resistance). "
        if latest_close and recent_high != recent_low:
            range_pos = ((latest_close - recent_low) / (recent_high - recent_low) * 100)
            text += f"Current price sits at {_fmt_num(range_pos)}% of this range. "

    if year_high and year_low and latest_close:
        text += f"52-week range: {_fmt_num(year_low)} to {_fmt_num(year_high)} PKR. "
        dist_from_high = ((latest_close / year_high) - 1) * 100 if year_high else None
        if dist_from_high is not None:
            text += f"Currently {_fmt_num(abs(dist_from_high))}% {'above' if dist_from_high > 0 else 'below'} 52-week high. "

    signals_bull = sum([
        1 if (ret_5 or 0) > 0 else 0,
        1 if (ret_20 or 0) > 0 else 0,
        1 if (ret_60 or 0) > 0 else 0,
        1 if ma20 and ma60 and ma20 > ma60 else 0,
        1 if latest_close and ma20 and latest_close > ma20 else 0,
        1 if rsi_val and 40 < rsi_val < 70 else 0,
    ])
    total_signals = 6
    signals_bear = total_signals - signals_bull

    if signals_bull >= 5:
        text += f"Overall momentum: STRONG BULLISH ({signals_bull}/{total_signals} bullish signals). "
    elif signals_bull >= 4:
        text += f"Overall momentum: MODERATELY BULLISH ({signals_bull}/{total_signals} bullish signals). "
    elif signals_bull >= 3:
        text += f"Overall momentum: MIXED/TRANSITIONAL ({signals_bull}/{total_signals} bullish signals). "
    elif signals_bull >= 2:
        text += f"Overall momentum: MODERATELY BEARISH ({signals_bear}/{total_signals} bearish signals). "
    else:
        text += f"Overall momentum: STRONG BEARISH ({signals_bear}/{total_signals} bearish signals). "

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
        high = _to_float(row.get("high")) if "high" in row.keys() else None
        low = _to_float(row.get("low")) if "low" in row.keys() else None
        direction = "gained" if (change or 0) > 0 else "declined" if (change or 0) < 0 else "unchanged"
        text += f"On {row['date']}: closed at {_fmt_num(close)} PKR, {direction} {_fmt_num(abs(change or 0))}%, volume {_fmt_num(vol, 0)}"
        if high and low:
            intraday_range = ((high - low) / low * 100) if low > 0 else 0
            text += f", intraday range {_fmt_num(low)}-{_fmt_num(high)} ({_fmt_num(intraday_range)}% spread)"
        text += ". "

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

    # Cumulative 5-session performance
    first_close = _to_float(sessions[-1]["close"]) if sessions else None
    last_close = _to_float(sessions[0]["close"]) if sessions else None
    if first_close and last_close and first_close > 0:
        cum_return = ((last_close / first_close) - 1) * 100
        text += f"Cumulative 5-session return: {_fmt_num(cum_return)}%. "

    return {
        "id": f"db::priceaction::{symbol}",
        "stock": symbol,
        "text": text,
        "source": "psx_platform.db",
        "doc_type": "price_action",
        "published_at": sessions[0]["date"],
    }


def _build_weekly_summary_doc(symbol: str, rows: list[sqlite3.Row]) -> dict[str, Any] | None:
    """Build weekly summary for last 4 weeks — helps answer 'this week' questions."""
    if len(rows) < 10:
        return None

    text = f"{symbol} weekly performance summary (last 4 weeks): "
    week_data = []
    for week_idx in range(4):
        start = week_idx * 5
        end = min(start + 5, len(rows))
        if end <= start or start >= len(rows):
            break
        week_rows = rows[start:end]
        closes = [_to_float(r["close"]) for r in week_rows]
        closes = [c for c in closes if c is not None]
        volumes = [_to_float(r["volume"]) for r in week_rows]
        volumes = [v for v in volumes if v is not None]
        if not closes:
            continue

        week_open = closes[-1]
        week_close = closes[0]
        week_high = max(closes)
        week_low = min(closes)
        week_return = _pct_change(week_close, week_open)
        avg_vol = sum(volumes) / len(volumes) if volumes else 0

        label = f"Week {week_idx + 1} (most recent)" if week_idx == 0 else f"Week {week_idx + 1}"
        dates = f"{week_rows[-1]['date']} to {week_rows[0]['date']}"
        text += (
            f"{label} ({dates}): opened ~{_fmt_num(week_open)}, closed {_fmt_num(week_close)}, "
            f"high {_fmt_num(week_high)}, low {_fmt_num(week_low)}, "
            f"return {_fmt_num(week_return)}% ({_trend_word(week_return)}), "
            f"avg volume {_fmt_num(avg_vol, 0)}. "
        )
        week_data.append({"return": week_return, "idx": week_idx})

    # Week-over-week trend
    if len(week_data) >= 2:
        improving = all(
            (week_data[i]["return"] or 0) > (week_data[i + 1]["return"] or 0)
            for i in range(len(week_data) - 1)
        )
        deteriorating = all(
            (week_data[i]["return"] or 0) < (week_data[i + 1]["return"] or 0)
            for i in range(len(week_data) - 1)
        )
        if improving:
            text += "TREND: Week-over-week returns are IMPROVING — bullish momentum building. "
        elif deteriorating:
            text += "TREND: Week-over-week returns are DETERIORATING — momentum fading. "
        else:
            text += "TREND: Week-over-week performance is MIXED — no clear directional bias. "

    return {
        "id": f"db::weekly::{symbol}",
        "stock": symbol,
        "text": text,
        "source": "psx_platform.db",
        "doc_type": "trend",
        "published_at": rows[0]["date"],
    }


def _build_monthly_summary_doc(symbol: str, rows: list[sqlite3.Row]) -> dict[str, Any] | None:
    """Build monthly summary for last 3 months — helps answer performance questions."""
    if len(rows) < 22:
        return None

    text = f"{symbol} monthly performance summary (last 3 months): "
    for month_idx in range(3):
        start = month_idx * 22
        end = min(start + 22, len(rows))
        if end <= start or start >= len(rows):
            break
        month_rows = rows[start:end]
        closes = [_to_float(r["close"]) for r in month_rows]
        closes = [c for c in closes if c is not None]
        volumes = [_to_float(r["volume"]) for r in month_rows]
        volumes = [v for v in volumes if v is not None]
        if not closes:
            continue

        month_open = closes[-1]
        month_close = closes[0]
        month_high = max(closes)
        month_low = min(closes)
        month_return = _pct_change(month_close, month_open)
        total_vol = sum(volumes)

        label = "Current month" if month_idx == 0 else f"Month-{month_idx + 1}"
        text += (
            f"{label} ({month_rows[-1]['date']} to {month_rows[0]['date']}): "
            f"open {_fmt_num(month_open)}, close {_fmt_num(month_close)}, "
            f"high {_fmt_num(month_high)}, low {_fmt_num(month_low)}, "
            f"return {_fmt_num(month_return)}% ({_trend_word(month_return)}), "
            f"total volume {_fmt_num(total_vol, 0)}. "
        )

    return {
        "id": f"db::monthly::{symbol}",
        "stock": symbol,
        "text": text,
        "source": "psx_platform.db",
        "doc_type": "historical",
        "published_at": rows[0]["date"],
    }


def _build_key_levels_doc(symbol: str, rows: list[sqlite3.Row]) -> dict[str, Any] | None:
    """Build support/resistance and key technical levels document."""
    if len(rows) < 20:
        return None

    closes = [_to_float(r["close"]) for r in rows]
    closes = [c for c in closes if c is not None]
    if len(closes) < 20:
        return None

    latest_close = closes[0]

    # Various support/resistance levels
    high_20 = max(closes[:20])
    low_20 = min(closes[:20])
    high_60 = max(closes[:min(60, len(closes))])
    low_60 = min(closes[:min(60, len(closes))])

    # Pivot points (classic)
    if len(rows) >= 2:
        prev = rows[1]
        h = _to_float(prev.get("high")) if "high" in prev.keys() else None
        l = _to_float(prev.get("low")) if "low" in prev.keys() else None
        c = _to_float(prev["close"])
        if h and l and c:
            pivot = (h + l + c) / 3
            r1 = 2 * pivot - l
            s1 = 2 * pivot - h
            r2 = pivot + (h - l)
            s2 = pivot - (h - l)
        else:
            pivot = r1 = s1 = r2 = s2 = None
    else:
        pivot = r1 = s1 = r2 = s2 = None

    text = f"{symbol} key technical levels: "
    text += f"Current price: {_fmt_num(latest_close)} PKR. "
    text += f"20-day support: {_fmt_num(low_20)}, 20-day resistance: {_fmt_num(high_20)}. "
    text += f"60-day support: {_fmt_num(low_60)}, 60-day resistance: {_fmt_num(high_60)}. "

    if pivot:
        text += (
            f"Classic pivot points — Pivot: {_fmt_num(pivot)}, "
            f"R1: {_fmt_num(r1)}, R2: {_fmt_num(r2)}, "
            f"S1: {_fmt_num(s1)}, S2: {_fmt_num(s2)}. "
        )

    # Distance from key levels
    if latest_close and high_20:
        dist_res = ((high_20 - latest_close) / latest_close * 100) if latest_close > 0 else 0
        dist_sup = ((latest_close - low_20) / latest_close * 100) if latest_close > 0 else 0
        text += f"Distance to 20-day resistance: +{_fmt_num(dist_res)}%, distance to 20-day support: -{_fmt_num(dist_sup)}%. "

        if dist_res < 2:
            text += "NEAR RESISTANCE — price testing upper boundary, potential breakout or rejection. "
        elif dist_sup < 2:
            text += "NEAR SUPPORT — price testing lower boundary, potential bounce or breakdown. "

    return {
        "id": f"db::keylevels::{symbol}",
        "stock": symbol,
        "text": text,
        "source": "psx_platform.db",
        "doc_type": "trend",
        "published_at": rows[0]["date"],
    }


def _build_volume_analysis_doc(symbol: str, rows: list[sqlite3.Row]) -> dict[str, Any] | None:
    """Build detailed volume analysis document."""
    if len(rows) < 10:
        return None

    volumes = [_to_float(r["volume"]) for r in rows[:60]]
    volumes = [v for v in volumes if v is not None and v > 0]
    if len(volumes) < 5:
        return None

    latest_vol = volumes[0]
    avg_vol_5 = sum(volumes[:5]) / min(5, len(volumes))
    avg_vol_20 = sum(volumes[:20]) / min(20, len(volumes))
    avg_vol_60 = sum(volumes[:min(60, len(volumes))]) / min(60, len(volumes))
    max_vol = max(volumes[:20]) if volumes else 0
    min_vol = min(volumes[:20]) if volumes else 0

    text = f"{symbol} volume analysis: "
    text += f"Latest volume: {_fmt_num(latest_vol, 0)} shares. "
    text += f"5-day avg volume: {_fmt_num(avg_vol_5, 0)}, 20-day avg: {_fmt_num(avg_vol_20, 0)}, 60-day avg: {_fmt_num(avg_vol_60, 0)}. "
    text += f"20-day volume range: {_fmt_num(min_vol, 0)} to {_fmt_num(max_vol, 0)}. "

    # Volume trend
    if avg_vol_5 > avg_vol_20 * 1.5:
        text += "VOLUME SURGE: 5-day average is 1.5x above 20-day — significant increase in trading activity, possible institutional activity. "
    elif avg_vol_5 > avg_vol_20 * 1.2:
        text += "Volume expanding: 5-day average above 20-day — rising interest. "
    elif avg_vol_5 < avg_vol_20 * 0.6:
        text += "Volume contracting: 5-day average well below 20-day — declining interest, possible consolidation. "
    else:
        text += "Volume is at normal levels relative to recent averages. "

    # Price-volume relationship
    recent_changes = [_to_float(r["change_pct"]) for r in rows[:5]]
    recent_changes = [c for c in recent_changes if c is not None]
    if recent_changes and avg_vol_5:
        avg_change = sum(recent_changes) / len(recent_changes)
        if avg_change > 0.5 and avg_vol_5 > avg_vol_20:
            text += "BULLISH CONFIRMATION: Price rising on above-average volume — strong conviction behind the move. "
        elif avg_change > 0.5 and avg_vol_5 < avg_vol_20 * 0.8:
            text += "WEAK RALLY: Price rising on below-average volume — lack of conviction, move may not sustain. "
        elif avg_change < -0.5 and avg_vol_5 > avg_vol_20:
            text += "BEARISH CONFIRMATION: Price falling on above-average volume — selling pressure is strong. "
        elif avg_change < -0.5 and avg_vol_5 < avg_vol_20 * 0.8:
            text += "LIGHT SELLING: Price falling on low volume — may be normal profit-taking rather than distribution. "

    return {
        "id": f"db::volume::{symbol}",
        "stock": symbol,
        "text": text,
        "source": "psx_platform.db",
        "doc_type": "price_action",
        "published_at": rows[0]["date"],
    }


# ---------------------------------------------------------------------------
# PSX Company Page Scraper — fundamentals, announcements, dividends
# ---------------------------------------------------------------------------

def _scrape_psx_company_docs(symbol: str) -> list[dict[str, Any]]:
    """Scrape dps.psx.com.pk/company/{symbol} for fundamentals and announcements."""
    docs: list[dict[str, Any]] = []
    url = f"https://dps.psx.com.pk/company/{symbol}"

    try:
        resp = requests.get(url, timeout=15, headers=HEADERS)
        if resp.status_code != 200:
            return docs
    except Exception:
        return docs

    soup = BeautifulSoup(resp.text, "html.parser")

    # --- Company profile info ---
    profile_parts = []

    # Company name from page title or header
    header = soup.select_one("h1, .company-name, .profile-header")
    if header:
        profile_parts.append(f"Company: {header.get_text(' ', strip=True)}")

    # Sector/industry info
    for label_el in soup.select(".info-label, dt, th"):
        label_text = label_el.get_text(" ", strip=True).lower()
        value_el = label_el.find_next_sibling() or label_el.find_next()
        if not value_el:
            continue
        value_text = value_el.get_text(" ", strip=True)
        if not value_text or len(value_text) > 200:
            continue

        if any(k in label_text for k in ["sector", "industry", "listed in", "market"]):
            profile_parts.append(f"Sector/Industry: {value_text}")
        elif any(k in label_text for k in ["shares", "outstanding", "free float", "market cap"]):
            profile_parts.append(f"{label_text.title()}: {value_text}")
        elif any(k in label_text for k in ["face value", "par value"]):
            profile_parts.append(f"Face value: {value_text}")
        elif "eps" in label_text:
            profile_parts.append(f"EPS: {value_text}")
        elif any(k in label_text for k in ["p/e", "pe ratio", "price earning"]):
            profile_parts.append(f"P/E ratio: {value_text}")
        elif any(k in label_text for k in ["book value", "nav"]):
            profile_parts.append(f"Book value: {value_text}")
        elif "dividend" in label_text:
            profile_parts.append(f"Dividend: {value_text}")

    if profile_parts:
        docs.append({
            "id": f"psx::profile::{symbol}",
            "stock": symbol,
            "text": f"{symbol} PSX company profile: " + " | ".join(profile_parts),
            "source": "dps.psx.com.pk",
            "doc_type": "fundamentals",
            "published_at": datetime.utcnow().strftime("%Y-%m-%d"),
        })

    # --- Announcements / Corporate actions ---
    announcements = []
    for row in soup.select("table tr"):
        text = row.get_text(" ", strip=True)
        if not text or len(text) < 15:
            continue
        text_lower = text.lower()
        if any(k in text_lower for k in [
            "financial", "report", "dividend", "board", "announcement",
            "agm", "meeting", "bonus", "right", "earnings", "quarter",
            "annual", "half year", "interim", "result", "profit", "loss",
            "transmission", "compliance"
        ]):
            announcements.append(text[:500])

    if announcements:
        announcement_text = f"{symbol} PSX corporate announcements and filings: "
        for i, ann in enumerate(announcements[:15]):
            announcement_text += f"[{i+1}] {ann}. "

        docs.append({
            "id": f"psx::announcements::{symbol}",
            "stock": symbol,
            "text": announcement_text,
            "source": "dps.psx.com.pk",
            "doc_type": "announcement",
            "published_at": datetime.utcnow().strftime("%Y-%m-%d"),
        })

    # --- Financial highlights tables ---
    financial_parts = []
    for table in soup.select("table"):
        header_row = table.select_one("tr")
        if not header_row:
            continue
        header_text = header_row.get_text(" ", strip=True).lower()
        if not any(k in header_text for k in [
            "eps", "revenue", "profit", "loss", "dividend",
            "earning", "financial", "quarter", "half year", "annual"
        ]):
            continue

        for row in table.select("tr")[1:6]:
            cells = [td.get_text(" ", strip=True) for td in row.select("td, th")]
            if cells and len(cells) >= 2:
                financial_parts.append(" | ".join(cells))

    if financial_parts:
        docs.append({
            "id": f"psx::financials::{symbol}",
            "stock": symbol,
            "text": f"{symbol} financial highlights from PSX: " + " || ".join(financial_parts[:20]),
            "source": "dps.psx.com.pk",
            "doc_type": "fundamentals",
            "published_at": datetime.utcnow().strftime("%Y-%m-%d"),
        })

    return docs


# ---------------------------------------------------------------------------
# Market-wide breadth documents
# ---------------------------------------------------------------------------

def _build_market_breadth_docs(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Build market-wide analysis documents for MARKET scope queries."""
    docs: list[dict[str, Any]] = []

    try:
        # Get latest date
        latest_date_row = conn.execute(
            "SELECT MAX(date) as latest_date FROM stocks"
        ).fetchone()
        if not latest_date_row or not latest_date_row["latest_date"]:
            return docs
        latest_date = latest_date_row["latest_date"]

        # Market breadth for latest session
        breadth = conn.execute(
            """
            SELECT
                COUNT(*) as total_stocks,
                SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) as advancers,
                SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) as decliners,
                SUM(CASE WHEN change_pct = 0 OR change_pct IS NULL THEN 1 ELSE 0 END) as unchanged,
                AVG(change_pct) as avg_change,
                SUM(volume) as total_volume,
                AVG(volume) as avg_volume
            FROM stocks
            WHERE date = ?
            """,
            (latest_date,),
        ).fetchone()

        if breadth and breadth["total_stocks"]:
            adv = int(breadth["advancers"] or 0)
            dec = int(breadth["decliners"] or 0)
            total = int(breadth["total_stocks"] or 0)
            avg_ch = _to_float(breadth["avg_change"])
            adv_ratio = (adv / total * 100) if total > 0 else 0

            breadth_signal = "BULLISH"
            if adv_ratio > 65:
                breadth_signal = "STRONGLY BULLISH"
            elif adv_ratio > 55:
                breadth_signal = "MODERATELY BULLISH"
            elif adv_ratio > 45:
                breadth_signal = "NEUTRAL/MIXED"
            elif adv_ratio > 35:
                breadth_signal = "MODERATELY BEARISH"
            else:
                breadth_signal = "STRONGLY BEARISH"

            text = (
                f"PSX market breadth analysis for {latest_date}: "
                f"Total stocks traded: {total}. "
                f"Advancers: {adv} ({_fmt_num(adv_ratio)}%), Decliners: {dec} ({_fmt_num(dec / total * 100 if total > 0 else 0)}%), "
                f"Unchanged: {int(breadth['unchanged'] or 0)}. "
                f"Average market change: {_fmt_num(avg_ch)}%. "
                f"Total market volume: {_fmt_num(_to_float(breadth['total_volume']), 0)} shares. "
                f"Breadth signal: {breadth_signal}. "
                f"Advance/Decline ratio: {_fmt_num(adv / max(dec, 1), 2)}. "
            )

            docs.append({
                "id": f"market::breadth::{latest_date}",
                "stock": "MARKET",
                "text": text,
                "source": "psx_platform.db",
                "doc_type": "trend",
                "published_at": latest_date,
            })

        # Top gainers and losers
        top_gainers = conn.execute(
            """
            SELECT symbol, close, change_pct, volume
            FROM stocks
            WHERE date = ? AND change_pct IS NOT NULL
            ORDER BY change_pct DESC
            LIMIT 10
            """,
            (latest_date,),
        ).fetchall()

        top_losers = conn.execute(
            """
            SELECT symbol, close, change_pct, volume
            FROM stocks
            WHERE date = ? AND change_pct IS NOT NULL
            ORDER BY change_pct ASC
            LIMIT 10
            """,
            (latest_date,),
        ).fetchall()

        if top_gainers:
            text = f"PSX top gainers on {latest_date}: "
            for i, r in enumerate(top_gainers):
                text += f"{i+1}. {r['symbol']}: {_fmt_num(r['close'])} PKR (+{_fmt_num(r['change_pct'])}%), vol {_fmt_num(r['volume'], 0)}. "
            docs.append({
                "id": f"market::gainers::{latest_date}",
                "stock": "MARKET",
                "text": text,
                "source": "psx_platform.db",
                "doc_type": "price_action",
                "published_at": latest_date,
            })

        if top_losers:
            text = f"PSX top losers on {latest_date}: "
            for i, r in enumerate(top_losers):
                text += f"{i+1}. {r['symbol']}: {_fmt_num(r['close'])} PKR ({_fmt_num(r['change_pct'])}%), vol {_fmt_num(r['volume'], 0)}. "
            docs.append({
                "id": f"market::losers::{latest_date}",
                "stock": "MARKET",
                "text": text,
                "source": "psx_platform.db",
                "doc_type": "price_action",
                "published_at": latest_date,
            })

        # Most active by volume
        most_active = conn.execute(
            """
            SELECT symbol, close, change_pct, volume
            FROM stocks
            WHERE date = ? AND volume IS NOT NULL
            ORDER BY volume DESC
            LIMIT 10
            """,
            (latest_date,),
        ).fetchall()

        if most_active:
            text = f"PSX most actively traded stocks on {latest_date}: "
            for i, r in enumerate(most_active):
                text += f"{i+1}. {r['symbol']}: volume {_fmt_num(r['volume'], 0)}, close {_fmt_num(r['close'])} PKR ({_fmt_num(r['change_pct'])}%). "
            docs.append({
                "id": f"market::volume_leaders::{latest_date}",
                "stock": "MARKET",
                "text": text,
                "source": "psx_platform.db",
                "doc_type": "price_action",
                "published_at": latest_date,
            })

        # Multi-day market trend (last 5 sessions)
        market_days = conn.execute(
            """
            SELECT date,
                   COUNT(*) as total,
                   SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) as advancers,
                   SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) as decliners,
                   AVG(change_pct) as avg_change,
                   SUM(volume) as total_volume
            FROM stocks
            WHERE date IN (SELECT DISTINCT date FROM stocks ORDER BY date DESC LIMIT 5)
            GROUP BY date
            ORDER BY date DESC
            """,
        ).fetchall()

        if market_days and len(market_days) >= 2:
            text = "PSX market trend over last 5 sessions: "
            for day in market_days:
                adv = int(day["advancers"] or 0)
                dec = int(day["decliners"] or 0)
                text += (
                    f"{day['date']}: advancers {adv}, decliners {dec}, "
                    f"avg change {_fmt_num(day['avg_change'])}%, "
                    f"total volume {_fmt_num(_to_float(day['total_volume']), 0)}. "
                )
            # Trend assessment
            changes = [_to_float(d["avg_change"]) for d in market_days]
            changes = [c for c in changes if c is not None]
            if changes:
                improving = all(changes[i] >= changes[i+1] for i in range(len(changes)-1))
                deteriorating = all(changes[i] <= changes[i+1] for i in range(len(changes)-1))
                if improving:
                    text += "Market trend: IMPROVING — each session better than the previous. "
                elif deteriorating:
                    text += "Market trend: DETERIORATING — each session worse than the previous. "
                else:
                    text += "Market trend: MIXED — no consistent direction across sessions. "

            docs.append({
                "id": "market::multiday_trend",
                "stock": "MARKET",
                "text": text,
                "source": "psx_platform.db",
                "doc_type": "trend",
                "published_at": latest_date,
            })

    except Exception as exc:
        logger.warning("Failed to build market breadth docs: %s", exc)

    return docs


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

            # Fetch detailed rows for this symbol
            trend_rows = conn.execute(
                """
                SELECT date, open, high, low, close, change_pct, volume
                FROM stocks
                WHERE symbol = ?
                ORDER BY date DESC
                LIMIT 252
                """,
                (symbol,),
            ).fetchall()

            # 2. Multi-timeframe trend analysis
            trend_doc = _build_trend_doc(symbol, trend_rows)
            if trend_doc is not None:
                docs.append(trend_doc)

            # 3. Recent price action detail
            price_doc = _build_price_action_doc(symbol, trend_rows)
            if price_doc is not None:
                docs.append(price_doc)

            # 4. Weekly summary (last 4 weeks)
            weekly_doc = _build_weekly_summary_doc(symbol, trend_rows)
            if weekly_doc is not None:
                docs.append(weekly_doc)

            # 5. Monthly summary (last 3 months)
            monthly_doc = _build_monthly_summary_doc(symbol, trend_rows)
            if monthly_doc is not None:
                docs.append(monthly_doc)

            # 6. Key technical levels (support/resistance/pivots)
            levels_doc = _build_key_levels_doc(symbol, trend_rows)
            if levels_doc is not None:
                docs.append(levels_doc)

            # 7. Volume analysis
            volume_doc = _build_volume_analysis_doc(symbol, trend_rows)
            if volume_doc is not None:
                docs.append(volume_doc)

        # 8. Market-wide breadth documents
        docs.extend(_build_market_breadth_docs(conn))

        return docs
    finally:
        conn.close()


def _load_sentiment_news_docs(limit_rows: int = 12000, per_symbol_headlines: int = 15) -> list[dict[str, Any]]:
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
            HAVING total >= 2
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

            # Sentiment trend (compare recent 7 days vs older)
            recent_7d = conn.execute(
                """
                SELECT AVG(score) as recent_avg
                FROM sentiment
                WHERE symbol = ? AND analyzed_at >= datetime('now', '-7 day')
                """,
                (symbol,),
            ).fetchone()
            older_avg = conn.execute(
                """
                SELECT AVG(score) as older_avg
                FROM sentiment
                WHERE symbol = ? AND analyzed_at < datetime('now', '-7 day') AND analyzed_at >= datetime('now', '-60 day')
                """,
                (symbol,),
            ).fetchone()

            trend_note = ""
            recent_sc = _to_float(recent_7d["recent_avg"]) if recent_7d else None
            older_sc = _to_float(older_avg["older_avg"]) if older_avg else None
            if recent_sc is not None and older_sc is not None:
                diff = recent_sc - older_sc
                if diff > 0.05:
                    trend_note = f"Sentiment IMPROVING: 7-day avg ({_fmt_num(recent_sc, 3)}) is higher than 60-day avg ({_fmt_num(older_sc, 3)}). "
                elif diff < -0.05:
                    trend_note = f"Sentiment DETERIORATING: 7-day avg ({_fmt_num(recent_sc, 3)}) is lower than 60-day avg ({_fmt_num(older_sc, 3)}). "
                else:
                    trend_note = f"Sentiment STABLE: 7-day avg ({_fmt_num(recent_sc, 3)}) vs 60-day avg ({_fmt_num(older_sc, 3)}). "

            text = (
                f"{symbol} sentiment analysis (last 60 days): "
                f"Average sentiment score {_fmt_num(avg_sc, 3)} from {total} news items analyzed. "
                f"Breakdown: {pos} positive ({_fmt_num(pos_ratio)}%), "
                f"{neg} negative ({_fmt_num(neg_ratio)}%), {neu} neutral. "
                f"Interpretation: {interpretation}. "
                f"{trend_note}"
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
                    impact = "strong positive signal — likely bullish catalyst"
                elif score > 0.1:
                    impact = "mildly positive signal"
                elif score < -0.3:
                    impact = "strong negative signal — potential risk factor"
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


def _load_psx_company_data(max_symbols: int = 200) -> list[dict[str, Any]]:
    """Load company fundamentals from PSX website for all tracked symbols."""
    db_path = DATA_DIR / "psx_platform.db"
    if not db_path.exists():
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        symbols = conn.execute(
            """
            SELECT DISTINCT symbol
            FROM stocks
            ORDER BY symbol ASC
            LIMIT ?
            """,
            (max_symbols,),
        ).fetchall()
    finally:
        conn.close()

    docs: list[dict[str, Any]] = []
    for row in symbols:
        symbol = str(row["symbol"]).upper()
        try:
            symbol_docs = _scrape_psx_company_docs(symbol)
            docs.extend(symbol_docs)
        except Exception as exc:
            logger.debug("PSX scrape failed for %s: %s", symbol, exc)

    return docs


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

def load_documents(include_psx_scrape: bool = True) -> list[dict[str, Any]]:
    """Load all documents for the RAG index.

    Args:
        include_psx_scrape: If True, scrape dps.psx.com.pk for company data.
                           Set to False for faster reindex during development.
    """
    docs: list[dict[str, Any]] = []

    # File-based sources
    docs.extend(_load_txt_docs())
    docs.extend(_load_csv_docs())
    docs.extend(_load_reports_docs())

    # Database-generated deep analysis docs
    docs.extend(_load_existing_psx_data())
    docs.extend(_load_sentiment_news_docs())

    # PSX website company data (fundamentals, announcements)
    if include_psx_scrape:
        try:
            psx_docs = _load_psx_company_data(max_symbols=200)
            docs.extend(psx_docs)
            logger.info("Loaded %d PSX company documents", len(psx_docs))
        except Exception as exc:
            logger.warning("PSX company scrape failed, continuing without: %s", exc)

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
