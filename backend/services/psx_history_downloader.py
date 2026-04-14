from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from config import CSV_PATH, DATA_DIR, DB_PATH, STOCKS_NAME_PATH
from services.market_data import load_watchlist

PSX_BASE = "https://dps.psx.com.pk"
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

REQUIRED_COLUMNS = [
    "SYMBOL",
    "LDCP",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "CHANGE",
    "CHANGE (%)",
    "VOLUME",
    "DATE",
    "TIMESTAMP",
]


@dataclass
class DownloadStats:
    symbols_total: int
    symbols_ok: int
    symbols_failed: int
    rows_downloaded: int
    output_csv: str
    failed_symbols: list[str]
    fallback_symbols: list[str]


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    raw = raw.replace(",", "").replace("%", "").replace("Rs.", "").replace("(", "").replace(")", "")
    try:
        return float(raw)
    except ValueError:
        return None


def _safe_date(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%b %d, %Y", "%d %b %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        return pd.to_datetime(raw, errors="raise").strftime("%Y-%m-%d")
    except Exception:
        return None


def _extract_records_from_json(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        for key in (
            "data",
            "prices",
            "historical",
            "history",
            "records",
            "result",
            "rows",
            "dataset",
        ):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [x for x in candidate if isinstance(x, dict)]
        if all(k in payload for k in ("date", "close")):
            return [payload]
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def _normalize_price_record(symbol: str, row: dict[str, Any]) -> dict[str, Any] | None:
    date = _safe_date(
        row.get("date")
        or row.get("Date")
        or row.get("trading_date")
        or row.get("display_date")
        or row.get("session_date")
    )
    if not date:
        return None

    close = _safe_float(row.get("close") or row.get("Close") or row.get("price") or row.get("close_price"))
    if close is None:
        return None

    open_p = _safe_float(row.get("open") or row.get("Open") or row.get("open_price"))
    high = _safe_float(row.get("high") or row.get("High") or row.get("high_price"))
    low = _safe_float(row.get("low") or row.get("Low") or row.get("low_price"))
    ldcp = _safe_float(row.get("ldcp") or row.get("LDCP") or row.get("previous_close") or row.get("prev_close"))
    change = _safe_float(row.get("change") or row.get("Change") or row.get("diff") or row.get("price_change"))
    change_pct = _safe_float(
        row.get("change_pct")
        or row.get("change_percent")
        or row.get("CHANGE (%)")
        or row.get("percent_change")
        or row.get("pct")
    )
    volume = _safe_float(row.get("volume") or row.get("Volume") or row.get("vol") or row.get("trade_volume"))
    timestamp = row.get("timestamp") or row.get("time") or f"{date} 00:00:00"

    return {
        "SYMBOL": symbol,
        "LDCP": ldcp,
        "OPEN": open_p,
        "HIGH": high,
        "LOW": low,
        "CLOSE": close,
        "CHANGE": change,
        "CHANGE (%)": change_pct,
        "VOLUME": volume,
        "DATE": date,
        "TIMESTAMP": str(timestamp),
    }


def _extract_json_candidates_from_html(html: str) -> list[Any]:
    candidates: list[Any] = []
    scripts = re.findall(r"<script[^>]*>([\\s\\S]*?)</script>", html, flags=re.IGNORECASE)

    array_patterns = [
        r"(\[[\\s\\S]{40,}?\])",
    ]

    object_patterns = [
        r"(\{[\\s\\S]{60,}?\})",
    ]

    for script in scripts:
        lower = script.lower()
        if not any(token in lower for token in ("histor", "price", "chart", "timeseries", "volume", "close")):
            continue

        for pat in array_patterns + object_patterns:
            for match in re.finditer(pat, script):
                raw = match.group(1).strip()
                if len(raw) < 30:
                    continue
                try:
                    parsed = json.loads(raw)
                    candidates.append(parsed)
                except Exception:
                    continue

    return candidates


def _candidate_history_urls(symbol: str) -> list[str]:
    s = symbol.upper()
    return [
        f"{PSX_BASE}/company/{s}/historical",
        f"{PSX_BASE}/company/{s}/history",
        f"{PSX_BASE}/timeseries/eod/{s}",
        f"{PSX_BASE}/timeseries/{s}",
        f"{PSX_BASE}/api/company/{s}/historical",
        f"{PSX_BASE}/api/company/{s}/history",
        f"{PSX_BASE}/company/{s}",
    ]


def fetch_symbol_history(symbol: str, session: requests.Session | None = None, timeout_sec: int = 10) -> list[dict[str, Any]]:
    symbol = str(symbol).strip().upper()
    if not symbol:
        return []

    s = session or requests.Session()
    records: list[dict[str, Any]] = []

    for url in _candidate_history_urls(symbol):
        try:
            resp = s.get(url, timeout=max(3, int(timeout_sec)), headers=REQUEST_HEADERS)
            if resp.status_code >= 400:
                continue

            content_type = str(resp.headers.get("Content-Type", "")).lower()

            if "json" in content_type:
                payload = resp.json()
                for row in _extract_records_from_json(payload):
                    normalized = _normalize_price_record(symbol, row)
                    if normalized:
                        records.append(normalized)
                if records:
                    break
                continue

            html = resp.text
            for candidate in _extract_json_candidates_from_html(html):
                for row in _extract_records_from_json(candidate):
                    normalized = _normalize_price_record(symbol, row)
                    if normalized:
                        records.append(normalized)

            if records:
                break
        except Exception:
            continue

    if not records:
        return []

    # Deduplicate by symbol/date, keeping latest seen record
    dedup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in records:
        key = (row["SYMBOL"], row["DATE"])
        dedup[key] = row

    rows = sorted(dedup.values(), key=lambda r: (r["SYMBOL"], r["DATE"]))
    return rows


def _symbols_from_db(limit: int | None = None) -> list[str]:
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        q = "SELECT DISTINCT symbol FROM stocks ORDER BY symbol ASC"
        rows = conn.execute(q).fetchall()
        symbols = [str(r[0]).strip().upper() for r in rows if str(r[0]).strip()]
        return symbols[:limit] if limit and limit > 0 else symbols
    finally:
        conn.close()


def discover_symbols(limit: int | None = None) -> list[str]:
    watch = load_watchlist(STOCKS_NAME_PATH)
    symbols = sorted({str(s).strip().upper() for s in watch.keys() if str(s).strip()})
    symbols = [s for s in symbols if re.fullmatch(r"[A-Z]{3,8}", s)]
    if not symbols:
        symbols = _symbols_from_db(limit=None)
    symbols = [s for s in symbols if re.fullmatch(r"[A-Z]{3,8}", s)]
    if limit and limit > 0:
        symbols = symbols[:limit]
    return symbols


def download_psx_history_all(
    output_csv: str | Path | None = None,
    symbols_limit: int | None = None,
    keep_existing_if_empty: bool = True,
    timeout_sec: int = 10,
) -> DownloadStats:
    out_path = Path(output_csv) if output_csv else CSV_PATH
    out_path.parent.mkdir(parents=True, exist_ok=True)

    symbols = discover_symbols(limit=symbols_limit)
    if not symbols:
        raise RuntimeError("No symbols found from watchlist or database.")

    collected: list[dict[str, Any]] = []
    failed: list[str] = []
    fallback_symbols: list[str] = []
    session = requests.Session()

    existing_by_symbol: dict[str, list[dict[str, Any]]] = {}
    if keep_existing_if_empty and out_path.exists():
        try:
            old_df = pd.read_csv(out_path)
            if set(REQUIRED_COLUMNS).issubset(set(old_df.columns)):
                old_df = old_df[REQUIRED_COLUMNS].copy()
                old_df["SYMBOL"] = old_df["SYMBOL"].astype(str).str.strip().str.upper()
                old_df = old_df.dropna(subset=["SYMBOL"])
                for symbol, part in old_df.groupby("SYMBOL"):
                    existing_by_symbol[str(symbol)] = part.to_dict(orient="records")
        except Exception:
            existing_by_symbol = {}

    for symbol in symbols:
        rows = fetch_symbol_history(symbol, session=session, timeout_sec=timeout_sec)
        if not rows:
            fallback_rows = existing_by_symbol.get(symbol, [])
            if fallback_rows:
                collected.extend(fallback_rows)
                fallback_symbols.append(symbol)
            else:
                failed.append(symbol)
            continue
        collected.extend(rows)

    if not collected:
        if keep_existing_if_empty and out_path.exists():
            return DownloadStats(
                symbols_total=len(symbols),
                symbols_ok=0,
                symbols_failed=len(symbols),
                rows_downloaded=0,
                output_csv=str(out_path),
                failed_symbols=failed,
                fallback_symbols=fallback_symbols,
            )
        raise RuntimeError("PSX history download returned no rows for all symbols.")

    df = pd.DataFrame(collected)
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[REQUIRED_COLUMNS]
    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["SYMBOL", "DATE", "CLOSE"])
    df = df.drop_duplicates(subset=["SYMBOL", "DATE"], keep="last")
    df = df.sort_values(["SYMBOL", "DATE"]).reset_index(drop=True)
    df.to_csv(out_path, index=False)

    symbols_ok = int(df["SYMBOL"].nunique())
    return DownloadStats(
        symbols_total=len(symbols),
        symbols_ok=symbols_ok,
        symbols_failed=max(0, len(symbols) - symbols_ok),
        rows_downloaded=int(len(df)),
        output_csv=str(out_path),
        failed_symbols=failed,
        fallback_symbols=fallback_symbols,
    )


__all__ = [
    "DownloadStats",
    "discover_symbols",
    "download_psx_history_all",
    "fetch_symbol_history",
    "_normalize_price_record",
]
