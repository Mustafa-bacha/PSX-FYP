from __future__ import annotations

import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from config import DB_PATH, LIVE_QUOTE_CACHE_MINUTES, STOCKS_NAME_PATH

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}


def load_watchlist(path: Path | None = None) -> dict[str, str]:
    candidates = [
        path or STOCKS_NAME_PATH,
        (path or STOCKS_NAME_PATH).parent / "Stocks_names.txt",
        (path or STOCKS_NAME_PATH).parent / "stocks_names.txt",
    ]

    file_path = next((p for p in candidates if p.exists()), None)
    if not file_path:
        return {}

    mapping: dict[str, str] = {}
    for line in file_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        raw = line.strip()
        if not raw:
            continue
        upper = raw.upper()
        if upper.startswith("SYMBOL") and "COMPANY" in upper:
            continue

        parts = re.split(r"\s+", raw, maxsplit=1)
        if not parts:
            continue

        symbol = parts[0].strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{2,20}", symbol):
            continue
        company = parts[1].strip() if len(parts) > 1 else symbol
        mapping[symbol] = company

    return mapping


def is_symbol_allowed(symbol: str) -> bool:
    watch = load_watchlist()
    if not watch:
        return True
    return symbol.strip().upper() in watch


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_num(text: str | None) -> float | None:
    if not text:
        return None
    value = text.replace("Rs.", "").replace(",", "").strip()
    value = value.replace("(", "").replace(")", "").replace("%", "")
    try:
        return float(value)
    except Exception:  # noqa: BLE001
        return None


def _fetch_quote_from_psx(symbol: str) -> dict[str, Any] | None:
    url = f"https://dps.psx.com.pk/company/{symbol}"
    resp = requests.get(url, timeout=20, headers=HEADERS)
    resp.raise_for_status()
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    close_node = soup.select_one(".quote__close")
    change_node = soup.select_one(".change__value")
    pct_node = soup.select_one(".change__percent")

    close = _parse_num(close_node.get_text(" ", strip=True) if close_node else None)
    change = _parse_num(change_node.get_text(" ", strip=True) if change_node else None)
    change_pct = _parse_num(pct_node.get_text(" ", strip=True) if pct_node else None)

    if close is None:
        close_m = re.search(r'quote__close"\s*>\s*Rs\.\s*([0-9][0-9,]*(?:\.[0-9]+)?)', html, re.I)
        if close_m:
            close = _parse_num(close_m.group(1))

    if change is None:
        change_m = re.search(r'change__value"\s*>\s*([\-\+]?[0-9][0-9,]*(?:\.[0-9]+)?)', html, re.I)
        if change_m:
            change = _parse_num(change_m.group(1))

    if change_pct is None:
        pct_m = re.search(r'change__percent"\s*>\s*\(?\s*([\-\+]?[0-9][0-9,]*(?:\.[0-9]+)?)\s*%\s*\)?', html, re.I)
        if pct_m:
            change_pct = _parse_num(pct_m.group(1))

    if close is None:
        return None

    return {
        "symbol": symbol,
        "close": close,
        "change": change if change is not None else 0.0,
        "change_pct": change_pct if change_pct is not None else 0.0,
        "source": "psx",
        "fetched_at": datetime.utcnow().isoformat(sep=" "),
    }


def _get_cached_quote(symbol: str, max_age_minutes: int) -> dict[str, Any] | None:
    cutoff = (datetime.utcnow() - timedelta(minutes=max_age_minutes)).isoformat(sep=" ")
    conn = _conn()
    try:
        row = conn.execute(
            """
            SELECT symbol, close, change, change_pct, source, fetched_at
            FROM live_quotes_cache
            WHERE symbol = ? AND fetched_at >= ?
            """,
            (symbol, cutoff),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_cached_quotes_bulk(symbols: list[str], max_age_minutes: int | None = None) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}

    max_age = max_age_minutes if max_age_minutes is not None else LIVE_QUOTE_CACHE_MINUTES
    cutoff = (datetime.utcnow() - timedelta(minutes=max_age)).isoformat(sep=" ")

    placeholders = ",".join(["?"] * len(symbols))
    conn = _conn()
    try:
        rows = conn.execute(
            f"""
            SELECT symbol, close, change, change_pct, source, fetched_at
            FROM live_quotes_cache
            WHERE symbol IN ({placeholders}) AND fetched_at >= ?
            """,
            (*symbols, cutoff),
        ).fetchall()
        return {r["symbol"]: dict(r) for r in rows}
    finally:
        conn.close()


def _store_quote(quote: dict[str, Any]) -> None:
    conn = _conn()
    try:
        conn.execute(
            """
            INSERT INTO live_quotes_cache(symbol, close, change, change_pct, source, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                close=excluded.close,
                change=excluded.change,
                change_pct=excluded.change_pct,
                source=excluded.source,
                fetched_at=excluded.fetched_at
            """,
            (
                quote["symbol"],
                quote.get("close"),
                quote.get("change"),
                quote.get("change_pct"),
                quote.get("source", "psx"),
                quote.get("fetched_at", datetime.utcnow().isoformat(sep=" ")),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_live_quote(symbol: str, force_refresh: bool = False, allow_network: bool = True) -> dict[str, Any] | None:
    symbol = symbol.strip().upper()

    if not force_refresh:
        cached = _get_cached_quote(symbol, LIVE_QUOTE_CACHE_MINUTES)
        if cached:
            return cached

    if allow_network:
        try:
            quote = _fetch_quote_from_psx(symbol)
            if quote:
                _store_quote(quote)
                return quote
        except Exception as exc:  # noqa: BLE001
            print(f"Live quote fetch failed for {symbol}: {exc}")

    # fallback to any cached row, even if stale
    conn = _conn()
    try:
        row = conn.execute(
            """
            SELECT symbol, close, change, change_pct, source, fetched_at
            FROM live_quotes_cache
            WHERE symbol = ?
            """,
            (symbol,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_live_quotes_bulk(
    symbols: list[str],
    force_refresh: bool = False,
    allow_network: bool = True,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not symbols:
        return out

    with ThreadPoolExecutor(max_workers=min(12, max(1, len(symbols)))) as executor:
        futures = {
            executor.submit(get_live_quote, sym, force_refresh, allow_network): sym
            for sym in symbols
        }
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                quote = fut.result()
                if quote:
                    out[sym] = quote
            except Exception as exc:  # noqa: BLE001
                print(f"Bulk quote fetch failed for {sym}: {exc}")
    return out
