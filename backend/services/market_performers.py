from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from config import MARKET_PERFORMERS_CACHE_MINUTES, MARKET_PERFORMERS_CACHE_PATH

PSX_HOME_URL = "https://dps.psx.com.pk/performers"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

TITLE_MAP = {
    "TOP ACTIVE STOCKS": "top_active_stocks",
    "TOP ADVANCERS": "top_advancers",
    "TOP DECLINERS": "top_decliners",
}


def _parse_float(text: str | None) -> float | None:
    if not text:
        return None
    cleaned = text.replace(",", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", cleaned)
    return float(match.group(0)) if match else None


def _parse_int(text: str | None) -> int | None:
    if not text:
        return None
    cleaned = text.replace(",", "")
    match = re.search(r"\d+", cleaned)
    return int(match.group(0)) if match else None


def _parse_change_and_pct(text: str) -> tuple[float | None, float | None]:
    matches = re.findall(r"[-+]?\d+(?:\.\d+)?", text.replace(",", ""))
    if not matches:
        return None, None
    change = float(matches[0])
    pct = float(matches[1]) if len(matches) > 1 else None
    return change, pct


def _read_cache(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def _write_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _is_cache_fresh(cache_payload: dict[str, Any], minutes: int) -> bool:
    fetched_at = cache_payload.get("fetched_at")
    if not fetched_at:
        return False
    try:
        ts = datetime.fromisoformat(fetched_at)
    except ValueError:
        return False
    return datetime.utcnow() - ts <= timedelta(minutes=minutes)


def _extract_table_rows(table) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    candidates = table.select("tbody tr") or table.find_all("tr")

    for tr in candidates:
        cols = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if len(cols) < 4:
            continue
        if cols[0].strip().upper() in {"", "SYMBOL"}:
            continue

        symbol = re.sub(r"\s+", " ", cols[0]).strip().upper()
        change, change_pct = _parse_change_and_pct(cols[2])
        rows.append(
            {
                "symbol": symbol,
                "price": _parse_float(cols[1]),
                "change": change,
                "change_pct": change_pct,
                "volume": _parse_int(cols[3]),
                "raw_change": cols[2],
            }
        )

    return rows


def _scrape_market_performers() -> dict[str, Any]:
    resp = requests.get(PSX_HOME_URL, timeout=30, headers=HEADERS)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    performers = {
        "top_active_stocks": [],
        "top_advancers": [],
        "top_decliners": [],
    }

    for heading in soup.find_all(["h1", "h2", "h3", "h4"]):
        title = " ".join(heading.get_text(" ", strip=True).split()).upper()
        key = TITLE_MAP.get(title)
        if not key:
            continue

        table = heading.find_next("table")
        if not table:
            continue

        parsed = _extract_table_rows(table)
        if parsed and not performers[key]:
            performers[key] = parsed[:10]

    if not any(performers.values()):
        raise ValueError("Unable to parse market performers from PSX page")

    as_of_match = re.search(r"As of\s+([^\n<]+)", resp.text, re.IGNORECASE)
    as_of = as_of_match.group(1).strip() if as_of_match else None

    return {
        "source": "psx",
        "url": PSX_HOME_URL,
        "as_of": as_of,
        "fetched_at": datetime.utcnow().isoformat(sep=" "),
        "performers": performers,
    }


def get_market_performers(force_refresh: bool = False) -> dict[str, Any]:
    cache = _read_cache(MARKET_PERFORMERS_CACHE_PATH)

    if not force_refresh and cache and _is_cache_fresh(cache, MARKET_PERFORMERS_CACHE_MINUTES):
        cache["cache_status"] = "hit"
        return cache

    try:
        payload = _scrape_market_performers()
        payload["cache_status"] = "miss"
        _write_cache(MARKET_PERFORMERS_CACHE_PATH, payload)
        return payload
    except Exception as exc:  # noqa: BLE001
        if cache:
            cache["cache_status"] = "stale_fallback"
            cache["warning"] = f"Live PSX fetch failed: {exc}"
            return cache
        raise
