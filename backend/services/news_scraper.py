from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from config import DB_PATH, NEWS_CACHE_HOURS, REPORTS_DIR
from models.sentiment_analyzer import analyze_text
from services.market_data import load_watchlist

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}
NEWS_SOURCES = [
    ("dawn", "https://www.dawn.com/business"),
]


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_cache_table() -> None:
    conn = _conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS news_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                source TEXT NOT NULL,
                headline TEXT NOT NULL,
                fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, source, headline)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_news_cache_symbol_fetched_at ON news_cache(symbol, fetched_at DESC)"
        )
        conn.commit()
    finally:
        conn.close()


def _symbol_keywords(symbol: str) -> set[str]:
    watch = load_watchlist()
    company = watch.get(symbol.upper(), "")

    keywords = {symbol.upper(), symbol.lower(), symbol.capitalize()}
    if company:
        keywords.add(company)
        for token in re.findall(r"[A-Za-z]{3,}", company):
            keywords.add(token)
    return keywords


def _headline_matches_symbol(headline: str, keywords: set[str]) -> bool:
    words = set(re.findall(r"[A-Za-z0-9_]+", headline))
    if words.intersection(keywords):
        return True

    upper_headline = headline.upper()
    return any(k.upper() in upper_headline for k in keywords)


def _fetch_source(source_name: str, url: str) -> list[str]:
    resp = requests.get(url, timeout=15, headers=HEADERS)
    if source_name == "dawn" and resp.status_code == 403:
        # Dawn sometimes blocks direct page fetches; fallback to RSS feed.
        rss = requests.get("https://www.dawn.com/feeds/business", timeout=15, headers=HEADERS)
        rss.raise_for_status()
        soup = BeautifulSoup(rss.text, "xml")
        titles = [t.get_text(" ", strip=True) for t in soup.select("item > title") if t.get_text(" ", strip=True)]
        return list(dict.fromkeys(titles))

    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    headlines: list[str] = []

    if source_name == "dawn":
        for article in soup.select("article"):
            text = article.get_text(" ", strip=True)
            if text:
                headlines.append(text)
    elif source_name == "thenews":
        for node in soup.select("h2, h3, .title, a"):
            text = node.get_text(" ", strip=True)
            if text and len(text) > 20:
                headlines.append(text)
    else:  # brecorder
        for node in soup.select("a"):
            text = node.get_text(" ", strip=True)
            if text and len(text) > 20:
                headlines.append(text)

    # De-duplicate preserving order
    deduped = list(dict.fromkeys(headlines))
    return deduped


def _get_cached(symbol: str, hours: int) -> list[str]:
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat(sep=" ")
    conn = _conn()
    try:
        rows = conn.execute(
            """
            SELECT headline
            FROM news_cache
            WHERE symbol = ? AND fetched_at >= ?
            ORDER BY fetched_at DESC
            """,
            (symbol, cutoff),
        ).fetchall()
        return [r["headline"] for r in rows]
    finally:
        conn.close()


def _store_cache(symbol: str, source: str, headlines: Iterable[str]) -> None:
    conn = _conn()
    try:
        conn.executemany(
            """
            INSERT OR IGNORE INTO news_cache(symbol, source, headline, fetched_at)
            VALUES (?, ?, ?, ?)
            """,
            [(symbol, source, h, datetime.utcnow().isoformat(sep=" ")) for h in headlines],
        )
        conn.commit()
    finally:
        conn.close()


def _scrape_psx_company_updates(symbol: str, max_items: int = 12) -> tuple[list[str], list[dict[str, str]]]:
    url = f"https://dps.psx.com.pk/company/{symbol}"
    resp = requests.get(url, timeout=20, headers=HEADERS)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    updates: list[str] = []
    reports: list[dict[str, str]] = []

    # Announcements rows often include date + title + optional PDF links.
    for row in soup.select("table tr"):
        text = row.get_text(" ", strip=True)
        if not text or len(text) < 18:
            continue
        if any(k in text.lower() for k in ["financial", "report", "transmission", "board", "announcement", "dividend"]):
            updates.append(text)

    # Financial report PDFs (usually from financials.psx.com.pk)
    for link in soup.select('a[href*="DownloadPDF.php"], a[href$=".pdf"], a[href*="financials.psx.com.pk"]'):
        href = (link.get("href") or "").strip()
        if not href:
            continue
        full_url = urljoin(url, href)
        title = link.get_text(" ", strip=True) or "PSX Financial Report"
        reports.append({"title": title, "url": full_url})

    updates = list(dict.fromkeys(updates))[:max_items]
    dedup_reports = []
    seen_urls: set[str] = set()
    for report in reports:
        if report["url"] in seen_urls:
            continue
        seen_urls.add(report["url"])
        dedup_reports.append(report)

    return updates, dedup_reports[:max_items]


def _download_psx_reports(symbol: str, reports: list[dict[str, str]], max_downloads: int = 3) -> list[str]:
    if not reports:
        return []

    target_dir = REPORTS_DIR / symbol.upper()
    target_dir.mkdir(parents=True, exist_ok=True)

    downloaded_titles: list[str] = []
    for report in reports[:max_downloads]:
        try:
            rid_match = re.search(r"id=(\d+)", report["url"])
            if rid_match:
                fname = f"report_{rid_match.group(1)}.pdf"
            else:
                fname = re.sub(r"[^A-Za-z0-9._-]+", "_", report["title"])[:80] + ".pdf"
            out_path = target_dir / fname

            if not out_path.exists():
                r = requests.get(report["url"], timeout=25, headers=HEADERS)
                r.raise_for_status()
                out_path.write_bytes(r.content)

            downloaded_titles.append(f"PSX Report: {report['title']}")
        except Exception as exc:  # noqa: BLE001
            print(f"PSX report download failed for {symbol}: {exc}")

    return downloaded_titles


def scrape_headlines(symbol: str, max_headlines: int = 20) -> list[dict[str, str]]:
    symbol = symbol.strip().upper()
    _ensure_cache_table()

    cached = _get_cached(symbol, NEWS_CACHE_HOURS)
    if len(cached) >= max_headlines:
        return [{"headline": h, "source": "cache"} for h in cached[:max_headlines]]

    keywords = _symbol_keywords(symbol)
    matched: list[dict[str, str]] = []

    for source_name, url in NEWS_SOURCES:
        try:
            raw_headlines = _fetch_source(source_name, url)
            filtered = [h for h in raw_headlines if _headline_matches_symbol(h, keywords)]
            if filtered:
                _store_cache(symbol, source_name, filtered)
                matched.extend([{"headline": h, "source": source_name} for h in filtered])
        except Exception as exc:  # noqa: BLE001
            print(f"News source fetch failed ({source_name}): {exc}")

    try:
        psx_updates, psx_reports = _scrape_psx_company_updates(symbol)
        if psx_updates:
            _store_cache(symbol, "psx_announcements", psx_updates)
            matched.extend([{"headline": h, "source": "psx_announcements"} for h in psx_updates])

        report_headlines = _download_psx_reports(symbol, psx_reports)
        if report_headlines:
            _store_cache(symbol, "psx_reports", report_headlines)
            matched.extend([{"headline": h, "source": "psx_reports"} for h in report_headlines])
    except Exception as exc:  # noqa: BLE001
        print(f"PSX updates fetch failed ({symbol}): {exc}")

    combined = [{"headline": h, "source": "cache"} for h in cached] + matched
    dedup: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in combined:
        headline = item["headline"]
        if headline in seen:
            continue
        seen.add(headline)
        dedup.append(item)

    return dedup[:max_headlines]


def run_sentiment_pipeline(symbol: str) -> None:
    symbol = symbol.strip().upper()
    headlines = scrape_headlines(symbol)
    if not headlines:
        return

    rows = []
    analyzed_at = datetime.utcnow().isoformat(sep=" ")
    for item in headlines:
        headline = item["headline"]
        source = item.get("source", "news")
        sentiment = analyze_text(headline)
        rows.append(
            (
                symbol,
                float(sentiment["score"]),
                sentiment["label"],
                source,
                headline,
                analyzed_at,
            )
        )

    conn = _conn()
    try:
        conn.executemany(
            """
            INSERT INTO sentiment(symbol, score, label, source, headline, analyzed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()
