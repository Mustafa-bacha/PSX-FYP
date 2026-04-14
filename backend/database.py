from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pandas as pd

from config import DB_PATH


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    ldcp REAL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    change REAL,
    change_pct REAL,
    volume REAL,
    date DATE NOT NULL,
    timestamp DATETIME,
    UNIQUE(symbol, date)
);

CREATE TABLE IF NOT EXISTS predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    predicted_price REAL,
    predicted_direction TEXT,
    confidence REAL,
    prediction_date DATE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sentiment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    score REAL,
    label TEXT,
    source TEXT,
    headline TEXT,
    analyzed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS live_quotes_cache (
    symbol TEXT PRIMARY KEY,
    close REAL,
    change REAL,
    change_pct REAL,
    source TEXT DEFAULT 'psx',
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stocks_symbol_date ON stocks(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_symbol_analyzed_at ON sentiment(symbol, analyzed_at DESC);
CREATE INDEX IF NOT EXISTS idx_live_quotes_fetched_at ON live_quotes_cache(fetched_at DESC);
"""


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db_cursor(commit: bool = False) -> Iterator[sqlite3.Cursor]:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        if commit:
            conn.commit()
    finally:
        cursor.close()
        conn.close()


def init_db() -> None:
    with db_cursor(commit=True) as cur:
        cur.executescript(SCHEMA_SQL)


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "SYMBOL": "symbol",
        "LDCP": "ldcp",
        "OPEN": "open",
        "HIGH": "high",
        "LOW": "low",
        "CLOSE": "close",
        "CHANGE": "change",
        "CHANGE (%)": "change_pct",
        "VOLUME": "volume",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
    }
    df = df.rename(columns=rename_map)

    required_cols = set(rename_map.values())
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    numeric_cols = [
        "ldcp",
        "open",
        "high",
        "low",
        "close",
        "change",
        "change_pct",
        "volume",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["symbol", "close", "date"])
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")

    return df[list(rename_map.values())]


def ingest_csv(csv_path: str | Path) -> int:
    inserted, _ = ingest_csv_with_symbols(csv_path)
    return inserted


def ingest_csv_with_symbols(csv_path: str | Path) -> tuple[int, list[str]]:
    init_db()
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    df = _normalize_dataframe(df)

    records = [
        (
            row.symbol,
            float(row.ldcp) if pd.notna(row.ldcp) else None,
            float(row.open) if pd.notna(row.open) else None,
            float(row.high) if pd.notna(row.high) else None,
            float(row.low) if pd.notna(row.low) else None,
            float(row.close) if pd.notna(row.close) else None,
            float(row.change) if pd.notna(row.change) else None,
            float(row.change_pct) if pd.notna(row.change_pct) else None,
            float(row.volume) if pd.notna(row.volume) else None,
            row.date,
            row.timestamp.isoformat(sep=" ") if pd.notna(row.timestamp) else None,
        )
        for row in df.itertuples(index=False)
    ]

    insert_sql = """
        INSERT OR IGNORE INTO stocks (
            symbol, ldcp, open, high, low, close, change, change_pct, volume, date, timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    inserted = 0
    inserted_symbols: set[str] = set()
    with db_cursor(commit=True) as cur:
        for rec in records:
            cur.execute(insert_sql, rec)
            if cur.rowcount == 1:
                inserted += 1
                inserted_symbols.add(str(rec[0]).strip().upper())

    return int(inserted), sorted(inserted_symbols)


def count_stock_rows() -> int:
    with db_cursor() as cur:
        return int(cur.execute("SELECT COUNT(*) AS c FROM stocks").fetchone()["c"])


def count_symbols() -> int:
    with db_cursor() as cur:
        return int(
            cur.execute("SELECT COUNT(DISTINCT symbol) AS c FROM stocks").fetchone()["c"]
        )
