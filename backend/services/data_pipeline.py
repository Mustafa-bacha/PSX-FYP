from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from config import DB_PATH


def load_and_clean_csv(path: str) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
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

    required = {"symbol", "close", "date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {sorted(missing)}")

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
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    df = df.dropna(subset=["symbol", "close", "date"])
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    return df


def _read_sql(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()


def get_stock_history(symbol: str, days: int = 365) -> pd.DataFrame:
    symbol = symbol.strip().upper()
    if days <= 0:
        raise ValueError("days must be > 0")

    query = """
        SELECT symbol, date, open, high, low, close, volume, change, change_pct, ldcp, timestamp
        FROM stocks
        WHERE symbol = ?
                    AND date >= (
                            SELECT date(MAX(date), ?)
                            FROM stocks
                            WHERE symbol = ?
                    )
        ORDER BY date ASC
    """
    day_window = f"-{int(days)} day"
    df = _read_sql(query, (symbol, day_window, symbol))
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def get_all_symbols() -> list[str]:
    query = "SELECT DISTINCT symbol FROM stocks ORDER BY symbol ASC"
    df = _read_sql(query)
    return df["symbol"].dropna().astype(str).tolist() if not df.empty else []


def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)

    avg_gain = gains.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0)


def compute_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    required = {"close"}
    if not required.issubset(df.columns):
        raise ValueError("DataFrame must contain close column")

    result = df.copy()
    result = result.sort_values("date").reset_index(drop=True)

    close = pd.to_numeric(result["close"], errors="coerce")

    result["MA_7"] = close.rolling(window=7, min_periods=1).mean()
    result["MA_20"] = close.rolling(window=20, min_periods=1).mean()
    result["MA_50"] = close.rolling(window=50, min_periods=1).mean()

    result["EMA_10"] = close.ewm(span=10, adjust=False).mean()
    result["EMA_20"] = close.ewm(span=20, adjust=False).mean()
    result["EMA_50"] = close.ewm(span=50, adjust=False).mean()

    result["RSI_14"] = _compute_rsi(close, period=14)

    ema_12 = close.ewm(span=12, adjust=False).mean()
    ema_26 = close.ewm(span=26, adjust=False).mean()
    result["MACD"] = ema_12 - ema_26
    result["MACD_signal"] = result["MACD"].ewm(span=9, adjust=False).mean()

    bb_mid = close.rolling(window=20, min_periods=1).mean()
    bb_std = close.rolling(window=20, min_periods=1).std(ddof=0).fillna(0.0)
    result["BB_mid"] = bb_mid
    result["BB_upper"] = bb_mid + 2 * bb_std
    result["BB_lower"] = bb_mid - 2 * bb_std

    return result
