import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from services import data_pipeline


class TestDataPipeline(unittest.TestCase):
    def test_compute_technical_indicators_adds_expected_columns(self):
        rows = 80
        df = pd.DataFrame(
            {
                "symbol": ["ABC"] * rows,
                "date": pd.date_range("2024-01-01", periods=rows, freq="D"),
                "close": [100 + i * 0.5 for i in range(rows)],
                "volume": [1000 + (i % 10) * 50 for i in range(rows)],
            }
        )

        out = data_pipeline.compute_technical_indicators(df)

        for col in [
            "MA_7",
            "MA_20",
            "MA_50",
            "RSI_14",
            "MACD",
            "MACD_signal",
            "BB_upper",
            "BB_lower",
            "BB_mid",
        ]:
            self.assertIn(col, out.columns)

        self.assertFalse(out["MA_7"].isna().all())

    def test_get_all_symbols_and_history_from_temp_db(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "test.db"
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    CREATE TABLE stocks (
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
                    )
                    """
                )
                conn.executemany(
                    """
                    INSERT INTO stocks(symbol, open, high, low, close, volume, change, change_pct, date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        ("AAA", 10, 11, 9, 10.5, 1000, 0.5, 5.0, "2025-01-01"),
                        ("AAA", 11, 12, 10, 11.0, 1200, 0.5, 4.7, "2025-01-02"),
                        ("BBB", 20, 21, 19, 20.3, 800, 0.3, 1.5, "2025-01-02"),
                    ],
                )
                conn.commit()
            finally:
                conn.close()

            original_db = data_pipeline.DB_PATH
            data_pipeline.DB_PATH = db_path
            try:
                symbols = data_pipeline.get_all_symbols()
                self.assertEqual(symbols, ["AAA", "BBB"])

                hist = data_pipeline.get_stock_history("AAA", days=365)
                self.assertEqual(len(hist), 2)
                self.assertEqual(hist.iloc[-1]["close"], 11.0)
            finally:
                data_pipeline.DB_PATH = original_db


if __name__ == "__main__":
    unittest.main()
