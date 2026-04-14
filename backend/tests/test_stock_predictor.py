import tempfile
import unittest
from pathlib import Path

import pandas as pd

from models import stock_predictor


class TestStockPredictor(unittest.TestCase):
    def _synthetic_df(self, rows=320):
        dates = pd.date_range("2023-01-01", periods=rows, freq="D")
        close = [100 + i * 0.2 + ((i % 7) - 3) * 0.05 for i in range(rows)]
        volume = [10000 + (i % 20) * 250 for i in range(rows)]
        return pd.DataFrame(
            {
                "symbol": ["TST"] * rows,
                "date": dates,
                "open": [c - 0.3 for c in close],
                "high": [c + 0.6 for c in close],
                "low": [c - 0.8 for c in close],
                "close": close,
                "volume": volume,
                "change": [0.1] * rows,
                "change_pct": [0.1] * rows,
                "ldcp": [c - 0.1 for c in close],
                "timestamp": dates,
            }
        )

    def test_train_model_returns_metrics_and_saves_artifacts(self):
        df = self._synthetic_df()

        with tempfile.TemporaryDirectory() as td:
            model_dir = Path(td)
            original_model_dir = stock_predictor.MODEL_DIR
            original_loader = stock_predictor._load_symbol_df
            stock_predictor.MODEL_DIR = model_dir
            try:
                metrics = stock_predictor.train_model("TST", df=df)
                self.assertIn("mae", metrics)
                self.assertIn("rmse", metrics)
                self.assertIn("direction_accuracy", metrics)

                self.assertTrue((model_dir / "TST_price.pt").exists())
                self.assertTrue((model_dir / "TST_direction.pt").exists())
                self.assertTrue((model_dir / "TST_scaler.pkl").exists())
                self.assertTrue((model_dir / "TST_metrics.json").exists())

                stock_predictor._load_symbol_df = lambda _symbol: df.copy()
                pred = stock_predictor.predict_next_day("TST")
                self.assertEqual(pred["symbol"], "TST")
                self.assertIn(pred["predicted_direction"], {"UP", "DOWN"})
            finally:
                stock_predictor.MODEL_DIR = original_model_dir
                stock_predictor._load_symbol_df = original_loader


if __name__ == "__main__":
    unittest.main()
