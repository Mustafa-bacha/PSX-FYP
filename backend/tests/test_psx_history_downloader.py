import unittest

from services.psx_history_downloader import _normalize_price_record


class TestPsxHistoryDownloader(unittest.TestCase):
    def test_normalize_price_record_basic(self):
        row = {
            "date": "2026-04-12",
            "open": "100.5",
            "high": "110.0",
            "low": "99.2",
            "close": "108.8",
            "change": "2.1",
            "change_pct": "1.97%",
            "volume": "1,234,000",
            "ldcp": "106.7",
        }
        rec = _normalize_price_record("HBL", row)
        self.assertIsNotNone(rec)
        assert rec is not None
        self.assertEqual(rec["SYMBOL"], "HBL")
        self.assertEqual(rec["DATE"], "2026-04-12")
        self.assertEqual(rec["CLOSE"], 108.8)
        self.assertEqual(rec["VOLUME"], 1234000.0)

    def test_normalize_price_record_rejects_missing_close(self):
        row = {"date": "2026-04-12", "open": "100.5"}
        self.assertIsNone(_normalize_price_record("HBL", row))


if __name__ == "__main__":
    unittest.main()
