import unittest

from rag_chatbot.data_ingestion import _build_trend_doc


class TestRagDataIngestion(unittest.TestCase):
    def test_build_trend_doc_from_rows(self):
        rows = [
            {"date": "2026-04-12", "close": 110.0, "change_pct": 2.2, "volume": 1500000},
            {"date": "2026-04-11", "close": 108.0, "change_pct": 1.1, "volume": 1200000},
            {"date": "2026-04-10", "close": 106.0, "change_pct": 0.5, "volume": 1000000},
            {"date": "2026-04-09", "close": 104.0, "change_pct": -0.1, "volume": 900000},
            {"date": "2026-04-08", "close": 100.0, "change_pct": -0.8, "volume": 800000},
        ]

        doc = _build_trend_doc("TEST", rows)

        self.assertIsNotNone(doc)
        assert doc is not None
        self.assertEqual(doc["stock"], "TEST")
        self.assertEqual(doc["doc_type"], "trend")
        self.assertIn("Short-term (5-session)", doc["text"])
        self.assertIn("Latest close", doc["text"])


if __name__ == "__main__":
    unittest.main()
