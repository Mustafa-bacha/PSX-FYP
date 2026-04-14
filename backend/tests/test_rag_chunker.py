import unittest

from rag_chatbot.chunker import chunk_text


class TestRagChunker(unittest.TestCase):
    def test_chunk_text_basic(self):
        text = "A" * 2000
        chunks = chunk_text(text, chunk_size=500, overlap=100)
        self.assertGreaterEqual(len(chunks), 4)
        self.assertTrue(all(len(c) <= 500 for c in chunks))

    def test_chunk_text_empty(self):
        self.assertEqual(chunk_text("", chunk_size=100, overlap=10), [])


if __name__ == "__main__":
    unittest.main()
