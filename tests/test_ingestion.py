
import unittest
import asyncio
import os
import time
from unittest.mock import MagicMock, AsyncMock
from src.orderbook.book import OrderBook
from src.orderbook.snapshot_manager import SnapshotManager
from src.ingest.processor import DataProcessor

class TestOrderBook(unittest.TestCase):
    def setUp(self):
        self.mock_sm = MagicMock(spec=SnapshotManager)
        self.book = OrderBook("ETHUSDT", self.mock_sm)

    def test_apply_depth_update(self):
        # Initial state empty
        self.assertEqual(len(self.book.bids), 0)
        
        # Add bid
        data = {
            'u': 100, 'U': 90,
            'b': [['2000.50', '1.5']],
            'a': [['2001.00', '2.0']]
        }
        self.book.apply_depth_update(data)
        
        self.assertEqual(self.book.bids[2000.50], 1.5)
        self.assertEqual(self.book.asks[2001.00], 2.0)
        
        # Update bid and remove ask
        data2 = {
            'u': 101, 'U': 101,
            'b': [['2000.50', '3.0']], # Update qty
            'a': [['2001.00', '0.0']]  # Remove
        }
        self.book.apply_depth_update(data2)
        
        self.assertEqual(self.book.bids[2000.50], 3.0)
        self.assertNotIn(2001.00, self.book.asks)

class TestSnapshotManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = "tests/test_snapshots"
        self.sm = SnapshotManager(storage_dir=self.test_dir, ttl_seconds=1, max_storage_bytes=1000)
    
    def tearDown(self):
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_save_and_load(self):
        data = {"test": 123}
        self.sm.save_snapshot("TEST", data)
        
        # Find file
        files = os.listdir(self.test_dir)
        self.assertEqual(len(files), 1)
        
        loaded = self.sm.load_snapshot(f"{self.test_dir}/{files[0]}")
        self.assertEqual(loaded, data)

    def test_cleanup_ttl(self):
        data = {"test": 1}
        self.sm.save_snapshot("TEST1", data)
        
        # Wait for TTL
        time.sleep(1.1)
        
        # Save another to trigger cleanup
        self.sm.save_snapshot("TEST2", data)
        
        files = os.listdir(self.test_dir)
        # TEST1 should be gone, TEST2 remains
        self.assertEqual(len(files), 1)
        self.assertTrue("TEST2" in files[0])

if __name__ == '__main__':
    unittest.main()
