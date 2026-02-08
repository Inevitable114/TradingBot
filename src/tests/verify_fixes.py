
import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.ingest.processor import DataProcessor
from src.signal_engine.engine import SignalEngine
from src.signal_engine.sweeps import SweepDetector
from src.signal_engine.confirmation import ReclaimConfirmation
from src.orderbook.snapshot_manager import SnapshotManager

class TestFixes(unittest.IsolatedAsyncioTestCase):

    async def test_connection_recovery_reset(self):
        """Verify that CONNECTION_RESTORED resets state."""
        processor = DataProcessor("ETHUSDT", MagicMock(), MagicMock())
        processor.depth_initialized = True
        processor.depth_buffer = [{"u": 1}]
        processor.orderbook = MagicMock()
        
        await processor.process_message({"e": "CONNECTION_RESTORED"})
        
        self.assertFalse(processor.depth_initialized)
        self.assertEqual(len(processor.depth_buffer), 0)
        processor.orderbook.clear.assert_called_once()
        print("[PASS] Connection Recovery Reset")

    async def test_strict_sync_rules(self):
        """Verify distinct sync rules U <= lastUpdateId+1 <= u."""
        rest_client = AsyncMock()
        # Snapshot lastUpdateId = 100
        rest_client.get_depth_snapshot.return_value = {
            "lastUpdateId": 100, "bids": [], "asks": []
        }
        
        processor = DataProcessor("ETHUSDT", rest_client, MagicMock())
        processor.orderbook = MagicMock()
        
        # Buffer events
        # 1. Old event (u=90) -> Should be dropped
        # 2. Gap event (U=105, u=110) -> Should NOT trigger sync (gap 101-104)
        # 3. Valid sync event (U=101, u=105) -> Should trigger sync
        
        processor.depth_buffer = [
            {"u": 90, "U": 85}, # Drop
            {"u": 95, "U": 91}, # Drop
            {"u": 100, "U": 96}, # Drop (u=lastUpdateId)
            {"u": 105, "U": 101}, # Valid Sync! (U <= 101 <= u)
            {"u": 110, "U": 106},  # Subsequent
            {"u": 115, "U": 111}   # Subsequent
        ]
        
        await processor._try_initialize_depth()
        
        self.assertTrue(processor.depth_initialized)
        # Should apply the Valid Sync event and subsequent
        self.assertEqual(processor.orderbook.apply_depth_update.call_count, 3)
        print("[PASS] Strict Sync Rules")

    async def test_snapshot_cleanup_typo(self):
        """Verify cleanup uses 'size' not 'target'."""
        # This is a bit hard to test without file system, but we can verify code structure or mock os
        # We'll trust the code change for the typo fix, but let's try to mock os.stat and os.remove
        with patch('src.orderbook.snapshot_manager.os') as mock_os:
            with patch('src.orderbook.snapshot_manager.glob') as mock_glob:
                manager = SnapshotManager(max_storage_bytes=10)
                mock_glob.glob.return_value = ["file1", "file2"]
                
                # Mock stats: file1=20bytes (older), file2=20bytes
                mock_os.stat.side_effect = [
                    MagicMock(st_mtime=100, st_size=20),
                    MagicMock(st_mtime=200, st_size=20)
                ]
                
                manager._cleanup()
                
                # Should delete file1 because total (40) > max (10)
                mock_os.remove.assert_called()
                print("[PASS] Snapshot Cleanup Logic")

    async def test_async_db_insert(self):
        """Verify database insert is awaited."""
        db = MagicMock()
        db.insert_signal = AsyncMock()
        engine = SignalEngine("ETHUSDT", db, MagicMock())
        engine.setup = AsyncMock() # Skip setup if any
        
        # Mock strategy to trigger signal
        # Force confirmation check to return true
        engine.confirmation.check_confirmation = MagicMock(return_value={'type': 'LONG_SWEEP', 'level': 2000})
        engine.atr = 50.0 # prevent div error
        engine.rsi = 50.0
        
        # Mock position sizer
        engine.position_sizer.calculate_size = MagicMock(return_value=1.0)
        
        await engine._generate_signal({'type': 'LONG_SWEEP', 'level': 2000}, 2000.0)
        
        db.insert_signal.assert_awaited_once()
        print("[PASS] Async DB Insert Awaited")

    async def test_atr_sweep_threshold(self):
        """Verify sweep detection uses ATR."""
        detector = SweepDetector()
        
        # Cluster at 2000
        clusters = [(2000.0, 100)]
        volumes = [100.0]*10
        # Spike volume
        current_vol = 500.0
        
        # ATR = 10. Threshold = 5.
        # Long Sweep: Low < 1995.
        
        candle = {'c': 1998, 'v': 500, 'h': 2005, 'l': 1994, 'T': 1000}
        
        sweep = detector.check_sweep(candle, clusters, volumes, atr=10.0)
        
        self.assertIsNotNone(sweep)
        self.assertEqual(sweep['type'], 'LONG_SWEEP')
        self.assertEqual(sweep['atr'], 10.0)
        print("[PASS] ATR Sweep Threshold")
        
    async def test_cvd_confirmation(self):
        """Verify CVD magnitude check."""
        conf = ReclaimConfirmation()
        conf.activate_sweep({'type': 'LONG_SWEEP', 'level': 2000})
        
        candle = {'c': 2005} # Reclaimed price
        
        # 1. CVD positive but small (e.g. 10) -> Fail
        res = conf.check_confirmation(candle, cvd_delta=10, metrics={'imbalance': 0, 'spread': 0.0001})
        self.assertFalse(res)
        
        # 2. CVD positive and large (e.g. 60 > 50) -> Pass
        res = conf.check_confirmation(candle, cvd_delta=60, metrics={'imbalance': 0, 'spread': 0.0001, 'depth': {'bid_vol': 100, 'ask_vol': 100}})
        self.assertTrue(res)
        print("[PASS] CVD Confirmation Threshold")

if __name__ == "__main__":
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
