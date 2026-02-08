
import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add root to path
sys.path.append(os.getcwd())

from src.signal_engine.indicators import Indicators
from src.signal_engine.sweeps import SweepDetector

class TestMath(unittest.TestCase):
    def test_rsi_bounds(self):
        # Create a series of prices
        prices = [100 + i + (i%2)*2 for i in range(50)] # Volatile up trend
        rsi = Indicators.calculate_rsi(prices, 14)
        print(f"RSI: {rsi}")
        self.assertTrue(0 <= rsi <= 100, "RSI must be between 0 and 100")
        
    def test_atr_positive(self):
        highs = [105.0] * 20
        lows = [95.0] * 20
        closes = [100.0] * 20
        atr = Indicators.calculate_atr(highs, lows, closes, 14)
        print(f"ATR: {atr}")
        self.assertTrue(atr > 0, "ATR must be positive for volatile data")
        
    def test_sweep_threshold_logic(self):
        detector = SweepDetector(cluster_threshold_pct=0.002) # 0.2%
        level = 2000.0
        
        # Test 1: Price exactly at threshold (should NOT trigger strictly less than)
        threshold_price = 2000.0 * (1 - 0.002) # 1996.0
        
        # Candle hitting exactly 1996.0
        # High must be < 2000 * 1.002 (2004) to avoid Short Sweep
        candle_fail = {'c': 2000, 'v': 1000, 'h': 2000, 'l': 1996.0, 'T': 1000} 
        clusters = [(level, 100)]
        volumes = [100] * 10
        
        res = detector.check_sweep(candle_fail, clusters, volumes)
        self.assertIsNone(res, "Should not trigger if price equals limit (strict <)")
        
        # Test 2: Price below threshold (1995.9)
        candle_pass = {'c': 2000, 'v': 1000, 'h': 2000, 'l': 1995.9, 'T': 2000}
        res = detector.check_sweep(candle_pass, clusters, volumes)
        self.assertIsNotNone(res, "Should trigger if price is below limit")
        self.assertEqual(res['type'], 'LONG_SWEEP')

if __name__ == '__main__':
    unittest.main()
