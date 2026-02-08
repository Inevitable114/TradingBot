
import asyncio
import logging
import sys
import os

# Add root to path
sys.path.append(os.getcwd())

from unittest.mock import MagicMock, AsyncMock
from src.signal_engine.engine import SignalEngine

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SignalVerifier")

async def verify_logic():
    logger.info("--- Starting Signal Engine Logic Verification ---")
    
    # 1. Mock Dependencies
    mock_db = MagicMock()
    mock_redis = AsyncMock()
    
    # Mock Redis to return data for CVD and Clusters
    # Liquidation Cluster at 2000
    mock_redis.get_window_data.side_effect = [
        [], # CVD 1
        [{'p': 2000.0, 'q': 100.0}], # Clusters (Level 2000, Vol 100) - List of Dicts
        [], # CVD 2
        [], # Clusters 2
        [], # CVD 3
        [], # Clusters 3
    ]
    
    # 2. Init Engine
    engine = SignalEngine("ETHUSDT", mock_db, mock_redis)
    # Inject notifier mock
    engine.notifier = AsyncMock()
    
    # 3. Simulate Data Stream
    # Scenario: Long Sweep at 2000
    # Cluster is at 2000.
    # Candle 1: Drop below 2000 (Low 1995), Close above (2005) -> Sweep + Reclaim?
    # Actually, SweepDetector needs Low < Level * (1 - 0.002) = 1996.
    
    logger.info("Step 1: Feed Candle causing SWEEP...")
    candle_sweep = {'c': 1990.0, 'v': 500.0, 'h': 2010.0, 'l': 1990.0, 'T': 1000}
    # We need history for volume avg
    engine.candle_history = [{'v': 100} for _ in range(10)]
    
    # Manually set clusters for testing since Redis mock is complex
    # Format: List of dicts {'p': price, 'q': quantity}
    engine.clusters.update([{'p': 2000.0, 'q': 1000.0}]) 
    
    # Run Engine for Candle 1 (Sweep Trigger)
    # Low 1990 < 2000 * 0.998 (1996) -> Valid Long Sweep
    await engine.on_candle_closed(candle_sweep, ob_imbalance=0.1)
    
    if engine.confirmation.sweep_event:
        logger.info("✅ SUCCESS: Sweep Detected and Activated for Confirmation.")
    else:
        logger.error("❌ FAILURE: Sweep NOT detected.")
        return

    logger.info("Step 2: Feed Candle causing CONFIRMATION...")
    # Candle 2: Bullish Close (2010 > 2000), Positive CVD, Good OB
    candle_confirm = {'c': 2010.0, 'v': 600.0, 'h': 2020.0, 'l': 2000.0, 'T': 62000}
    
    # Mock calculate_cvd to return +500 (Bullish) for engine call
    # We cheat and patch the method directly for simplicity or keep mocking redis
    engine._calculate_cvd = AsyncMock(return_value=500.0)
    
    # Run Engine for Candle 2
    # Reclaim: Close 2010 > Level 2000 (Yes)
    # CVD: 500 > 0 (Yes)
    # OB: 0.1 > -0.3 (Yes)
    await engine.on_candle_closed(candle_confirm, ob_imbalance=0.1)
    
    # 4. Verify Signal Generation
    if mock_db.insert_signal.called:
        logger.info("✅ SUCCESS: Signal Generated and Inserted to DB.")
        signal = mock_db.insert_signal.call_args[0][0]
        logger.info(f"Signal Details: {signal}")
    else:
        logger.error("❌ FAILURE: Signal generation NOT triggered.")
        
    if engine.notifier.send_signal.called:
        logger.info("✅ SUCCESS: Telegram Notification Triggered.")
    else:
        logger.error("❌ FAILURE: Telegram Notification NOT triggered.")

if __name__ == "__main__":
    asyncio.run(verify_logic())
