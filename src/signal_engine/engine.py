
import logging
import os
import time
import asyncio
from typing import Dict, Any, List
from .indicators import Indicators
from .clusters import LiquidationClusters
from .sweeps import SweepDetector
from .confirmation import ReclaimConfirmation
from src.risk_manager.position_sizer import PositionSizer
from src.utils.db import DatabaseClient
from src.utils.redis_client import RedisClient

logger = logging.getLogger(__name__)

class SignalEngine:
    """
    Orchestrates the trading strategy.
    1. Ingestion -> Candle update
    2. Update Indicators & Clusters
    3. Check Sweep
    4. Check Reclaim (Confirmation)
    5. Generate Signal
    """

    def __init__(self, symbol: str, db: DatabaseClient, redis: RedisClient):
        self.symbol = symbol
        self.db = db
        self.redis = redis
        
        # Components
        self.clusters = LiquidationClusters()
        self.sweep_detector = SweepDetector()
        self.confirmation = ReclaimConfirmation()
        
        capital = float(os.getenv("CAPITAL", 1500.0))
        risk_pct = float(os.getenv("INITIAL_RISK_PCT", 0.01))
        self.position_sizer = PositionSizer(capital_usdt=capital, risk_pct=risk_pct)
        
        # State
        self.current_candle = {}
        self.candle_history: List[Dict] = [] # Store last 100 candles
        self.atr = 0.0
        self.rsi = 50.0

    async def on_candle_closed(self, candle: Dict[str, Any], metrics: Dict[str, Any] = None):
        """
        Called when a candle closes.
        candle: {c, v, h, l, T}
        metrics: {imbalance, spread, depth}
        """
        if metrics is None:
            metrics = {}
        self.current_candle = candle
        close = float(candle['c'])
        timestamp = candle['T']
        
        # 1. Update History
        self.candle_history.append(candle)
        if len(self.candle_history) > 100:
            self.candle_history.pop(0)

        # 2. Update Indicators (ATR, RSI)
        await self._update_indicators()

        # 3. Calculate CVD for this candle
        end_ts = timestamp
        start_ts = end_ts - 59999 # Approx 1m
        
        cvd_delta = await self._calculate_cvd(start_ts, end_ts)
        ob_imbalance = metrics.get('imbalance', 0.0)
        logger.info(f"Candle Closed: {close} | CVD: {cvd_delta:.2f} | OB: {ob_imbalance:.2f} | ATR: {self.atr:.2f} | RSI: {self.rsi:.2f}")

        # 4. Update Clusters
        await self._update_clusters()
        active_clusters = self.clusters.get_clusters()
        
        # 5. Check Strategy
        volumes = [float(c['v']) for c in self.candle_history]
        
        sweep = self.sweep_detector.check_sweep(candle, active_clusters, volumes, self.atr)
        if sweep:
            self.confirmation.activate_sweep(sweep)
        
        # 6. Check Confirmation (Calculated every candle for active sweep)
        confirmed_sweep = self.confirmation.check_confirmation(candle, cvd_delta, metrics)
        
        if confirmed_sweep:
             await self._generate_signal(confirmed_sweep, close)

    async def _update_indicators(self):
        """Calculates ATR and RSI from history."""
        if len(self.candle_history) < 14:
            return

        closes = [float(c['c']) for c in self.candle_history]
        highs = [float(c['h']) for c in self.candle_history]
        lows = [float(c['l']) for c in self.candle_history]
        
        self.atr = Indicators.calculate_atr(highs, lows, closes, 14)
        self.rsi = Indicators.calculate_rsi(closes, 14)

    async def _calculate_cvd(self, start_ts: int, end_ts: int) -> float:
        """Fetches trades from Redis and calculates CVD delta."""
        key = f"trades:{self.symbol}"
        try:
            trades = await self.redis.get_window_data(key, start_ts, end_ts)
            return Indicators.calculate_cvd_delta(trades)
        except Exception as e:
            logger.error(f"Error calculating CVD: {e}")
            return 0.0

    async def _update_clusters(self):
        """Fetches liquidations and updates clusters."""
        try:
            # Get liquidations from last 1 hour
            now = int(time.time() * 1000)
            start = now - (60 * 60 * 1000) 
            key = f"liquidations:{self.symbol}"
            
            liqs = await self.redis.get_window_data(key, start, now)
            
            # Feed to clusters (naive rebuild or update)
            # If LiquidationClusters handles raw lists, we just pass it.
            # We assume it has an 'update' or we re-instantiate if it's stateless.
            # Let's assume we call a method to add them.
            self.clusters.update(liqs)
            
        except Exception as e:
            logger.error(f"Error updating clusters: {e}")

    async def _generate_signal(self, sweep_event: Dict, entry_price: float):
        """Generates and persists a trade signal."""
        direction = "LONG" if sweep_event['type'] == 'LONG_SWEEP' else "SHORT"
        
        # SL Calculation
        stop_loss = 0.0
        # ATR logic
        atr_buffer = self.atr if self.atr > 0 else (entry_price * 0.005)
        
        if direction == "LONG":
            stop_loss = entry_price - (atr_buffer * 1.5)
        else:
            stop_loss = entry_price + (atr_buffer * 1.5)
            
        # Refine SL: Don't let it be too close
        dist_pct = abs(entry_price - stop_loss) / entry_price
        if dist_pct < 0.002: # Min 0.2% SL
             stop_loss = entry_price * (0.998 if direction == "LONG" else 1.002)

        # Size
        size = self.position_sizer.calculate_size(entry_price, stop_loss)
        
        signal = {
            'timestamp': int(time.time() * 1000),
            'symbol': self.symbol,
            'side': direction,
            'entry_price': entry_price,
            'stop_loss': round(stop_loss, 2),
            'targets': {'tp1': round(entry_price + (2*atr_buffer) if direction == 'LONG' else entry_price - (2*atr_buffer), 2)},
            'position_size': size,
            'confidence': 80.0 + (5.0 if abs(self.rsi - 50) > 20 else 0), # Boost if RSI extreme
            'actionable': size > 0,
            'reasons': [sweep_event['type'], f'CVD Confirmed', f'RSI {self.rsi:.1f}']
        }
        
        logger.info(f"Generating Signal: {signal}")
        await self.db.insert_signal(signal)
        
        if hasattr(self, 'notifier') and self.notifier:
            await self.notifier.send_signal(signal)

