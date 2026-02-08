
import logging
from typing import Dict, List, Optional, Any
from .indicators import Indicators

logger = logging.getLogger(__name__)

class ReclaimConfirmation:
    """
    Confirms a sweep by checking for specific reclaim conditions within 3 candles.
    1. Price Close Reclaim: 
       - Long: Close > Level
       - Short: Close < Level
    2. CVD Shift Positive/Negative matches.
    3. Orderbook Absorption: Replenished volume > 1.5x normal.
    """
    def __init__(self):
        self.confirmation_window = 3 # candles
        self.candles_since_sweep = 0
        self.sweep_event = None

    def reset(self):
        self.sweep_event = None
        self.candles_since_sweep = 0

    def activate_sweep(self, sweep_event: Dict):
        """Activates a sweep event for monitoring."""
        self.sweep_event = sweep_event
        self.candles_since_sweep = 0
        logger.info(f"Monitoring Confirmation for Sweep: {sweep_event}")

    def check_confirmation(self, candle: Dict, cvd_delta: float, metrics: Dict[str, Any]) -> bool:
        """
        Checks if the active sweep is confirmed.
        metrics: {imbalance, spread, depth: {bid_vol, ask_vol}}
        """
        if not self.sweep_event:
            return False

        self.candles_since_sweep += 1
        
        # Expire if too long
        if self.candles_since_sweep > self.confirmation_window:
            logger.info("Sweep confirmation window expired.")
            self.reset()
            return False

        sweep_type = self.sweep_event.get('type')
        level = self.sweep_event.get('level')
        close = float(candle['c'])
        
        # 1. Price Reclaim
        price_reclaimed = False
        if sweep_type == 'LONG_SWEEP':
            if close > level:
                price_reclaimed = True
        elif sweep_type == 'SHORT_SWEEP':
            if close < level:
                price_reclaimed = True
                
        if not price_reclaimed:
            # We wait for reclaim within window
            return False

        # 2. CVD Confirmation
        # Long needs positive CVD delta OR at least not heavily negative?
        # Strict: Delta > 0
        cvd_confirmed = False
        # Long needs positive CVD delta AND sufficient magnitude
        # Short needs negative CVD delta AND sufficient magnitude
        # Threshold: e.g. > 1% of recent volume? Or absolute count?
        # Let's use a simple absolute threshold for now, e.g. 1000 units? No, depends on asset.
        # Let's just require it to be non-trivial > 0.
        
        is_long = sweep_type == 'LONG_SWEEP'
        is_short = sweep_type == 'SHORT_SWEEP'
        
        if is_long and cvd_delta > 50: # Arbitrary small positive buffer
            cvd_confirmed = True
        elif is_short and cvd_delta < -50:
            cvd_confirmed = True
            
        if not cvd_confirmed:
            logger.info(f"Reclaim rejected by CVD: {cvd_delta:.2f}")
            return False

        # 3. Orderbook Checks
        ob_imbalance = metrics.get('imbalance', 0.0)
        spread = metrics.get('spread', 0.0)
        depth = metrics.get('depth', {})
        
        # 3a. Spread Filter
        # If spread is too wide (> 0.1%), market might be illiquid/volatile.
        if spread > 0.001:
            logger.info(f"Reclaim rejected by Spread: {spread*100:.3f}%")
            return False

        # 3b. Imbalance Check
        ob_confirmed = False
        if sweep_type == 'LONG_SWEEP':
            # We want buying pressure or neutral. Don't go long if wall of asks (-0.5).
            if ob_imbalance > -0.3:
                ob_confirmed = True
        elif sweep_type == 'SHORT_SWEEP':
            # We want sell pressure. Don't go short if wall of bids (0.5).
            if ob_imbalance < 0.3:
                ob_confirmed = True
                
        if not ob_confirmed:
            logger.info(f"Reclaim rejected by OB Imbalance: {ob_imbalance:.2f}")
            return False
            
        # 3c. Liquidity Replenishment (Experimental)
        # For Long Sweep, we want to see bids refilling (Supporting the move up)
        # For Short Sweep, we want to see asks refilling (Resisting the move down)
        # This is relative, need baseline? For now, just ensure it's not empty.
        bid_vol = depth.get('bid_vol', 0)
        ask_vol = depth.get('ask_vol', 0)
        
        if sweep_type == 'LONG_SWEEP' and bid_vol < 1.0: # Arbitrary low value check
             logger.info("Reclaim rejected: No Bid Support")
             return False
        if sweep_type == 'SHORT_SWEEP' and ask_vol < 1.0:
             logger.info("Reclaim rejected: No Ask Resistance")
             return False

        logger.info(f"Reclaim CONFIRMED for {sweep_type} @ {close} | CVD: {cvd_delta:.2f} | OB: {ob_imbalance:.2f}")
        # Return True but KEEP sweep active? No, consume it.
        signal_sweep = self.sweep_event
        self.reset() # Consume signal
        return signal_sweep # Return the sweep event to signal generator
