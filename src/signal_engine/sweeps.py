
import logging
from typing import Dict, List, Optional
from .indicators import Indicators

logger = logging.getLogger(__name__)

class SweepDetector:
    """
    Detects Liquidity Checker events.
    Rules:
    1. Price crosses Support/Resistance (Cluster Level) by >= 0.20%
    2. Volume Spike >= 3x recent 1m average.
    """
    def __init__(self, cluster_threshold_pct: float = 0.002):
        self.cluster_threshold_pct = cluster_threshold_pct # 0.20%
        self.state = "IDLE" 
        self.active_sweeps: Dict[float, int] = {} # Level -> Timestamp
        self.deduplication_window = 15 * 60 * 1000 # 15 minutes

    def check_sweep(self, candle: Dict, clusters: List[tuple], recent_volumes: List[float], atr: float = 0.0) -> Optional[Dict]:
        """
        Checks current candle for sweep against clusters.
        candle: {'c': close, 'v': volume, 'h': high, 'l': low}
        """
        close = float(candle['c'])
        vol = float(candle['v'])
        high = float(candle['h'])
        low = float(candle['l'])
        timestamp = candle.get('T')
        
        # Cleanup old sweeps
        cutoff = timestamp - self.deduplication_window
        self.active_sweeps = {l: t for l, t in self.active_sweeps.items() if t > cutoff}

        # 1. Volume Check (Dynamic)
        if not recent_volumes:
            avg_vol = 1.0
        else:
            # Use median to avoid outliers skewing the average
            import statistics
            avg_vol = statistics.median(recent_volumes[-10:]) if len(recent_volumes) >= 10 else sum(recent_volumes) / len(recent_volumes)
            
        if vol < 2.5 * avg_vol:
            return None # No significant volume spike
            
        detected_sweep = None
        
        # Dynamic Threshold based on ATR
        # If ATR is 0 (startup), fallback to fixed pct
        threshold = atr * 0.5 if atr > 0 else close * self.cluster_threshold_pct
        
        for level, level_vol in clusters:
            # Check for existing recent sweep on this level
            if level in self.active_sweeps:
                continue

            # Downward Sweep (Potential Long Setup)
            # Price went below level by threshold, but closed above? 
            # Or just touched it? Usually sweep means taking out the level.
            if low < level - threshold:
                detected_sweep = {
                    'type': 'LONG_SWEEP',
                    'level': level,
                    'price': close,
                    'timestamp': timestamp,
                    'atr': atr
                }
                
            # Upward Sweep (Potential Short Setup)
            elif high > level + threshold:
                detected_sweep = {
                    'type': 'SHORT_SWEEP',
                    'level': level,
                    'price': close,
                    'timestamp': timestamp,
                    'atr': atr
                }
            
            if detected_sweep:
                # Mark as active to avoid spam
                self.active_sweeps[level] = timestamp
                break # Return first valid sweep (or collect all?)

        if detected_sweep:
            logger.info(f"Sweep Detected: {detected_sweep}")
            # self.state = "SWEEP_DETECTED" # Removed simple state, managed by engine
            
        return detected_sweep
