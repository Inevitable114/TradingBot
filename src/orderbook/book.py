
import logging
import asyncio
import time
from typing import Dict, List, Any, Optional
from .snapshot_manager import SnapshotManager

logger = logging.getLogger(__name__)

class OrderBook:
    """
    Maintains a local Order Book for a symbol.
    - bids/asks: Dict[price, quantity]
    - Updates: Replaces quantity for price, deletes if quantity is 0.
    """
    def __init__(self, symbol: str, snapshot_manager: SnapshotManager):
        self.symbol = symbol
        self.snapshot_manager = snapshot_manager
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id = 0
        self.last_snapshot_time = 0
        self.SNAPSHOT_INTERVAL = 10  # seconds

    def clear(self):
        """Resets the orderbook state."""
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = 0
        self.last_snapshot_time = 0

    def apply_depth_update(self, data: Dict[str, Any]):
        """
        Applies a depth update event (diff) to the orderbook.
        data format: {'b': [[price, qty], ...], 'a': [[price, qty], ...], 'u': updateId, 'U': firstUpdateId}
        """
        u = data.get('u')
        U = data.get('U')
        
        # Simple ID check (production should be more robust with queues)
        if u <= self.last_update_id:
            return

        self._update_side(self.bids, data.get('b', []))
        self._update_side(self.asks, data.get('a', []))
        
        self.last_update_id = u
        
        # Check if it's time to snapshot
        now = time.time()
        if now - self.last_snapshot_time >= self.SNAPSHOT_INTERVAL:
            self._save_snapshot()
            self.last_snapshot_time = now

    def _update_side(self, book_side: Dict[float, float], updates: List[List[str]]):
        for price_str, qty_str in updates:
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0.0:
                book_side.pop(price, None)
            else:
                book_side[price] = qty

    def _save_snapshot(self):
        """Saves current state using SnapshotManager."""
        snapshot_data = {
            'symbol': self.symbol,
            'lastUpdateId': self.last_update_id,
            'bids': list(self.bids.items()),
            'asks': list(self.asks.items()),
            'timestamp': time.time()
        }
        self.snapshot_manager.save_snapshot(self.symbol, snapshot_data)

    def get_imbalance(self, depth: int = 5) -> float:
        """
        Calculates orderbook imbalance at top N levels.
        Imbalance = (BidVol - AskVol) / (BidVol + AskVol)
        Range: [-1, 1]
        """
        # Sort and take top N
        best_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:depth]
        best_asks = sorted(self.asks.items(), key=lambda x: x[0])[:depth]
        
        bid_vol = sum(vol for _, vol in best_bids)
        ask_vol = sum(vol for _, vol in best_asks)
        
        if bid_vol + ask_vol == 0:
            return 0.0
            
        return (bid_vol - ask_vol) / (bid_vol + ask_vol)

    def get_spread(self) -> float:
        """
        Calculates the relative bid-ask spread.
        Returns percentage (e.g., 0.0005 for 0.05%).
        """
        if not self.bids or not self.asks:
            return 0.0
            
        best_bid = max(self.bids.keys())
        best_ask = min(self.asks.keys())
        
        if best_bid == 0:
            return 0.0
            
        return (best_ask - best_bid) / best_bid

    def get_liquidity_depth(self, distance_pct: float = 0.005) -> Dict[str, float]:
        """
        Calculates total volume within distance_pct of mid price.
        Returns {'bid_vol': float, 'ask_vol': float}
        """
        if not self.bids or not self.asks:
            return {'bid_vol': 0.0, 'ask_vol': 0.0}

        best_bid = max(self.bids.keys())
        best_ask = min(self.asks.keys())
        mid_price = (best_bid + best_ask) / 2
        
        bid_threshold = mid_price * (1 - distance_pct)
        ask_threshold = mid_price * (1 + distance_pct)
        
        bid_vol = sum(q for p, q in self.bids.items() if p >= bid_threshold)
        ask_vol = sum(q for p, q in self.asks.items() if p <= ask_threshold)
        
        return {'bid_vol': bid_vol, 'ask_vol': ask_vol}
