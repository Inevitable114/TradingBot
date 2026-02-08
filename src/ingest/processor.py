
import logging
import asyncio
from typing import Dict, Any, List
from src.orderbook.book import OrderBook
from src.orderbook.snapshot_manager import SnapshotManager
from src.ingest.rest_client import BinanceRestClient
from src.utils.redis_client import RedisClient

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Processes raw WebSocket messages from Binance and routes them 
    to appropriate handlers or stores. Manages OrderBook state.
    """
    def __init__(self, symbol: str, rest_client: BinanceRestClient, redis_client: RedisClient):
        self.symbol = symbol
        self.rest_client = rest_client
        self.redis_client = redis_client
        self.snapshot_manager = SnapshotManager()
        self.orderbook = OrderBook(symbol, self.snapshot_manager)
        
        self.depth_initialized = False
        self.depth_buffer: List[Dict[str, Any]] = []
        self.latest_price = 0.0

    async def process_message(self, msg: Dict[str, Any]):
        """
        Main entry point for processing a message.
        Expected format from combined stream: {"stream": "...", "data": {...}}
        """
        if "e" in msg and msg["e"] == "CONNECTION_RESTORED":
            logger.warning("Connection restored. Resetting state for gap recovery...")
            self.depth_initialized = False
            self.depth_buffer.clear()
            self.orderbook.clear() # Add clear method to Orderbook or just re-init
            return
            
        if "data" not in msg:
            return

        stream_name = msg.get("stream", "")
        data = msg.get("data", {})

        try:
            if "@depth" in stream_name:
                await self._handle_depth(data)
            elif "@aggTrade" in stream_name:
                await self._handle_trade(data)
            elif "@forceOrder" in stream_name:
                await self._handle_liquidation(data)
            elif "@kline" in stream_name:
                await self._handle_kline(data)
        except Exception as e:
            logger.error(f"Error processing message {stream_name}: {e}")

    async def _handle_depth(self, data: Dict[str, Any]):
        """
        Processes orderbook depth updates.
        Handles buffering and initialization.
        """
        if not self.depth_initialized:
            self.depth_buffer.append(data)
            await self._try_initialize_depth()
        else:
            self.orderbook.apply_depth_update(data)

    async def _try_initialize_depth(self):
        """
        Attempts to initialize the orderbook by fetching a snapshot
        and applying buffered events following strict Binance rules.
        """
        if self.depth_initialized:
            return

        # Wait for buffer to have enough data to cover the snapshot gap
        if len(self.depth_buffer) < 5: 
             return

        snapshot = await self.rest_client.get_depth_snapshot(self.symbol)
        if not snapshot:
             return

        last_update_id = snapshot.get('lastUpdateId')
        
        # Initialize book with snapshot
        self.orderbook.bids = {float(p): float(q) for p, q in snapshot.get('bids', [])}
        self.orderbook.asks = {float(p): float(q) for p, q in snapshot.get('asks', [])}
        self.orderbook.last_update_id = last_update_id
        
        # Replay buffer with strict rules
        # 1. Drop events where u <= lastUpdateId
        # 2. Find first event where U <= lastUpdateId + 1 <= u
        # 3. Apply subsequent events in sequence
        
        valid_events = []
        sync_found = False
        
        for event in self.depth_buffer:
            u = event.get('u')
            U = event.get('U')
            
            if u <= last_update_id:
                continue
            
            if not sync_found:
                if U <= last_update_id + 1 and u >= last_update_id + 1:
                    sync_found = True
                    valid_events.append(event)
                else:
                    # Gap detected or future event waiting for sync
                    continue
            else:
                # Continuous check
                # In strict mode, we should check U == prev_u + 1
                valid_events.append(event)

        if sync_found:
            for event in valid_events:
                self.orderbook.apply_depth_update(event)
        
            self.depth_initialized = True
            self.depth_buffer.clear()
            logger.info(f"Orderbook initialized for {self.symbol} at updateId {self.orderbook.last_update_id}")
        else:
            # If we went through whole buffer and didn't find sync, 
            # we might need to wait for more events or retry snapshot if buffer is too old.
            # For now, just keep buffering.
            if len(self.depth_buffer) > 1000:
                logger.warning("Depth buffer too large without sync. Clearing to retry.")
                self.depth_buffer.clear()

    async def _handle_trade(self, data: Dict[str, Any]):
        """
        Processes trade execution data (aggTrades).
        Event: aggTrade
        """
        # data keys: p=price, q=qty, m=is_buyer_maker, T=timestamp, a=aggTradeId
        try:
             # Normalize for Redis
             trade_event = {
                 'p': float(data['p']),
                 'q': float(data['q']),
                 'm': data['m'],
                 'T': data['T']
             }
             await self.redis_client.add_trade(self.symbol, trade_event)
             
             self.latest_price = trade_event['p']
        except Exception as e:
            logger.error(f"Error handling trade: {e}")

    async def _handle_liquidation(self, data: Dict[str, Any]):
        """
        Processes liquidation orders.
        Event: forceOrder
        """
        try:
            order = data.get('o', {})
            liq_event = {
                'p': float(order.get('p', 0)),
                'q': float(order.get('q', 0)),
                'S': order.get('S'),
                'T': data.get('E', 0) # Event time
            }
            
            logger.info(f"Liquidation detected: {liq_event['S']} {liq_event['q']} @ {liq_event['p']}")
            await self.redis_client.add_liquidation(self.symbol, liq_event)
        except Exception as e:
            logger.error(f"Error handling liquidation: {e}")

    async def _handle_kline(self, data: Dict[str, Any]):
        """
        Processes kline (candle) data.
        Event: kline
        """
        k = data.get('k', {})
        is_closed = k.get('x', False)
        
        if is_closed:
            logger.info(f"Candle closed: {k.get('c')}")
            # Trigger Signal Engine
            if hasattr(self, 'signal_engine') and self.signal_engine:
                 try:
                     # Normalize candle for engine
                     candle = {
                         'c': k.get('c'),
                         'v': k.get('v'),
                         'h': k.get('h'),
                         'l': k.get('l'),
                         'T': k.get('T')
                     }
                     # Get Orderbook Metrics
                     metrics = {
                         'imbalance': self.orderbook.get_imbalance(depth=10),
                         'spread': self.orderbook.get_spread(),
                         'depth': self.orderbook.get_liquidity_depth(distance_pct=0.005) # 0.5% depth
                     }
                     
                     await self.signal_engine.on_candle_closed(candle, metrics)
                 except Exception as e:
                     logger.error(f"Signal Engine Error: {e}")
