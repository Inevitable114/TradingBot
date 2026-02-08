# ETH Futures Signal Engine - OPTIMIZED CODEBASE
Generated after Final Audit & Strategy Optimization.

## File: src/main.py
```python

import asyncio
import os
import logging
import signal
from src.ingest.stream_manager import BinanceStreamManager
from src.ingest.processor import DataProcessor
from src.ingest.rest_client import BinanceRestClient
from src.utils.redis_client import RedisClient
from src.utils.db import DatabaseClient
from src.signal_engine.engine import SignalEngine
from src.notifier.telegram_bot import TelegramBot

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

async def shutdown(signal, loop, components):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info(f"Received exit signal {signal.name}...")
    
    for name, component in components.items():
        logger.info(f"Closing {name}...")
        try:
            if hasattr(component, 'stop'):
                await component.stop()
            elif hasattr(component, 'close'):
                await component.close()
        except Exception as e:
            logger.error(f"Error closing {name}: {e}")
            
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    logger.info("Cancelling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def main():
    logger.info("ETH Futures Signal Engine Starting...")
    
    # Configuration
    symbol = os.getenv("SYMBOL", "ethusdt")
    streams = ["@depth@100ms", "@aggTrade", "@forceOrder", "@kline_1m"]
    
    # Initialize Infrastructure
    redis_client = RedisClient()
    db_client = DatabaseClient()
    rest_client = BinanceRestClient()
    telegram_bot = TelegramBot()
    
    # Connect
    try:
        await redis_client.connect()
        db_client.connect()
        await rest_client.start()
        await telegram_bot.start()
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return

    # Initialize Logic
    signal_engine = SignalEngine(symbol, db_client, redis_client)
    # Inject notifier if engine needs it, or handled via DB/Observer
    # For now, let's allow Engine to trigger notifier or main loop?
    # Engine is "autonomous", so it should verify and notify.
    # Refactor Engine to accept notifier or just add it here.
    signal_engine.notifier = telegram_bot # Quick injection
    
    # Initialize Processor
    processor = DataProcessor(symbol, rest_client, redis_client)
    processor.signal_engine = signal_engine # Inject engine
    
    # Initialize Stream Manager
    stream_manager = BinanceStreamManager(
        symbol=symbol,
        streams=streams,
        callback=processor.process_message
    )
    
    # Register Signal Handlers
    loop = asyncio.get_running_loop()
    components = {
        "StreamManager": stream_manager,
        "RestClient": rest_client,
        "RedisClient": redis_client,
        "DatabaseClient": db_client,
        "TelegramBot": telegram_bot
    }
    
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop, components))
        )

    # Start Ingestion
    await stream_manager.start()
    
    # Keep alive
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("Main loop cancelled")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
```

## File: src/ingest/stream_manager.py
```python

import asyncio
import json
import logging
import aiohttp
import websockets
from typing import List, Callable, Dict, Optional

logger = logging.getLogger(__name__)

class BinanceStreamManager:
    """
    Manages WebSocket connections to Binance Futures.
    Handles auto-reconnection and stream subscriptions.
    """
    WS_URL = "wss://fstream.binance.com/stream"

    def __init__(self, symbol: str, streams: List[str], callback: Callable[[dict], None]):
        self.symbol = symbol.lower()
        self.streams = streams
        self.callback = callback
        self.running = False
        self.ws = None
        self._reconnect_delay = 1

    async def start(self):
        """Starts the WebSocket manager."""
        self.running = True
        asyncio.create_task(self._connect_loop())

    async def stop(self):
        """Stops the WebSocket manager."""
        self.running = False
        if self.ws:
            await self.ws.close()

    async def _connect_loop(self):
        """Main connection loop with backoff strategy."""
        combined_streams = "/".join([f"{self.symbol}{s}" for s in self.streams])
        url = f"{self.WS_URL}?streams={combined_streams}"

        while self.running:
            try:
                logger.info(f"Connecting to Binance WS: {url}")
                async with websockets.connect(url) as ws:
                    self.ws = ws
                    self._reconnect_delay = 1  # Reset backoff on successful connection
                    logger.info("Connected to Binance WS.")
                    
                    # Notify processor to reset state if needed
                    await self.callback({"e": "CONNECTION_RESTORED"})
                    
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=60)
                            data = json.loads(msg)
                            await self.callback(data)
                        except asyncio.TimeoutError:
                            logger.warning("WebSocket timeout, sending pong...")
                            await ws.pong()
                        except websockets.ConnectionClosed:
                            logger.warning("WebSocket connection closed.")
                            await self.callback({"e": "CONNECTION_LOST"})
                            break
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")

            except Exception as e:
                logger.error(f"WebSocket connection failed: {e}")
            
            if self.running:
                logger.info(f"Reconnecting in {self._reconnect_delay}s...")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 60)
```

## File: src/ingest/processor.py
```python

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
        if "data" not in msg:
            return

        if "e" in msg and msg["e"] == "CONNECTION_RESTORED":
            logger.warning("Connection restored. Resetting state for gap recovery...")
            self.depth_initialized = False
            self.depth_buffer.clear()
            self.orderbook.clear() # Add clear method to Orderbook or just re-init
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
        and applying buffered events.
        """
        # Avoid concurrent initialization attempts or spamming
        if self.depth_initialized:
            return

        # Simple check: if buffer is big enough, try fetching snapshot
        # In a real system, we'd trigger this explicitly after connected
        if len(self.depth_buffer) > 5: 
             snapshot = await self.rest_client.get_depth_snapshot(self.symbol)
             if not snapshot:
                 return

             last_update_id = snapshot.get('lastUpdateId')
             
             # Initialize book with snapshot
             # For simpler implementation, we'll manually set logic here or add method to OrderBook
             # We assume OrderBook can ingest snapshot format or we parse it
             self.orderbook.bids = {float(p): float(q) for p, q in snapshot.get('bids', [])}
             self.orderbook.asks = {float(p): float(q) for p, q in snapshot.get('asks', [])}
             self.orderbook.last_update_id = last_update_id
             
             # Replay buffer
             for event in self.depth_buffer:
                 u = event.get('u')
                 U = event.get('U')
                 if u <= last_update_id:
                     continue
                 self.orderbook.apply_depth_update(event)
            
             self.depth_initialized = True
             self.depth_buffer.clear()
             logger.info(f"Orderbook initialized for {self.symbol} at updateId {self.orderbook.last_update_id}")

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
                     # Get Orderbook Imbalance
                     imbalance = self.orderbook.get_imbalance(depth=10)
                     
                     await self.signal_engine.on_candle_closed(candle, imbalance)
                 except Exception as e:
                     logger.error(f"Signal Engine Error: {e}")
```

## File: src/ingest/rest_client.py
```python

import aiohttp
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class BinanceRestClient:
    """
    Async REST client for Binance Futures public endpoints.
    """
    BASE_URL = "https://fapi.binance.com"

    def __init__(self):
        self.session = None

    async def start(self):
        self.session = aiohttp.ClientSession()

    async def stop(self):
        if self.session:
            await self.session.close()

    async def get_depth_snapshot(self, symbol: str, limit: int = 1000) -> Optional[Dict[str, Any]]:
        """
        Fetches the initial orderbook snapshot.
        GET /fapi/v1/depth
        """
        url = f"{self.BASE_URL}/fapi/v1/depth"
        params = {"symbol": symbol.upper(), "limit": limit}
        
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.error(f"Failed to fetch depth: {resp.status} {await resp.text()}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching depth snapshot: {e}")
            return None

    async def get_klines(self, symbol: str, interval: str, limit: int = 100) -> Optional[list]:
        """
        Fetches historical klines.
        GET /fapi/v1/klines
        """
        url = f"{self.BASE_URL}/fapi/v1/klines"
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}

        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.error(f"Failed to fetch klines: {resp.status} {await resp.text()}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching klines: {e}")
            return None
```

## File: src/signal_engine/engine.py
```python

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

    async def on_candle_closed(self, candle: Dict[str, Any], ob_imbalance: float = 0.0):
        """
        Called when a candle closes.
        candle: {c, v, h, l, T}
        ob_imbalance: Orderbook Imbalance [-1, 1]
        """
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
        logger.info(f"Candle Closed: {close} | CVD: {cvd_delta:.2f} | OB: {ob_imbalance:.2f} | ATR: {self.atr:.2f} | RSI: {self.rsi:.2f}")

        # 4. Update Clusters
        await self._update_clusters()
        active_clusters = self.clusters.get_clusters()
        
        # 5. Check Strategy
        volumes = [float(c['v']) for c in self.candle_history]
        
        sweep = self.sweep_detector.check_sweep(candle, active_clusters, volumes)
        if sweep:
            self.confirmation.activate_sweep(sweep)
        
        # 6. Check Confirmation (Calculated every candle for active sweep)
        confirmed_sweep = self.confirmation.check_confirmation(candle, cvd_delta, ob_imbalance)
        
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
        self.db.insert_signal(signal)
        
        if hasattr(self, 'notifier') and self.notifier:
            await self.notifier.send_signal(signal)

```

## File: src/signal_engine/indicators.py
```python

import pandas as pd
import numpy as np
from typing import List, Dict

class Indicators:
    """
    Technical Indicator calculations.
    """
    
    @staticmethod
    def calculate_ema(prices: List[float], period: int) -> float:
        if len(prices) < period:
            return 0.0
        return pd.Series(prices).ewm(span=period, adjust=False).mean().iloc[-1]

    @staticmethod
    def calculate_rsi(prices: List[float], period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0
        
        delta = pd.Series(prices).diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        return 100 - (100 / (1 + rs)).iloc[-1]

    @staticmethod
    def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
            
        high = pd.Series(highs)
        low = pd.Series(lows)
        close = pd.Series(closes)
        
        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean().iloc[-1]

    @staticmethod
    def calculate_vwap(df: pd.DataFrame) -> float:
        """
        Calculates VWAP for the given session dataframe.
        Expected columns: 'price', 'volume'
        """
        if df.empty:
            return 0.0
        
        v = df['volume'].values
        p = df['price'].values
        return (p * v).sum() / v.sum()

    @staticmethod
    def calculate_cvd_delta(trades: List[Dict]) -> float:
        """
        Calculates the delta (Buy Vol - Sell Vol) for a list of trades.
        Trades must have 'q' (qty) and 'm' (is_buyer_maker).
        """
        buy_vol = 0.0
        sell_vol = 0.0
        
        for t in trades:
            qty = float(t['q'])
            is_buyer_maker = t['m']
            if is_buyer_maker:
                sell_vol += qty
            else:
                buy_vol += qty
                
        return buy_vol - sell_vol
```

## File: src/signal_engine/clusters.py
```python

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple

class LiquidationClusters:
    """
    Detects Liquidation Clusters from a stream of liquidation events.
    Uses a sliding window to aggregate volume per price bin.
    """
    def __init__(self, bin_size_pct: float = 0.0025, window_minutes: int = 30):
        self.bin_size_pct = bin_size_pct
        self.window_minutes = window_minutes
        self.liquidations: List[Dict] = [] # List of {price, qty, timestamp}

    def update(self, liquidations: List[Dict]):
        """
        Updates the internal state with a fresh list of liquidations.
        Expected format: {'p': price, 'q': qty, 'S': side, 'T': timestamp}
        """
        self.liquidations = []
        for liq in liquidations:
            self.liquidations.append({
                'price': float(liq.get('p', 0)),
                'qty': float(liq.get('q', 0)),
                'timestamp': liq.get('T', 0)
            })

    def _cleanup(self, current_ts: int):
        """Removes events older than window."""
        cutoff = current_ts - (self.window_minutes * 60 * 1000)
        self.liquidations = [e for e in self.liquidations if e['timestamp'] > cutoff]

    def get_clusters(self) -> List[Tuple[float, float]]:
        """
        Returns a list of (price_level, volume) for significant clusters.
        Significant = Top 3 or > Mean + 2*StdDev
        """
        if not self.liquidations:
            return []

        df = pd.DataFrame(self.liquidations)
        
        # Determine current price reference (approximate from liquidations or pass it in)
        # Using mean price of recent liquidations as anchor isn't great, 
        # but for binning we just need absolute price buckets.
        
        # Binning: Round price to nearest bin_size
        # But bin size is percentage... 0.25%.
        # So we really need log prices or just fixed width if price doesn't move much.
        # User said "Bin liquidation prices (0.25% bins)".
        # This implies dynamic bins based on price? 
        # Or fixed bins relative to a baseline? 
        # Simpler: Round(price / bin_step) * bin_step? 
        # If Price=2000, 0.25% = 5. So bins of size 5.
        
        # Let's use the average price of the window to determine bin size
        avg_price = df['price'].mean()
        bin_step = avg_price * self.bin_size_pct
        
        df['bin'] = (df['price'] / bin_step).round() * bin_step
        
        gb = df.groupby('bin')['qty'].sum().reset_index()
        
        if gb.empty:
            return []
            
        mean_vol = gb['qty'].mean()
        std_vol = gb['qty'].std(ddof=0)
        if pd.isna(std_vol): std_vol = 0
        
        threshold = mean_vol + (2 * std_vol)
        
        # Filter
        significant = gb[gb['qty'] > threshold]
        
        # Also always include Top 3
        top3 = gb.sort_values('qty', ascending=False).head(3)
        
        # Combine
        clusters = pd.concat([significant, top3]).drop_duplicates()
        
        return list(zip(clusters['bin'], clusters['qty']))
```

## File: src/signal_engine/sweeps.py
```python

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

    def check_sweep(self, candle: Dict, clusters: List[tuple], recent_volumes: List[float]) -> Optional[Dict]:
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

        # 1. Volume Check
        avg_vol = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 1.0
        if vol < 3 * avg_vol:
            return None # No volume spike
            
        detected_sweep = None
        
        for level, level_vol in clusters:
            # Check for existing recent sweep on this level
            if level in self.active_sweeps:
                continue

            # Downward Sweep (Potential Long Setup)
            if low < level * (1 - self.cluster_threshold_pct):
                # We went significantly below the level
                detected_sweep = {
                    'type': 'LONG_SWEEP',
                    'level': level,
                    'price': close,
                    'timestamp': timestamp
                }
                
            # Upward Sweep (Potential Short Setup)
            elif high > level * (1 + self.cluster_threshold_pct):
                detected_sweep = {
                    'type': 'SHORT_SWEEP',
                    'level': level,
                    'price': close,
                    'timestamp': timestamp
                }
            
            if detected_sweep:
                # Mark as active to avoid spam
                self.active_sweeps[level] = timestamp
                break # Return first valid sweep (or collect all?)

        if detected_sweep:
            logger.info(f"Sweep Detected: {detected_sweep}")
            # self.state = "SWEEP_DETECTED" # Removed simple state, managed by engine
            
        return detected_sweep
```

## File: src/signal_engine/confirmation.py
```python

import logging
from typing import Dict, List, Optional
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

    def check_confirmation(self, candle: Dict, cvd_delta: float, ob_imbalance: float) -> bool:
        """
        Checks if the active sweep is confirmed.
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
        if sweep_type == 'LONG_SWEEP' and cvd_delta > 0:
            cvd_confirmed = True
        elif sweep_type == 'SHORT_SWEEP' and cvd_delta < 0:
            cvd_confirmed = True
            
        if not cvd_confirmed:
            logger.info(f"Reclaim rejected by CVD: {cvd_delta:.2f}")
            return False

        # 3. Orderbook Imbalance Check
        # We want OB to support the move or at least not oppose it strongly.
        # Imbalance range: [-1, 1] (1 = All Bids, -1 = All Asks)
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

        logger.info(f"Reclaim CONFIRMED for {sweep_type} @ {close} | CVD: {cvd_delta:.2f} | OB: {ob_imbalance:.2f}")
        # Return True but KEEP sweep active? No, consume it.
        signal_sweep = self.sweep_event
        self.reset() # Consume signal
        return signal_sweep # Return the sweep event to signal generator
```

## File: src/orderbook/book.py
```python

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
```

## File: src/orderbook/snapshot_manager.py
```python

import os
import json
import gzip
import time
import logging
import glob
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class SnapshotManager:
    """
    Manages storage of orderbook snapshots.
    - Saves as gzip compressed JSON.
    - Enforces TTL (Time To Live).
    - Enforces Max Storage Cap (Size).
    """
    def __init__(self, storage_dir: str = "snapshots", 
                 max_storage_bytes: int = 200 * 1024 * 1024, 
                 ttl_seconds: int = 4 * 60 * 60):
        self.storage_dir = storage_dir
        self.max_storage_bytes = max_storage_bytes
        self.ttl_seconds = ttl_seconds
        
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    def save_snapshot(self, symbol: str, data: Dict[str, Any]):
        """Saves a compressed snapshot."""
        timestamp = int(time.time() * 1000)
        filename = f"{self.storage_dir}/{symbol}_{timestamp}.json.gz"
        
        try:
            json_str = json.dumps(data)
            with gzip.open(filename, 'wt', encoding='utf-8') as f:
                f.write(json_str)
            logger.debug(f"Saved snapshot: {filename}")
            
            # trigger cleanup after save
            self._cleanup()
            
        except Exception as e:
            logger.error(f"Failed to save snapshot {filename}: {e}")

    def load_snapshot(self, filename: str) -> Dict[str, Any]:
        """Loads a compressed snapshot."""
        try:
            with gzip.open(filename, 'rt', encoding='utf-8') as f:
                content = f.read()
                return json.loads(content)
        except Exception as e:
            logger.error(f"Failed to load snapshot {filename}: {e}")
            return {}

    def _cleanup(self):
        """
        Enforces storage policies:
        1. TTL: Delete files older than TTL.
        2. Cap: Delete oldest files until under MAX_STORAGE_BYTES.
        """
        files = glob.glob(f"{self.storage_dir}/*.json.gz")
        if not files:
            return

        # Get file stats
        file_stats = []
        total_size = 0
        now = time.time()

        for f in files:
            try:
                stat = os.stat(f)
                mtime = stat.st_mtime
                size = stat.st_size
                file_stats.append({'path': f, 'mtime': mtime, 'size': size})
                total_size += size
            except Exception:
                continue

        # Sort by age (oldest first)
        file_stats.sort(key=lambda x: x['mtime'])

        # 1. TTL Cleanup
        remaining_files = []
        for f in file_stats:
            if now - f['mtime'] > self.ttl_seconds:
                try:
                    os.remove(f['path'])
                    total_size -= f['size']
                    logger.info(f"Deleted expired snapshot: {f['path']}")
                except Exception as e:
                    logger.error(f"Error deleting {f['path']}: {e}")
            else:
                remaining_files.append(f)

        # 2. Storage Cap Cleanup
        while total_size > self.max_storage_bytes and remaining_files:
            target = remaining_files.pop(0)  # Remove oldest
            try:
                os.remove(target['path'])
                total_size -= target['target']
                logger.warning(f"Storage cap exceeded. Deleted: {target['path']}")
            except Exception as e:
                logger.error(f"Error deleting {target['path']}: {e}")
```

## File: src/utils/redis_client.py
```python

import redis.asyncio as redis
import os
import json
import logging
import time
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class RedisClient:
    """
    Async Redis client for managing sliding window data.
    Uses ZSETs for time-series data (score=timestamp).
    """
    def __init__(self):
        self.host = os.getenv("REDIS_HOST", "localhost")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        self.redis = None

    async def connect(self):
        """Establishes connection to Redis."""
        try:
            self.redis = redis.Redis(host=self.host, port=self.port, decode_responses=True)
            await self.redis.ping()
            logger.info("Connected to Redis.")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def close(self):
        if self.redis:
            await self.redis.close()

    async def add_trade(self, symbol: str, trade_data: Dict[str, Any]):
        """
        Adds a trade to the sliding window ZSET.
        Key: trades:{symbol}
        Score: timestamp (ms)
        Member: JSON string
        """
        key = f"trades:{symbol}"
        timestamp = trade_data['T']  # Trade time
        # Add random suffix to timestamp to avoid collisions if needed, 
        # but for simple aggTrades, duplicates might be updates. 
        # Better: use tradeId as part of member or just unique member.
        member = json.dumps(trade_data)
        
        await self.redis.zadd(key, {member: timestamp})
        
        # Trim old data (keep last 1 hour max for safety)
        # In production, specialized cleanup or shorter windows might be better
        # For CVD 5m, we need at least 5 mins.
        # Let's clean older than 1 hour periodically to be safe.
        
    async def add_liquidation(self, symbol: str, liq_data: Dict[str, Any]):
        """
        Adds a liquidation event.
        Key: liquidations:{symbol}
        """
        key = f"liquidations:{symbol}"
        timestamp = liq_data.get('T', time.time() * 1000)
        member = json.dumps(liq_data)
        await self.redis.zadd(key, {member: timestamp})

    async def get_window_data(self, key: str, start_ts: int, end_ts: int = -1) -> List[Dict[str, Any]]:
        """
        Retrieves data from ZSET within score range.
        If end_ts is -1, goes to infinity.
        """
        if end_ts == -1:
            end_ts = "+inf"
            
        data = await self.redis.zrangebyscore(key, start_ts, end_ts)
        return [json.loads(d) for d in data]

    async def cleanup_old_data(self, key: str, max_age_ms: int):
        """
        Removes data older than max_age_ms from now.
        """
        now = time.time() * 1000
        min_score = 0
        max_score = now - max_age_ms
        await self.redis.zremrangebyscore(key, min_score, max_score)
```

## File: src/utils/db.py
```python

import psycopg2
from psycopg2 import pool
import os
import logging
import json
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseClient:
    """
    PostgreSQL Client using connection pooling.
    Handles schema creation and data insertion.
    """
    def __init__(self):
        self.min_conn = 1
        self.max_conn = 5
        self.pool = None
        
        self.user = os.getenv("POSTGRES_USER", "admin")
        self.password = os.getenv("POSTGRES_PASSWORD", "securepassword")
        self.db = os.getenv("POSTGRES_DB", "trade_signals")
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")

    def connect(self):
        """Initializes connection pool."""
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(
                self.min_conn, self.max_conn,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.db
            )
            self._init_schema()
            logger.info("Connected to PostgreSQL.")
        except Exception as e:
            logger.error(f"Failed to connect to DB: {e}")
            raise

    @contextmanager
    def get_cursor(self):
        conn = self.pool.getconn()
        try:
            yield conn.cursor()
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"DB Error: {e}")
            raise
        finally:
            self.pool.putconn(conn)

    def _init_schema(self):
        """Creates necessary tables if not exist."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS signals (
                id SERIAL PRIMARY KEY,
                timestamp BIGINT,
                symbol VARCHAR(20),
                side VARCHAR(4),
                entry_price DECIMAL,
                stop_loss DECIMAL,
                targets JSONB,
                confidence DECIMAL,
                actionable BOOLEAN,
                reasons JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                level VARCHAR(10),
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        ]
        with self.get_cursor() as cur:
            for q in queries:
                cur.execute(q)

    import asyncio

    async def insert_signal(self, signal_data: dict):
        """Async wrapper for inserting a generated signal."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._insert_signal_sync, signal_data)

    def _insert_signal_sync(self, signal_data: dict):
        """Synchronous insertion logic."""
        query = """
        INSERT INTO signals (timestamp, symbol, side, entry_price, stop_loss, targets, confidence, actionable, reasons)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.get_cursor() as cur:
            cur.execute(query, (
                signal_data.get('timestamp'),
                signal_data.get('symbol'),
                signal_data.get('side'),
                signal_data.get('entry_price'),
                signal_data.get('stop_loss'),
                json.dumps(signal_data.get('targets')),
                signal_data.get('confidence'),
                signal_data.get('actionable'),
                json.dumps(signal_data.get('reasons'))
            ))

    def close(self):
        if self.pool:
            self.pool.closeall()
```

## File: src/risk_manager/position_sizer.py
```python

import math
import logging

logger = logging.getLogger(__name__)

class PositionSizer:
    """
    Calculates trade size based on risk parameters.
    Default Risk: 1% of Capital.
    Formula: Risk_Amount / (Entry - StopLoss)
    """
    def __init__(self, capital_usdt: float = 20.0, risk_pct: float = 0.01):
        self.capital = capital_usdt
        self.risk_pct = risk_pct
        self.risk_amount = self.capital * self.risk_pct

    def calculate_size(self, entry_price: float, stop_loss: float, leverage: int = 1) -> float:
        """
        Calculates position size in base asset (e.g. ETH).
        """
        if entry_price <= 0 or stop_loss <= 0:
            return 0.0
            
        risk_per_unit = abs(entry_price - stop_loss)
        
        # If stop loss is too tight (0), unexpected error
        if risk_per_unit == 0:
            logger.error("Stop loss equals entry price!")
            return 0.0
            
        position_size = self.risk_amount / risk_per_unit
        
        # Apply leverage constraint check? No, leverage just determines margin required.
        # Max Position Value = Capital * Leverage
        max_notional = self.capital * leverage
        notional_value = position_size * entry_price
        
        if notional_value > max_notional:
            logger.warning(f"Position size {notional_value} exceeds max leverage {max_notional}. Capping.")
            position_size = max_notional / entry_price
            
        # Min Notional Check (Binance Futures is usually 5 USDT)
        if position_size * entry_price < 5.0:
            logger.warning(f"Position value {position_size * entry_price} < 5.0 USDT. Skipping.")
            return 0.0
            
        return position_size

    def round_step_size(self, quantity: float, step_size: float) -> float:
        """
        Rounds quantity to exchange step size.
        """
        precision = int(round(-math.log(step_size, 10), 0))
        return round(quantity, precision)
```

## File: src/notifier/telegram_bot.py
```python

import logging
import os
import aiohttp
import asyncio

logger = logging.getLogger(__name__)

class TelegramBot:
    """
    Async Telegram Notifier.
    """
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        self.session = None

    async def start(self):
        if not self.token or not self.chat_id:
            logger.warning("Telegram token or chat_id not set. Notifications disabled.")
            return
        self.session = aiohttp.ClientSession()

    async def stop(self):
        if self.session:
            await self.session.close()

    async def send_message(self, message: str):
        """Sends a message to the configured chat."""
        if not self.session:
            return

        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        try:
            async with self.session.post(self.base_url, json=payload) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to send Telegram message: {await resp.text()}")
                else:
                    logger.info("Telegram message sent.")
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")

    async def send_signal(self, signal: dict):
        """Formats and sends a trade signal."""
        msg = f"""
🚨 *TRADE SIGNAL* 🚨
Symbol: *{signal.get('symbol')}*
Side: *{signal.get('side')}*
Entry: `{signal.get('entry_price')}`
SL: `{signal.get('stop_loss')}`
Target: `{signal.get('targets', {}).get('tp1', 'N/A')}`
Size: {signal.get('position_size')}
Confidence: {signal.get('confidence')}%
Reasons: {', '.join(signal.get('reasons', []))}
"""
        await self.send_message(msg)
```

