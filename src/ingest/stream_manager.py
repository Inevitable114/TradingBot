
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
