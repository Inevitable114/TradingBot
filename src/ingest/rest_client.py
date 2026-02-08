
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
