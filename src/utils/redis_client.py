
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
        
        # Trim old data (keep last 1 hour max)
        await self.cleanup_old_data(key, 60 * 60 * 1000)
        
    async def add_liquidation(self, symbol: str, liq_data: Dict[str, Any]):
        """
        Adds a liquidation event.
        Key: liquidations:{symbol}
        """
        key = f"liquidations:{symbol}"
        timestamp = liq_data.get('T', time.time() * 1000)
        member = json.dumps(liq_data)
        await self.redis.zadd(key, {member: timestamp})
        
        # Cleanup
        await self.cleanup_old_data(key, 24 * 60 * 60 * 1000) # Keep 24h for clusters

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
