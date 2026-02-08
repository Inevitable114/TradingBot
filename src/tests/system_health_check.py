
import asyncio
import logging
import os
import sys
import aiohttp
import time

# Add root to path
sys.path.append(os.getcwd())

from src.utils.redis_client import RedisClient
from src.utils.db import DatabaseClient
from src.notifier.telegram_bot import TelegramBot

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("HealthCheck")

async def check_binance():
    async with aiohttp.ClientSession() as session:
        async with session.get('https://fapi.binance.com/fapi/v1/time') as resp:
            if resp.status == 200:
                data = await resp.json()
                logger.info(f"✅ Binance API: Connected (Server Time: {data['serverTime']})")
                return True
            else:
                logger.error(f"❌ Binance API: Failed ({resp.status})")
                return False

async def main():
    logger.info("--- Starting Full System Health Check ---")
    all_passed = True
    
    # 1. Redis Check
    try:
        redis = RedisClient()
        await redis.connect()
        await redis.redis.ping() # Access underlying redis instance
        logger.info("✅ Redis: Connected & Pingable")
    except Exception as e:
        logger.error(f"❌ Redis: Failed - {e}")
        all_passed = False

    # 2. Database Check
    try:
        db = DatabaseClient()
        db.connect()
        with db.get_cursor() as cur:
            cur.execute("SELECT 1")
            res = cur.fetchone()
            if res[0] == 1:
                 logger.info("✅ PostgreSQL: Connected & Queryable")
            else:
                 logger.error("❌ PostgreSQL: Query Failed")
                 all_passed = False
    except Exception as e:
        logger.error(f"❌ PostgreSQL: Failed - {e}")
        all_passed = False

    # 3. Binance API Check
    if not await check_binance():
        all_passed = False
        
    # 4. Telegram Notification Check
    try:
        bot = TelegramBot()
        await bot.start()
        msg = "🟢 **System Health Check**: ALL SYSTEMS OPERATIONAL" if all_passed else "🔴 **System Health Check**: FAILURES DETECTED"
        await bot.send_message(msg)
        await bot.stop()
        logger.info("✅ Telegram: Notification Sent")
    except Exception as e:
        logger.error(f"❌ Telegram: Failed - {e}")
        all_passed = False

    if all_passed:
        logger.info(">>> SYSTEM STATUS: 100% OPERATIONAL <<<")
        exit(0)
    else:
        logger.error(">>> SYSTEM STATUS: ISSUES FOUND <<<")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
