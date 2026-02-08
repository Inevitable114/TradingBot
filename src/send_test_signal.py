
import asyncio
import os
import logging
from src.notifier.telegram_bot import TelegramBot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    logger.info("Testing Telegram Bot Connection...")
    bot = TelegramBot()
    await bot.start()
    
    test_signal = {
        'symbol': 'TEST-ETHUSDT',
        'side': 'LONG',
        'entry_price': 2000.0,
        'stop_loss': 1990.0,
        'targets': {'tp1': 2020.0},
        'position_size': 0.5,
        'confidence': 99.9,
        'reasons': ['Manual Test', 'System Verification']
    }
    
    await bot.send_signal(test_signal)
    await bot.stop()
    logger.info("Test signal sent. Check your Telegram.")

if __name__ == "__main__":
    asyncio.run(main())
