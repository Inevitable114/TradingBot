
import asyncio
import os
import logging
from src.notifier.telegram_bot import TelegramBot

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    logger.info(f"Testing Telegram with Token: {token[:5]}... and Chat ID: {chat_id}")
    
    bot = TelegramBot()
    await bot.start()
    
    message = "🔔 *Test Notification* from ETH Futures Engine.\nIf you see this, connectivity is working!"
    logger.info("Sending message...")
    
    await bot.send_message(message)
    await bot.stop()

if __name__ == "__main__":
    try:
        asyncio.run(test())
    except Exception as e:
        logger.error(f"Test failed: {e}")
