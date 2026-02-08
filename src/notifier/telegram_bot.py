
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
