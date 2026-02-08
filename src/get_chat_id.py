
import asyncio
import os
import aiohttp
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_chat_id():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token or "your_token_here" in token:
        logger.error("Please set TELEGRAM_BOT_TOKEN in .env first!")
        return

    url = f"https://api.telegram.org/bot{token}/getUpdates"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            
            if not data.get("ok"):
                logger.error(f"Error: {data}")
                return
                
            updates = data.get("result", [])
            if not updates:
                logger.warning("No messages found. Please send a message (e.g., 'Hello') to your bot first!")
                return
                
            last_update = updates[-1]
            chat_id = last_update.get("message", {}).get("chat", {}).get("id")
            name = last_update.get("message", {}).get("chat", {}).get("first_name")
            
            print(f"\n✅ FOUND CHAT ID for {name}: {chat_id}")
            print(f"👉 Copy this ID and paste it into TELEGRAM_CHAT_ID in your .env file.\n")

if __name__ == "__main__":
    asyncio.run(get_chat_id())
