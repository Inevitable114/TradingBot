
import asyncio
import os
import aiohttp
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_bot_info():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token or "your_token_here" in token:
        logger.error("Please set TELEGRAM_BOT_TOKEN in .env first!")
        return

    url = f"https://api.telegram.org/bot{token}/getMe"
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            
            if not data.get("ok"):
                logger.error(f"Error: {data}")
                return
                
            result = data.get("result", {})
            first_name = result.get("first_name")
            username = result.get("username")
            
            print(f"\n🤖 BOT FOUND!")
            print(f"Name: {first_name}")
            print(f"Username: @{username}")
            print(f"\n👉 Search for '@{username}' in Telegram and click Start!")

if __name__ == "__main__":
    asyncio.run(get_bot_info())
