
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
