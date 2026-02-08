
import psycopg2
from psycopg2 import pool
import os
import logging
import json
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseClient:
    """
    PostgreSQL Client using connection pooling.
    Handles schema creation and data insertion.
    """
    def __init__(self):
        self.min_conn = 1
        self.max_conn = 5
        self.pool = None
        
        self.user = os.getenv("POSTGRES_USER", "admin")
        self.password = os.getenv("POSTGRES_PASSWORD", "securepassword")
        self.db = os.getenv("POSTGRES_DB", "trade_signals")
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")

    def connect(self):
        """Initializes connection pool."""
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(
                self.min_conn, self.max_conn,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.db
            )
            self._init_schema()
            logger.info("Connected to PostgreSQL.")
        except Exception as e:
            logger.error(f"Failed to connect to DB: {e}")
            raise

    @contextmanager
    def get_cursor(self):
        conn = self.pool.getconn()
        try:
            yield conn.cursor()
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"DB Error: {e}")
            raise
        finally:
            self.pool.putconn(conn)

    def _init_schema(self):
        """Creates necessary tables if not exist."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS signals (
                id SERIAL PRIMARY KEY,
                timestamp BIGINT,
                symbol VARCHAR(20),
                side VARCHAR(4),
                entry_price DECIMAL,
                stop_loss DECIMAL,
                targets JSONB,
                confidence DECIMAL,
                actionable BOOLEAN,
                reasons JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                level VARCHAR(10),
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        ]
        with self.get_cursor() as cur:
            for q in queries:
                cur.execute(q)

    import asyncio

    async def insert_signal(self, signal_data: dict):
        """Async wrapper for inserting a generated signal."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._insert_signal_sync, signal_data)

    def _insert_signal_sync(self, signal_data: dict):
        """Synchronous insertion logic."""
        query = """
        INSERT INTO signals (timestamp, symbol, side, entry_price, stop_loss, targets, confidence, actionable, reasons)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.get_cursor() as cur:
            cur.execute(query, (
                signal_data.get('timestamp'),
                signal_data.get('symbol'),
                signal_data.get('side'),
                signal_data.get('entry_price'),
                signal_data.get('stop_loss'),
                json.dumps(signal_data.get('targets')),
                signal_data.get('confidence'),
                signal_data.get('actionable'),
                json.dumps(signal_data.get('reasons'))
            ))

    def close(self):
        if self.pool:
            self.pool.closeall()
