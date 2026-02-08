
import os
import json
import gzip
import time
import logging
import glob
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class SnapshotManager:
    """
    Manages storage of orderbook snapshots.
    - Saves as gzip compressed JSON.
    - Enforces TTL (Time To Live).
    - Enforces Max Storage Cap (Size).
    """
    def __init__(self, storage_dir: str = "snapshots", 
                 max_storage_bytes: int = 200 * 1024 * 1024, 
                 ttl_seconds: int = 4 * 60 * 60):
        self.storage_dir = storage_dir
        self.max_storage_bytes = max_storage_bytes
        self.ttl_seconds = ttl_seconds
        
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    def save_snapshot(self, symbol: str, data: Dict[str, Any]):
        """Saves a compressed snapshot."""
        timestamp = int(time.time() * 1000)
        filename = f"{self.storage_dir}/{symbol}_{timestamp}.json.gz"
        
        try:
            json_str = json.dumps(data)
            with gzip.open(filename, 'wt', encoding='utf-8') as f:
                f.write(json_str)
            logger.debug(f"Saved snapshot: {filename}")
            
            # trigger cleanup after save
            self._cleanup()
            
        except Exception as e:
            logger.error(f"Failed to save snapshot {filename}: {e}")

    def load_snapshot(self, filename: str) -> Dict[str, Any]:
        """Loads a compressed snapshot."""
        try:
            with gzip.open(filename, 'rt', encoding='utf-8') as f:
                content = f.read()
                return json.loads(content)
        except Exception as e:
            logger.error(f"Failed to load snapshot {filename}: {e}")
            return {}

    def _cleanup(self):
        """
        Enforces storage policies:
        1. TTL: Delete files older than TTL.
        2. Cap: Delete oldest files until under MAX_STORAGE_BYTES.
        """
        files = glob.glob(f"{self.storage_dir}/*.json.gz")
        if not files:
            return

        # Get file stats
        file_stats = []
        total_size = 0
        now = time.time()

        for f in files:
            try:
                stat = os.stat(f)
                mtime = stat.st_mtime
                size = stat.st_size
                file_stats.append({'path': f, 'mtime': mtime, 'size': size})
                total_size += size
            except Exception:
                continue

        # Sort by age (oldest first)
        file_stats.sort(key=lambda x: x['mtime'])

        # 1. TTL Cleanup
        remaining_files = []
        for f in file_stats:
            if now - f['mtime'] > self.ttl_seconds:
                try:
                    os.remove(f['path'])
                    total_size -= f['size']
                    logger.info(f"Deleted expired snapshot: {f['path']}")
                except Exception as e:
                    logger.error(f"Error deleting {f['path']}: {e}")
            else:
                remaining_files.append(f)

        # 2. Storage Cap Cleanup
        while total_size > self.max_storage_bytes and remaining_files:
            target = remaining_files.pop(0)  # Remove oldest
            try:
                os.remove(target['path'])
                total_size -= target['size']
                logger.warning(f"Storage cap exceeded. Deleted: {target['path']}")
            except Exception as e:
                logger.error(f"Error deleting {target['path']}: {e}")
