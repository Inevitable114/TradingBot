
import os
import ast

def audit_file(filepath, checks):
    """
    Reads a file and checks for specific strings or AST patterns.
    checks: List of (description, check_function)
    """
    if not os.path.exists(filepath):
        print(f"[FAIL] File not found: {filepath}")
        return

    with open(filepath, 'r') as f:
        content = f.read()

    print(f"\nAuditing {filepath}...")
    for desc, check_fn in checks:
        if check_fn(content):
            print(f"[PASS] {desc}")
        else:
            print(f"[FAIL] {desc}")

# Define Checks

def check_snapshot_fix(content):
    return "total_size -= target['size']" in content and "total_size -= target['target']" not in content

def check_connection_restored(content):
    return 'if "e" in msg and msg["e"] == "CONNECTION_RESTORED":' in content

def check_strict_sync(content):
    return "if U <= last_update_id + 1 and u >= last_update_id + 1:" in content

def check_redis_cleanup(content):
    return "await self.cleanup_old_data" in content

def check_await_db(content):
    return "await self.db.insert_signal(signal)" in content

def check_spread_logic(content):
    return "metrics.get('spread', 0.0)" in content and "metrics.get('depth', {})" in content

# Run Audit

base_dir = "/Users/dikshantgupta/.gemini/antigravity/scratch/eth-futures-signal-engine/src"

audit_file(f"{base_dir}/orderbook/snapshot_manager.py", [
    ("Snapshot Cleanup Variable Fix (target['size'])", check_snapshot_fix)
])

audit_file(f"{base_dir}/ingest/processor.py", [
    ("Connection Recovery Logic (CONNECTION_RESTORED check)", check_connection_restored),
    ("Strict Binance Sync Rules (U <= last + 1 <= u)", check_strict_sync)
])

audit_file(f"{base_dir}/utils/redis_client.py", [
    ("Redis Sliding Window Automatic Cleanup", check_redis_cleanup)
])

audit_file(f"{base_dir}/signal_engine/engine.py", [
    ("Async Database Insert Awaited", check_await_db)
])

audit_file(f"{base_dir}/signal_engine/confirmation.py", [
    ("Spread and Depth Monitoring Implemented", check_spread_logic)
])
