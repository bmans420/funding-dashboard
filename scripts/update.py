#!/usr/bin/env python3
"""Intelligent auto-update script for funding rate data.

Runs hourly via LaunchAgent. Handles:
- Auto-discovery of new assets on existing exchanges
- Intelligent gap backfill after downtime
- Dynamic funding intervals (no hardcoded assumptions)
- Independent per-exchange error handling
"""

import sys
import os
import time
import logging
import traceback
import json
from datetime import datetime
from statistics import median

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import Database
from exchanges import EXCHANGE_MAP, get_hip3_adapters
import yaml

# ── Logging ──────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger("auto_update")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

fh = logging.FileHandler("logs/auto_update.log")
fh.setFormatter(formatter)
logger.addHandler(fh)

sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

# ── Config ───────────────────────────────────────────────────────────────────
ALLOWED_HIP3_DEPLOYERS = {"xyz", "cash", "flx", "hyna"}
MAX_BACKFILL_DAYS = 365
NEW_ASSET_HISTORY_DAYS = 30
FAILURE_TRACKER_PATH = "logs/failure_tracker.json"


def load_failure_tracker():
    try:
        with open(FAILURE_TRACKER_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def save_failure_tracker(tracker):
    with open(FAILURE_TRACKER_PATH, "w") as f:
        json.dump(tracker, f, indent=2)


def detect_typical_interval_ms(db, exchange):
    """Detect typical funding interval in ms from recent data."""
    times = db.get_distinct_funding_times(exchange, 20)
    if len(times) < 2:
        return 8 * 3600 * 1000  # default 8h
    intervals = [times[i] - times[i + 1] for i in range(len(times) - 1)]
    intervals = [iv for iv in intervals if iv > 0]
    if not intervals:
        return 8 * 3600 * 1000
    return int(median(intervals))


def get_db_symbols(db, exchange):
    """Get all distinct symbols in DB for an exchange."""
    symbols = db.get_symbols_for_exchange(exchange)
    return set(symbols)


def backfill_symbol(db, adapter, symbol, now_ms):
    """Backfill missing data for one exchange+symbol. Returns record count."""
    last_ts = db.get_latest_funding_time(adapter.name, symbol)

    if last_ts:
        gap_ms = now_ms - last_ts
        gap_hours = gap_ms / 3_600_000
        # Skip if gap is tiny (less than 30 min)
        if gap_hours < 0.5:
            return 0
        # Cap at MAX_BACKFILL_DAYS
        max_ms = MAX_BACKFILL_DAYS * 86_400_000
        if gap_ms > max_ms:
            logger.info(f"  {adapter.name} {symbol}: Gap {gap_hours:.0f}h > {MAX_BACKFILL_DAYS}d, capping backfill")
            start_ms = now_ms - max_ms
        else:
            start_ms = last_ts + 1
    else:
        # New symbol, no data — fetch NEW_ASSET_HISTORY_DAYS
        start_ms = now_ms - (NEW_ASSET_HISTORY_DAYS * 86_400_000)

    # Fetch with pagination
    all_records = []
    current_start = start_ms
    for _ in range(500):
        try:
            records = adapter.fetch_funding_history(
                symbol=symbol,
                start_time=current_start,
                end_time=now_ms,
            )
        except Exception as e:
            logger.error(f"  {adapter.name} {symbol}: fetch error: {e}")
            break

        if not records:
            break
        all_records.extend(records)

        last_time = max(r['funding_time'] for r in records)
        if last_time >= now_ms - 1000 or last_time <= current_start:
            break
        current_start = last_time + 1
        time.sleep(0.15)

    if all_records:
        db.insert_funding_rates(all_records)
    return len(all_records)


def detect_interval_changes(db, adapter_name, symbol):
    """Check for funding interval changes and log them."""
    # Get last 10 funding times for this exchange+symbol
    response = db.client.table('funding_rates').select(
        'funding_time'
    ).eq('exchange', adapter_name).eq('symbol', symbol).order(
        'funding_time', desc=True
    ).limit(10).execute()
    
    if not response.data or len(response.data) < 3:
        return
        
    times = [row['funding_time'] for row in response.data]
    intervals_h = [(times[i] - times[i + 1]) / 3_600_000 for i in range(len(times) - 1)]
    if len(intervals_h) >= 2:
        recent = intervals_h[0]
        prev = intervals_h[1]
        if abs(recent - prev) > 0.5:
            logger.info(f"  Interval change detected: {symbol} on {adapter_name} "
                        f"({prev:.1f}hr → {recent:.1f}hr)")


def process_exchange(db, adapter, now_ms, failure_tracker):
    """Process one exchange: discover new assets, backfill gaps. Returns stats dict."""
    name = adapter.name
    stats = {"records": 0, "new_assets": [], "errors": 0}

    try:
        # Discover available symbols from API
        api_symbols = set(adapter.get_available_symbols())
        db_symbols = get_db_symbols(db, name)
        new_symbols = api_symbols - db_symbols

        logger.info(f"{name}: {len(api_symbols)} symbols on API, {len(db_symbols)} in DB"
                     + (f" ({len(new_symbols)} NEW: {', '.join(sorted(new_symbols)[:10])})" if new_symbols else ""))

        # Process new symbols first (full history)
        for sym in sorted(new_symbols):
            logger.info(f"  {name} {sym}: New asset, fetching {NEW_ASSET_HISTORY_DAYS}-day history...")
            count = backfill_symbol(db, adapter, sym, now_ms)
            logger.info(f"  {name} {sym}: Inserted {count} historical records")
            stats["records"] += count
            stats["new_assets"].append(sym)
            time.sleep(0.2)

        # Backfill existing symbols
        all_symbols = sorted(api_symbols & db_symbols)
        for sym in all_symbols:
            count = backfill_symbol(db, adapter, sym, now_ms)
            if count > 0:
                logger.info(f"  {name} {sym}: Inserted {count} new records")
                detect_interval_changes(db, name, sym)
            stats["records"] += count
            time.sleep(0.1)

        # Clear failure counter on success
        failure_tracker.pop(name, None)

    except Exception as e:
        logger.error(f"{name}: FAILED - {e}")
        logger.error(traceback.format_exc())
        stats["errors"] = 1
        # Track consecutive failures
        count = failure_tracker.get(name, 0) + 1
        failure_tracker[name] = count
        if count >= 3:
            logger.warning(f"⚠️  {name}: {count} consecutive failures - check API status!")

    return stats


def main():
    logger.info("=" * 60)
    logger.info("Starting Auto-Update Cycle")
    logger.info("=" * 60)

    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    # Initialize database connection using DATABASE_URL
    try:
        db = Database()
    except ValueError as e:
        logger.error(f"Database connection failed: {e}")
        logger.error("Make sure DATABASE_URL environment variable is set")
        return
    now_ms = int(time.time() * 1000)
    failure_tracker = load_failure_tracker()

    total_records = 0
    total_new = []
    exchanges_ok = 0
    exchanges_fail = 0

    # Process standard exchanges
    for name, conf in config.get("exchanges", {}).items():
        if not conf.get("enabled", False) or name not in EXCHANGE_MAP:
            continue
        logger.info(f"Checking {name}...")
        adapter = EXCHANGE_MAP[name]()
        stats = process_exchange(db, adapter, now_ms, failure_tracker)
        total_records += stats["records"]
        total_new.extend(stats["new_assets"])
        if stats["errors"]:
            exchanges_fail += 1
        else:
            exchanges_ok += 1

    # Process HIP3 deployers (only allowed ones)
    logger.info("Checking HIP3 deployers...")
    try:
        hip3_adapters = get_hip3_adapters()
        for name, adapter in hip3_adapters.items():
            deployer = name.replace("hl-", "")
            if deployer not in ALLOWED_HIP3_DEPLOYERS:
                logger.info(f"  Skipping {name} (not in allowed deployers)")
                continue
            logger.info(f"Checking {name}...")
            stats = process_exchange(db, adapter, now_ms, failure_tracker)
            total_records += stats["records"]
            total_new.extend(stats["new_assets"])
            if stats["errors"]:
                exchanges_fail += 1
            else:
                exchanges_ok += 1
    except Exception as e:
        logger.error(f"HIP3 discovery failed: {e}")
        exchanges_fail += 1

    save_failure_tracker(failure_tracker)

    # Summary
    total_exchanges = exchanges_ok + exchanges_fail
    logger.info("-" * 60)
    logger.info("Update Cycle Complete")
    logger.info(f"  Total records inserted: {total_records:,}")
    if total_new:
        logger.info(f"  New assets discovered: {len(total_new)} ({', '.join(total_new[:20])})")
    logger.info(f"  Exchanges updated: {exchanges_ok}/{total_exchanges}"
                + (f" ({exchanges_fail} failed)" if exchanges_fail else ""))
    next_run = datetime.fromtimestamp(time.time() + 3600).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"  Next scheduled run: {next_run}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
