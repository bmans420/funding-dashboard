#!/usr/bin/env python3
"""Fetch Binance perpetual open interest data and save to PostgreSQL database."""

import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import Database

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/oi_update.log"),
    ],
)
logger = logging.getLogger("update_oi")

BASE = "https://fapi.binance.com"
TOP_N = 10


def main():
    logger.info("Starting Binance OI update")
    
    # Initialize database connection using DATABASE_URL
    try:
        db = Database()
    except ValueError as e:
        logger.error(f"Database connection failed: {e}")
        logger.error("Make sure DATABASE_URL environment variable is set")
        return
    session = requests.Session()
    session.headers["User-Agent"] = "FundingDashboard/1.0"

    # 1. Get all perpetual symbols
    try:
        resp = session.get(f"{BASE}/fapi/v1/exchangeInfo", timeout=30)
        resp.raise_for_status()
        info = resp.json()
    except Exception as e:
        logger.critical(f"Failed to fetch exchangeInfo: {e}")
        return

    perp_symbols = []
    for s in info.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING" and s["symbol"].endswith("USDT"):
            perp_symbols.append(s["symbol"])

    logger.info(f"Found {len(perp_symbols)} perpetual contracts")

    # 2. Batch fetch prices
    prices = {}
    try:
        resp = session.get(f"{BASE}/fapi/v1/ticker/price", timeout=30)
        resp.raise_for_status()
        for item in resp.json():
            prices[item["symbol"]] = float(item["price"])
    except Exception as e:
        logger.error(f"Failed to batch fetch prices: {e}")

    # 3. Fetch OI using concurrent requests for speed
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = []
    failures = 0

    def fetch_oi(sym):
        try:
            resp = session.get(f"{BASE}/fapi/v1/openInterest", params={"symbol": sym}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            oi_qty = float(data.get("openInterest", 0))
            price = prices.get(sym, 0)
            if price <= 0 or oi_qty <= 0:
                return None
            oi_usd = oi_qty * price
            clean = sym
            for suffix in ("USDT", "BUSD", "USDC"):
                if clean.endswith(suffix):
                    clean = clean[: -len(suffix)]
                    break
            return {"symbol": clean, "oi_usd": oi_usd}
        except Exception as e:
            logger.error(f"Failed to fetch OI for {sym}: {e}")
            return "FAIL"

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_oi, sym): sym for sym in perp_symbols}
        for future in as_completed(futures):
            result = future.result()
            if result == "FAIL":
                failures += 1
            elif result is not None:
                results.append(result)

    total = len(perp_symbols)
    if failures > total * 0.3:
        logger.critical(f"High failure rate: {failures}/{total} symbols failed")

    # 4. Sort and take top N
    results.sort(key=lambda x: x["oi_usd"], reverse=True)
    top = results[:TOP_N]

    # 5. Save to database
    timestamp = datetime.now(timezone.utc)
    for item in top:
        item["timestamp"] = timestamp
    
    try:
        inserted = db.insert_oi_data(top)
        logger.info(f"Saved {inserted} OI records to database")
        for i, item in enumerate(top, 1):
            logger.info(f"  #{i} {item['symbol']}: ${item['oi_usd']/1e9:.2f}B")
    except Exception as e:
        logger.critical(f"Failed to save to database: {e}")
        return

    logger.info(f"OI update complete ({failures} failures out of {total} symbols)")


if __name__ == "__main__":
    main()
