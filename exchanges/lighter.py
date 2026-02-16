"""Lighter exchange adapter - uses official Lighter API."""

import logging
import time
from typing import List, Optional
from .base import BaseExchangeAdapter

logger = logging.getLogger(__name__)


class LighterAdapter(BaseExchangeAdapter):
    name = "lighter"
    funding_interval_hours = 1.0  # 1hr funding intervals
    base_url = "https://mainnet.zklighter.elliot.ai"

    # The /api/v1/fundings endpoint returns rates ~100x larger than the
    # /api/v1/funding-rates endpoint. Dividing by 100 produces decimal rates
    # consistent with other exchanges.
    FUNDINGS_RATE_DIVISOR = 100.0

    def __init__(self):
        super().__init__()
        self._symbol_to_market_id = None

    def get_symbol_name(self, base: str) -> str:
        return base

    def _get_symbol_map(self) -> dict:
        """Fetch symbol -> market_id mapping from orderBooks endpoint."""
        if self._symbol_to_market_id is not None:
            return self._symbol_to_market_id

        url = f"{self.base_url}/api/v1/orderBooks"
        data = self._request("GET", url)
        if not data or 'order_books' not in data:
            logger.error(f"[{self.name}] Failed to fetch orderBooks")
            self._symbol_to_market_id = {}
            return self._symbol_to_market_id

        self._symbol_to_market_id = {}
        for book in data['order_books']:
            sym = book.get('symbol', '')
            mid = book.get('market_id')
            if sym and mid is not None:
                self._symbol_to_market_id[sym] = int(mid)

        logger.info(f"[{self.name}] Discovered {len(self._symbol_to_market_id)} markets")
        return self._symbol_to_market_id

    def get_available_symbols(self) -> List[str]:
        sym_map = self._get_symbol_map()
        return sorted(sym_map.keys())

    def fetch_funding_history(self, symbol: str, start_time: Optional[int] = None,
                              end_time: Optional[int] = None, limit: int = 1000) -> List[dict]:
        """Fetch historical funding rates from /api/v1/fundings with backward pagination.

        The Lighter API returns up to 750 records ending at end_timestamp,
        ignoring start_timestamp. We paginate backward by moving end_timestamp.

        Args:
            symbol: Base symbol (e.g., 'BTC')
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
        """
        sym_map = self._get_symbol_map()
        lighter_sym = self.get_symbol_name(symbol)

        market_id = sym_map.get(lighter_sym)
        if market_id is None:
            logger.info(f"[{self.name}] {symbol}: not found in orderBooks")
            return []

        # Convert ms to seconds
        target_start_sec = (start_time // 1000) if start_time else int(time.time()) - 86400 * 365
        cursor_end_sec = (end_time // 1000) if end_time else int(time.time())

        all_records = []

        for _ in range(500):  # Safety limit
            url = f"{self.base_url}/api/v1/fundings"
            params = {
                'market_id': market_id,
                'resolution': '1h',
                'start_timestamp': target_start_sec,
                'end_timestamp': cursor_end_sec,
                'count_back': 750,
            }

            data = self._request("GET", url, params)
            if not data or data.get('code') != 200:
                logger.error(f"[{self.name}] API error for {symbol}: {data}")
                break

            fundings = data.get('fundings', [])
            if not fundings:
                break

            for item in fundings:
                ts_sec = int(item['timestamp'])
                raw_rate = float(item.get('rate', '0'))
                rate = raw_rate / self.FUNDINGS_RATE_DIVISOR

                # Apply direction: short means longs receive (negate)
                direction = item.get('direction', 'long')
                if direction == 'short':
                    rate = -abs(rate)

                funding_time_ms = ts_sec * 1000
                rec = self._make_record(
                    symbol=symbol,
                    funding_rate=rate,
                    funding_time=funding_time_ms,
                )
                if rec:
                    all_records.append(rec)

            # Check if we've reached far enough back
            oldest_ts = min(int(f['timestamp']) for f in fundings)
            if oldest_ts <= target_start_sec:
                break  # We have all data back to target

            # Move cursor backward: end at the oldest record - 1
            cursor_end_sec = oldest_ts - 1

            if len(fundings) < 750:
                break  # No more data available

            time.sleep(0.15)

        # Filter to requested range
        if start_time:
            all_records = [r for r in all_records if r['funding_time'] >= start_time]
        if end_time:
            all_records = [r for r in all_records if r['funding_time'] <= end_time]

        # Sort ascending by time
        all_records.sort(key=lambda r: r['funding_time'])

        logger.info(f"[{self.name}] {symbol}: fetched {len(all_records)} historical records")
        return all_records

    def fetch_all_current_rates(self) -> List[dict]:
        """Fetch ALL current Lighter funding rates in one API call."""
        url = f"{self.base_url}/api/v1/funding-rates"
        data = self._request("GET", url)
        if not data or 'funding_rates' not in data:
            return []

        records = []
        now_ms = int(time.time() * 1000)
        funding_time = now_ms - (now_ms % 3600000)

        for item in data['funding_rates']:
            if item.get('exchange') != 'lighter':
                continue
            symbol = item.get('symbol', '')
            rate = float(item.get('rate', 0))
            rec = self._make_record(
                symbol=symbol,
                funding_rate=rate,
                funding_time=funding_time,
            )
            if rec:
                records.append(rec)

        logger.info(f"[{self.name}] Fetched {len(records)} current rates")
        return records
