"""Bybit funding rate adapter."""

import logging
import time
from typing import List, Optional
from .base import BaseExchangeAdapter

logger = logging.getLogger(__name__)


class BybitAdapter(BaseExchangeAdapter):
    name = "bybit"
    funding_interval_hours = 8.0
    base_url = "https://api.bybit.com"

    def get_symbol_name(self, base: str) -> str:
        return f"{base}USDT"

    def get_available_symbols(self) -> List[str]:
        """Get all available USDT linear perpetual symbols from Bybit."""
        url = f"{self.base_url}/v5/market/instruments-info"
        params = {"category": "linear", "limit": "1000"}
        data = self._request("GET", url, params)
        if not data or data.get("retCode") != 0:
            logger.error(f"[{self.name}] Failed to fetch instruments: {data}")
            return []

        symbols = []
        for item in data.get("result", {}).get("list", []):
            if (item.get('status') == 'Trading' and
                    item.get('settleCoin') == 'USDT' and
                    item.get('contractType') == 'LinearPerpetual'):
                # Extract base from symbol like "BTCUSDT" -> "BTC"
                sym = item.get('symbol', '')
                if sym.endswith('USDT'):
                    symbols.append(sym[:-4])

        logger.info(f"[{self.name}] Found {len(symbols)} perpetual markets")
        return sorted(symbols)

    def fetch_funding_history(self, symbol: str, start_time: Optional[int] = None,
                              end_time: Optional[int] = None, limit: int = 200) -> List[dict]:
        """Fetch from /v5/market/funding/history. Bybit returns newest first."""
        pair = self.get_symbol_name(symbol)
        url = f"{self.base_url}/v5/market/funding/history"

        all_records = []
        cursor_end = end_time
        target_start = start_time or int((time.time() - 86400 * 365) * 1000)

        for _ in range(100):
            params = {"category": "linear", "symbol": pair, "limit": "200"}
            if cursor_end:
                params["endTime"] = str(cursor_end)

            data = self._request("GET", url, params)
            if not data or data.get("retCode") != 0:
                logger.error(f"[{self.name}] API error: {data}")
                break

            items = data.get("result", {}).get("list", [])
            if not items:
                break

            batch = []
            for item in items:
                rec = self._make_record(
                    symbol=symbol,
                    funding_rate=float(item["fundingRate"]),
                    funding_time=int(item["fundingRateTimestamp"]),
                )
                if rec:
                    batch.append(rec)

            all_records.extend(batch)

            oldest = min(int(item["fundingRateTimestamp"]) for item in items)
            if oldest <= target_start:
                break
            cursor_end = oldest - 1

            time.sleep(0.2)

        if start_time:
            all_records = [r for r in all_records if r['funding_time'] >= start_time]
        return all_records
