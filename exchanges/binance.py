"""Binance Futures funding rate adapter."""

import logging
from typing import List, Optional
from .base import BaseExchangeAdapter

logger = logging.getLogger(__name__)


class BinanceAdapter(BaseExchangeAdapter):
    name = "binance"
    funding_interval_hours = 8.0
    base_url = "https://fapi.binance.com"

    def get_symbol_name(self, base: str) -> str:
        return f"{base}USDT"

    def get_available_symbols(self) -> List[str]:
        """Get all available USDT perpetual symbols from Binance."""
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        data = self._request("GET", url)
        if not data or 'symbols' not in data:
            logger.error(f"[{self.name}] Failed to fetch exchangeInfo")
            return []

        symbols = []
        for item in data['symbols']:
            if (item.get('contractType') == 'PERPETUAL' and
                    item.get('quoteAsset') == 'USDT' and
                    item.get('status') == 'TRADING'):
                symbols.append(item['baseAsset'])

        logger.info(f"[{self.name}] Found {len(symbols)} perpetual markets")
        return sorted(symbols)

    def fetch_funding_history(self, symbol: str, start_time: Optional[int] = None,
                              end_time: Optional[int] = None, limit: int = 1000) -> List[dict]:
        """Fetch from /fapi/v1/fundingRate"""
        pair = self.get_symbol_name(symbol)
        url = f"{self.base_url}/fapi/v1/fundingRate"
        params = {"symbol": pair, "limit": min(limit, 1000)}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        data = self._request("GET", url, params)
        if not data:
            return []

        records = []
        for item in data:
            rec = self._make_record(
                symbol=symbol,
                funding_rate=float(item["fundingRate"]),
                funding_time=int(item["fundingTime"]),
            )
            if rec:
                records.append(rec)
        return records
