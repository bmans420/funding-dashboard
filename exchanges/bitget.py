"""Bitget funding rate adapter."""

import logging
from typing import List, Optional
from .base import BaseExchangeAdapter

logger = logging.getLogger(__name__)


class BitgetAdapter(BaseExchangeAdapter):
    name = "bitget"
    funding_interval_hours = 8.0
    base_url = "https://api.bitget.com"

    def get_symbol_name(self, base: str) -> str:
        return f"{base}USDT"

    def get_available_symbols(self) -> List[str]:
        """Get all available USDT futures symbols from Bitget."""
        url = f"{self.base_url}/api/v2/mix/market/tickers"
        params = {"productType": "USDT-FUTURES"}
        data = self._request("GET", url, params)
        if not data or data.get("code") != "00000":
            logger.error(f"[{self.name}] Failed to fetch tickers: {data}")
            return []

        symbols = []
        for item in data.get("data", []):
            sym = item.get('symbol', '')
            if sym.endswith('USDT'):
                symbols.append(sym[:-4])

        logger.info(f"[{self.name}] Found {len(symbols)} perpetual markets")
        return sorted(symbols)

    def fetch_funding_history(self, symbol: str, start_time: Optional[int] = None,
                              end_time: Optional[int] = None, limit: int = 100) -> List[dict]:
        """Fetch from /api/v2/mix/market/history-fund-rate"""
        pair = self.get_symbol_name(symbol)
        url = f"{self.base_url}/api/v2/mix/market/history-fund-rate"
        params = {"symbol": pair, "productType": "USDT-FUTURES", "pageSize": str(min(limit, 100))}
        if end_time:
            params["endTime"] = str(end_time)

        data = self._request("GET", url, params)
        if not data or data.get("code") != "00000":
            logger.error(f"[{self.name}] API error: {data}")
            return []

        records = []
        for item in data.get("data", []):
            rec = self._make_record(
                symbol=symbol,
                funding_rate=float(item["fundingRate"]),
                funding_time=int(item["fundingTime"]),
            )
            if rec:
                records.append(rec)
        return records
