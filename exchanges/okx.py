"""OKX funding rate adapter."""

import logging
from typing import List, Optional
from .base import BaseExchangeAdapter

logger = logging.getLogger(__name__)


class OKXAdapter(BaseExchangeAdapter):
    name = "okx"
    funding_interval_hours = 8.0
    base_url = "https://www.okx.com"

    def get_symbol_name(self, base: str) -> str:
        return f"{base}-USDT-SWAP"

    def get_available_symbols(self) -> List[str]:
        """Get all available USDT-denominated perpetual swaps from OKX."""
        url = f"{self.base_url}/api/v5/public/instruments"
        params = {"instType": "SWAP"}
        data = self._request("GET", url, params)
        if not data or data.get("code") != "0":
            logger.error(f"[{self.name}] Failed to fetch instruments: {data}")
            return []

        symbols = []
        for item in data.get("data", []):
            inst_id = item.get('instId', '')
            # Filter for USDT-denominated: like "BTC-USDT-SWAP"
            if inst_id.endswith('-USDT-SWAP'):
                base = inst_id.replace('-USDT-SWAP', '')
                symbols.append(base)

        logger.info(f"[{self.name}] Found {len(symbols)} perpetual markets")
        return sorted(symbols)

    def fetch_funding_history(self, symbol: str, start_time: Optional[int] = None,
                              end_time: Optional[int] = None, limit: int = 100) -> List[dict]:
        """Fetch from /api/v5/public/funding-rate-history"""
        inst_id = self.get_symbol_name(symbol)
        url = f"{self.base_url}/api/v5/public/funding-rate-history"
        params = {"instId": inst_id, "limit": str(min(limit, 100))}
        if end_time:
            params["before"] = str(end_time)

        data = self._request("GET", url, params)
        if not data or data.get("code") != "0":
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
