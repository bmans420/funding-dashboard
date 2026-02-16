"""Hyperliquid funding rate adapter."""

import logging
import time
from typing import List, Optional
from .base import BaseExchangeAdapter

logger = logging.getLogger(__name__)


class HyperliquidAdapter(BaseExchangeAdapter):
    name = "hyperliquid"
    funding_interval_hours = 1.0
    base_url = "https://api.hyperliquid.xyz"

    def get_symbol_name(self, base: str) -> str:
        return base  # Hyperliquid uses plain symbols

    def get_available_symbols(self) -> List[str]:
        """Get all available perpetual symbols from Hyperliquid."""
        url = f"{self.base_url}/info"
        try:
            resp = self.session.post(url, json={"type": "meta"}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"[{self.name}] Failed to fetch meta: {e}")
            return []

        universe = data.get('universe', [])
        symbols = [item['name'] for item in universe if 'name' in item]
        logger.info(f"[{self.name}] Found {len(symbols)} perpetual markets")
        return sorted(symbols)

    def fetch_funding_history(self, symbol: str, start_time: Optional[int] = None,
                              end_time: Optional[int] = None, limit: int = 500) -> List[dict]:
        """Fetch from Hyperliquid info API."""
        url = f"{self.base_url}/info"

        payload = {
            "type": "fundingHistory",
            "coin": self.get_symbol_name(symbol),
            "startTime": start_time or int((time.time() - 86400 * 365) * 1000),
        }
        if end_time:
            payload["endTime"] = end_time

        try:
            logger.info(f"[{self.name}] POST {url} payload={payload}")
            resp = self.session.post(url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"[{self.name}] Request error: {e}")
            return []

        if not isinstance(data, list):
            logger.error(f"[{self.name}] Unexpected response: {type(data)}")
            return []

        records = []
        for item in data:
            funding_rate = float(item.get("fundingRate", 0))
            ft = item.get("time")
            if isinstance(ft, str):
                from datetime import datetime
                try:
                    dt = datetime.fromisoformat(ft.replace("Z", "+00:00"))
                    funding_time = int(dt.timestamp() * 1000)
                except:
                    continue
            else:
                funding_time = int(ft)

            rec = self._make_record(
                symbol=symbol,
                funding_rate=funding_rate,
                funding_time=funding_time,
            )
            if rec:
                records.append(rec)
        return records
