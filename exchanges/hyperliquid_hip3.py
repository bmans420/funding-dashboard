"""Hyperliquid HIP3 deployer perp adapters.

Each HIP3 deployer (xyz, km, cash, etc.) becomes a separate exchange
so the dashboard shows one column per deployer.
"""

import logging
import time
from typing import List, Optional, Dict
from .base import BaseExchangeAdapter

logger = logging.getLogger(__name__)

HIP3_API = "https://api.hyperliquid.xyz"


def discover_hip3_deployers() -> List[Dict]:
    """Fetch all HIP3 perp deployers from the Hyperliquid API.
    Returns list of dicts with keys: name, fullName, deployer, markets.
    """
    import requests
    s = requests.Session()
    s.headers.update({'User-Agent': 'FundingDashboard/1.0'})

    try:
        resp = s.post(f"{HIP3_API}/info", json={"type": "perpDexs"}, timeout=30)
        resp.raise_for_status()
        dexs = resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch perpDexs: {e}")
        return []

    deployers = []
    for entry in dexs:
        if entry is None:
            continue  # skip main dex
        name = entry.get('name', '')
        if not name:
            continue
        # Fetch universe for this deployer
        try:
            resp2 = s.post(f"{HIP3_API}/info", json={"type": "meta", "dex": name}, timeout=15)
            resp2.raise_for_status()
            meta = resp2.json()
            universe = meta.get('universe', [])
            markets = []
            for u in universe:
                mkt_name = u.get('name', '')
                if ':' in mkt_name:
                    # "xyz:TSLA" -> asset = "TSLA"
                    asset = mkt_name.split(':', 1)[1]
                else:
                    asset = mkt_name
                if not u.get('isDelisted', False):
                    markets.append({'coin': mkt_name, 'asset': asset})
        except Exception as e:
            logger.error(f"Failed to fetch meta for dex={name}: {e}")
            markets = []

        deployers.append({
            'name': name,
            'fullName': entry.get('fullName', ''),
            'deployer': entry.get('deployer', ''),
            'markets': markets,
        })
        logger.info(f"HIP3 deployer '{name}' ({entry.get('fullName', '')}): {len(markets)} active markets")

    return deployers


class HIP3Adapter(BaseExchangeAdapter):
    """Adapter for a single HIP3 deployer's perp markets."""

    funding_interval_hours = 1.0
    base_url = HIP3_API

    def __init__(self, deployer_name: str, markets: Optional[List[Dict]] = None):
        super().__init__()
        self.deployer_name = deployer_name
        self.name = f"hl-{deployer_name}"
        # markets: list of {'coin': 'xyz:TSLA', 'asset': 'TSLA'}
        self._markets = markets or []

    def get_symbol_name(self, base: str) -> str:
        """Convert clean symbol to deployer:asset format for API calls."""
        return f"{self.deployer_name}:{base}"

    def get_available_symbols(self) -> List[str]:
        """Return clean asset names (TSLA, NVDA, etc.)."""
        if self._markets:
            return sorted(set(m['asset'] for m in self._markets))

        # Fetch dynamically
        try:
            resp = self.session.post(f"{self.base_url}/info",
                                     json={"type": "meta", "dex": self.deployer_name},
                                     timeout=30)
            resp.raise_for_status()
            meta = resp.json()
        except Exception as e:
            logger.error(f"[{self.name}] Failed to fetch meta: {e}")
            return []

        universe = meta.get('universe', [])
        symbols = []
        for u in universe:
            if u.get('isDelisted', False):
                continue
            mkt_name = u.get('name', '')
            asset = mkt_name.split(':', 1)[1] if ':' in mkt_name else mkt_name
            symbols.append(asset)

        logger.info(f"[{self.name}] Found {len(symbols)} markets")
        return sorted(set(symbols))

    def fetch_funding_history(self, symbol: str, start_time: Optional[int] = None,
                              end_time: Optional[int] = None, limit: int = 500) -> List[dict]:
        """Fetch funding history using deployer:asset coin format."""
        url = f"{self.base_url}/info"
        coin = self.get_symbol_name(symbol)

        payload = {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_time or int((time.time() - 86400 * 365) * 1000),
        }
        if end_time:
            payload["endTime"] = end_time

        try:
            logger.info(f"[{self.name}] POST {url} coin={coin}")
            resp = self.session.post(url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"[{self.name}] Request error for {coin}: {e}")
            return []

        if not isinstance(data, list):
            logger.error(f"[{self.name}] Unexpected response for {coin}: {type(data)}")
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
