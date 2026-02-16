"""Abstract base class for exchange adapters."""

import time
import logging
import requests
from abc import ABC, abstractmethod
from typing import List, Optional

logger = logging.getLogger(__name__)


class BaseExchangeAdapter(ABC):
    name: str = ""
    funding_interval_hours: float = 8.0
    base_url: str = ""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'FundingDashboard/1.0'})

    @abstractmethod
    def fetch_funding_history(self, symbol: str, start_time: Optional[int] = None,
                              end_time: Optional[int] = None, limit: int = 1000) -> List[dict]:
        """Fetch historical funding rates. Returns list of dicts with keys:
        exchange, symbol, funding_rate, funding_time, interval_hours"""
        pass

    @abstractmethod
    def get_symbol_name(self, base: str) -> str:
        """Convert base symbol (BTC) to exchange-specific format (BTCUSDT)."""
        pass

    @abstractmethod
    def get_available_symbols(self) -> List[str]:
        """Query the exchange API for ALL available perpetual market symbols.
        Returns list of normalized base symbols (e.g., ['BTC', 'ETH', 'SOL'])."""
        pass

    def _request(self, method: str, url: str, params: dict = None,
                 max_retries: int = 3) -> Optional[dict]:
        """HTTP request with exponential backoff."""
        for attempt in range(max_retries):
            try:
                logger.info(f"[{self.name}] {method} {url} params={params}")
                resp = self.session.request(method, url, params=params, timeout=30)
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"[{self.name}] Rate limited, waiting {wait}s")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                wait = 2 ** attempt
                logger.error(f"[{self.name}] Request error (attempt {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(wait)
        return None

    def validate_rate(self, rate: float) -> bool:
        """Validate funding rate is within reasonable range."""
        return -0.01 <= rate <= 0.01  # -1% to +1%

    def _make_record(self, symbol: str, funding_rate: float,
                     funding_time: int) -> Optional[dict]:
        if not self.validate_rate(funding_rate):
            logger.warning(f"[{self.name}] Rate {funding_rate} for {symbol} out of range, skipping")
            return None
        return {
            'exchange': self.name,
            'symbol': symbol,
            'funding_rate': funding_rate,
            'funding_time': funding_time,
            'interval_hours': self.funding_interval_hours,
        }
