"""Orchestrates data collection from all exchanges."""

import time
import logging
import yaml
from typing import List, Optional, Set
from db.database import Database
from exchanges import EXCHANGE_MAP, get_hip3_adapters

logger = logging.getLogger(__name__)


class Collector:
    def __init__(self, config_path: str = "config.yaml", db: Optional[Database] = None):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.db = db or Database()
        # symbols from config is now optional — used as filter if present
        self._config_symbols = self.config.get('symbols', None)
        # Lazily populated HIP3 adapters
        self._hip3_adapters: dict = {}
        self._hip3_discovered = False

    @property
    def symbols(self) -> List[str]:
        """Backward compat: return config symbols or empty list."""
        return self._config_symbols or []

    def get_enabled_exchanges(self) -> List[str]:
        enabled = []
        for name, conf in self.config.get('exchanges', {}).items():
            if conf.get('enabled', False) and name in EXCHANGE_MAP:
                enabled.append(name)
        return enabled

    def discover_all_symbols(self) -> List[str]:
        """Query all enabled exchanges and return the union of all available symbols."""
        exchanges = self.get_enabled_exchanges()
        all_symbols: Set[str] = set()

        for exchange_name in exchanges:
            try:
                adapter = EXCHANGE_MAP[exchange_name]()
                syms = adapter.get_available_symbols()
                logger.info(f"[{exchange_name}] Discovered {len(syms)} symbols")
                all_symbols.update(syms)
            except Exception as e:
                logger.error(f"[{exchange_name}] Failed to discover symbols: {e}")

        # Also discover HIP3 symbols
        for name, adapter in self._hip3_adapters.items():
            try:
                syms = adapter.get_available_symbols()
                logger.info(f"[{name}] Discovered {len(syms)} symbols")
                all_symbols.update(syms)
            except Exception as e:
                logger.error(f"[{name}] Failed to discover symbols: {e}")

        result = sorted(all_symbols)
        logger.info(f"Total unique symbols across all exchanges: {len(result)}")
        return result

    def collect_all(self, symbols: Optional[List[str]] = None,
                    start_time: Optional[int] = None, days_back: int = 30,
                    discover: bool = False):
        """Collect funding rates for all enabled exchanges and symbols.

        If discover=True, dynamically fetch all available symbols from exchanges.
        If symbols is provided, use that list.
        Otherwise fall back to config symbols.
        """
        if discover:
            symbols = self.discover_all_symbols()
        elif symbols is None:
            symbols = self.symbols

        if not symbols:
            logger.warning("No symbols to collect. Use discover=True or provide symbols.")
            return

        exchanges = self.get_enabled_exchanges()

        if not start_time:
            start_time = int((time.time() - 86400 * days_back) * 1000)

        end_time = int(time.time() * 1000)

        logger.info(f"Collecting {len(symbols)} symbols from {len(exchanges)} exchanges, "
                     f"days_back={days_back}")

        for exchange_name in exchanges:
            adapter = EXCHANGE_MAP[exchange_name]()
            for symbol in symbols:
                self._collect_exchange_symbol(adapter, symbol, start_time, end_time)

        # Collect HIP3 deployer markets
        if discover and not self._hip3_discovered:
            self._hip3_adapters = get_hip3_adapters()
            self._hip3_discovered = True

        for name, adapter in self._hip3_adapters.items():
            hip3_symbols = adapter.get_available_symbols()
            for symbol in hip3_symbols:
                self._collect_exchange_symbol(adapter, symbol, start_time, end_time)

    def _collect_exchange_symbol(self, adapter, symbol: str,
                                 start_time: int, end_time: int):
        """Collect data for one exchange/symbol pair with pagination."""
        logger.info(f"Collecting {symbol} from {adapter.name}")

        # Check if we already have data
        latest = self.db.get_latest_funding_time(adapter.name, symbol)
        if latest and latest > start_time:
            start_time = latest + 1  # Don't re-fetch

        all_records = []
        current_start = start_time
        max_iterations = 500

        for _ in range(max_iterations):
            try:
                records = adapter.fetch_funding_history(
                    symbol=symbol,
                    start_time=current_start,
                    end_time=end_time,
                )
            except Exception as e:
                logger.error(f"[{adapter.name}] Error fetching {symbol}: {e}")
                self.db.log_fetch(adapter.name, symbol, "fetch_funding_history",
                                  "error", error=str(e))
                break

            if not records:
                break

            all_records.extend(records)

            # Paginate forward
            last_time = max(r['funding_time'] for r in records)
            if last_time >= end_time - 1000 or last_time <= current_start:
                break
            current_start = last_time + 1

            time.sleep(0.2)

        if all_records:
            self.db.insert_funding_rates(all_records)
            self.db.log_fetch(adapter.name, symbol, "fetch_funding_history",
                              "success", records_fetched=len(all_records))
            logger.info(f"[{adapter.name}] {symbol}: inserted {len(all_records)} records")
        else:
            self.db.log_fetch(adapter.name, symbol, "fetch_funding_history",
                              "empty", records_fetched=0)
            logger.info(f"[{adapter.name}] {symbol}: no new records")

    def update(self, symbols: Optional[List[str]] = None, days_back: int = 2,
               discover: bool = False):
        """Incremental update - fetch recent data."""
        self.collect_all(symbols=symbols, days_back=days_back, discover=discover)
