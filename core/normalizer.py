"""Time normalization logic for cross-exchange funding rate comparison."""

import logging
from typing import List, Dict, Optional
from db.database import Database

logger = logging.getLogger(__name__)


class TimeNormalizer:
    """Normalizes funding rates across exchanges with different intervals.
    
    Uses the longest-interval exchange as reference and aligns all others
    to cover the exact same time periods.
    """

    def __init__(self, db: Database):
        self.db = db

    def get_normalized_rates(self, symbol: str, start_time_ms: int,
                             end_time_ms: int) -> Dict[str, dict]:
        """Get normalized funding rate sums for all exchanges.
        
        Returns dict: {exchange: {rate_sum, count, start, end, apr}}
        """
        exchanges = self.db.get_exchanges_for_symbol(symbol)
        if not exchanges:
            return {}

        # Find reference exchange (longest interval)
        ref_info = self.db.get_reference_timestamps(symbol, start_time_ms, end_time_ms)
        if not ref_info or not ref_info['timestamps']:
            # Fallback: just sum whatever we have per exchange
            return self._simple_sum(symbol, exchanges, start_time_ms, end_time_ms)

        ref_exchange = ref_info['exchange']
        ref_timestamps = ref_info['timestamps']

        if len(ref_timestamps) < 1:
            return self._simple_sum(symbol, exchanges, start_time_ms, end_time_ms)

        # The actual period covered by reference exchange
        actual_start = ref_timestamps[0]
        actual_end = ref_timestamps[-1]

        results = {}
        for exchange in exchanges:
            rates = self.db.get_funding_rates(symbol, exchange, actual_start, end_time_ms)
            if not rates:
                continue
            rate_sum = sum(r['funding_rate'] for r in rates)
            results[exchange] = {
                'rate_sum': rate_sum,
                'count': len(rates),
                'start': actual_start,
                'end': actual_end,
                'interval_hours': rates[0]['interval_hours'] if rates else 8,
            }

        return results

    def _simple_sum(self, symbol: str, exchanges: List[str],
                    start_time: int, end_time: int) -> Dict[str, dict]:
        results = {}
        for exchange in exchanges:
            rates = self.db.get_funding_rates(symbol, exchange, start_time, end_time)
            if not rates:
                continue
            rate_sum = sum(r['funding_rate'] for r in rates)
            results[exchange] = {
                'rate_sum': rate_sum,
                'count': len(rates),
                'start': start_time,
                'end': end_time,
                'interval_hours': rates[0]['interval_hours'] if rates else 8,
            }
        return results
