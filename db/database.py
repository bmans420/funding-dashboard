"""Database operations using direct PostgreSQL connection via psycopg2."""

import os
import time
from typing import List, Dict, Optional, Any
from contextlib import contextmanager

import psycopg2
import psycopg2.extras


class Database:
    def __init__(self, database_url: str = None):
        """Initialize PostgreSQL connection.

        Args:
            database_url: PostgreSQL connection string.
                If None, reads from DATABASE_URL env var,
                then falls back to Streamlit secrets.
        """
        if database_url:
            self.database_url = database_url
        else:
            self.database_url = os.getenv('DATABASE_URL')
            if not self.database_url:
                try:
                    import streamlit as st
                    self.database_url = st.secrets["database"]["url"]
                except Exception:
                    raise ValueError(
                        "DATABASE_URL environment variable or st.secrets database.url is required"
                    )

    @contextmanager
    def _conn(self):
        """Context manager for a database connection."""
        conn = psycopg2.connect(self.database_url)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def insert_funding_rates(self, records: List[dict]) -> int:
        """Insert funding rate records using upsert for conflict handling."""
        if not records:
            return 0

        for record in records:
            if 'fetched_at' not in record:
                record['fetched_at'] = int(time.time())

        sql = """
            INSERT INTO funding_rates (exchange, symbol, funding_rate, funding_time, interval_hours, fetched_at)
            VALUES (%(exchange)s, %(symbol)s, %(funding_rate)s, %(funding_time)s, %(interval_hours)s, %(fetched_at)s)
            ON CONFLICT (exchange, symbol, funding_time) DO UPDATE SET
                funding_rate = EXCLUDED.funding_rate,
                interval_hours = EXCLUDED.interval_hours,
                fetched_at = EXCLUDED.fetched_at
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, records, page_size=500)
                return len(records)

    def log_fetch(self, exchange: str, symbol: str, endpoint: str,
                  status: str, records_fetched: int = 0, error: str = None):
        """Log a fetch operation."""
        sql = """
            INSERT INTO fetch_log (exchange, symbol, endpoint, status, records_fetched, error_message, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange, symbol, endpoint, status, records_fetched, error, int(time.time())))

    def get_funding_rates(self, symbol: str, exchange: str,
                          start_time: int, end_time: int) -> List[Dict]:
        """Get funding rates for a symbol/exchange in time range."""
        sql = """
            SELECT funding_rate, funding_time, interval_hours
            FROM funding_rates
            WHERE symbol = %s AND exchange = %s AND funding_time >= %s AND funding_time <= %s
            ORDER BY funding_time ASC
        """
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (symbol, exchange, start_time, end_time))
                return cur.fetchall()

    def get_latest_funding_time(self, exchange: str, symbol: str) -> Optional[int]:
        """Get the latest funding time for an exchange/symbol."""
        sql = """
            SELECT funding_time FROM funding_rates
            WHERE exchange = %s AND symbol = %s
            ORDER BY funding_time DESC LIMIT 1
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange, symbol))
                row = cur.fetchone()
                return row[0] if row else None

    def get_available_symbols(self) -> List[str]:
        """Get all distinct symbols from funding_rates."""
        sql = "SELECT DISTINCT symbol FROM funding_rates ORDER BY symbol"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return [row[0] for row in cur.fetchall()]

    def get_exchanges_for_symbol(self, symbol: str) -> List[str]:
        """Get all exchanges that have data for a symbol."""
        sql = "SELECT DISTINCT exchange FROM funding_rates WHERE symbol = %s ORDER BY exchange"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (symbol,))
                return [row[0] for row in cur.fetchall()]

    def get_reference_timestamps(self, symbol: str, start_time: int, end_time: int) -> dict:
        """Get the exchange with longest interval and its timestamps."""
        sql = """
            WITH ref AS (
                SELECT exchange, interval_hours
                FROM funding_rates
                WHERE symbol = %s
                GROUP BY exchange, interval_hours
                ORDER BY interval_hours DESC
                LIMIT 1
            ),
            ts AS (
                SELECT f.funding_time
                FROM funding_rates f
                JOIN ref r ON f.exchange = r.exchange
                WHERE f.symbol = %s AND f.funding_time >= %s AND f.funding_time <= %s
                ORDER BY f.funding_time ASC
            )
            SELECT
                (SELECT exchange FROM ref),
                (SELECT interval_hours FROM ref),
                COALESCE(array_agg(funding_time) FILTER (WHERE funding_time IS NOT NULL), ARRAY[]::bigint[])
            FROM ts
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (symbol, symbol, start_time, end_time))
                row = cur.fetchone()
                if row and row[0]:
                    return {
                        'exchange': row[0],
                        'interval_hours': row[1],
                        'timestamps': list(row[2]) if row[2] else []
                    }
                return {}

    def get_exchange_status(self) -> List[Dict[str, Any]]:
        """Get per-exchange update status."""
        sql = "SELECT get_exchange_status()"
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()
                    if row and row[0]:
                        import json
                        result = row[0] if isinstance(row[0], list) else json.loads(row[0])
                        return result
        except Exception:
            pass
        # Fallback
        sql2 = """
            SELECT exchange, COUNT(DISTINCT symbol) as symbol_count,
                   MAX(funding_time) as latest_funding_time,
                   MAX(fetched_at) as latest_fetched_at,
                   COUNT(*) as record_count
            FROM funding_rates GROUP BY exchange
        """
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql2)
                return cur.fetchall()

    def get_total_records(self) -> int:
        """Get total number of funding rate records."""
        sql = "SELECT COUNT(*) FROM funding_rates"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchone()[0]

    def get_last_update_time(self) -> Optional[int]:
        """Get the most recent fetched_at timestamp."""
        sql = "SELECT fetched_at FROM funding_rates ORDER BY fetched_at DESC LIMIT 1"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                return row[0] if row else None

    def insert_oi_data(self, oi_records: List[dict]) -> int:
        """Insert OI data records using upsert."""
        if not oi_records:
            return 0
        sql = """
            INSERT INTO oi_data (symbol, oi_usd)
            VALUES (%(symbol)s, %(oi_usd)s)
            ON CONFLICT (symbol) DO UPDATE SET
                oi_usd = EXCLUDED.oi_usd,
                timestamp = NOW()
        """
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, sql, oi_records, page_size=500)
                    return len(oi_records)
        except Exception:
            return 0

    def get_latest_oi_data(self, limit: int = 10) -> List[dict]:
        """Get latest OI data ordered by OI value."""
        sql = """
            SELECT symbol, oi_usd, timestamp
            FROM oi_data
            WHERE timestamp = (SELECT MAX(timestamp) FROM oi_data)
            ORDER BY oi_usd DESC
            LIMIT %s
        """
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (limit,))
                return cur.fetchall()

    def get_oi_symbols_map(self) -> Dict[str, int]:
        """Get symbol -> rank mapping for OI data."""
        latest_oi = self.get_latest_oi_data(1000)
        return {row['symbol']: rank + 1 for rank, row in enumerate(latest_oi)}

    def get_distinct_funding_times(self, exchange: str, limit: int) -> List[int]:
        """Get distinct funding times for an exchange, most recent first."""
        sql = """
            SELECT DISTINCT funding_time FROM funding_rates
            WHERE exchange = %s ORDER BY funding_time DESC LIMIT %s
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange, limit))
                return [row[0] for row in cur.fetchall()]

    def get_symbols_for_exchange(self, exchange: str) -> List[str]:
        """Get distinct symbols for an exchange."""
        sql = "SELECT DISTINCT symbol FROM funding_rates WHERE exchange = %s ORDER BY symbol"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange,))
                return [row[0] for row in cur.fetchall()]
