"""Supabase database operations using REST API."""

import os
import time
from typing import List, Dict, Optional, Any
from supabase import create_client, Client
import streamlit as st


class Database:
    def __init__(self, supabase_url: str = None, supabase_key: str = None):
        """Initialize Supabase client.
        
        Args:
            supabase_url: Supabase project URL. If None, reads from SUPABASE_URL env var 
                         or st.secrets["supabase"]["url"]
            supabase_key: Supabase anon key. If None, reads from SUPABASE_KEY env var
                         or st.secrets["supabase"]["key"]
        """
        # Get Supabase URL
        if supabase_url:
            self.supabase_url = supabase_url
        else:
            self.supabase_url = os.getenv('SUPABASE_URL')
            if not self.supabase_url:
                try:
                    self.supabase_url = st.secrets["supabase"]["url"]
                except (KeyError, AttributeError):
                    raise ValueError(
                        "SUPABASE_URL environment variable or st.secrets supabase.url is required"
                    )
        
        # Get Supabase Key
        if supabase_key:
            self.supabase_key = supabase_key
        else:
            self.supabase_key = os.getenv('SUPABASE_KEY')
            if not self.supabase_key:
                try:
                    self.supabase_key = st.secrets["supabase"]["key"]
                except (KeyError, AttributeError):
                    raise ValueError(
                        "SUPABASE_KEY environment variable or st.secrets supabase.key is required"
                    )
        
        # Create Supabase client
        self.client: Client = create_client(self.supabase_url, self.supabase_key)

    def insert_funding_rates(self, records: List[dict]) -> int:
        """Insert funding rate records using upsert for conflict handling."""
        if not records:
            return 0
        
        inserted = 0
        # Process in batches of 500 (Supabase limit)
        for i in range(0, len(records), 500):
            batch = records[i:i + 500]
            # Add fetched_at timestamp if not present
            for record in batch:
                if 'fetched_at' not in record:
                    record['fetched_at'] = int(time.time())
            
            try:
                response = self.client.table('funding_rates').upsert(
                    batch,
                    on_conflict='exchange,symbol,funding_time'
                ).execute()
                if hasattr(response, 'data') and response.data:
                    inserted += len(response.data)
            except Exception as e:
                # Supabase upsert doesn't provide row count for conflicts, 
                # so we estimate based on batch size
                print(f"Warning: batch upsert had issues: {e}")
                pass
        
        return inserted

    def log_fetch(self, exchange: str, symbol: str, endpoint: str,
                  status: str, records_fetched: int = 0, error: str = None):
        """Log a fetch operation."""
        self.client.table('fetch_log').insert({
            'exchange': exchange,
            'symbol': symbol,
            'endpoint': endpoint,
            'status': status,
            'records_fetched': records_fetched,
            'error_message': error,
            'timestamp': int(time.time())
        }).execute()

    def get_funding_rates(self, symbol: str, exchange: str,
                          start_time: int, end_time: int) -> List[Dict]:
        """Get funding rates for a symbol/exchange in time range."""
        response = self.client.table('funding_rates').select(
            'funding_rate', 'funding_time', 'interval_hours'
        ).eq('symbol', symbol).eq('exchange', exchange).gte(
            'funding_time', start_time
        ).lte('funding_time', end_time).order('funding_time', desc=False).execute()
        
        return response.data if response.data else []

    def get_latest_funding_time(self, exchange: str, symbol: str) -> Optional[int]:
        """Get the latest funding time for an exchange/symbol."""
        response = self.client.table('funding_rates').select(
            'funding_time'
        ).eq('exchange', exchange).eq('symbol', symbol).order(
            'funding_time', desc=True
        ).limit(1).execute()
        
        if response.data and len(response.data) > 0:
            return response.data[0]['funding_time']
        return None

    def get_available_symbols(self) -> List[str]:
        """Get all distinct symbols from funding_rates using RPC."""
        response = self.client.rpc('get_distinct_symbols').execute()
        if response.data:
            return [row['symbol'] for row in response.data]
        return []

    def get_exchanges_for_symbol(self, symbol: str) -> List[str]:
        """Get all exchanges that have data for a symbol using RPC."""
        response = self.client.rpc('get_exchanges_for_symbol', {
            'p_symbol': symbol
        }).execute()
        if response.data:
            return [row['exchange'] for row in response.data]
        return []

    def get_reference_timestamps(self, symbol: str, start_time: int, end_time: int) -> dict:
        """Get the exchange with longest interval and its timestamps using RPC."""
        response = self.client.rpc('get_reference_timestamps', {
            'p_symbol': symbol,
            'p_start': start_time,
            'p_end': end_time
        }).execute()
        
        if response.data:
            # RPC returns JSON, so we parse it
            result = response.data
            return {
                'exchange': result.get('exchange'),
                'interval_hours': result.get('interval_hours'),
                'timestamps': result.get('timestamps', [])
            }
        return {}

    def get_exchange_status(self) -> List[Dict[str, Any]]:
        """Get per-exchange update status using RPC."""
        response = self.client.rpc('get_exchange_status').execute()
        if response.data:
            return response.data
        return []

    def get_total_records(self) -> int:
        """Get total number of funding rate records."""
        response = self.client.table('funding_rates').select(
            'id', count='exact'
        ).limit(0).execute()
        return response.count if response.count is not None else 0

    def get_last_update_time(self) -> Optional[int]:
        """Get the most recent fetched_at timestamp."""
        response = self.client.table('funding_rates').select(
            'fetched_at'
        ).order('fetched_at', desc=True).limit(1).execute()
        
        if response.data and len(response.data) > 0:
            return response.data[0]['fetched_at']
        return None

    # OI Data methods
    def insert_oi_data(self, oi_records: List[dict]) -> int:
        """Insert OI data records using upsert."""
        if not oi_records:
            return 0
        
        try:
            response = self.client.table('oi_data').upsert(
                oi_records,
                on_conflict='symbol'
            ).execute()
            return len(response.data) if response.data else 0
        except Exception:
            return 0

    def get_latest_oi_data(self, limit: int = 10) -> List[dict]:
        """Get latest OI data, ordered by OI value - only latest timestamp."""
        # First get the latest timestamp
        latest_response = self.client.table('oi_data').select(
            'timestamp'
        ).order('timestamp', desc=True).limit(1).execute()
        
        if not latest_response.data:
            return []
        
        latest_timestamp = latest_response.data[0]['timestamp']
        
        # Get records for that timestamp, ordered by oi_usd desc
        response = self.client.table('oi_data').select(
            'symbol', 'oi_usd', 'timestamp'
        ).eq('timestamp', latest_timestamp).order(
            'oi_usd', desc=True
        ).limit(limit).execute()
        
        return response.data if response.data else []

    def get_oi_symbols_map(self) -> Dict[str, int]:
        """Get symbol -> rank mapping for OI data."""
        # Get all OI data for latest timestamp, ordered by oi_usd desc
        latest_oi = self.get_latest_oi_data(1000)  # Get all symbols
        
        # Create rank mapping (1-indexed)
        return {symbol['symbol']: rank + 1 for rank, symbol in enumerate(latest_oi)}

    # Helper methods for scripts/update.py
    def get_distinct_funding_times(self, exchange: str, limit: int) -> List[int]:
        """Get distinct funding times for an exchange, most recent first."""
        response = self.client.table('funding_rates').select(
            'funding_time'
        ).eq('exchange', exchange).order(
            'funding_time', desc=True
        ).limit(limit).execute()
        
        if response.data:
            # Remove duplicates while preserving order
            seen = set()
            unique_times = []
            for row in response.data:
                time_val = row['funding_time']
                if time_val not in seen:
                    seen.add(time_val)
                    unique_times.append(time_val)
            return unique_times
        return []

    def get_symbols_for_exchange(self, exchange: str) -> List[str]:
        """Get distinct symbols for an exchange."""
        response = self.client.table('funding_rates').select(
            'symbol'
        ).eq('exchange', exchange).execute()
        
        if response.data:
            # Deduplicate in Python since Supabase doesn't have DISTINCT in select
            symbols = set(row['symbol'] for row in response.data)
            return sorted(list(symbols))
        return []