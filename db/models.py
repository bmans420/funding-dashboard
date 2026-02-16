"""Database schema and setup instructions for Supabase.

Since the Supabase REST API cannot run CREATE TABLE statements directly,
you must set up the database schema manually through the Supabase SQL Editor.

SETUP INSTRUCTIONS:
1. Go to your Supabase project dashboard
2. Navigate to SQL Editor
3. Create a new query and paste the entire SQL below
4. Run the query to create all tables and functions

This setup creates:
- Tables: funding_rates, fetch_log, oi_data
- Indexes for performance
- RPC functions for complex queries
- Proper constraints and relationships
"""

SETUP_SQL = """
-- ======================================================================
-- FUNDING DASHBOARD DATABASE SCHEMA
-- ======================================================================
-- Run this entire script in your Supabase SQL Editor

-- Table for funding rate data
CREATE TABLE IF NOT EXISTS funding_rates (
    id SERIAL PRIMARY KEY,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    funding_rate DOUBLE PRECISION NOT NULL,
    funding_time BIGINT NOT NULL,
    interval_hours DOUBLE PRECISION NOT NULL,
    fetched_at BIGINT NOT NULL,
    CONSTRAINT unique_funding_record UNIQUE(exchange, symbol, funding_time)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_funding_exchange_symbol_time
    ON funding_rates(exchange, symbol, funding_time);

CREATE INDEX IF NOT EXISTS idx_funding_symbol_time
    ON funding_rates(symbol, funding_time);

CREATE INDEX IF NOT EXISTS idx_funding_fetched_at
    ON funding_rates(fetched_at DESC);

-- Table for fetch operation logs
CREATE TABLE IF NOT EXISTS fetch_log (
    id SERIAL PRIMARY KEY,
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    endpoint TEXT NOT NULL,
    status VARCHAR(20) NOT NULL,
    records_fetched INTEGER DEFAULT 0,
    error_message TEXT,
    timestamp BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fetch_log_timestamp
    ON fetch_log(timestamp DESC);

-- Table for Open Interest data
CREATE TABLE IF NOT EXISTS oi_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    oi_usd DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_oi_symbol_timestamp UNIQUE(symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_oi_symbol_timestamp
    ON oi_data(symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_oi_timestamp_oi_usd
    ON oi_data(timestamp DESC, oi_usd DESC);

-- ======================================================================
-- RPC FUNCTIONS FOR COMPLEX QUERIES
-- ======================================================================

-- Get distinct symbols for the symbols dropdown
CREATE OR REPLACE FUNCTION get_distinct_symbols()
RETURNS TABLE(symbol TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT fr.symbol::TEXT
    FROM funding_rates fr 
    ORDER BY fr.symbol;
END;
$$ LANGUAGE plpgsql STABLE;

-- Get exchanges that have data for a specific symbol
CREATE OR REPLACE FUNCTION get_exchanges_for_symbol(p_symbol TEXT)
RETURNS TABLE(exchange TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT fr.exchange::TEXT
    FROM funding_rates fr 
    WHERE fr.symbol = p_symbol 
    ORDER BY fr.exchange;
END;
$$ LANGUAGE plpgsql STABLE;

-- Get reference timestamps for data normalization
-- This finds the exchange with the longest interval for a symbol and returns its timestamps
CREATE OR REPLACE FUNCTION get_reference_timestamps(p_symbol TEXT, p_start BIGINT, p_end BIGINT)
RETURNS JSON AS $$
DECLARE
    result JSON;
BEGIN
    WITH ref AS (
        SELECT exchange, interval_hours
        FROM funding_rates 
        WHERE symbol = p_symbol
        GROUP BY exchange, interval_hours
        ORDER BY interval_hours DESC 
        LIMIT 1
    ),
    ts AS (
        SELECT f.funding_time
        FROM funding_rates f 
        JOIN ref r ON f.exchange = r.exchange
        WHERE f.symbol = p_symbol 
          AND f.funding_time >= p_start 
          AND f.funding_time <= p_end
        ORDER BY f.funding_time ASC
    )
    SELECT json_build_object(
        'exchange', (SELECT exchange FROM ref),
        'interval_hours', (SELECT interval_hours FROM ref),
        'timestamps', COALESCE((SELECT json_agg(funding_time) FROM ts), '[]'::json)
    ) INTO result;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql STABLE;

-- Get comprehensive exchange status for the dashboard
-- Returns health status, update times, and record counts per exchange
CREATE OR REPLACE FUNCTION get_exchange_status()
RETURNS JSON AS $$
DECLARE
    result JSON;
BEGIN
    WITH now_ms AS (
        SELECT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT as val
    ),
    exchange_stats AS (
        SELECT 
            exchange,
            MAX(funding_time) as last_update_ts,
            COUNT(CASE WHEN funding_time > (SELECT val FROM now_ms) - 3600000 THEN 1 END) as records_last_hour,
            COUNT(CASE WHEN funding_time > (SELECT val FROM now_ms) - 86400000 THEN 1 END) as records_last_day
        FROM funding_rates
        GROUP BY exchange
    ),
    intervals AS (
        SELECT DISTINCT ON (exchange) 
            exchange, 
            (
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY diff)
                FROM (
                    SELECT funding_time - LAG(funding_time) OVER (ORDER BY funding_time) as diff
                    FROM (
                        SELECT DISTINCT funding_time 
                        FROM funding_rates f2 
                        WHERE f2.exchange = e.exchange 
                        ORDER BY funding_time DESC 
                        LIMIT 11
                    ) sub
                ) diffs
                WHERE diff IS NOT NULL AND diff > 0
            ) as typical_interval_ms
        FROM exchange_stats e
    )
    SELECT COALESCE(
        json_agg(
            json_build_object(
                'exchange', es.exchange,
                'last_update_ts', es.last_update_ts,
                'records_last_hour', es.records_last_hour,
                'records_last_day', es.records_last_day,
                'typical_interval_ms', COALESCE(i.typical_interval_ms, 28800000),
                'age_ms', (SELECT val FROM now_ms) - COALESCE(es.last_update_ts, 0),
                'status', CASE
                    WHEN es.last_update_ts IS NULL OR ((SELECT val FROM now_ms) - es.last_update_ts) > 86400000 THEN 'failed'
                    WHEN ((SELECT val FROM now_ms) - es.last_update_ts) > 2 * COALESCE(i.typical_interval_ms, 28800000) THEN 'stale'
                    ELSE 'current'
                END
            ) ORDER BY es.last_update_ts DESC NULLS LAST
        ), 
        '[]'::json
    ) INTO result
    FROM exchange_stats es
    LEFT JOIN intervals i ON es.exchange = i.exchange;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql STABLE;

-- Get stock symbols (symbols traded on Hyperliquid stock exchanges)
CREATE OR REPLACE FUNCTION get_stock_symbols()
RETURNS TABLE(symbol TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT fr.symbol::TEXT
    FROM funding_rates fr 
    WHERE fr.exchange IN ('hl-xyz', 'hl-cash') 
    ORDER BY fr.symbol;
END;
$$ LANGUAGE plpgsql STABLE;

-- ======================================================================
-- SETUP COMPLETE
-- ======================================================================
-- Your database is now ready for the funding dashboard!
-- 
-- Next steps:
-- 1. Set SUPABASE_URL and SUPABASE_KEY environment variables
-- 2. Run the application: streamlit run app.py
-- 3. Set up GitHub Actions with the new environment variables
"""

# For backwards compatibility, provide the SQL as a constant
SCHEMA = SETUP_SQL