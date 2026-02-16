#!/usr/bin/env python3
"""Migration script from SQLite to Supabase PostgreSQL via REST API.

Uses SUPABASE_URL and SUPABASE_KEY (from Supabase Dashboard → Settings → API).
No database password needed — works with the anon/service key.
"""

import os
import sys
import sqlite3
import time

try:
    from supabase import create_client
except ImportError:
    print("❌ Missing dependency: pip install supabase")
    sys.exit(1)


BATCH_SIZE = 500  # Supabase REST API batch limit


def main():
    print("=" * 60)
    print("SQLite → Supabase Migration (REST API)")
    print("=" * 60)

    # --- credentials ---
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("❌ Set SUPABASE_URL and SUPABASE_KEY environment variables")
        print("   Find them at: Supabase Dashboard → Settings → API")
        return 1

    # --- SQLite source ---
    sqlite_path = "funding_rates.db"
    if not os.path.exists(sqlite_path):
        print(f"❌ SQLite database not found at {sqlite_path}")
        return 1

    print(f"📂 Source: {sqlite_path}")
    print(f"🚀 Destination: {url}")
    print()

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row

    # --- connect to Supabase ---
    try:
        sb = create_client(url, key)
        print("✅ Connected to Supabase")
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return 1

    # --- count source records ---
    cur = sqlite_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM funding_rates")
    funding_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM fetch_log")
    log_count = cur.fetchone()[0]

    print(f"📊 SQLite contains:")
    print(f"   - {funding_count:,} funding_rates records")
    print(f"   - {log_count:,} fetch_log records")
    print()

    if funding_count == 0 and log_count == 0:
        print("⚠️  Nothing to migrate")
        return 0

    resp = input("Proceed? (y/N): ").strip().lower()
    if resp != "y":
        print("Cancelled")
        return 0

    print("\n🚀 Starting migration...\n")
    t0 = time.time()

    # ── NOTE ─────────────────────────────────────────────────────────
    # You must create the tables in Supabase BEFORE running this script.
    # Go to Supabase Dashboard → SQL Editor and copy/paste the entire
    # content of db/setup.sql from this project, then click Run.
    #
    # This will create all necessary tables, indexes, and RPC functions.
    # ─────────────────────────────────────────────────────────────────

    # --- migrate funding_rates ---
    print("📈 Migrating funding_rates...")
    cur.execute(
        "SELECT exchange, symbol, funding_rate, funding_time, interval_hours, fetched_at "
        "FROM funding_rates ORDER BY id"
    )
    migrated_funding = 0
    batch = []

    for row in cur:
        batch.append({
            "exchange": row["exchange"],
            "symbol": row["symbol"],
            "funding_rate": row["funding_rate"],
            "funding_time": row["funding_time"],
            "interval_hours": row["interval_hours"],
            "fetched_at": row["fetched_at"],
        })
        if len(batch) >= BATCH_SIZE:
            try:
                sb.table("funding_rates").upsert(
                    batch,
                    on_conflict="exchange,symbol,funding_time"
                ).execute()
                migrated_funding += len(batch)
                print(f"   ✅ {migrated_funding:,}/{funding_count:,}")
            except Exception as e:
                print(f"   ⚠️  Batch failed ({len(batch)} rows): {e}")
            batch = []

    if batch:
        try:
            sb.table("funding_rates").upsert(
                batch,
                on_conflict="exchange,symbol,funding_time"
            ).execute()
            migrated_funding += len(batch)
        except Exception as e:
            print(f"   ⚠️  Final batch failed: {e}")

    print(f"✅ Funding rates: {migrated_funding:,} records")

    # --- migrate fetch_log ---
    print("\n📝 Migrating fetch_log...")
    cur.execute(
        "SELECT exchange, symbol, endpoint, status, records_fetched, error_message, timestamp "
        "FROM fetch_log ORDER BY id"
    )
    migrated_logs = 0
    batch = []

    for row in cur:
        batch.append({
            "exchange": row["exchange"],
            "symbol": row["symbol"],
            "endpoint": row["endpoint"],
            "status": row["status"],
            "records_fetched": row["records_fetched"],
            "error_message": row["error_message"],
            "timestamp": row["timestamp"],
        })
        if len(batch) >= BATCH_SIZE:
            try:
                sb.table("fetch_log").insert(batch).execute()
                migrated_logs += len(batch)
                print(f"   ✅ {migrated_logs:,}/{log_count:,}")
            except Exception as e:
                print(f"   ⚠️  Batch failed: {e}")
            batch = []

    if batch:
        try:
            sb.table("fetch_log").insert(batch).execute()
            migrated_logs += len(batch)
        except Exception as e:
            print(f"   ⚠️  Final batch failed: {e}")

    print(f"✅ Fetch logs: {migrated_logs:,} records")

    # --- verify ---
    print("\n🔍 Verifying...")
    try:
        r = sb.table("funding_rates").select("id", count="exact").limit(1).execute()
        pg_funding = r.count or 0
        r = sb.table("fetch_log").select("id", count="exact").limit(1).execute()
        pg_logs = r.count or 0
        print(f"   Supabase funding_rates: {pg_funding:,}")
        print(f"   Supabase fetch_log:     {pg_logs:,}")
        if pg_funding >= migrated_funding:
            print("   ✅ Verification passed")
        else:
            print("   ⚠️  Some records may be missing")
    except Exception as e:
        print(f"   ⚠️  Verification failed: {e}")

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"MIGRATION COMPLETE")
    print(f"{'=' * 60}")
    print(f"📈 Funding rates: {migrated_funding:,}")
    print(f"📝 Fetch logs:    {migrated_logs:,}")
    print(f"⏱️  Time: {elapsed:.1f}s")
    print()
    print("Next steps:")
    print("  1. Verify db/setup.sql was run in Supabase SQL Editor")
    print("  2. Set SUPABASE_URL and SUPABASE_KEY environment variables")
    print("     (Dashboard → Settings → API)")
    print("  3. Test: SUPABASE_URL=... SUPABASE_KEY=... streamlit run app.py")
    print("  4. Deploy to Streamlit Cloud with supabase secrets")
    print("=" * 60)

    sqlite_conn.close()
    return 0


if __name__ == "__main__":
    exit(main())
