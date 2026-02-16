"""Funding rate data quality validation script."""

import sqlite3
import sys
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'funding_rates.db')
REPORT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data_quality_report.txt')

INTERVAL_MAP = {
    'binance': 8, 'bybit': 8, 'okx': 8, 'bitget': 8,
    'hyperliquid': 1, 'lighter': 1,
}


def ms_to_utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')


def validate():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    now_ms = int(time.time() * 1000)

    # Get all exchange+symbol combos
    combos = c.execute(
        "SELECT exchange, symbol, COUNT(*) as cnt, MIN(funding_time), MAX(funding_time) "
        "FROM funding_rates GROUP BY exchange, symbol ORDER BY exchange, symbol"
    ).fetchall()

    # Check duplicates globally
    dupes = c.execute(
        "SELECT exchange, symbol, funding_time, COUNT(*) as cnt "
        "FROM funding_rates GROUP BY exchange, symbol, funding_time HAVING cnt > 1"
    ).fetchall()

    dupe_set = set()
    for exch, sym, ft, cnt in dupes:
        dupe_set.add((exch, sym))

    lines = []
    detail_lines = []
    summary_rows = []

    total_issues = 0
    total_dupes = len(dupes)

    for exch, sym, count, min_t, max_t in combos:
        nominal_interval_h = INTERVAL_MAP.get(exch, 8)
        nominal_interval_ms = nominal_interval_h * 3600 * 1000

        span_days = (max_t - min_t) / (86400 * 1000)
        expected = int(span_days * 24 / nominal_interval_h) + 1 if span_days > 0 else 1
        coverage = min(count / expected * 100, 100) if expected > 0 else 0

        # Fetch timestamps for gap/frequency analysis
        rows = c.execute(
            "SELECT funding_time FROM funding_rates WHERE exchange=? AND symbol=? ORDER BY funding_time",
            (exch, sym)
        ).fetchall()
        timestamps = [r[0] for r in rows]

        # Analyze intervals
        gaps = []
        intervals = []
        for i in range(1, len(timestamps)):
            delta_ms = timestamps[i] - timestamps[i - 1]
            delta_h = delta_ms / 3600000
            intervals.append(delta_h)
            if delta_ms > 2.1 * nominal_interval_ms:
                gaps.append((timestamps[i - 1], timestamps[i], delta_h))

        # Detect frequency changes - group consecutive intervals by detected period
        freq_periods = []
        if intervals:
            def bucket(h):
                if h < 1.5: return 1
                if h < 3: return 2
                if h < 6: return 4
                return 8

            current_bucket = bucket(intervals[0])
            period_start = timestamps[0]
            period_count = 1

            for i, iv in enumerate(intervals[1:], 2):
                b = bucket(iv)
                if b != current_bucket:
                    freq_periods.append((current_bucket, period_start, timestamps[i - 1], period_count))
                    current_bucket = b
                    period_start = timestamps[i - 1]
                    period_count = 1
                else:
                    period_count += 1

            freq_periods.append((current_bucket, period_start, timestamps[-1], period_count))

        has_dupe = (exch, sym) in dupe_set
        dupe_count = sum(1 for d in dupes if d[0] == exch and d[1] == sym)

        summary_rows.append({
            'exchange': exch,
            'symbol': sym,
            'records': count,
            'expected': expected,
            'coverage': coverage,
            'span_days': span_days,
            'gaps': len(gaps),
            'dupes': dupe_count,
        })

        # Detail output for issues
        if gaps or has_dupe or len(freq_periods) > 1:
            detail_lines.append(f"\n{'='*60}")
            detail_lines.append(f"  {exch.upper()} / {sym}")
            detail_lines.append(f"{'='*60}")

            if len(freq_periods) > 1:
                detail_lines.append("  Frequency changes detected:")
                for fh, fs, fe, fc in freq_periods:
                    days = (fe - fs) / 86400000
                    detail_lines.append(f"    {fh}hr funding for ~{days:.0f} days ({fc} records) "
                                        f"[{ms_to_utc(fs)} → {ms_to_utc(fe)}]")

            if gaps:
                detail_lines.append(f"  Gaps (>{nominal_interval_h * 2}hr):")
                for gs, ge, gh in gaps[:10]:
                    detail_lines.append(f"    {ms_to_utc(gs)} → {ms_to_utc(ge)} ({gh:.1f}hr)")
                if len(gaps) > 10:
                    detail_lines.append(f"    ... and {len(gaps) - 10} more gaps")

            if has_dupe:
                detail_lines.append(f"  ⚠ ERROR: {dupe_count} duplicate records!")

    # Build report
    lines.append("=" * 90)
    lines.append("  FUNDING RATE DATA QUALITY REPORT")
    lines.append(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("=" * 90)

    # Stats
    exchanges = set(r['exchange'] for r in summary_rows)
    symbols = set(r['symbol'] for r in summary_rows)
    lines.append(f"\n  Exchanges: {len(exchanges)} ({', '.join(sorted(exchanges))})")
    lines.append(f"  Symbols:   {len(symbols)}")
    lines.append(f"  Combos:    {len(summary_rows)}")
    lines.append(f"  Total duplicates: {total_dupes}")

    # Summary table
    lines.append(f"\n{'─'*90}")
    hdr = f"  {'Exchange':<14} {'Symbol':<10} {'Records':>8} {'Expected':>9} {'Cvg%':>6} {'Days':>6} {'Gaps':>5} {'Dupes':>6}"
    lines.append(hdr)
    lines.append(f"{'─'*90}")

    for r in sorted(summary_rows, key=lambda x: (x['exchange'], x['symbol'])):
        flag = " ⚠" if r['dupes'] > 0 else ""
        lines.append(f"  {r['exchange']:<14} {r['symbol']:<10} {r['records']:>8} {r['expected']:>9} "
                      f"{r['coverage']:>5.1f}% {r['span_days']:>5.0f}d {r['gaps']:>5} {r['dupes']:>5}{flag}")

    lines.append(f"{'─'*90}")

    # Coverage summary per exchange
    lines.append(f"\n  Coverage by exchange:")
    for exch in sorted(exchanges):
        exch_rows = [r for r in summary_rows if r['exchange'] == exch]
        avg_cov = sum(r['coverage'] for r in exch_rows) / len(exch_rows)
        total_gaps = sum(r['gaps'] for r in exch_rows)
        lines.append(f"    {exch:<14} {len(exch_rows):>4} symbols  avg coverage: {avg_cov:.1f}%  total gaps: {total_gaps}")

    if detail_lines:
        lines.append(f"\n\n{'='*90}")
        lines.append("  DETAILED ISSUES")
        lines.append("=" * 90)
        lines.extend(detail_lines)

    lines.append(f"\n{'='*90}")
    lines.append("  END OF REPORT")
    lines.append("=" * 90)

    report = '\n'.join(lines)

    with open(REPORT_PATH, 'w') as f:
        f.write(report)

    print(report)
    print(f"\nReport saved to: {REPORT_PATH}")

    conn.close()


if __name__ == '__main__':
    validate()
