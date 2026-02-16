#!/usr/bin/env python3
"""Bootstrap script - download historical funding rate data."""

import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.collector import Collector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/bootstrap.log'),
    ]
)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Bootstrap funding rate data')
    parser.add_argument('--days', type=int, default=365, help='Days of history')
    parser.add_argument('--symbols', nargs='+', default=None,
                        help='Symbols to fetch (use ALL to auto-discover)')
    parser.add_argument('--discover', '--all', action='store_true',
                        help='Auto-discover all available symbols from all exchanges')
    args = parser.parse_args()

    collector = Collector()

    # --symbols ALL is equivalent to --discover
    discover = args.discover or (args.symbols and len(args.symbols) == 1 and args.symbols[0].upper() == 'ALL')

    if discover:
        print("Discovering all available symbols from all exchanges...")
        symbols = collector.discover_all_symbols()
        print(f"Found {len(symbols)} unique symbols: {symbols[:20]}{'...' if len(symbols) > 20 else ''}")
        print(f"Bootstrapping {args.days} days for all {len(symbols)} symbols")
        collector.collect_all(symbols=symbols, days_back=args.days)
    else:
        symbols = args.symbols or collector.symbols
        if not symbols:
            print("No symbols configured. Use --discover or --symbols ALL to auto-detect, or --symbols BTC ETH ...")
            sys.exit(1)
        print(f"Bootstrapping {args.days} days for: {symbols}")
        collector.collect_all(symbols=symbols, days_back=args.days)

    print("Done!")


if __name__ == "__main__":
    main()
