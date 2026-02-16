#!/bin/bash
cd /Users/man/.openclaw/workspace/funding-dashboard
mkdir -p logs data

# Always run funding rate update
python3 scripts/update.py >> logs/auto_update.log 2>&1

# Run OI update every 4 hours (at hours 0, 4, 8, 12, 16, 20)
HOUR=$(date +%-H)
if [ $((HOUR % 4)) -eq 0 ]; then
    python3 scripts/update_oi.py >> logs/oi_update.log 2>&1
fi

exit $?
