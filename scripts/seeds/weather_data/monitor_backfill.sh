#!/bin/bash
# Monitor 2024 weather data backfill progress

echo "========================================"
echo "2024 Weather Data Backfill Monitor"
echo "========================================"
echo ""

# Check if process is running
if ps aux | grep -q "[f]etch_daily_all_windfarms.py"; then
    echo "✓ Backfill process is RUNNING"
    PID=$(ps aux | grep "[f]etch_daily_all_windfarms.py" | awk '{print $2}')
    echo "  Process ID: $PID"
else
    echo "✗ Backfill process is NOT running"
fi

echo ""
echo "----------------------------------------"
echo "Latest Log Output:"
echo "----------------------------------------"
tail -20 weather_2024_backfill.log

echo ""
echo "----------------------------------------"
echo "Progress Summary:"
echo "----------------------------------------"

# Count completed days from log
COMPLETED=$(grep -c "COMPLETE" weather_2024_backfill.log 2>/dev/null || echo "0")
echo "Days completed: $COMPLETED / 366"

# Calculate percentage
if [ "$COMPLETED" -gt 0 ]; then
    PERCENT=$(awk "BEGIN {printf \"%.1f\", ($COMPLETED/366)*100}")
    echo "Progress: $PERCENT%"
fi

# Estimate remaining time
if [ "$COMPLETED" -gt 0 ]; then
    AVG_TIME=4.5  # minutes per day
    REMAINING=$((366 - COMPLETED))
    TOTAL_MINUTES=$(awk "BEGIN {printf \"%.0f\", $REMAINING * $AVG_TIME}")
    HOURS=$(awk "BEGIN {printf \"%.1f\", $TOTAL_MINUTES / 60}")
    echo "Estimated time remaining: ~$HOURS hours"
fi

echo ""
echo "----------------------------------------"
echo "Database Status:"
echo "----------------------------------------"

# Count records in database
cd "$(dirname "$0")/../../.." && poetry run python -c "
import asyncio
from app.core.database import get_session_factory
from sqlalchemy import text

async def check():
    SessionLocal = get_session_factory()
    async with SessionLocal() as db:
        # Total records
        result = await db.execute(text('SELECT COUNT(*) FROM weather_data WHERE source=\\'ERA5\\''))
        total = result.scalar()

        # Distinct dates
        result = await db.execute(text('SELECT COUNT(DISTINCT DATE(hour)) FROM weather_data WHERE source=\\'ERA5\\''))
        dates = result.scalar()

        # Date range
        result = await db.execute(text('SELECT MIN(DATE(hour)), MAX(DATE(hour)) FROM weather_data WHERE source=\\'ERA5\\''))
        date_range = result.fetchone()

        print(f'Total records: {total:,}')
        print(f'Distinct dates: {dates}')
        if date_range[0]:
            print(f'Date range: {date_range[0]} to {date_range[1]}')

asyncio.run(check())
" 2>/dev/null || echo "Database query failed"

echo ""
echo "----------------------------------------"
echo "Storage Usage:"
echo "----------------------------------------"
GRIB_SIZE=$(du -sh grib_files/daily/ 2>/dev/null | awk '{print $1}')
GRIB_COUNT=$(ls -1 grib_files/daily/*.grib 2>/dev/null | wc -l)
echo "GRIB files: $GRIB_COUNT files, $GRIB_SIZE total"

echo ""
echo "========================================"
echo "Commands:"
echo "  Monitor live: tail -f weather_2024_backfill.log"
echo "  Stop process: kill $PID"
echo "  Verify data: poetry run python scripts/seeds/weather_data/verify_timezone_fix.py"
echo "========================================"
