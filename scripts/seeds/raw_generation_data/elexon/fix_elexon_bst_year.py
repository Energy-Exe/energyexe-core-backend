"""Fix ELEXON raw data BST offset in period_start/period_end for a given year.

Generalized from fix_2019_bst.py (Fix 6) to work with any year.

Problem: Raw CSV-imported data has period_start stored as if UK local time = UTC.
During BST months (late March - late October), this causes a 1-hour shift.

Fix: Recalculate period_start and period_end from settlement_date + settlement_period
using correct Europe/London timezone conversion, then update in-place.

Usage:
    poetry run python scripts/seeds/raw_generation_data/elexon/fix_elexon_bst_year.py --year 2017
"""

import argparse
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory
from sqlalchemy import text


UK_TZ = ZoneInfo('Europe/London')
UTC_TZ = ZoneInfo('UTC')


async def fix_month(db, year: int, month: int, include_entsoe_backfill: bool = False) -> int:
    """Fix all records for a single month using a direct SQL UPDATE.

    Uses PostgreSQL make_timestamptz to correctly convert settlement_date
    (UK local time) to UTC, then adds settlement_period offset.
    """
    from calendar import monthrange
    days_in_month = monthrange(year, month)[1]

    # Build source_type filter
    if include_entsoe_backfill:
        source_type_clause = "(source_type = 'csv' OR source_type = 'entsoe_backfill')"
    else:
        source_type_clause = "source_type = 'csv'"

    # Update in smaller sub-batches by day to avoid timeouts
    total_fixed = 0
    for day in range(1, days_in_month + 1):
        day_start = datetime(year, month, day, tzinfo=UTC_TZ)
        if day == days_in_month:
            if month == 12:
                day_end = datetime(year + 1, 1, 1, tzinfo=UTC_TZ)
            else:
                day_end = datetime(year, month + 1, 1, tzinfo=UTC_TZ)
        else:
            day_end = datetime(year, month, day + 1, tzinfo=UTC_TZ)

        result = await db.execute(text(f"""
            UPDATE generation_data_raw
            SET period_start = (
                    make_timestamptz(
                        SUBSTRING(data->>'settlement_date', 1, 4)::int,
                        SUBSTRING(data->>'settlement_date', 5, 2)::int,
                        SUBSTRING(data->>'settlement_date', 7, 2)::int,
                        0, 0, 0,
                        'Europe/London'
                    ) + (((data->>'settlement_period')::int - 1) * interval '30 minutes')
                ),
                period_end = (
                    make_timestamptz(
                        SUBSTRING(data->>'settlement_date', 1, 4)::int,
                        SUBSTRING(data->>'settlement_date', 5, 2)::int,
                        SUBSTRING(data->>'settlement_date', 7, 2)::int,
                        0, 0, 0,
                        'Europe/London'
                    ) + (((data->>'settlement_period')::int - 1) * interval '30 minutes')
                    + interval '30 minutes'
                )
            WHERE source = 'ELEXON'
            AND {source_type_clause}
            AND period_start >= :day_start
            AND period_start < :day_end
            AND data->>'settlement_date' IS NOT NULL
            AND data->>'settlement_period' IS NOT NULL
        """), {"day_start": day_start, "day_end": day_end})

        total_fixed += result.rowcount
        await db.commit()

    return total_fixed


async def verify_fix(db, year: int) -> None:
    """Verify the fix by checking sample records for the given year."""
    print(f"\n=== VERIFICATION ({year}) ===")

    # Check April 1 SP1 - should be 23:00 UTC March 31 (during BST)
    sd_apr1 = f"{year}0401"
    result = await db.execute(text(
        "SELECT period_start, data->>'settlement_date' as sd, data->>'settlement_period' as sp "
        "FROM generation_data_raw "
        "WHERE source = 'ELEXON' AND source_type = 'csv' "
        "AND data->>'settlement_date' = :sd "
        "AND data->>'settlement_period' = '1' "
        "LIMIT 1"
    ), {"sd": sd_apr1})
    row = result.fetchone()
    if row:
        ps = str(row[0])
        expected = f"{year - 1}-12-31" if False else f"{year}-03-31 23:00"  # Apr 1 SP1 -> Mar 31 23:00 UTC
        correct = f"{year}-03-31 23:00" in ps
        print(f"  Apr 1 SP1: period_start={row[0]} {'CORRECT' if correct else f'WRONG (expected {year}-03-31 23:00 UTC)'}")
    else:
        print(f"  Apr 1 SP1: No records found for settlement_date={sd_apr1}")

    # Check June 15 SP1 - should be 23:00 UTC June 14 (during BST)
    sd_jun15 = f"{year}0615"
    result2 = await db.execute(text(
        "SELECT period_start "
        "FROM generation_data_raw "
        "WHERE source = 'ELEXON' AND source_type = 'csv' "
        "AND data->>'settlement_date' = :sd "
        "AND data->>'settlement_period' = '1' "
        "LIMIT 1"
    ), {"sd": sd_jun15})
    row2 = result2.fetchone()
    if row2:
        ps2 = str(row2[0])
        correct2 = f"{year}-06-14 23:00" in ps2
        print(f"  Jun 15 SP1: period_start={row2[0]} {'CORRECT' if correct2 else f'WRONG (expected {year}-06-14 23:00 UTC)'}")
    else:
        print(f"  Jun 15 SP1: No records found for settlement_date={sd_jun15}")

    # Check Jan 15 SP1 - should be 00:00 UTC Jan 15 (GMT, no offset)
    sd_jan15 = f"{year}0115"
    result3 = await db.execute(text(
        "SELECT period_start "
        "FROM generation_data_raw "
        "WHERE source = 'ELEXON' AND source_type = 'csv' "
        "AND data->>'settlement_date' = :sd "
        "AND data->>'settlement_period' = '1' "
        "LIMIT 1"
    ), {"sd": sd_jan15})
    row3 = result3.fetchone()
    if row3:
        ps3 = str(row3[0])
        correct3 = f"{year}-01-15 00:00" in ps3
        print(f"  Jan 15 SP1: period_start={row3[0]} {'CORRECT' if correct3 else f'WRONG (expected {year}-01-15 00:00 UTC)'}")
    else:
        print(f"  Jan 15 SP1: No records found for settlement_date={sd_jan15}")

    # Check a sample BMU's daily record counts around BST spring forward
    # Find a BMU that has data for this year
    bmu_result = await db.execute(text(
        "SELECT identifier FROM generation_data_raw "
        "WHERE source = 'ELEXON' AND source_type = 'csv' "
        "AND period_start >= :year_start AND period_start < :year_end "
        "LIMIT 1"
    ), {
        "year_start": datetime(year, 1, 1, tzinfo=UTC_TZ),
        "year_end": datetime(year + 1, 1, 1, tzinfo=UTC_TZ),
    })
    bmu_row = bmu_result.fetchone()
    if bmu_row:
        bmu_id = bmu_row[0]
        # Find BST spring forward date for this year (last Sunday in March)
        from calendar import monthrange
        march_days = monthrange(year, 3)[1]
        spring_forward = datetime(year, 3, march_days)
        while spring_forward.weekday() != 6:  # Sunday
            spring_forward -= timedelta(days=1)

        sf_start = spring_forward - timedelta(days=2)
        sf_end = spring_forward + timedelta(days=3)

        print(f"\n  Daily records for {bmu_id} around BST spring forward ({spring_forward.strftime('%b %d')}):")
        r4 = await db.execute(text(
            "SELECT DATE(period_start AT TIME ZONE 'UTC') as day, COUNT(*) "
            "FROM generation_data_raw "
            "WHERE source = 'ELEXON' AND source_type = 'csv' "
            "AND identifier = :bmu "
            "AND period_start >= :start AND period_start < :end "
            "GROUP BY DATE(period_start AT TIME ZONE 'UTC') ORDER BY day"
        ), {
            "bmu": bmu_id,
            "start": datetime(sf_start.year, sf_start.month, sf_start.day, tzinfo=UTC_TZ),
            "end": datetime(sf_end.year, sf_end.month, sf_end.day, tzinfo=UTC_TZ),
        })
        for row in r4.fetchall():
            print(f"    {row[0]}: {row[1]} records")


async def main():
    parser = argparse.ArgumentParser(description="Fix ELEXON BST offset for a given year")
    parser.add_argument("--year", type=int, required=True, help="Year to fix (e.g. 2017)")
    args = parser.parse_args()

    year = args.year
    include_entsoe_backfill = (year == 2016)

    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        print("=" * 60)
        print(f"ELEXON {year} BST FIX")
        if include_entsoe_backfill:
            print("  (including entsoe_backfill records)")
        print("=" * 60)

        # Fix ALL months for consistency — GMT months will have no effective change
        months_to_fix = list(range(1, 13))
        print(f"\nFixing months: {months_to_fix}")
        print("(GMT months will have no effective change)")

        total_fixed = 0
        for month in months_to_fix:
            fixed = await fix_month(db, year, month, include_entsoe_backfill)
            total_fixed += fixed
            print(f"  {year}-{month:02d}: {fixed:,} records updated")

        print(f"\nTotal records updated: {total_fixed:,}")

        await verify_fix(db, year)


if __name__ == "__main__":
    asyncio.run(main())
