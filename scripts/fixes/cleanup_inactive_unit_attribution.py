"""Repair generation_data rows mis-attributed to inactive generation units.

Background
----------
Before the daily/elexon/monthly aggregators were taught to filter `is_active=True`,
they sometimes attributed data to decommissioned/expanded "phase" units. The
classic offender is Raggovidda (windfarm_id=7206), where 64,356 rows landed on
"Phase 3" (12.9 MW capacity) when the actual operating unit was "Raggovidda"
(45 MW). Per-record `capacity_factor` for those rows is 3-3.5x too high
(avg 181%, max 359%).

This script finds every generation_data row whose `generation_unit_id` points
to an inactive unit, and re-runs the (now fixed) daily aggregator for the
affected (windfarm, source, day) tuples. The aggregator's `clear_existing_data`
deletes the old rows by (source, hour, windfarm_id) and re-writes them with
correct unit attribution.

Run:
    # Dry run — list windfarms that would be repaired and their date ranges.
    poetry run python scripts/fixes/cleanup_inactive_unit_attribution.py

    # Execute — actually re-aggregate the affected ranges.
    poetry run python scripts/fixes/cleanup_inactive_unit_attribution.py --execute

    # Restrict to one windfarm (Raggovidda):
    poetry run python scripts/fixes/cleanup_inactive_unit_attribution.py --execute --windfarm-id 7206

Out of scope:
- EIA (monthly source). Audit found 0 EIA corrupt rows so this script handles
  only daily sources (NVE/ENTSOE/ELEXON/TAIPOWER). If EIA ever shows
  corruption, run the monthly processor for the affected months.
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Ensure project root on sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import text

from app.core.database import get_session_factory
from scripts.seeds.aggregate_generation_data.process_generation_data_daily import (
    DailyGenerationProcessor,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Sources that this script repairs. EIA (monthly) is excluded — no corruption
# detected in audit.
DAILY_SOURCES = {'NVE', 'ENTSOE', 'ELEXON', 'TAIPOWER'}


async def find_corrupt_groups(db, windfarm_id: Optional[int] = None):
    """Return list of (windfarm_id, source, min_hour, max_hour, n_rows) tuples
    for rows attributed to inactive units."""
    where = "gu.is_active = false"
    params = {}
    if windfarm_id is not None:
        where += " AND gu.windfarm_id = :wf_id"
        params['wf_id'] = windfarm_id

    sql = f"""
        SELECT
          gu.windfarm_id,
          gd.source,
          MIN(gd.hour) AS min_hour,
          MAX(gd.hour) AS max_hour,
          COUNT(*) AS n_rows
        FROM generation_data gd
        JOIN generation_units gu ON gd.generation_unit_id = gu.id
        WHERE {where}
        GROUP BY gu.windfarm_id, gd.source
        ORDER BY n_rows DESC
    """
    result = await db.execute(text(sql), params)
    return result.fetchall()


async def stats_for_windfarm(db, windfarm_id: int):
    """Capture per-windfarm CF/orphan stats so we can compare before/after."""
    sql = """
        SELECT
          (SELECT COUNT(*) FROM generation_data gd
            JOIN generation_units gu ON gd.generation_unit_id = gu.id
            WHERE gu.is_active = false AND gu.windfarm_id = :wf_id) AS rows_on_inactive,
          (SELECT MAX(capacity_factor) FROM generation_data WHERE windfarm_id = :wf_id) AS max_cf,
          (SELECT COUNT(*) FROM generation_data WHERE windfarm_id = :wf_id AND capacity_factor > 1.05) AS rows_cf_over_1
    """
    result = await db.execute(text(sql), {'wf_id': windfarm_id})
    row = result.fetchone()
    return {
        'rows_on_inactive': row[0],
        'max_cf': float(row[1]) if row[1] else None,
        'rows_cf_over_1': row[2],
    }


def daterange(start_day: datetime, end_day: datetime):
    """Yield each UTC day in [start_day, end_day] inclusive."""
    d = start_day.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end = end_day.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    while d <= end:
        yield d
        d += timedelta(days=1)


async def repair_windfarm_source(
    session_factory,
    shared_cache,
    shared_commercial_dates,
    windfarm_id: int,
    source: str,
    min_hour: datetime,
    max_hour: datetime,
    dry_run: bool,
):
    """Re-aggregate one (windfarm, source) for its corrupt date range.

    Reuses the pre-loaded units cache to skip the ~45s reload per windfarm.
    Uses a fresh session per call so the asyncpg connection stays warm and we
    don't run into prepared-statement timeouts on long-lived sessions.
    """
    days = list(daterange(min_hour, max_hour))
    logger.info(
        f"  windfarm_id={windfarm_id} source={source}: "
        f"{len(days)} days from {min_hour.date()} to {max_hour.date()}"
    )

    if dry_run:
        return {'days': len(days), 'reaggregated': False}

    async with session_factory() as db:
        # Pre-clean: delete ALL generation_data rows in the repair range whose
        # generation_unit_id belongs to this windfarm (active OR inactive),
        # OR whose windfarm_id matches. This catches the case where a unit's
        # generation_units.windfarm_id was changed but old generation_data rows
        # still have the old windfarm_id (e.g., Raggovidda's unit 12805 has
        # rows under both windfarm_id=7206 and windfarm_id=8772). Without this,
        # process_day's clear_existing_data only sees the windfarm_id=7206 rows
        # and we hit (hour, unit_id, source) unique-constraint conflicts on
        # re-insert.
        from sqlalchemy import text as _sql_text
        delete_result = await db.execute(_sql_text("""
            DELETE FROM generation_data
            WHERE source = :src
              AND hour >= :min_h
              AND hour <= :max_h
              AND (
                generation_unit_id IN (SELECT id FROM generation_units WHERE windfarm_id = :wf_id)
                OR windfarm_id = :wf_id
              )
        """), {'src': source, 'wf_id': windfarm_id, 'min_h': min_hour, 'max_h': max_hour})
        await db.commit()
        logger.info(f"  Pre-cleaned {delete_result.rowcount} existing rows in range")

        processor = DailyGenerationProcessor(db, dry_run=False)
        # Reuse the pre-loaded cache — skip the expensive reload.
        processor.generation_units_cache = shared_cache
        processor.windfarm_commercial_dates = shared_commercial_dates

        # Commit every BATCH_DAYS so a single failure doesn't lose a year of work.
        BATCH_DAYS = 30
        batch_count = 0
        for i, day in enumerate(days):
            try:
                await processor.process_day(
                    day,
                    sources=[source],
                    skip_load_units=True,  # Cache already populated above.
                    skip_commit=True,
                    windfarm_id=windfarm_id,
                )
            except Exception as e:
                logger.error(f"  Day {day.date()} failed: {e}")
                await db.rollback()
                raise
            batch_count += 1
            if batch_count >= BATCH_DAYS:
                await db.commit()
                batch_count = 0
            if (i + 1) % 100 == 0:
                logger.info(f"    ...{i + 1}/{len(days)} days processed")

        if batch_count > 0:
            await db.commit()
        logger.info(f"  Committed re-aggregation for windfarm={windfarm_id} source={source}")

    return {'days': len(days), 'reaggregated': True}


async def main(execute: bool, only_windfarm: Optional[int]):
    SF = get_session_factory()
    # Use a short-lived session for the audit/stats reads; the per-windfarm
    # repair calls open their own sessions.
    async with SF() as db:
        groups = await find_corrupt_groups(db, windfarm_id=only_windfarm)
        if not groups:
            print("No generation_data rows attributed to inactive units. Nothing to repair.")
            return

        # Filter: keep only daily sources WITH a windfarm_id; surface excluded.
        daily_groups = [g for g in groups if g[1] in DAILY_SOURCES and g[0] is not None]
        excluded_source = [g for g in groups if g[1] not in DAILY_SOURCES]
        excluded_orphan = [g for g in groups if g[1] in DAILY_SOURCES and g[0] is None]
        if excluded_source:
            print(f"\n[skipped] {len(excluded_source)} groups with non-daily sources (audit manually):")
            for wf, src, min_h, max_h, n in excluded_source:
                print(f"  windfarm_id={wf} source={src} rows={n} ({min_h} -> {max_h})")
        if excluded_orphan:
            print(f"\n[skipped] {len(excluded_orphan)} groups with NULL windfarm_id "
                  f"(unit detached from windfarm — can't re-aggregate, delete manually):")
            for _, src, min_h, max_h, n in excluded_orphan:
                print(f"  source={src} rows={n} ({min_h} -> {max_h})")

        print(f"\n{'DRY RUN ' if not execute else ''}Repairing {len(daily_groups)} (windfarm, source) groups")
        print(f"{'-' * 78}")

        affected_windfarms = sorted({g[0] for g in daily_groups})

        # Snapshot before stats per windfarm.
        before_stats = {wf: await stats_for_windfarm(db, wf) for wf in affected_windfarms}

        # Pre-load the generation_units cache once (~45s on remote RDS) and
        # share it across all per-windfarm repairs. Skipped on dry-run.
        shared_cache = {}
        shared_commercial_dates = {}
        if execute:
            print("\n  Loading generation_units cache (one-time, ~45s)...")
            preload_processor = DailyGenerationProcessor(db, dry_run=False)
            await preload_processor.load_generation_units()
            shared_cache = preload_processor.generation_units_cache
            shared_commercial_dates = preload_processor.windfarm_commercial_dates
            print(f"  Loaded {len(shared_cache)} cache keys")

        total_days = 0
        for wf, src, min_h, max_h, n in daily_groups:
            print(f"\n  windfarm_id={wf} source={src} corrupt_rows={n}")
            print(f"    range: {min_h} → {max_h}")
            result = await repair_windfarm_source(
                SF, shared_cache, shared_commercial_dates,
                wf, src, min_h, max_h, dry_run=not execute,
            )
            total_days += result['days']

        print(f"\n{'-' * 78}")
        print(f"Total day-aggregations {'planned' if not execute else 'run'}: {total_days}")

        if execute:
            # After stats and report — use a fresh session so we read post-commit state.
            print(f"\n{'-' * 78}")
            print(f"{'windfarm':>10} {'before_orphans':>16} {'after_orphans':>15} "
                  f"{'before_max_cf':>15} {'after_max_cf':>14} {'before_cf>1':>13} {'after_cf>1':>12}")
            def fmt_cf(v):
                return f"{v:.3f}" if v is not None else "-"

            async with SF() as fresh_db:
                for wf in affected_windfarms:
                    after = await stats_for_windfarm(fresh_db, wf)
                    b = before_stats[wf]
                    print(
                        f"{wf:>10} "
                        f"{b['rows_on_inactive']:>16} "
                        f"{after['rows_on_inactive']:>15} "
                        f"{fmt_cf(b['max_cf']):>15} "
                        f"{fmt_cf(after['max_cf']):>14} "
                        f"{b['rows_cf_over_1']:>13} "
                        f"{after['rows_cf_over_1']:>12}"
                    )
        else:
            print("\nRe-run with --execute to actually re-aggregate.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true',
                        help='Actually run the re-aggregation (default: dry run).')
    parser.add_argument('--windfarm-id', type=int, default=None,
                        help='Restrict to a single windfarm.')
    args = parser.parse_args()
    asyncio.run(main(execute=args.execute, only_windfarm=args.windfarm_id))
