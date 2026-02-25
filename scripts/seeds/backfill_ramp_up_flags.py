"""
Backfill ramp-up flags on existing generation_data records.

Steps:
1. For each generation unit with ramp-up boundaries, bulk UPDATE is_ramp_up = TRUE
2. Recalculate CF for records that were previously NULLed (pre-commercial)
3. Process in batches per unit using raw SQL for performance

Usage:
    poetry run python scripts/seeds/backfill_ramp_up_flags.py
    poetry run python scripts/seeds/backfill_ramp_up_flags.py --dry-run
    poetry run python scripts/seeds/backfill_ramp_up_flags.py --windfarm-id 7248
"""

import asyncio
import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.core.config import get_settings
from app.utils.ramp_up import is_in_ramp_up_period

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def backfill_ramp_up_flags(dry_run: bool = False, windfarm_id: int = None):
    """Backfill is_ramp_up flags for all generation units with ramp-up dates."""
    settings = get_settings()
    engine = create_async_engine(settings.database_url_async, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        # Step 1: Load all generation units with their windfarm dates
        unit_query = text("""
            SELECT
                gu.id AS unit_id,
                gu.code,
                gu.first_power_date,
                gu.start_date,
                gu.commercial_operational_date AS unit_cod,
                gu.ramp_up_end_date AS unit_ramp_end,
                gu.windfarm_id,
                w.first_power_date AS wf_first_power,
                w.commercial_operational_date AS wf_cod,
                w.ramp_up_end_date AS wf_ramp_end
            FROM generation_units gu
            LEFT JOIN windfarms w ON gu.windfarm_id = w.id
            WHERE gu.is_active = TRUE
        """)

        if windfarm_id:
            unit_query = text("""
                SELECT
                    gu.id AS unit_id,
                    gu.code,
                    gu.first_power_date,
                    gu.start_date,
                    gu.commercial_operational_date AS unit_cod,
                    gu.ramp_up_end_date AS unit_ramp_end,
                    gu.windfarm_id,
                    w.first_power_date AS wf_first_power,
                    w.commercial_operational_date AS wf_cod,
                    w.ramp_up_end_date AS wf_ramp_end
                FROM generation_units gu
                LEFT JOIN windfarms w ON gu.windfarm_id = w.id
                WHERE gu.is_active = TRUE AND gu.windfarm_id = :windfarm_id
            """)

        params = {'windfarm_id': windfarm_id} if windfarm_id else {}
        result = await session.execute(unit_query, params)
        units = result.all()

        logger.info(f"Found {len(units)} generation units to process")

        total_flagged = 0
        total_cf_restored = 0

        for row in units:
            unit_info = {
                'first_power_date': row.first_power_date,
                'start_date': row.start_date,
                'commercial_operational_date': row.unit_cod,
                'unit_commercial_operational_date': row.unit_cod,
                'unit_ramp_up_end_date': row.unit_ramp_end,
                'windfarm_first_power_date': row.wf_first_power,
                'windfarm_commercial_operational_date': row.wf_cod,
                'windfarm_ramp_up_end_date': row.wf_ramp_end,
            }

            # Test a sample date to check if this unit has ramp-up boundaries
            # Use a date far in the past — if even that isn't ramp-up, skip
            test_date = date(2000, 1, 1)
            has_any_ramp = False

            # Determine actual ramp boundaries for this unit
            from app.utils.ramp_up import _to_date, DEFAULT_RAMP_UP_MONTHS
            from dateutil.relativedelta import relativedelta

            ramp_start = (
                _to_date(unit_info.get('first_power_date'))
                or _to_date(unit_info.get('windfarm_first_power_date'))
                or _to_date(unit_info.get('start_date'))
            )
            ramp_end = (
                _to_date(unit_info.get('unit_ramp_up_end_date'))
                or _to_date(unit_info.get('windfarm_ramp_up_end_date'))
            )
            if not ramp_end:
                cod = (
                    _to_date(unit_info.get('commercial_operational_date'))
                    or _to_date(unit_info.get('windfarm_commercial_operational_date'))
                )
                if cod:
                    ramp_end = cod + relativedelta(months=DEFAULT_RAMP_UP_MONTHS)

            if not ramp_start or not ramp_end:
                continue

            logger.info(
                f"Unit {row.unit_id} ({row.code}): ramp-up {ramp_start} → {ramp_end}"
            )

            if dry_run:
                # Count how many records would be affected
                count_result = await session.execute(
                    text("""
                        SELECT COUNT(*) FROM generation_data
                        WHERE generation_unit_id = :unit_id
                          AND hour >= :ramp_start AND hour < :ramp_end
                    """),
                    {
                        'unit_id': row.unit_id,
                        'ramp_start': datetime.combine(ramp_start, datetime.min.time()),
                        'ramp_end': datetime.combine(ramp_end, datetime.min.time()),
                    }
                )
                count = count_result.scalar()
                logger.info(f"  [DRY RUN] Would flag {count} records as ramp-up")
                total_flagged += count
                continue

            # Step 2: Bulk UPDATE is_ramp_up = TRUE for records in ramp-up period
            update_result = await session.execute(
                text("""
                    UPDATE generation_data
                    SET is_ramp_up = TRUE, updated_at = NOW()
                    WHERE generation_unit_id = :unit_id
                      AND hour >= :ramp_start AND hour < :ramp_end
                      AND is_ramp_up = FALSE
                """),
                {
                    'unit_id': row.unit_id,
                    'ramp_start': datetime.combine(ramp_start, datetime.min.time()),
                    'ramp_end': datetime.combine(ramp_end, datetime.min.time()),
                }
            )
            flagged = update_result.rowcount
            total_flagged += flagged
            logger.info(f"  Flagged {flagged} records as ramp-up")

            # Step 3: Recalculate CF for records that were previously NULLed
            # (pre-commercial logic set capacity_factor=NULL, capacity_mw=NULL)
            cf_result = await session.execute(
                text("""
                    UPDATE generation_data gd
                    SET
                        capacity_factor = LEAST(gd.generation_mwh / gu.capacity_mw, 9.9999),
                        capacity_mw = gu.capacity_mw,
                        is_ramp_up = TRUE,
                        updated_at = NOW()
                    FROM generation_units gu
                    WHERE gd.generation_unit_id = gu.id
                      AND gd.generation_unit_id = :unit_id
                      AND gd.capacity_factor IS NULL
                      AND gd.generation_mwh > 0
                      AND gu.capacity_mw > 0
                      AND gd.hour >= :ramp_start AND gd.hour < :ramp_end
                """),
                {
                    'unit_id': row.unit_id,
                    'ramp_start': datetime.combine(ramp_start, datetime.min.time()),
                    'ramp_end': datetime.combine(ramp_end, datetime.min.time()),
                }
            )
            cf_restored = cf_result.rowcount
            total_cf_restored += cf_restored
            if cf_restored > 0:
                logger.info(f"  Restored CF for {cf_restored} previously-NULLed records")

        if not dry_run:
            await session.commit()
            logger.info(
                f"Backfill complete: {total_flagged} records flagged, "
                f"{total_cf_restored} CF values restored"
            )
        else:
            logger.info(
                f"[DRY RUN] Would flag {total_flagged} records total"
            )

    await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description="Backfill ramp-up flags on generation_data")
    parser.add_argument('--dry-run', action='store_true', help="Show what would be updated without making changes")
    parser.add_argument('--windfarm-id', type=int, help="Only process units for a specific windfarm")
    args = parser.parse_args()

    asyncio.run(backfill_ramp_up_flags(dry_run=args.dry_run, windfarm_id=args.windfarm_id))


if __name__ == '__main__':
    main()
