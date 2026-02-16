"""Export ALL hourly generation data for ELEXON windfarms to CSV (all periods)."""

import asyncio
import csv
import sys
from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta
from sqlalchemy import select, and_, func, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.models.generation_data import GenerationData
from app.models.generation_unit import GenerationUnit
from app.models.windfarm import Windfarm
from app.core.config import get_settings


async def export_all_elexon_hourly(output_file: str):
    """Export all ELEXON windfarm hourly generation data to CSV."""

    settings = get_settings()

    engine = create_async_engine(
        str(settings.DATABASE_URL),
        echo=False,
        connect_args={"command_timeout": 120},
    )
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    total_rows = 0

    async with async_session() as db:
        # Get all ELEXON windfarms
        wf_query = select(Windfarm.id, Windfarm.code, Windfarm.name).where(
            Windfarm.id.in_(
                select(GenerationData.windfarm_id).where(
                    GenerationData.source == "ELEXON"
                ).distinct()
            )
        ).order_by(Windfarm.name)
        result = await db.execute(wf_query)
        windfarms = {row.id: {"code": row.code, "name": row.name} for row in result.all()}
        print(f"Found {len(windfarms)} ELEXON windfarms")

        # Get all ELEXON generation units
        gu_query = select(
            GenerationUnit.id, GenerationUnit.code, GenerationUnit.name,
            GenerationUnit.fuel_type, GenerationUnit.capacity_mw,
        ).where(GenerationUnit.source == "ELEXON")
        result = await db.execute(gu_query)
        gen_units = {
            row.id: {
                "code": row.code,
                "name": row.name,
                "fuel_type": row.fuel_type,
                "capacity_mw": row.capacity_mw,
            }
            for row in result.all()
        }
        print(f"Found {len(gen_units)} ELEXON generation units")

        # Find date range
        range_query = select(
            func.min(GenerationData.hour),
            func.max(GenerationData.hour),
        ).where(GenerationData.source == "ELEXON")
        result = await db.execute(range_query)
        min_hour, max_hour = result.one()

        if not min_hour or not max_hour:
            print("No ELEXON data found in generation_data table.")
            await engine.dispose()
            return

        print(f"Date range: {min_hour} to {max_hour}")

        # Open CSV and write header
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "hour",
                "windfarm_id",
                "windfarm_code",
                "windfarm_name",
                "generation_unit_id",
                "unit_code",
                "unit_name",
                "fuel_type",
                "source",
                "generation_mwh",
                "metered_mwh",
                "curtailed_mwh",
                "capacity_mw",
                "capacity_factor",
            ])

            # Process month by month to avoid memory issues
            current = min_hour.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            end = max_hour

            while current <= end:
                next_month = current + relativedelta(months=1)

                print(f"  {current.strftime('%Y-%m')}...", end=" ", flush=True)

                query = (
                    select(GenerationData)
                    .where(
                        and_(
                            GenerationData.source == "ELEXON",
                            GenerationData.hour >= current,
                            GenerationData.hour < next_month,
                        )
                    )
                    .order_by(GenerationData.hour, GenerationData.windfarm_id)
                )

                result = await db.execute(query)
                rows = result.scalars().all()

                for row in rows:
                    wf = windfarms.get(row.windfarm_id, {})
                    gu = gen_units.get(row.generation_unit_id, {})
                    writer.writerow([
                        row.hour.strftime("%Y-%m-%d %H:%M:%S") if row.hour else "",
                        row.windfarm_id or "",
                        wf.get("code", ""),
                        wf.get("name", ""),
                        row.generation_unit_id or "",
                        gu.get("code", ""),
                        gu.get("name", ""),
                        gu.get("fuel_type", ""),
                        row.source,
                        float(row.generation_mwh) if row.generation_mwh is not None else "",
                        float(row.metered_mwh) if row.metered_mwh is not None else "",
                        float(row.curtailed_mwh) if row.curtailed_mwh is not None else "",
                        float(row.capacity_mw) if row.capacity_mw is not None else "",
                        float(row.capacity_factor) if row.capacity_factor is not None else "",
                    ])

                month_rows = len(rows)
                total_rows += month_rows
                print(f"{month_rows:,} rows")

                current = next_month

    await engine.dispose()

    print(f"\nExport complete: {output_file}")
    print(f"Total rows: {total_rows:,}")

    file_size = Path(output_file).stat().st_size
    if file_size > 1024 * 1024 * 1024:
        print(f"File size: {file_size / (1024 * 1024 * 1024):.2f} GB")
    elif file_size > 1024 * 1024:
        print(f"File size: {file_size / (1024 * 1024):.2f} MB")
    else:
        print(f"File size: {file_size / 1024:.2f} KB")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export ALL ELEXON hourly generation data")
    parser.add_argument(
        "--output",
        type=str,
        default="elexon_all_hourly_data.csv",
        help="Output CSV file path (default: elexon_all_hourly_data.csv)",
    )
    args = parser.parse_args()

    print("Exporting ALL ELEXON windfarm hourly data (all periods)")
    asyncio.run(export_all_elexon_hourly(args.output))
