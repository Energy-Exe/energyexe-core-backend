"""Script to export ELEXON hourly generation data to CSV."""

import asyncio
import csv
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from dateutil.relativedelta import relativedelta

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.models.generation_data import GenerationData
from app.models.windfarm import Windfarm
from app.core.config import get_settings


async def export_elexon_hourly(
    start_date: date,
    end_date: date,
    output_file: str,
):
    """Export ELEXON hourly generation data to CSV, processing month by month."""

    settings = get_settings()

    # Create async engine
    engine = create_async_engine(str(settings.DATABASE_URL), echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    total_rows = 0

    async with async_session() as db:
        # First, get all ELEXON windfarm IDs
        windfarm_query = select(Windfarm.id, Windfarm.code, Windfarm.name).where(
            Windfarm.id.in_(
                select(GenerationData.windfarm_id).where(
                    GenerationData.source == 'ELEXON'
                ).distinct()
            )
        )
        result = await db.execute(windfarm_query)
        windfarms = {row.id: {'code': row.code, 'name': row.name} for row in result.all()}

        print(f"Found {len(windfarms)} ELEXON windfarms")

        # Open file and write header
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'hour',
                'windfarm_id',
                'windfarm_code',
                'windfarm_name',
                'source',
                'generation_mwh',
                'capacity_factor',
            ])

            # Process month by month
            current_start = start_date
            while current_start <= end_date:
                # Calculate month end
                current_end = min(
                    current_start + relativedelta(months=1) - timedelta(days=1),
                    end_date
                )

                # Convert dates to datetime for query
                start_dt = datetime.combine(current_start, datetime.min.time())
                end_dt = datetime.combine(current_end, datetime.max.time())

                print(f"Processing {current_start.strftime('%Y-%m')}...", end=' ', flush=True)

                # Query generation data for this month
                query = select(GenerationData).where(
                    and_(
                        GenerationData.source == 'ELEXON',
                        GenerationData.hour >= start_dt,
                        GenerationData.hour <= end_dt,
                    )
                ).order_by(GenerationData.hour, GenerationData.windfarm_id)

                result = await db.execute(query)
                rows = result.scalars().all()

                # Write rows
                for row in rows:
                    wf = windfarms.get(row.windfarm_id, {})
                    writer.writerow([
                        row.hour.strftime('%Y-%m-%d %H:%M:%S'),
                        row.windfarm_id,
                        wf.get('code', ''),
                        wf.get('name', ''),
                        row.source,
                        float(row.generation_mwh) if row.generation_mwh else '',
                        float(row.capacity_factor) if row.capacity_factor else '',
                    ])

                print(f"{len(rows)} rows")
                total_rows += len(rows)

                # Move to next month
                current_start = current_start + relativedelta(months=1)

    await engine.dispose()

    print(f"\nExport complete: {output_file}")
    print(f"Total rows: {total_rows:,}")

    # Show file size
    file_size = Path(output_file).stat().st_size
    if file_size > 1024 * 1024 * 1024:
        print(f"File size: {file_size / (1024 * 1024 * 1024):.2f} GB")
    elif file_size > 1024 * 1024:
        print(f"File size: {file_size / (1024 * 1024):.2f} MB")
    else:
        print(f"File size: {file_size / 1024:.2f} KB")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Export ELEXON hourly generation data')
    parser.add_argument('--start', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, default='elexon_hourly_export.csv', help='Output file path')

    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end, '%Y-%m-%d').date()

    print(f"Exporting ELEXON hourly data from {start_date} to {end_date}")
    asyncio.run(export_elexon_hourly(start_date, end_date, args.output))
