"""Check EIA import status and data quality."""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession


async def check_eia_import_status():
    """Check the status of EIA data import."""

    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        print("\n" + "="*80)
        print(" "*25 + "📊 EIA IMPORT STATUS 📊")
        print("="*80)

        # 1. Total EIA records
        print("\n1️⃣  Total Records:")
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'EIA')
        )
        total_count = result.scalar() or 0
        print(f"   Total EIA records: {total_count:,}")

        if total_count == 0:
            print("\n   ⚠️  No EIA data found. Run import script first.")
            return

        # 2. Date range
        print("\n2️⃣  Date Range:")
        result = await db.execute(
            select(
                func.min(GenerationDataRaw.period_start),
                func.max(GenerationDataRaw.period_start)
            )
            .where(GenerationDataRaw.source == 'EIA')
        )
        date_range = result.first()
        if date_range and date_range[0]:
            print(f"   From: {date_range[0].strftime('%Y-%m')}")
            print(f"   To:   {date_range[1].strftime('%Y-%m')}")

            # Calculate span
            years = (date_range[1].year - date_range[0].year)
            months = (date_range[1].month - date_range[0].month)
            total_months = years * 12 + months
            print(f"   Span: {years} years, {months} months ({total_months} months)")

        # 3. Records by year
        print("\n3️⃣  Records by Year:")
        result = await db.execute(
            text("""
                SELECT
                    EXTRACT(YEAR FROM period_start) as year,
                    COUNT(*) as count,
                    COUNT(DISTINCT identifier) as unique_plants
                FROM generation_data_raw
                WHERE source = 'EIA'
                GROUP BY EXTRACT(YEAR FROM period_start)
                ORDER BY year
            """)
        )

        year_stats = result.fetchall()
        total_by_year = 0
        for year, count, plants in year_stats:
            print(f"   {int(year)}: {count:>6,} records ({plants:>3} plants)")
            total_by_year += count

        # 4. Unique plants
        print("\n4️⃣  Unique Plants:")
        result = await db.execute(
            select(func.count(func.distinct(GenerationDataRaw.identifier)))
            .where(GenerationDataRaw.source == 'EIA')
        )
        unique_plants = result.scalar() or 0
        print(f"   Unique plant IDs: {unique_plants}")

        # 5. Configured vs imported
        print("\n5️⃣  Configuration Status:")

        # Get configured units
        result = await db.execute(
            select(func.count(GenerationUnit.id))
            .where(GenerationUnit.source == 'EIA')
        )
        configured_units = result.scalar() or 0
        print(f"   Configured generation units: {configured_units}")

        # Get imported plants
        result = await db.execute(
            select(func.count(func.distinct(GenerationDataRaw.identifier)))
            .where(GenerationDataRaw.source == 'EIA')
        )
        imported_plants = result.scalar() or 0
        print(f"   Plants with data: {imported_plants}")

        if configured_units > 0:
            coverage = (imported_plants / configured_units) * 100
            print(f"   Coverage: {coverage:.1f}%")

        # 6. Plants with most data
        print("\n6️⃣  Top 10 Plants by Record Count:")
        result = await db.execute(
            text("""
                SELECT
                    identifier,
                    COUNT(*) as record_count,
                    MIN(period_start) as first_date,
                    MAX(period_start) as last_date
                FROM generation_data_raw
                WHERE source = 'EIA'
                GROUP BY identifier
                ORDER BY record_count DESC
                LIMIT 10
            """)
        )

        top_plants = result.fetchall()
        for plant_id, count, first_date, last_date in top_plants:
            years = last_date.year - first_date.year
            print(f"   Plant {plant_id}: {count:>4} records ({first_date.strftime('%Y-%m')} to {last_date.strftime('%Y-%m')}, {years} years)")

        # 7. Value statistics
        print("\n7️⃣  Generation Statistics (MWh):")
        result = await db.execute(
            text("""
                SELECT
                    COUNT(*) as count,
                    AVG(value_extracted) as avg_value,
                    MIN(value_extracted) as min_value,
                    MAX(value_extracted) as max_value,
                    SUM(value_extracted) as total_value
                FROM generation_data_raw
                WHERE source = 'EIA' AND value_extracted > 0
            """)
        )

        stats = result.first()
        if stats:
            print(f"   Records with data: {stats[0]:,}")
            print(f"   Average monthly generation: {stats[1]:.2f} MWh")
            print(f"   Min monthly generation: {stats[2]:.2f} MWh")
            print(f"   Max monthly generation: {stats[3]:.2f} MWh")
            print(f"   Total generation: {stats[4]:,.2f} MWh ({stats[4]/1000:.2f} GWh)")

        # 8. Monthly coverage
        print("\n8️⃣  Monthly Coverage:")
        result = await db.execute(
            text("""
                SELECT
                    TO_CHAR(period_start, 'YYYY-MM') as month,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT identifier) as plant_count
                FROM generation_data_raw
                WHERE source = 'EIA'
                GROUP BY TO_CHAR(period_start, 'YYYY-MM')
                ORDER BY month DESC
                LIMIT 12
            """)
        )

        monthly_coverage = result.fetchall()
        print("   Last 12 months:")
        for month, records, plants in monthly_coverage:
            print(f"   {month}: {records:>4} records ({plants:>3} plants)")

        # 9. Data quality checks
        print("\n9️⃣  Data Quality:")

        # Check for zero values
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'EIA')
            .where(GenerationDataRaw.value_extracted == 0)
        )
        zero_count = result.scalar() or 0
        print(f"   Records with zero generation: {zero_count:,} ({zero_count/total_count*100:.1f}%)")

        # Check for null values
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'EIA')
            .where(GenerationDataRaw.value_extracted.is_(None))
        )
        null_count = result.scalar() or 0
        print(f"   Records with null values: {null_count:,}")

        # Check period_type
        result = await db.execute(
            select(GenerationDataRaw.period_type, func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'EIA')
            .group_by(GenerationDataRaw.period_type)
        )
        period_types = result.fetchall()
        print(f"   Period types:")
        for period_type, count in period_types:
            print(f"     - {period_type}: {count:,} records")

        # 10. Sample records
        print("\n🔟 Sample Records:")
        result = await db.execute(
            select(GenerationDataRaw)
            .where(GenerationDataRaw.source == 'EIA')
            .where(GenerationDataRaw.value_extracted > 0)
            .order_by(GenerationDataRaw.period_start.desc())
            .limit(3)
        )

        samples = result.scalars().all()
        for i, record in enumerate(samples, 1):
            print(f"\n   Sample {i}:")
            print(f"     Plant ID: {record.identifier}")
            print(f"     Period: {record.period_start.strftime('%Y-%m')} to {record.period_end.strftime('%Y-%m')}")
            print(f"     Generation: {record.value_extracted:,.2f} {record.unit}")
            if record.data:
                import json
                data = json.loads(record.data)
                if 'plant_name' in data:
                    print(f"     Plant Name: {data['plant_name']}")

        print("\n" + "="*80)
        print(" "*25 + "✅ STATUS CHECK COMPLETE ✅")
        print("="*80)
        print()


if __name__ == "__main__":
    asyncio.run(check_eia_import_status())
