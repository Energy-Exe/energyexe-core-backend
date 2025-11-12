"""Check the import status of Energistyrelsen monthly generation data."""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
import json

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from sqlalchemy import select, func, text


async def check_import_status():
    """Check the status of Energistyrelsen data import."""
    
    print("="*80)
    print(" "*15 + "📊 ENERGISTYRELSEN IMPORT STATUS 📊")
    print("="*80)
    
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        # Total Energistyrelsen records
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'ENERGISTYRELSEN')
        )
        total_records = result.scalar() or 0
        
        print(f"\n📈 Total ENERGISTYRELSEN records: {total_records:,}")
        
        if total_records == 0:
            print("\n⚠️  No ENERGISTYRELSEN data found in database")
            print("   Run the import script first:")
            print("   poetry run python scripts/seeds/generation_data/energistyrelsen/import_parallel_optimized.py")
            return
        
        # Check period type
        result = await db.execute(
            select(
                GenerationDataRaw.period_type,
                func.count(GenerationDataRaw.id).label('count')
            )
            .where(GenerationDataRaw.source == 'ENERGISTYRELSEN')
            .group_by(GenerationDataRaw.period_type)
        )
        
        print(f"\n📅 Data by period type:")
        for row in result:
            print(f"   • {row.period_type}: {row.count:,} records")
        
        # Date range
        result = await db.execute(
            select(
                func.min(GenerationDataRaw.period_start),
                func.max(GenerationDataRaw.period_end)
            )
            .where(GenerationDataRaw.source == 'ENERGISTYRELSEN')
        )
        min_date, max_date = result.first()
        
        print(f"\n📅 Date range:")
        print(f"   • Earliest: {min_date}")
        print(f"   • Latest: {max_date}")
        
        if min_date and max_date:
            months_diff = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1
            print(f"   • Coverage: {months_diff} months")
        
        # Records by identifier (unit code)
        print(f"\n📊 Records by unit code (top 20):")
        
        result = await db.execute(
            select(
                GenerationDataRaw.identifier,
                func.count(GenerationDataRaw.id).label('count'),
                func.sum(GenerationDataRaw.value_extracted).label('total_mwh'),
                func.avg(GenerationDataRaw.value_extracted).label('avg_mwh')
            )
            .where(GenerationDataRaw.source == 'ENERGISTYRELSEN')
            .group_by(GenerationDataRaw.identifier)
            .order_by(func.sum(GenerationDataRaw.value_extracted).desc())
            .limit(20)
        )
        
        for row in result:
            # Get unit name from database
            unit_result = await db.execute(
                select(GenerationUnit.name)
                .where(GenerationUnit.code == row.identifier)
                .where(GenerationUnit.source == 'ENERGISTYRELSEN')
            )
            unit_name = unit_result.scalar() or 'Unknown'
            
            print(f"   • {row.identifier:15} ({unit_name:30}):")
            print(f"      - Records: {row.count:,}")
            print(f"      - Total: {row.total_mwh:,.0f} MWh")
            print(f"      - Avg/month: {row.avg_mwh:,.0f} MWh")
        
        # Statistics by year
        print(f"\n📅 Records by year:")
        
        result = await db.execute(
            text("""
                SELECT 
                    EXTRACT(YEAR FROM period_start) as year,
                    COUNT(*) as count,
                    COUNT(DISTINCT identifier) as unique_units,
                    SUM(value_extracted) as total_mwh,
                    AVG(value_extracted) as avg_mwh
                FROM generation_data_raw
                WHERE source = 'ENERGISTYRELSEN'
                GROUP BY EXTRACT(YEAR FROM period_start)
                ORDER BY year DESC
                LIMIT 10
            """)
        )
        
        for row in result:
            year = int(row.year) if row.year else 'Unknown'
            print(f"   • {year}:")
            print(f"      - Records: {row.count:,}")
            print(f"      - Units: {row.unique_units}")
            print(f"      - Total: {row.total_mwh:,.0f} MWh")
            print(f"      - Avg/month: {row.avg_mwh:,.0f} MWh")
        
        # Sample data
        print(f"\n📝 Sample data (latest 5 records):")
        
        result = await db.execute(
            select(GenerationDataRaw)
            .where(GenerationDataRaw.source == 'ENERGISTYRELSEN')
            .order_by(GenerationDataRaw.period_start.desc())
            .limit(5)
        )
        
        for idx, record in enumerate(result.scalars(), 1):
            print(f"\n   Record {idx}:")
            print(f"     • Period: {record.period_start.strftime('%Y-%m')} (monthly)")
            print(f"     • Unit Code: {record.identifier}")
            print(f"     • Value: {record.value_extracted:,.0f} {record.unit}")
            if record.data:
                print(f"     • Unit Name: {record.data.get('unit_name', 'N/A')}")
                print(f"     • GSRN: {record.data.get('gsrn', 'N/A')}")
        
        # Data quality check
        print(f"\n🔍 Data quality:")
        
        # Check for zero generation
        result = await db.execute(
            text("""
                SELECT COUNT(*)
                FROM generation_data_raw
                WHERE source = 'ENERGISTYRELSEN'
                AND value_extracted = 0
            """)
        )
        zero_generation = result.scalar() or 0
        
        if zero_generation > 0:
            print(f"   • Records with zero generation: {zero_generation:,} ({zero_generation/total_records*100:.1f}%)")
        
        # Check for negative values
        result = await db.execute(
            text("""
                SELECT COUNT(*)
                FROM generation_data_raw
                WHERE source = 'ENERGISTYRELSEN'
                AND value_extracted < 0
            """)
        )
        negative_values = result.scalar() or 0
        
        if negative_values > 0:
            print(f"   ⚠️ Records with negative values: {negative_values:,}")
        
        # Check configured vs imported units
        result = await db.execute(
            select(func.count(GenerationUnit.id))
            .where(GenerationUnit.source == 'ENERGISTYRELSEN')
        )
        total_configured = result.scalar() or 0
        
        # Count unique units with data
        result = await db.execute(
            select(func.count(func.distinct(GenerationDataRaw.identifier)))
            .where(GenerationDataRaw.source == 'ENERGISTYRELSEN')
        )
        imported_units = result.scalar() or 0
        
        print(f"\n📊 Unit coverage:")
        print(f"   • Configured units: {total_configured}")
        print(f"   • Units with data: {imported_units}")
        if total_configured > 0:
            print(f"   • Coverage: {imported_units/total_configured*100:.1f}%")
        
        # Monthly statistics
        print(f"\n📅 Monthly statistics:")
        
        result = await db.execute(
            text("""
                SELECT 
                    MIN(value_extracted) as min_mwh,
                    MAX(value_extracted) as max_mwh,
                    AVG(value_extracted) as avg_mwh,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value_extracted) as median_mwh
                FROM generation_data_raw
                WHERE source = 'ENERGISTYRELSEN'
                AND value_extracted > 0
            """)
        )
        
        stats = result.first()
        if stats:
            print(f"   • Min monthly generation: {stats.min_mwh:,.0f} MWh")
            print(f"   • Max monthly generation: {stats.max_mwh:,.0f} MWh")
            print(f"   • Avg monthly generation: {stats.avg_mwh:,.0f} MWh")
            print(f"   • Median monthly generation: {stats.median_mwh:,.0f} MWh")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    asyncio.run(check_import_status())