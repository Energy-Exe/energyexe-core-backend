"""Check the import status of NVE generation data."""

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
    """Check the status of NVE data import."""
    
    print("="*80)
    print(" "*20 + "📊 NVE IMPORT STATUS 📊")
    print("="*80)
    
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        # Total NVE records
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'NVE')
        )
        total_records = result.scalar() or 0
        
        print(f"\n📈 Total NVE records: {total_records:,}")
        
        if total_records == 0:
            print("\n⚠️  No NVE data found in database")
            print("   Run the import script first:")
            print("   poetry run python scripts/seeds/generation_data/nve/import_parallel_optimized.py")
            return
        
        # Date range
        result = await db.execute(
            select(
                func.min(GenerationDataRaw.period_start),
                func.max(GenerationDataRaw.period_end)
            )
            .where(GenerationDataRaw.source == 'NVE')
        )
        min_date, max_date = result.first()
        
        print(f"\n📅 Date range:")
        print(f"   • Earliest: {min_date}")
        print(f"   • Latest: {max_date}")
        
        # Records by identifier (unit code)
        print(f"\n📊 Records by unit code (top 20):")
        
        result = await db.execute(
            select(
                GenerationDataRaw.identifier,
                func.count(GenerationDataRaw.id).label('count')
            )
            .where(GenerationDataRaw.source == 'NVE')
            .group_by(GenerationDataRaw.identifier)
            .order_by(func.count(GenerationDataRaw.id).desc())
            .limit(20)
        )
        
        unit_stats = []
        for row in result:
            unit_stats.append({
                'code': row.identifier,
                'count': row.count
            })
            
            # Get unit name from database
            unit_result = await db.execute(
                select(GenerationUnit.name)
                .where(GenerationUnit.code == row.identifier)
                .where(GenerationUnit.source == 'NVE')
            )
            unit_name = unit_result.scalar() or 'Unknown'
            
            print(f"   • {row.identifier:10} ({unit_name:30}): {row.count:,} records")
        
        # Sample data
        print(f"\n📝 Sample data (latest 5 records):")
        
        result = await db.execute(
            select(GenerationDataRaw)
            .where(GenerationDataRaw.source == 'NVE')
            .order_by(GenerationDataRaw.period_start.desc())
            .limit(5)
        )
        
        for idx, record in enumerate(result.scalars(), 1):
            print(f"\n   Record {idx}:")
            print(f"     • Period: {record.period_start} to {record.period_end}")
            print(f"     • Unit Code: {record.identifier}")
            print(f"     • Value: {record.value_extracted} {record.unit}")
            if record.data:
                print(f"     • Unit Name: {record.data.get('unit_name', 'N/A')}")
        
        # Data quality check
        print(f"\n🔍 Data quality:")
        
        # Check for zero generation
        result = await db.execute(
            text("""
                SELECT COUNT(*)
                FROM generation_data_raw
                WHERE source = 'NVE'
                AND value_extracted = 0
            """)
        )
        zero_generation = result.scalar() or 0
        
        print(f"   • Records with zero generation: {zero_generation:,} ({zero_generation/total_records*100:.1f}%)")
        
        # Check for negative values
        result = await db.execute(
            text("""
                SELECT COUNT(*)
                FROM generation_data_raw
                WHERE source = 'NVE'
                AND value_extracted < 0
            """)
        )
        negative_values = result.scalar() or 0
        
        if negative_values > 0:
            print(f"   ⚠️ Records with negative values: {negative_values:,}")
        
        # Check configured vs imported units
        result = await db.execute(
            select(func.count(GenerationUnit.id))
            .where(GenerationUnit.source == 'NVE')
        )
        total_configured = result.scalar() or 0
        
        # Count unique units with data
        result = await db.execute(
            select(func.count(func.distinct(GenerationDataRaw.identifier)))
            .where(GenerationDataRaw.source == 'NVE')
        )
        imported_units = result.scalar() or 0
        
        print(f"\n📊 Unit coverage:")
        print(f"   • Configured units: {total_configured}")
        print(f"   • Units with data: {imported_units}")
        if total_configured > 0:
            print(f"   • Coverage: {imported_units/total_configured*100:.1f}%")
        
        if imported_units < total_configured:
            # Find missing units
            result = await db.execute(
                select(GenerationDataRaw.identifier)
                .where(GenerationDataRaw.source == 'NVE')
                .distinct()
            )
            imported_codes = {row[0] for row in result}
            
            result = await db.execute(
                select(GenerationUnit.code, GenerationUnit.name)
                .where(GenerationUnit.source == 'NVE')
            )
            
            missing = []
            for row in result:
                if row.code not in imported_codes:
                    missing.append((row.code, row.name))
            
            if missing:
                print(f"\n⚠️  Units without data ({len(missing)}):")
                for code, name in missing[:10]:  # Show first 10
                    print(f"   • {code:10} - {name}")
                if len(missing) > 10:
                    print(f"   ... and {len(missing) - 10} more")
        
        # Statistics by year
        print(f"\n📅 Records by year:")
        
        result = await db.execute(
            text("""
                SELECT 
                    EXTRACT(YEAR FROM period_start) as year,
                    COUNT(*) as count,
                    AVG(value_extracted) as avg_generation
                FROM generation_data_raw
                WHERE source = 'NVE'
                GROUP BY EXTRACT(YEAR FROM period_start)
                ORDER BY year DESC
                LIMIT 10
            """)
        )
        
        for row in result:
            year = int(row.year) if row.year else 'Unknown'
            print(f"   • {year}: {row.count:,} records (avg: {row.avg_generation:.2f} MWh)")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    asyncio.run(check_import_status())