"""Check the import status of Taipower generation data."""

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
    """Check the status of Taipower data import."""
    
    print("="*80)
    print(" "*20 + "📊 TAIPOWER IMPORT STATUS 📊")
    print("="*80)
    
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        # Total Taipower records
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'TAIPOWER')
        )
        total_records = result.scalar() or 0
        
        print(f"\n📈 Total Taipower records: {total_records:,}")
        
        if total_records == 0:
            print("\n⚠️  No Taipower data found in database")
            print("   Run the import script first:")
            print("   poetry run python scripts/seeds/generation_data/taipower/import_parallel_optimized.py")
            return
        
        # Date range
        result = await db.execute(
            select(
                func.min(GenerationDataRaw.period_start),
                func.max(GenerationDataRaw.period_end)
            )
            .where(GenerationDataRaw.source == 'TAIPOWER')
        )
        min_date, max_date = result.first()
        
        print(f"\n📅 Date range:")
        print(f"   • Earliest: {min_date}")
        print(f"   • Latest: {max_date}")
        
        # Records by identifier (unit code)
        print(f"\n📊 Records by unit code:")
        
        result = await db.execute(
            select(
                GenerationDataRaw.identifier,
                func.count(GenerationDataRaw.id).label('count')
            )
            .where(GenerationDataRaw.source == 'TAIPOWER')
            .group_by(GenerationDataRaw.identifier)
            .order_by(func.count(GenerationDataRaw.id).desc())
        )
        
        unit_stats = []
        for row in result:
            unit_stats.append({
                'code': row.identifier,
                'count': row.count
            })
            print(f"   • {row.identifier:20}: {row.count:,} records")
        
        # Sample data
        print(f"\n📝 Sample data (first 5 records):")
        
        result = await db.execute(
            select(GenerationDataRaw)
            .where(GenerationDataRaw.source == 'TAIPOWER')
            .order_by(GenerationDataRaw.period_start.desc())
            .limit(5)
        )
        
        for idx, record in enumerate(result.scalars(), 1):
            print(f"\n   Record {idx}:")
            print(f"     • Period: {record.period_start} to {record.period_end}")
            print(f"     • Unit Code: {record.identifier}")
            print(f"     • Value: {record.value_extracted} {record.unit}")
            if record.data:
                print(f"     • Capacity: {record.data.get('installed_capacity_mw', 'N/A')} MW")
                print(f"     • Capacity Factor: {record.data.get('capacity_factor', 'N/A')}")
        
        # Data quality check
        print(f"\n🔍 Data quality:")
        
        # Check for nulls in generation data
        result = await db.execute(
            text("""
                SELECT COUNT(*)
                FROM generation_data_raw
                WHERE source = 'Taipower'
                AND (data->>'generation_mw')::float = 0
            """)
        )
        zero_generation = result.scalar() or 0
        
        print(f"   • Records with zero generation: {zero_generation:,} ({zero_generation/total_records*100:.1f}%)")
        
        # Check configured vs imported units
        result = await db.execute(
            select(func.count(GenerationUnit.id))
            .where(GenerationUnit.source == 'TAIPOWER')
        )
        total_configured = result.scalar() or 0
        
        imported_units = len(unit_stats)
        
        print(f"\n📊 Unit coverage:")
        print(f"   • Configured units: {total_configured}")
        print(f"   • Units with data: {imported_units}")
        if total_configured > 0:
            print(f"   • Coverage: {imported_units/total_configured*100:.1f}%")
        else:
            print(f"   • Coverage: N/A (no configured units)")
        
        if imported_units < total_configured:
            # Find missing units
            result = await db.execute(
                select(GenerationDataRaw.identifier)
                .where(GenerationDataRaw.source == 'TAIPOWER')
                .distinct()
            )
            imported_codes = {row[0] for row in result}
            
            result = await db.execute(
                select(GenerationUnit.code, GenerationUnit.name)
                .where(GenerationUnit.source == 'TAIPOWER')
            )
            
            missing = []
            for row in result:
                if row.code not in imported_codes:
                    missing.append((row.code, row.name))
            
            if missing:
                print(f"\n⚠️  Units without data ({len(missing)}):")
                for code, name in missing:
                    print(f"   • {code:20} - {name}")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    asyncio.run(check_import_status())