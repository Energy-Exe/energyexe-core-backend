"""Check the status of imported ELEXON data in the database."""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from sqlalchemy import select, func, text, and_


async def get_import_statistics() -> Dict[str, Any]:
    """Get comprehensive statistics about imported ELEXON data."""
    
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        stats = {}
        
        # 1. Total records
        total_result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'ELEXON')
        )
        stats['total_records'] = total_result.scalar() or 0
        
        # 2. Date range
        date_range_result = await db.execute(
            select(
                func.min(GenerationDataRaw.period_start),
                func.max(GenerationDataRaw.period_end)
            )
            .where(GenerationDataRaw.source == 'ELEXON')
        )
        min_date, max_date = date_range_result.first()
        stats['date_range'] = {
            'start': min_date,
            'end': max_date
        }
        
        # 3. Unique BMU IDs
        unique_bmus_result = await db.execute(
            select(func.count(func.distinct(GenerationDataRaw.identifier)))
            .where(GenerationDataRaw.source == 'ELEXON')
        )
        stats['unique_bmus'] = unique_bmus_result.scalar() or 0
        
        # 4. Records by year
        year_stats_result = await db.execute(
            text("""
                SELECT 
                    EXTRACT(YEAR FROM period_start) as year,
                    COUNT(*) as count,
                    COUNT(DISTINCT identifier) as unique_bmus
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                GROUP BY EXTRACT(YEAR FROM period_start)
                ORDER BY year
            """)
        )
        stats['by_year'] = [
            {'year': int(row.year) if row.year else None, 'count': row.count, 'unique_bmus': row.unique_bmus}
            for row in year_stats_result
        ]
        
        # 5. Records by BMU (top 10)
        bmu_stats_result = await db.execute(
            text("""
                SELECT 
                    identifier,
                    COUNT(*) as count,
                    MIN(period_start) as first_record,
                    MAX(period_end) as last_record,
                    AVG(value_extracted) as avg_value
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                GROUP BY identifier
                ORDER BY count DESC
                LIMIT 10
            """)
        )
        stats['top_bmus'] = [
            {
                'identifier': row.identifier,
                'count': row.count,
                'first_record': row.first_record,
                'last_record': row.last_record,
                'avg_value': float(row.avg_value) if row.avg_value else 0
            }
            for row in bmu_stats_result
        ]
        
        # 6. Table size
        size_result = await db.execute(
            text("""
                SELECT 
                    pg_size_pretty(pg_total_relation_size('generation_data_raw')) as total_size,
                    pg_size_pretty(pg_relation_size('generation_data_raw')) as table_size,
                    pg_size_pretty(pg_indexes_size('generation_data_raw')) as indexes_size
            """)
        )
        size_row = size_result.first()
        stats['storage'] = {
            'total_size': size_row.total_size if size_row else 'Unknown',
            'table_size': size_row.table_size if size_row else 'Unknown',
            'indexes_size': size_row.indexes_size if size_row else 'Unknown'
        }
        
        # 7. Recent imports (last 24 hours)
        recent_result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(
                and_(
                    GenerationDataRaw.source == 'ELEXON',
                    GenerationDataRaw.created_at >= func.now() - text("INTERVAL '24 hours'")
                )
            )
        )
        stats['recent_imports'] = recent_result.scalar() or 0
        
        # 8. Check for gaps (sample)
        gap_check_result = await db.execute(
            text("""
                WITH date_series AS (
                    SELECT 
                        identifier,
                        period_start,
                        LAG(period_end) OVER (PARTITION BY identifier ORDER BY period_start) as prev_end
                    FROM generation_data_raw
                    WHERE source = 'ELEXON'
                    LIMIT 10000
                )
                SELECT COUNT(*) as gaps
                FROM date_series
                WHERE period_start > prev_end + INTERVAL '30 minutes'
            """)
        )
        stats['data_gaps'] = gap_check_result.scalar() or 0
        
        # 9. BMU matching with generation_units
        matching_result = await db.execute(
            text("""
                SELECT 
                    COUNT(DISTINCT gdr.identifier) as matched_bmus
                FROM generation_data_raw gdr
                INNER JOIN generation_units gu ON gdr.identifier = gu.code
                WHERE gdr.source = 'ELEXON' AND gu.source = 'ELEXON'
            """)
        )
        stats['matched_bmus'] = matching_result.scalar() or 0
        
        return stats


async def display_statistics():
    """Display import statistics in a formatted way."""
    
    print("\n" + "="*80)
    print(" "*25 + "📊 ELEXON IMPORT STATUS 📊")
    print("="*80)
    
    print("\n⏳ Fetching statistics from database...")
    
    stats = await get_import_statistics()
    
    # Overall Statistics
    print("\n" + "="*80)
    print("📈 OVERALL STATISTICS")
    print("-"*80)
    print(f"  • Total Records: {stats['total_records']:,}")
    print(f"  • Unique BMU IDs: {stats['unique_bmus']:,}")
    print(f"  • BMUs matched with generation_units: {stats['matched_bmus']:,}")
    
    if stats['date_range']['start'] and stats['date_range']['end']:
        print(f"  • Date Range: {stats['date_range']['start'].strftime('%Y-%m-%d')} to {stats['date_range']['end'].strftime('%Y-%m-%d')}")
        days = (stats['date_range']['end'] - stats['date_range']['start']).days
        print(f"  • Coverage: {days:,} days")
    
    print(f"  • Recent imports (last 24h): {stats['recent_imports']:,}")
    print(f"  • Data gaps detected (sample): {stats['data_gaps']:,}")
    
    # Storage Information
    print("\n" + "="*80)
    print("💾 STORAGE INFORMATION")
    print("-"*80)
    print(f"  • Total Size: {stats['storage']['total_size']}")
    print(f"  • Table Size: {stats['storage']['table_size']}")
    print(f"  • Indexes Size: {stats['storage']['indexes_size']}")
    
    # Records by Year
    if stats['by_year']:
        print("\n" + "="*80)
        print("📅 RECORDS BY YEAR")
        print("-"*80)
        print(f"  {'Year':<8} {'Records':<15} {'Unique BMUs':<15} {'Avg/Day':<10}")
        print("  " + "-"*50)
        
        for year_data in stats['by_year']:
            if year_data['year']:
                avg_per_day = year_data['count'] / 365
                print(f"  {year_data['year']:<8} {year_data['count']:<15,} {year_data['unique_bmus']:<15} {avg_per_day:,.0f}")
    
    # Top BMUs by Record Count
    if stats['top_bmus']:
        print("\n" + "="*80)
        print("🏆 TOP 10 BMUs BY RECORD COUNT")
        print("-"*80)
        print(f"  {'BMU ID':<15} {'Records':<12} {'Avg MW':<10} {'First Record':<20} {'Last Record':<20}")
        print("  " + "-"*85)
        
        for bmu in stats['top_bmus']:
            first = bmu['first_record'].strftime('%Y-%m-%d %H:%M') if bmu['first_record'] else 'N/A'
            last = bmu['last_record'].strftime('%Y-%m-%d %H:%M') if bmu['last_record'] else 'N/A'
            print(f"  {bmu['identifier']:<15} {bmu['count']:<12,} {bmu['avg_value']:<10.2f} {first:<20} {last:<20}")
    
    # Summary
    print("\n" + "="*80)
    print("📋 SUMMARY")
    print("-"*80)
    
    if stats['total_records'] > 0:
        print(f"  ✅ Successfully imported {stats['total_records']:,} records")
        print(f"  ✅ Data from {stats['unique_bmus']} unique BMU IDs")
        
        if stats['by_year']:
            years = [y['year'] for y in stats['by_year'] if y['year']]
            if years:
                print(f"  ✅ Covering years {min(years)} to {max(years)}")
        
        if stats['matched_bmus'] < stats['unique_bmus']:
            print(f"  ⚠️  Only {stats['matched_bmus']} BMUs match generation_units table")
            print(f"     ({stats['unique_bmus'] - stats['matched_bmus']} BMUs in data but not in generation_units)")
        
        if stats['data_gaps'] > 0:
            print(f"  ⚠️  {stats['data_gaps']} potential data gaps detected (sample check)")
        
        # Calculate average records per BMU per day
        if stats['date_range']['start'] and stats['date_range']['end'] and stats['unique_bmus'] > 0:
            days = (stats['date_range']['end'] - stats['date_range']['start']).days
            if days > 0:
                avg_per_bmu_per_day = stats['total_records'] / (stats['unique_bmus'] * days)
                expected = 48  # 48 half-hour periods per day
                completeness = (avg_per_bmu_per_day / expected) * 100
                print(f"\n  📊 Data Completeness:")
                print(f"     • Average records per BMU per day: {avg_per_bmu_per_day:.1f}")
                print(f"     • Expected (48 half-hour periods): 48")
                print(f"     • Estimated completeness: {completeness:.1f}%")
    else:
        print("  ❌ No ELEXON data found in database")
        print("     Run the import script first: poetry run python import_parallel_optimized.py")
    
    print("\n" + "="*80)
    print(" "*30 + "✨ CHECK COMPLETE ✨")
    print("="*80)
    print()


if __name__ == "__main__":
    asyncio.run(display_statistics())