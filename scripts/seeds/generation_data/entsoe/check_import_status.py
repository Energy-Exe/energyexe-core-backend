"""Check the status of imported ENTSOE data in the database."""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw
from app.models.generation_unit import GenerationUnit
from sqlalchemy import select, func, text, and_


async def get_import_statistics() -> Dict[str, Any]:
    """Get comprehensive statistics about imported ENTSOE data."""
    
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        stats = {}
        
        # 1. Total records
        total_result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'ENTSOE')
        )
        stats['total_records'] = total_result.scalar() or 0
        
        # 2. Date range
        date_range_result = await db.execute(
            select(
                func.min(GenerationDataRaw.period_start),
                func.max(GenerationDataRaw.period_end)
            )
            .where(GenerationDataRaw.source == 'ENTSOE')
        )
        min_date, max_date = date_range_result.first()
        stats['date_range'] = {
            'start': min_date,
            'end': max_date
        }
        
        # 3. Unique generation units
        unique_units_result = await db.execute(
            select(func.count(func.distinct(GenerationDataRaw.identifier)))
            .where(GenerationDataRaw.source == 'ENTSOE')
        )
        stats['unique_units'] = unique_units_result.scalar() or 0
        
        # 4. Resolution breakdown
        resolution_result = await db.execute(
            select(
                GenerationDataRaw.period_type,
                func.count(GenerationDataRaw.id)
            )
            .where(GenerationDataRaw.source == 'ENTSOE')
            .group_by(GenerationDataRaw.period_type)
        )
        stats['by_resolution'] = {
            row[0]: row[1] for row in resolution_result
        }
        
        # 5. Records by year
        year_stats_result = await db.execute(
            text("""
                SELECT 
                    EXTRACT(YEAR FROM period_start) as year,
                    COUNT(*) as count,
                    COUNT(DISTINCT identifier) as unique_units,
                    AVG(value_extracted) as avg_output
                FROM generation_data_raw
                WHERE source = 'ENTSOE'
                GROUP BY EXTRACT(YEAR FROM period_start)
                ORDER BY year
            """)
        )
        stats['by_year'] = [
            {
                'year': int(row.year) if row.year else None,
                'count': row.count,
                'unique_units': row.unique_units,
                'avg_output': float(row.avg_output) if row.avg_output else 0
            }
            for row in year_stats_result
        ]
        
        # 6. Top generation units by record count
        unit_stats_result = await db.execute(
            text("""
                SELECT 
                    gdr.identifier,
                    gu.name as unit_name,
                    COUNT(*) as count,
                    MIN(gdr.period_start) as first_record,
                    MAX(gdr.period_end) as last_record,
                    AVG(gdr.value_extracted) as avg_output,
                    MAX(gdr.value_extracted) as max_output
                FROM generation_data_raw gdr
                LEFT JOIN generation_units gu ON gdr.identifier = gu.code
                WHERE gdr.source = 'ENTSOE'
                GROUP BY gdr.identifier, gu.name
                ORDER BY count DESC
                LIMIT 10
            """)
        )
        stats['top_units'] = [
            {
                'identifier': row.identifier,
                'name': row.unit_name or 'Unknown',
                'count': row.count,
                'first_record': row.first_record,
                'last_record': row.last_record,
                'avg_output': float(row.avg_output) if row.avg_output else 0,
                'max_output': float(row.max_output) if row.max_output else 0
            }
            for row in unit_stats_result
        ]
        
        # 7. Countries/Areas
        area_result = await db.execute(
            text("""
                SELECT 
                    data->>'area_display_name' as area,
                    COUNT(*) as count
                FROM generation_data_raw
                WHERE source = 'ENTSOE' AND data->>'area_display_name' IS NOT NULL
                GROUP BY data->>'area_display_name'
                ORDER BY count DESC
            """)
        )
        stats['by_area'] = {
            row.area: row.count for row in area_result
        }
        
        # 8. Recent imports (last 24 hours)
        recent_result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(
                and_(
                    GenerationDataRaw.source == 'ENTSOE',
                    GenerationDataRaw.created_at >= func.now() - text("INTERVAL '24 hours'")
                )
            )
        )
        stats['recent_imports'] = recent_result.scalar() or 0
        
        # 9. Generation unit matching
        matching_result = await db.execute(
            text("""
                SELECT 
                    COUNT(DISTINCT gdr.identifier) as matched_units
                FROM generation_data_raw gdr
                INNER JOIN generation_units gu ON gdr.identifier = gu.code
                WHERE gdr.source = 'ENTSOE' AND gu.source = 'ENTSOE'
            """)
        )
        stats['matched_units'] = matching_result.scalar() or 0
        
        return stats


async def display_statistics():
    """Display import statistics in a formatted way."""
    
    print("\n" + "="*80)
    print(" "*25 + "📊 ENTSOE IMPORT STATUS 📊")
    print("="*80)
    
    print("\n⏳ Fetching statistics from database...")
    
    stats = await get_import_statistics()
    
    # Overall Statistics
    print("\n" + "="*80)
    print("📈 OVERALL STATISTICS")
    print("-"*80)
    print(f"  • Total Records: {stats['total_records']:,}")
    print(f"  • Unique Generation Units: {stats['unique_units']:,}")
    print(f"  • Units matched with generation_units table: {stats['matched_units']:,}")
    
    if stats['date_range']['start'] and stats['date_range']['end']:
        print(f"  • Date Range: {stats['date_range']['start'].strftime('%Y-%m-%d')} to {stats['date_range']['end'].strftime('%Y-%m-%d')}")
        days = (stats['date_range']['end'] - stats['date_range']['start']).days
        print(f"  • Coverage: {days:,} days")
    
    print(f"  • Recent imports (last 24h): {stats['recent_imports']:,}")
    
    # Resolution Breakdown
    if stats['by_resolution']:
        print("\n" + "="*80)
        print("⏰ DATA RESOLUTION")
        print("-"*80)
        for resolution, count in stats['by_resolution'].items():
            res_label = "Hourly" if resolution == "PT60M" else "15-minute" if resolution == "PT15M" else resolution
            print(f"  • {res_label}: {count:,} records")
    
    # Countries/Areas
    if stats['by_area']:
        print("\n" + "="*80)
        print("🌍 RECORDS BY AREA")
        print("-"*80)
        for area, count in sorted(stats['by_area'].items(), key=lambda x: x[1], reverse=True):
            print(f"  • {area}: {count:,} records")
    
    # Records by Year
    if stats['by_year']:
        print("\n" + "="*80)
        print("📅 RECORDS BY YEAR")
        print("-"*80)
        print(f"  {'Year':<8} {'Records':<15} {'Units':<10} {'Avg MW':<10}")
        print("  " + "-"*45)
        
        for year_data in stats['by_year']:
            if year_data['year']:
                print(f"  {year_data['year']:<8} {year_data['count']:<15,} {year_data['unique_units']:<10} {year_data['avg_output']:<10.1f}")
    
    # Top Generation Units
    if stats['top_units']:
        print("\n" + "="*80)
        print("🏆 TOP 10 GENERATION UNITS BY RECORD COUNT")
        print("-"*80)
        print(f"  {'Unit Name':<35} {'Records':<12} {'Avg MW':<10} {'Max MW':<10}")
        print("  " + "-"*70)
        
        for unit in stats['top_units']:
            name = unit['name'][:35]
            print(f"  {name:<35} {unit['count']:<12,} {unit['avg_output']:<10.1f} {unit['max_output']:<10.1f}")
    
    # Summary
    print("\n" + "="*80)
    print("📋 SUMMARY")
    print("-"*80)
    
    if stats['total_records'] > 0:
        print(f"  ✅ Successfully imported {stats['total_records']:,} records")
        print(f"  ✅ Data from {stats['unique_units']} unique generation units")
        
        if stats['by_year']:
            years = [y['year'] for y in stats['by_year'] if y['year']]
            if years:
                print(f"  ✅ Covering years {min(years)} to {max(years)}")
        
        if stats['matched_units'] < stats['unique_units']:
            print(f"  ⚠️  Only {stats['matched_units']} units match generation_units table")
            print(f"     ({stats['unique_units'] - stats['matched_units']} units in data but not configured)")
        
        # Calculate data completeness
        if stats['date_range']['start'] and stats['date_range']['end'] and stats['unique_units'] > 0:
            days = (stats['date_range']['end'] - stats['date_range']['start']).days
            if days > 0:
                # Assume hourly data as primary resolution
                expected_hourly = stats['unique_units'] * days * 24
                completeness = (stats['total_records'] / expected_hourly) * 100
                print(f"\n  📊 Data Completeness (assuming hourly):")
                print(f"     • Expected records: {expected_hourly:,}")
                print(f"     • Actual records: {stats['total_records']:,}")
                print(f"     • Estimated completeness: {completeness:.1f}%")
    else:
        print("  ❌ No ENTSOE data found in database")
        print("     Run the import script first: poetry run python import_parallel_optimized.py")
    
    print("\n" + "="*80)
    print(" "*30 + "✨ CHECK COMPLETE ✨")
    print("="*80)
    print()


if __name__ == "__main__":
    asyncio.run(display_statistics())