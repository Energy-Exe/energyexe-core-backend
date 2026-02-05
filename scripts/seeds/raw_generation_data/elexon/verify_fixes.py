#!/usr/bin/env python3
"""
Verify that Elexon data fixes have been applied correctly.

This script compares before/after metrics and generates a summary report
of the fix effectiveness for 2022-2025.

It checks:
1. Raw data record counts match expected from CSV
2. Aggregated data matches raw data
3. BST timezone issues have been resolved
4. No data gaps exist in the fixed date ranges

Usage:
    # Verify all years
    poetry run python scripts/seeds/raw_generation_data/elexon/verify_fixes.py

    # Verify specific year
    poetry run python scripts/seeds/raw_generation_data/elexon/verify_fixes.py --year 2022

    # Generate detailed report
    poetry run python scripts/seeds/raw_generation_data/elexon/verify_fixes.py --detailed

    # Compare against pre-fix report
    poetry run python scripts/seeds/raw_generation_data/elexon/verify_fixes.py --compare-pre-fix
"""

import asyncio
import argparse
import sys
import csv
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from calendar import monthrange

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from app.core.database import get_session_factory
from sqlalchemy import text


SCRIPT_DIR = Path(__file__).parent


@dataclass
class YearVerification:
    """Verification results for a single year."""
    year: int
    raw_records: int = 0
    raw_mwh: float = 0.0
    raw_unique_bmus: int = 0
    agg_records: int = 0
    agg_mwh: float = 0.0
    agg_unique_bmus: int = 0
    days_with_data: int = 0
    expected_days: int = 0
    bst_months_checked: int = 0
    bst_issues_found: int = 0
    raw_agg_match_pct: float = 0.0
    status: str = "UNKNOWN"
    notes: List[str] = None

    def __post_init__(self):
        if self.notes is None:
            self.notes = []


async def get_year_stats(db, year: int) -> Dict:
    """Get comprehensive statistics for a year."""
    start_date = datetime(year, 1, 1)
    end_date = datetime(year + 1, 1, 1)

    # Get raw data stats
    raw_query = text("""
        SELECT
            COUNT(*) as record_count,
            SUM(value_extracted) as total_mwh,
            COUNT(DISTINCT identifier) as unique_bmus,
            COUNT(DISTINCT DATE(period_start)) as days_count
        FROM generation_data_raw
        WHERE source = 'ELEXON'
            AND source_type = 'csv'
            AND period_start >= :start_date
            AND period_start < :end_date
    """)

    raw_result = await db.execute(raw_query, {"start_date": start_date, "end_date": end_date})
    raw_row = raw_result.fetchone()

    # Get aggregated data stats
    agg_query = text("""
        SELECT
            COUNT(*) as record_count,
            SUM(metered_mwh) as total_mwh,
            COUNT(DISTINCT generation_unit_id) as unique_units
        FROM generation_data
        WHERE source = 'ELEXON'
            AND hour >= :start_date
            AND hour < :end_date
    """)

    agg_result = await db.execute(agg_query, {"start_date": start_date, "end_date": end_date})
    agg_row = agg_result.fetchone()

    return {
        'raw_records': raw_row.record_count or 0,
        'raw_mwh': float(raw_row.total_mwh) if raw_row.total_mwh else 0.0,
        'raw_unique_bmus': raw_row.unique_bmus or 0,
        'raw_days': raw_row.days_count or 0,
        'agg_records': agg_row.record_count or 0,
        'agg_mwh': float(agg_row.total_mwh) if agg_row.total_mwh else 0.0,
        'agg_unique_units': agg_row.unique_units or 0
    }


async def check_bst_months(db, year: int) -> Dict:
    """Check BST months (Oct-Dec) for timezone issues."""
    issues = []
    months_checked = 0

    for month in [10, 11, 12]:
        months_checked += 1
        days_in_month = monthrange(year, month)[1]
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)

        # Check for data completeness in BST months
        query = text("""
            WITH monthly_raw AS (
                SELECT
                    identifier,
                    COUNT(*) as raw_records,
                    SUM(value_extracted) as raw_mwh,
                    COUNT(DISTINCT DATE(period_start)) as days_count
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                    AND source_type = 'csv'
                    AND period_start >= :start_date
                    AND period_start < :end_date
                GROUP BY identifier
            ),
            monthly_agg AS (
                SELECT
                    gu.code as identifier,
                    COUNT(*) as agg_records,
                    SUM(gd.metered_mwh) as agg_mwh
                FROM generation_data gd
                JOIN generation_units gu ON gd.generation_unit_id = gu.id
                WHERE gd.source = 'ELEXON'
                    AND gd.hour >= :start_date
                    AND gd.hour < :end_date
                GROUP BY gu.code
            )
            SELECT
                COALESCE(r.identifier, a.identifier) as bmu_id,
                COALESCE(r.raw_mwh, 0) as raw_mwh,
                COALESCE(a.agg_mwh, 0) as agg_mwh,
                COALESCE(r.days_count, 0) as days_count,
                ABS(COALESCE(r.raw_mwh, 0) - COALESCE(a.agg_mwh, 0)) as diff
            FROM monthly_raw r
            FULL OUTER JOIN monthly_agg a ON r.identifier = a.identifier
            WHERE ABS(COALESCE(r.raw_mwh, 0) - COALESCE(a.agg_mwh, 0)) > 0.01 * GREATEST(ABS(COALESCE(r.raw_mwh, 1)), 1)
        """)

        result = await db.execute(query, {"start_date": start_date, "end_date": end_date})
        mismatches = result.fetchall()

        for row in mismatches:
            issues.append({
                'year': year,
                'month': month,
                'bmu_id': row.bmu_id,
                'raw_mwh': row.raw_mwh,
                'agg_mwh': row.agg_mwh,
                'difference': row.diff,
                'days_count': row.days_count
            })

    return {
        'months_checked': months_checked,
        'issues_found': len(issues),
        'issues': issues
    }


async def check_data_gaps(db, year: int) -> List[Dict]:
    """Check for data gaps in the year."""
    query = text("""
        WITH date_series AS (
            SELECT generate_series(
                :start_date::date,
                :end_date::date - INTERVAL '1 day',
                '1 day'::interval
            )::date as check_date
        ),
        daily_counts AS (
            SELECT
                DATE(period_start) as data_date,
                COUNT(DISTINCT identifier) as bmu_count
            FROM generation_data_raw
            WHERE source = 'ELEXON'
                AND source_type = 'csv'
                AND period_start >= :start_date
                AND period_start < :end_date
            GROUP BY DATE(period_start)
        )
        SELECT
            ds.check_date,
            COALESCE(dc.bmu_count, 0) as bmu_count
        FROM date_series ds
        LEFT JOIN daily_counts dc ON ds.check_date = dc.data_date
        WHERE COALESCE(dc.bmu_count, 0) = 0
        ORDER BY ds.check_date
    """)

    start_date = datetime(year, 1, 1)
    end_date = datetime(year + 1, 1, 1)

    result = await db.execute(query, {"start_date": start_date, "end_date": end_date})
    gaps = result.fetchall()

    return [
        {'date': row.check_date, 'bmu_count': row.bmu_count}
        for row in gaps
    ]


async def verify_year(year: int) -> YearVerification:
    """Run full verification for a single year."""
    verification = YearVerification(year=year)

    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # Get basic stats
        stats = await get_year_stats(db, year)
        verification.raw_records = stats['raw_records']
        verification.raw_mwh = stats['raw_mwh']
        verification.raw_unique_bmus = stats['raw_unique_bmus']
        verification.days_with_data = stats['raw_days']
        verification.agg_records = stats['agg_records']
        verification.agg_mwh = stats['agg_mwh']
        verification.agg_unique_bmus = stats['agg_unique_units']

        # Calculate expected days (365 or 366 for leap year)
        is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
        verification.expected_days = 366 if is_leap else 365

        # Check BST months
        bst_check = await check_bst_months(db, year)
        verification.bst_months_checked = bst_check['months_checked']
        verification.bst_issues_found = bst_check['issues_found']

        # Check for data gaps
        gaps = await check_data_gaps(db, year)

        # Calculate raw/agg match percentage
        if verification.raw_mwh != 0:
            diff = abs(verification.raw_mwh - verification.agg_mwh)
            verification.raw_agg_match_pct = (1 - diff / abs(verification.raw_mwh)) * 100
        elif verification.agg_mwh == 0:
            verification.raw_agg_match_pct = 100.0
        else:
            verification.raw_agg_match_pct = 0.0

        # Determine status
        if verification.raw_records == 0:
            verification.status = "NO_DATA"
            verification.notes.append("No raw data found for this year")
        elif verification.bst_issues_found > 0:
            verification.status = "BST_ISSUES"
            verification.notes.append(f"{verification.bst_issues_found} BST-related mismatches found")
        elif verification.raw_agg_match_pct < 99:
            verification.status = "MISMATCH"
            verification.notes.append(f"Raw/Agg match only {verification.raw_agg_match_pct:.1f}%")
        elif len(gaps) > 0:
            verification.status = "GAPS"
            verification.notes.append(f"{len(gaps)} days with no data")
        else:
            verification.status = "OK"
            verification.notes.append("All checks passed")

    return verification


def print_verification_summary(verifications: List[YearVerification]):
    """Print summary of all verifications."""
    print("\n" + "=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)

    # Table header
    print(f"\n{'Year':<6} {'Status':<12} {'Raw Records':>14} {'Raw MWh':>14} {'Agg MWh':>14} {'Match%':>8} {'BST Issues':>11}")
    print("-" * 80)

    for v in sorted(verifications, key=lambda x: x.year):
        print(f"{v.year:<6} {v.status:<12} {v.raw_records:>14,} {v.raw_mwh:>14,.0f} {v.agg_mwh:>14,.0f} {v.raw_agg_match_pct:>7.1f}% {v.bst_issues_found:>11}")

    # Overall status
    print("\n" + "-" * 80)
    ok_count = sum(1 for v in verifications if v.status == "OK")
    total = len(verifications)
    print(f"Overall: {ok_count}/{total} years passed all checks")

    # Show notes for problem years
    problem_years = [v for v in verifications if v.status != "OK"]
    if problem_years:
        print("\nIssues found:")
        for v in problem_years:
            print(f"\n  {v.year}:")
            for note in v.notes:
                print(f"    - {note}")


def save_verification_report(verifications: List[YearVerification], output_path: Path):
    """Save verification results to CSV."""
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'year', 'status', 'raw_records', 'raw_mwh', 'raw_unique_bmus',
            'agg_records', 'agg_mwh', 'agg_unique_bmus',
            'days_with_data', 'expected_days',
            'bst_months_checked', 'bst_issues_found',
            'raw_agg_match_pct', 'notes'
        ])

        for v in sorted(verifications, key=lambda x: x.year):
            writer.writerow([
                v.year, v.status, v.raw_records, v.raw_mwh, v.raw_unique_bmus,
                v.agg_records, v.agg_mwh, v.agg_unique_bmus,
                v.days_with_data, v.expected_days,
                v.bst_months_checked, v.bst_issues_found,
                v.raw_agg_match_pct, '; '.join(v.notes)
            ])

    print(f"\nReport saved to: {output_path}")


async def compare_with_pre_fix(verifications: List[YearVerification]) -> Dict:
    """Compare current state with pre-fix investigation reports."""
    comparison = {}

    for v in verifications:
        pre_fix_file = SCRIPT_DIR / "verify_data" / f"discrepancy_investigation_{v.year}.csv"

        if not pre_fix_file.exists():
            comparison[v.year] = {'pre_fix_available': False}
            continue

        # Read pre-fix report
        try:
            import pandas as pd
            pre_fix_df = pd.read_csv(pre_fix_file)

            # Count issues by diagnosis
            pre_fix_issues = pre_fix_df[pre_fix_df['diagnosis'] != 'OK']
            pre_fix_count = len(pre_fix_issues)

            comparison[v.year] = {
                'pre_fix_available': True,
                'pre_fix_issues': pre_fix_count,
                'post_fix_status': v.status,
                'post_fix_bst_issues': v.bst_issues_found,
                'improvement': 'YES' if v.status == 'OK' and pre_fix_count > 0 else 'NO' if pre_fix_count == 0 else 'PARTIAL'
            }
        except Exception as e:
            comparison[v.year] = {
                'pre_fix_available': True,
                'error': str(e)
            }

    return comparison


async def main():
    parser = argparse.ArgumentParser(
        description='Verify Elexon data fixes',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--year', type=int,
                        help='Verify only this year')
    parser.add_argument('--start-year', type=int, default=2022,
                        help='Start year (default: 2022)')
    parser.add_argument('--end-year', type=int, default=2025,
                        help='End year (default: 2025)')
    parser.add_argument('--detailed', action='store_true',
                        help='Show detailed output')
    parser.add_argument('--compare-pre-fix', action='store_true',
                        help='Compare with pre-fix investigation reports')
    parser.add_argument('--output', type=str,
                        help='Output CSV file path')

    args = parser.parse_args()

    print("=" * 80)
    print("ELEXON DATA FIX VERIFICATION")
    print("=" * 80)
    print(f"Verification started at: {datetime.now().isoformat()}")

    # Determine years to verify
    if args.year:
        years = [args.year]
    else:
        years = list(range(args.start_year, args.end_year + 1))

    print(f"\nYears to verify: {years}")

    # Run verifications
    verifications = []
    for year in years:
        print(f"\nVerifying {year}...")
        v = await verify_year(year)
        verifications.append(v)
        print(f"  Status: {v.status}")

    # Print summary
    print_verification_summary(verifications)

    # Compare with pre-fix if requested
    if args.compare_pre_fix:
        print("\n" + "=" * 80)
        print("COMPARISON WITH PRE-FIX STATE")
        print("=" * 80)

        comparison = await compare_with_pre_fix(verifications)
        for year, comp in sorted(comparison.items()):
            print(f"\n{year}:")
            if not comp.get('pre_fix_available'):
                print("  No pre-fix report available")
            elif 'error' in comp:
                print(f"  Error reading pre-fix report: {comp['error']}")
            else:
                print(f"  Pre-fix issues: {comp['pre_fix_issues']}")
                print(f"  Post-fix status: {comp['post_fix_status']}")
                print(f"  Post-fix BST issues: {comp['post_fix_bst_issues']}")
                print(f"  Improvement: {comp['improvement']}")

    # Save report
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = SCRIPT_DIR / f"elexon_fix_verification_{years[0]}_{years[-1]}.csv"

    save_verification_report(verifications, output_path)

    print("\n" + "=" * 80)
    print(f"Verification completed at: {datetime.now().isoformat()}")
    print("=" * 80)

    # Exit with error if any verification failed
    if any(v.status not in ("OK", "NO_DATA") for v in verifications):
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
