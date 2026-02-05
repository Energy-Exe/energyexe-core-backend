#!/usr/bin/env python3
"""
Generate report for windfarms with exactly 1 BMU unit in ELEXON.
Shows month-by-month data from 2020-2024 with validation data side by side.
"""
import asyncio
import os
import sys
from datetime import datetime, date
from pathlib import Path
from functools import partial

import pandas as pd
import asyncpg
from dotenv import load_dotenv

# Make print flush immediately
print = partial(print, flush=True)

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
# asyncpg requires postgresql:// not postgresql+asyncpg://
if DATABASE_URL and DATABASE_URL.startswith("postgresql+asyncpg://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://", 1)

OUTPUT_FILE = Path(__file__).parent / "single_bmu_windfarm_report_2020_2024.csv"
VALIDATION_FILE = Path(__file__).parent / "verify_data" / "aggregated_validation_data.xlsx"


async def get_single_bmu_windfarms(conn):
    """Get windfarms that have exactly 1 ELEXON BMU."""
    query = """
    WITH bmu_counts AS (
        SELECT windfarm_id, COUNT(*) as bmu_count
        FROM generation_units
        WHERE source = 'ELEXON' AND windfarm_id IS NOT NULL
        GROUP BY windfarm_id
        HAVING COUNT(*) = 1
    )
    SELECT w.id as windfarm_id, w.name as windfarm_name,
           gu.id as unit_id, gu.code as bmu_id, gu.name as bmu_name
    FROM windfarms w
    JOIN bmu_counts bc ON bc.windfarm_id = w.id
    JOIN generation_units gu ON gu.windfarm_id = w.id AND gu.source = 'ELEXON'
    ORDER BY w.name
    """
    rows = await conn.fetch(query)
    return [(r['windfarm_id'], r['windfarm_name'], r['unit_id'], r['bmu_id'], r['bmu_name']) for r in rows]


async def get_monthly_raw_data(conn, bmu_id: str, start_year: int, end_year: int):
    """Get monthly aggregated raw data for a BMU by identifier."""
    query = """
    SELECT
        date_trunc('month', period_start)::date as month,
        SUM(value_extracted) as raw_mwh
    FROM generation_data_raw
    WHERE identifier = $1
      AND period_start >= $2
      AND period_start < $3
      AND source = 'ELEXON'
    GROUP BY date_trunc('month', period_start)
    ORDER BY month
    """
    start_date = date(start_year, 1, 1)
    end_date = date(end_year + 1, 1, 1)
    rows = await conn.fetch(query, bmu_id, start_date, end_date)
    return {str(r['month'])[:7]: float(r['raw_mwh']) for r in rows}


async def get_monthly_agg_data(conn, unit_id: int, start_year: int, end_year: int):
    """Get monthly aggregated generation data for a unit."""
    query = """
    SELECT
        date_trunc('month', hour)::date as month,
        SUM(generation_mwh) as agg_mwh
    FROM generation_data
    WHERE generation_unit_id = $1
      AND hour >= $2
      AND hour < $3
    GROUP BY date_trunc('month', hour)
    ORDER BY month
    """
    start_date = date(start_year, 1, 1)
    end_date = date(end_year + 1, 1, 1)
    rows = await conn.fetch(query, unit_id, start_date, end_date)
    return {str(r['month'])[:7]: float(r['agg_mwh']) for r in rows}


def load_validation_data():
    """Load validation data from Excel file."""
    if not VALIDATION_FILE.exists():
        print(f"Warning: Validation file not found: {VALIDATION_FILE}")
        return {}

    df = pd.read_excel(VALIDATION_FILE)
    # Create a dict: {bmu_id: {month: value}}
    validation = {}
    for _, row in df.iterrows():
        bmu_id = row['bmu_id']
        month = str(row['month'])[:7]  # Format: YYYY-MM
        value = row['net_monthly_generation']
        if bmu_id not in validation:
            validation[bmu_id] = {}
        validation[bmu_id][month] = value
    return validation


async def main():
    print("=" * 70, flush=True)
    print("Single-BMU Windfarm Report: 2020-2024", flush=True)
    print("=" * 70, flush=True)

    # Load validation data
    print("\nLoading validation data...")
    validation_data = load_validation_data()
    print(f"  Loaded validation data for {len(validation_data)} BMUs")

    # Connect to database
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Get single-BMU windfarms
        print("\nFinding windfarms with exactly 1 ELEXON BMU...")
        windfarms = await get_single_bmu_windfarms(conn)
        print(f"  Found {len(windfarms)} windfarms")

        if not windfarms:
            print("No windfarms found with single BMU!")
            return

        # Generate all months from 2020-01 to 2024-12
        all_months = []
        for year in range(2020, 2025):
            for month in range(1, 13):
                all_months.append(f"{year}-{month:02d}")

        # Collect report data
        report_rows = []

        for i, (wf_id, wf_name, unit_id, bmu_id, bmu_name) in enumerate(windfarms):
            print(f"  Processing {i+1}/{len(windfarms)}: {bmu_id} ({wf_name})")

            # Get aggregated data from database
            agg_data = await get_monthly_agg_data(conn, unit_id, 2020, 2024)
            bmu_validation = validation_data.get(bmu_id, {})

            # Create rows for each month
            for month in all_months:
                agg_mwh = agg_data.get(month, 0)
                val_mwh = bmu_validation.get(month, None)

                # Calculate differences: aggregated vs validation
                agg_vs_val_diff = None
                agg_vs_val_pct = None
                if val_mwh is not None and val_mwh != 0:
                    agg_vs_val_diff = agg_mwh - val_mwh
                    agg_vs_val_pct = (agg_vs_val_diff / val_mwh) * 100

                report_rows.append({
                    'windfarm_name': wf_name,
                    'bmu_id': bmu_id,
                    'bmu_name': bmu_name,
                    'month': month,
                    'agg_mwh': round(agg_mwh, 2),
                    'validation_mwh': round(val_mwh, 2) if val_mwh is not None else None,
                    'agg_vs_validation_diff': round(agg_vs_val_diff, 2) if agg_vs_val_diff is not None else None,
                    'agg_vs_validation_pct': round(agg_vs_val_pct, 2) if agg_vs_val_pct is not None else None,
                })

        # Create DataFrame and save
        df = pd.DataFrame(report_rows)
        df.to_csv(OUTPUT_FILE, index=False)

        print(f"\n{'=' * 70}")
        print(f"Report saved to: {OUTPUT_FILE}")
        print(f"Total rows: {len(df)}")
        print(f"Windfarms: {len(windfarms)}")
        print(f"Months: {len(all_months)}")

        # Summary statistics
        print(f"\n{'=' * 70}")
        print("SUMMARY STATISTICS: AGGREGATED vs VALIDATION")
        print(f"{'=' * 70}")

        # Check where we have validation data
        df_with_val = df[df['validation_mwh'].notna()]
        print(f"\nRows with validation data: {len(df_with_val)}")

        if len(df_with_val) > 0:
            print(f"\nDifference distribution:")
            print(f"  Mean diff: {df_with_val['agg_vs_validation_diff'].mean():.2f} MWh")
            print(f"  Mean % diff: {df_with_val['agg_vs_validation_pct'].mean():.2f}%")
            print(f"  Median % diff: {df_with_val['agg_vs_validation_pct'].median():.2f}%")

            # Flag significant differences (>1%)
            significant = df_with_val[abs(df_with_val['agg_vs_validation_pct']) > 1]
            print(f"\nMonths with >1% difference: {len(significant)}")

            if len(significant) > 0:
                print("\nLargest discrepancies (positive = agg > validation):")
                top_disc = significant.nlargest(10, 'agg_vs_validation_pct', keep='first')[
                    ['windfarm_name', 'bmu_id', 'month', 'agg_mwh', 'validation_mwh', 'agg_vs_validation_pct']
                ]
                print(top_disc.to_string(index=False))

                print("\nLargest discrepancies (negative = agg < validation):")
                bottom_disc = significant.nsmallest(10, 'agg_vs_validation_pct', keep='first')[
                    ['windfarm_name', 'bmu_id', 'month', 'agg_mwh', 'validation_mwh', 'agg_vs_validation_pct']
                ]
                print(bottom_disc.to_string(index=False))

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
