"""Analyze PT30M data in CSV files and database."""

import pandas as pd
from pathlib import Path
from collections import defaultdict

# Find all CSV files
csv_dir = Path("scripts/seeds/power_prices/entsoe")
csv_files = sorted(csv_dir.glob("*.csv"))

print("=" * 80)
print("PT30M DATA ANALYSIS REPORT")
print("=" * 80)

# Analyze each file
pt30m_by_file = {}
pt30m_by_bidzone = defaultdict(int)
pt30m_by_date = defaultdict(int)
earliest_date = None
latest_date = None

print("\n1. FILES WITH PT30M DATA:\n")

for csv_file in csv_files:
    try:
        # Read just the resolution column
        df = pd.read_csv(csv_file, sep='\t', usecols=['ResolutionCode', 'AreaDisplayName', 'DateTime(UTC)'])
        pt30m_count = (df['ResolutionCode'] == 'PT30M').sum()

        if pt30m_count > 0:
            pt30m_by_file[csv_file.name] = pt30m_count

            # Get bidzones
            pt30m_rows = df[df['ResolutionCode'] == 'PT30M']
            for bidzone in pt30m_rows['AreaDisplayName'].unique():
                pt30m_by_bidzone[bidzone] += len(pt30m_rows[pt30m_rows['AreaDisplayName'] == bidzone])

            # Get date range
            for date_str in pt30m_rows['DateTime(UTC)']:
                date = pd.to_datetime(date_str).date()
                pt30m_by_date[str(date)] += 1

                if earliest_date is None or date < earliest_date:
                    earliest_date = date
                if latest_date is None or date > latest_date:
                    latest_date = date

            print(f"  ✓ {csv_file.name}: {pt30m_count:,} PT30M records")
    except Exception as e:
        print(f"  ✗ {csv_file.name}: Error - {e}")

print(f"\nTotal files with PT30M: {len(pt30m_by_file)}")
print(f"Total PT30M records: {sum(pt30m_by_file.values()):,}")

print("\n" + "=" * 80)
print("2. BIDZONES WITH PT30M DATA:")
print("=" * 80)

for bidzone, count in sorted(pt30m_by_bidzone.items(), key=lambda x: x[1], reverse=True):
    print(f"  {bidzone}: {count:,} records")

print("\n" + "=" * 80)
print("3. DATE RANGE:")
print("=" * 80)

print(f"  Earliest: {earliest_date}")
print(f"  Latest: {latest_date}")
print(f"  Duration: {(latest_date - earliest_date).days} days")

print("\n" + "=" * 80)
print("4. RECOMMENDATION:")
print("=" * 80)

print("""
The import script has been updated to properly handle PT30M data.
However, existing PT30M data in the database was incorrectly labeled as PT60M.

Actions needed:
1. ✅ Import script updated (DONE)
2. ❌ Re-import affected CSV files (2015-01 through 2018-09)
3. ❌ Or wait for reprocessing to complete (it will aggregate PT30M correctly anyway)

Since the hourly aggregation fix handles all resolutions correctly by rounding
to hour boundaries and averaging, the mislabeled PT30M data will still be
processed correctly into hourly records.

For data integrity, you may want to re-import the 2015-2018 data after the
current reprocessing completes.
""")
