# Re-aggregation Commands for Date Filtering

## Overview
After implementing generation unit lifespan filtering, existing data needs to be re-aggregated to properly handle unit start_date and end_date restrictions.

## Affected Data Sources

Based on analysis, the following sources have units with date restrictions that need reprocessing:

| Source | Units with Dates | Date Range to Reprocess | Days |
|--------|-----------------|------------------------|------|
| **NVE** | 63 units | 2002-01-01 to 2024-12-31 | 8,401 |
| **ELEXON** | 218 units | 2013-04-01 to 2024-02-14 | 3,972 |
| **ENTSOE** | 27 units | 2015-01-01 to 2024-12-31 | 3,653 |
| **TAIPOWER** | 29 units | Data exists but dates vary | Varies |
| **EIA** | 1,526 units | Not in daily processor | N/A |
| **ENERGISTYRELSEN** | 312 units | Monthly data (skipped) | N/A |
| **EEX** | 33 units | Not in daily processor | N/A |

## Re-aggregation Commands

### Option 1: Single Day Processing

Process a specific date for testing:

```bash
# Test with a single day first
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_daily.py \
  --source NVE \
  --date 2020-06-12

# Check the results with --dry-run flag
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_daily.py \
  --source NVE \
  --date 2020-06-12 \
  --dry-run
```

### Option 2: Batch Processing Script (Recommended)

Use the batch processing script for large date ranges:

```bash
# NVE - Process all historical data (will take several hours)
poetry run python scripts/reprocess_with_date_filtering.py \
  --source NVE \
  --start-date 2002-01-01 \
  --end-date 2024-12-31 \
  --batch-size 30

# ELEXON - Process from 2013 onwards
poetry run python scripts/reprocess_with_date_filtering.py \
  --source ELEXON \
  --start-date 2013-04-01 \
  --end-date 2024-02-14 \
  --batch-size 30

# ENTSOE - Process from 2015 onwards
poetry run python scripts/reprocess_with_date_filtering.py \
  --source ENTSOE \
  --start-date 2015-01-01 \
  --end-date 2024-12-31 \
  --batch-size 30

# TAIPOWER - If needed
poetry run python scripts/reprocess_with_date_filtering.py \
  --source TAIPOWER \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --batch-size 30
```

### Option 3: Process Recent Data Only (Quick Fix)

If you only want to fix recent data:

```bash
# Last 90 days for NVE
poetry run python scripts/reprocess_with_date_filtering.py \
  --source NVE \
  --start-date 2024-10-01 \
  --end-date 2024-12-31 \
  --batch-size 30

# Last 90 days for ENTSOE
poetry run python scripts/reprocess_with_date_filtering.py \
  --source ENTSOE \
  --start-date 2024-10-01 \
  --end-date 2024-12-31 \
  --batch-size 30
```

### Option 4: Manual Year-by-Year Processing

For more control, process year by year:

```bash
# NVE - Process 2024 only
poetry run python scripts/reprocess_with_date_filtering.py \
  --source NVE \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --batch-size 30

# NVE - Process 2023
poetry run python scripts/reprocess_with_date_filtering.py \
  --source NVE \
  --start-date 2023-01-01 \
  --end-date 2023-12-31 \
  --batch-size 30
```

## Utility Commands

### Check Affected Date Ranges
```bash
# See which dates need reprocessing
poetry run python scripts/check_affected_date_ranges.py
```

### Verify Data After Reprocessing
```bash
# Check a specific date to verify the fix
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_daily.py \
  --source NVE \
  --date 2020-06-12 \
  --check
```

## Processing Time Estimates

Based on typical processing speeds:
- **NVE**: ~8,400 days → 4-6 hours
- **ELEXON**: ~4,000 days → 2-3 hours
- **ENTSOE**: ~3,600 days → 2-3 hours
- **TAIPOWER**: Variable, typically 1-2 hours

## Recommended Order

1. **Test First**: Run a single day with --dry-run to verify
2. **Recent Data**: Process last 30-90 days for immediate impact
3. **Full Historical**: Run full reprocessing during off-peak hours

## Monitoring Progress

The batch script will show progress like:
```
2024-01-01: 1,234 raw → 456 saved
2024-01-02: 1,345 raw → 467 saved
...
```

## After Reprocessing

1. Verify that capacity values are NULL for units outside operational dates
2. Check capacity factors are properly calculated
3. Review windfarm total capacities for accuracy

## Cleanup

After successful reprocessing, remove temporary scripts:
```bash
rm scripts/check_affected_date_ranges.py
rm scripts/reprocess_with_date_filtering.py
```