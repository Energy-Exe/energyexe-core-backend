# Generation Data Aggregation Pipeline

Transforms raw generation data from multiple sources into standardized hourly records.

## Quick Commands

```bash
# Process all sources for a date range (adjust start date based on source)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2002-01-01 --end 2024-12-31

# Process a specific source
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2020-01-01 --end 2024-12-31 --source ENTSOE

# Dry run (preview without changes)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2020-01-01 --end 2024-12-31 --dry-run

# Process single day
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_daily.py --date 2024-01-15

# Run sources in parallel (open 4 terminals - based on actual data availability)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2002-01-01 --end 2024-12-31 --source NVE
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2013-04-01 --end 2024-12-31 --source ELEXON
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2015-01-01 --end 2024-12-31 --source ENTSOE
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2020-08-01 --end 2024-12-31 --source TAIPOWER
```

To run elexon in different sources
```
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2013-06-08 --end 2015-12-31 --source ELEXON
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2016-01-01 --end 2019-12-31 --source ELEXON
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2020-01-01 --end 2024-02-14 --source ELEXON
```
## Source-Specific Handling

### 1. ENTSOE (European Network)
- **Resolution**: 15-minute or hourly intervals
- **Transformation**:
  - 15-minute data: Average 4 values to get hourly MW, then convert to MWh
  - Hourly data: Direct copy (MW = MWh for 1-hour period)
- **Special Handling**: None required
- **Expected Records**: ~109 units × 24 hours = ~2,616 records/day

### 2. ELEXON (UK)
- **Resolution**: 30-minute intervals
- **Transformation**: Average 2 half-hour MW values to get hourly average, not sum
- **Special Handling**:
  - IMPORTANT: Use averaging (not summing) per harmonization rules
  - Formula: `(MW_00:00 + MW_00:30) / 2 = hourly_MW = hourly_MWh`
- **Expected Records**: ~147 units × 48 half-hours = ~7,056 raw → ~3,528 hourly records/day

### 3. TAIPOWER (Taiwan)
- **Resolution**: Hourly
- **Transformation**: Direct copy, check timezone (UTC+8 to UTC if needed)
- **Special Handling**:
  - Skip records where `value_extracted` is None
  - Handle capacity from raw data or fallback to generation_units table
  - Timezone conversion may be needed
- **Expected Records**: ~21 units × 24 hours = ~504 records/day

### 4. NVE (Norway)
- **Resolution**: Hourly
- **Transformation**: Aggregate multiple raw records for same unit/hour (sum generation values)
- **Special Handling**:
  - Multiple raw records per unit/hour (e.g., 3 records for different sub-units)
  - Sum all generation values for the same unit/timestamp
  - Example: Unit "2" might have 3 records (16.08, 0.2, 37.248 MWh) = 53.528 MWh total
- **Expected Records**: ~30 units × 24 hours × 3 sub-records = ~720 raw → ~552 hourly records/day

### 5. ENERGISTYRELSEN (Denmark)
- **Resolution**: Monthly totals
- **Transformation**: Currently skipped in daily processing (needs separate monthly handler)
- **Special Handling**:
  - Monthly data doesn't fit daily processing model
  - Would need to distribute monthly total across days if daily granularity needed
- **Expected Records**: Variable, depends on number of units reporting

## Data Flow

```
generation_data_raw (source-specific format)
    ↓
transform_[source]() function
    ↓
HourlyRecord (standardized format)
    ↓
clear_existing_data() (idempotent)
    ↓
save_hourly_records()
    ↓
generation_data (final table)
```

## Key Features

### Idempotent Processing
- Deletes existing data for date/source before inserting
- Safe to re-run without creating duplicates

### Capacity Factor Calculation
- Formula: `generation_mwh / capacity_mw`
- Capped at 9.9999 to fit database column (NUMERIC(5,4))
- NULL if capacity is 0 or missing

### Quality Metrics
- `quality_score`: Based on data completeness
- `completeness`: Ratio of actual to expected data points
- `quality_flag`: HIGH/MEDIUM/LOW based on score

### Error Handling
- Day-by-day processing continues even if individual days fail
- Transaction rollback on source failure to prevent partial data
- Comprehensive JSON logging of all results and errors

## Scripts

### `process_generation_data_robust.py`
Main processing script:
- Processes any date range day-by-day
- Continues on errors (logs failures)
- Creates JSON logs in `generation_processing_logs/`
- Supports resume from checkpoint
- Shows progress with ETA

### `process_generation_data_daily.py`
Core transformation logic:
- Contains source-specific transform functions
- Handles database operations
- Implements harmonization rules

## Monitoring Progress

```bash
# Check processing status
tail -f generation_processing_logs/*.json

# Monitor database progress
poetry run python -c "
import asyncio
from sqlalchemy import text
from app.core.database import get_session_factory

async def check():
    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        result = await db.execute(text('''
            SELECT source,
                   COUNT(DISTINCT DATE(hour)) as days,
                   MIN(hour)::date as first_date,
                   MAX(hour)::date as last_date,
                   COUNT(*) as records
            FROM generation_data
            GROUP BY source
        '''))

        for row in result:
            print(f'{row.source}: {row.days} days, {row.first_date} to {row.last_date}, {row.records:,} records')

asyncio.run(check())
"
```

## Common Issues

**Process gets stuck**: Usually at ELEXON (large dataset). Kill with Ctrl+C, process smaller date ranges or sources separately.

**Duplicate key errors**: Data already exists. Script deletes before inserting (idempotent). Use `--dry-run` to preview.

**Capacity factor > 9.9999**: Database column limit. Values are automatically capped.

**Memory issues**: Process smaller date ranges or fewer parallel sources.

## Performance

- **Processing speed**: ~3-5 seconds per day per source
- **Full run (2000-2024)**: ~5-6 hours with 5 parallel sources