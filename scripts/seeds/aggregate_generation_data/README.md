# Generation Data Aggregation Pipeline

Transforms raw generation data from multiple sources into standardized hourly or monthly records.

## Quick Commands

### Daily/Hourly Data Processing

```bash
# Process all hourly sources for a date range (ENTSOE, ELEXON, TAIPOWER, NVE)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2002-01-01 --end 2024-12-31

# Process a specific source (daily mode - default)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2020-01-01 --end 2024-12-31 --source ENTSOE

# Process month-by-month (MUCH FASTER for large datasets like NVE - ~30x faster)
# Recommended for NVE: processes all days in a month in one transaction
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2002-01-01 --end 2024-12-31 --source NVE --monthly

# Dry run (preview without changes)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2020-01-01 --end 2024-12-31 --dry-run

# Process single day
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_daily.py --date 2024-01-15

# Run sources in parallel (open 4 terminals - based on actual data availability)
# Use --monthly for NVE (much faster: ~5 minutes vs ~3 hours for full date range)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2002-01-01 --end 2024-12-31 --source NVE --monthly
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2013-04-01 --end 2024-12-31 --source ELEXON
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2015-01-01 --end 2024-12-31 --source ENTSOE
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2020-08-01 --end 2024-12-31 --source TAIPOWER
```

To run elexon in different ranges:
```bash
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2013-06-08 --end 2015-12-31 --source ELEXON
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2016-01-01 --end 2019-12-31 --source ELEXON
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py --start 2020-01-01 --end 2024-02-14 --source ELEXON
```

### Monthly Data Processing

```bash
# Process all monthly sources (EIA, ENERGISTYRELSEN)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py --start 2001-01 --end 2025-07

# Process specific monthly source
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py --start 2001-01 --end 2025-07 --source EIA
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py --start 2002-01 --end 2025-12 --source ENERGISTYRELSEN

# Dry run (preview without changes)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py --start 2020-01 --end 2024-12 --dry-run

# Run monthly sources in parallel (open 2 terminals)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py --start 2001-01 --end 2025-07 --source EIA
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py --start 2002-01 --end 2025-12 --source ENERGISTYRELSEN
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
- **Resolution**: 30-minute intervals (settlement periods)
- **Transformation**: Sum 2 half-hour MWh values to get hourly total
- **Special Handling**:
  - IMPORTANT: Each 30-min value represents MWh generated in that period
  - Formula: `MWh_00:00 + MWh_00:30 = hourly_MWh`
  - Example: 10 MWh (00:00-00:30) + 12 MWh (00:30-01:00) = 22 MWh for the hour
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

### 5. ENERGISTYRELSEN (Denmark) - Monthly Data
- **Resolution**: Monthly totals
- **Transformation**: Processed by `process_generation_data_monthly.py` (skipped in daily processing)
- **Special Handling**:
  - Monthly data doesn't fit daily processing model
  - Stored with `source_resolution='monthly'`
  - `hour` field = first day of month (e.g., 2024-01-01 00:00:00)
  - Capacity factor = generation_mwh / (capacity_mw × 730 hours)
- **Expected Records**: ~312 units × 276 months = ~86,112 monthly records
- **Data Source**: GSRN-based Danish wind turbines

### 6. EIA (United States) - Monthly Data
- **Resolution**: Monthly totals (wind plants only)
- **Transformation**: Processed by `process_generation_data_monthly.py`
- **Special Handling**:
  - Filters for wind data only (fuel_type='WND')
  - Plant ID maps directly to generation_unit.code
  - Stored with `source_resolution='monthly'`
  - `hour` field = first day of month
  - Capacity factor = generation_mwh / (capacity_mw × 730 hours)
- **Expected Records**: ~1,498 wind plants × 12 months × 25 years = ~450K monthly records
- **Data Source**: EIA-923 Monthly Generation and Fuel Consumption Reports (2001-2025)

## Data Flow

### Hourly Data Flow (ENTSOE, ELEXON, TAIPOWER, NVE)

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
generation_data (final table, source_resolution='hourly')
```

### Monthly Data Flow (EIA, ENERGISTYRELSEN)

```
generation_data_raw (source-specific format, period_type='month')
    ↓
transform_[source]() function (monthly processor)
    ↓
MonthlyRecord (standardized format)
    ↓
clear_existing_data() (idempotent, by month)
    ↓
save_monthly_records()
    ↓
generation_data (final table, source_resolution='monthly', hour=first of month)
```

## Key Features

### Idempotent Processing
- Deletes existing data for date/source before inserting
- Safe to re-run without creating duplicates

### Capacity Factor Calculation

**Hourly Data:**
- Formula: `generation_mwh / capacity_mw`
- Represents utilization for that specific hour
- Capped at 9.9999 to fit database column (NUMERIC(5,4))
- NULL if capacity is 0 or missing

**Monthly Data:**
- Formula: `generation_mwh / (capacity_mw × 730 hours)`
- 730 hours = average month length (30.4 days × 24 hours)
- Represents average utilization over the month
- Capped at 9.9999 to fit database column
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

### Hourly/Daily Processing Scripts

#### `process_generation_data_robust.py`
Main processing script for hourly data (ENTSOE, ELEXON, TAIPOWER, NVE):
- Processes any date range day-by-day
- Continues on errors (logs failures)
- Creates JSON logs in `generation_processing_logs/`
- Supports resume from checkpoint
- Shows progress with ETA
- Sources: ENTSOE, ELEXON, TAIPOWER, NVE

#### `process_generation_data_daily.py`
Core transformation logic for hourly data:
- Contains source-specific transform functions
- Handles database operations
- Implements harmonization rules
- Called by `process_generation_data_robust.py`

### Monthly Processing Scripts

#### `process_generation_data_monthly.py`
Main processing script for monthly data (EIA, ENERGISTYRELSEN):
- Processes any month range month-by-month
- Creates one record per unit per month
- Stores with `source_resolution='monthly'`
- Calculates monthly capacity factor
- Idempotent processing (safe to re-run)
- Sources: EIA, ENERGISTYRELSEN
- Arguments:
  - `--start YYYY-MM`: Start month (e.g., 2020-01)
  - `--end YYYY-MM`: End month (e.g., 2024-12)
  - `--source [EIA|ENERGISTYRELSEN]`: Process specific source only
  - `--dry-run`: Preview without database changes

## Monitoring Progress

### Check Daily/Hourly Processing Status

```bash
# Check processing logs
tail -f generation_processing_logs/*.json

# Monitor hourly database progress
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
            WHERE source_resolution = 'hourly'
            GROUP BY source
        '''))

        for row in result:
            print(f'{row.source}: {row.days} days, {row.first_date} to {row.last_date}, {row.records:,} records')

asyncio.run(check())
"
```

### Check Monthly Processing Status

```bash
# Monitor monthly database progress
poetry run python -c "
import asyncio
from sqlalchemy import text
from app.core.database import get_session_factory

async def check():
    AsyncSessionLocal = get_session_factory()
    async with AsyncSessionLocal() as db:
        result = await db.execute(text('''
            SELECT source,
                   COUNT(DISTINCT TO_CHAR(hour, 'YYYY-MM')) as months,
                   MIN(hour) as first_month,
                   MAX(hour) as last_month,
                   COUNT(*) as records,
                   COUNT(DISTINCT generation_unit_id) as units
            FROM generation_data
            WHERE source_resolution = 'monthly'
            GROUP BY source
        '''))

        for row in result:
            print(f'{row.source}: {row.units} units, {row.months} months, {row.first_month.strftime(\"%Y-%m\")} to {row.last_month.strftime(\"%Y-%m\")}, {row.records:,} records')

asyncio.run(check())
"
```

## Common Issues

**Process gets stuck**: Usually at ELEXON (large dataset). Kill with Ctrl+C, process smaller date ranges or sources separately.

**Duplicate key errors**: Data already exists. Script deletes before inserting (idempotent). Use `--dry-run` to preview.

**Capacity factor > 9.9999**: Database column limit. Values are automatically capped.

**Memory issues**: Process smaller date ranges or fewer parallel sources.

## Performance

### Hourly Data Processing
- **Processing speed**: ~3-5 seconds per day per source
- **Full run (2000-2024)**: ~5-6 hours with 4 parallel sources (ENTSOE, ELEXON, TAIPOWER, NVE)

### Monthly Data Processing
- **Processing speed**: ~1-2 seconds per month per source
- **EIA full run (2001-2025)**: ~5-10 minutes for 1,498 plants × 12 months × 25 years
- **ENERGISTYRELSEN full run (2002-2025)**: ~5-10 minutes for 312 turbines × 276 months
- **Both sources in parallel**: ~10-15 minutes total