# EIA Data Import Guide

This directory contains scripts and documentation for importing wind generation data from the U.S. Energy Information Administration (EIA).

## Overview

EIA provides monthly wind generation data for power plants across the United States. We support two import methods:

1. **Excel Import**: Bulk historical data from downloaded EIA Excel files (2000-2025)
2. **API Import**: Recent data via EIA's open data API (last 6 months)

## Prerequisites

### For Excel Imports
- Download monthly EIA-923 files from [EIA's website](https://www.eia.gov/electricity/data/eia923/)
- Place Excel files in the `data/` subdirectory
- Files should be named like: `EIA923_Schedules_2_3_4_5_M_XX_YYYY_Final_Revision.xlsx`

### For API Imports
- Obtain a free API key from [EIA Open Data](https://www.eia.gov/opendata/register.php)
- Add to `.env` file:
  ```
  EIA_API_KEY=your_api_key_here
  ```

## Database Schema

Data is stored in the `generation_data_raw` table with these key fields:

- `source`: "EIA"
- `source_type`: "excel" or "api"
- `identifier`: Plant code (e.g., "67722")
- `period_start`: First day of month (e.g., 2025-02-01)
- `period_end`: First day of next month (e.g., 2025-03-01)
- `period_type`: "month"
- `value_extracted`: Generation in MWh
- `data`: JSONB with full details (plant_name, state, fuel_type, etc.)

## Import Methods

### Method 1: Excel File Import (Historical Data)

**Best for**: Bulk historical imports (2000-2025), large date ranges

```bash
# Import all Excel files in data/ directory
poetry run python scripts/seeds/raw_generation_data/eia/import_parallel_optimized.py

# Options:
--workers 4           # Number of parallel workers (default: 4)
--no-clean           # Don't clear existing EIA data before import
--sample 5           # Process only first N files (for testing)
```

**Example:**
```bash
# Full import with 4 workers
poetry run python scripts/seeds/raw_generation_data/eia/import_parallel_optimized.py \
    --workers 4

# Test with first 2 files only
poetry run python scripts/seeds/raw_generation_data/eia/import_parallel_optimized.py \
    --sample 2

# Import without clearing existing data
poetry run python scripts/seeds/raw_generation_data/eia/import_parallel_optimized.py \
    --no-clean
```

**What it does:**
- Processes Excel files in parallel for speed
- Filters for wind (WND) fuel type only
- Matches plant codes against configured generation units in database
- Uses PostgreSQL COPY for ultra-fast bulk inserts
- Deduplicates records automatically
- Typical speed: ~1,000 records/second

### Method 2: API Import (Recent Data)

**Best for**: Fetching recent data (last few months), keeping data up-to-date, targeted date ranges

```bash
# Fetch data from API for a specific date range
poetry run python scripts/seeds/raw_generation_data/eia/import_from_api.py \
    --start-year YYYY --start-month MM \
    --end-year YYYY --end-month MM

# Options:
--dry-run            # Preview without storing data
```

**Example:**
```bash
# Fetch data for first half of 2025
poetry run python scripts/seeds/raw_generation_data/eia/import_from_api.py \
    --start-year 2025 --start-month 1 \
    --end-year 2025 --end-month 6

# Fetch most recent months (e.g., last 3 months)
poetry run python scripts/seeds/raw_generation_data/eia/import_from_api.py \
    --start-year 2025 --start-month 5 \
    --end-year 2025 --end-month 7

# Dry run to preview what data is available
poetry run python scripts/seeds/raw_generation_data/eia/import_from_api.py \
    --start-year 2025 --start-month 1 \
    --end-year 2025 --end-month 6 \
    --dry-run
```

**Important Notes:**

⚠️ **EIA Data Availability**

- EIA data has a 1-2 month publication lag
- Requesting future months (e.g., Aug-Oct 2025 in October 2025) returns no data
- The API returns only data that has been published for the requested date range
- Always request historical date ranges (at least 2 months in the past)
- Use `--dry-run` to preview what data is available before importing

**What it does:**
- Fetches data for all configured EIA plants (1,537 units, 1,355 unique plant codes)
- Automatically deduplicates by plant code (multiple phases share same code)
- Processes in batches of 10 plants per API call (136 batches total)
- **Smart retry logic**: If a batch fails, automatically splits into smaller batches and retries
  - Batch of 10 fails → Retry as 2 batches of 5
  - Batch of 5 fails → Retry as smaller batches
  - Continues until individual plant codes
  - Maximizes data recovery from problematic batches
- Uses bulk upsert to avoid duplicates
- Takes ~10-15 minutes for full run

**Note about Phase-Based Plants:**
- 182 generation units share codes with other phases (e.g., code `56291` has 6 units)
- This represents repowering/expansion phases at the same facility
- The API returns plant-level data (all phases combined)
- Individual phases are differentiated by `start_date` and `end_date` in the database
- This matches the NVE data model for multi-phase plants

## Data Processing Workflow

After importing raw data, you must run aggregation to process it:

### For Monthly Data (EIA):
```bash
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py \
    --source EIA --start YYYY-MM --end YYYY-MM
```

**Example:**
```bash
# Aggregate EIA data for first half of 2025
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py \
    --source EIA --start 2025-01 --end 2025-06
```

This will:
- Calculate monthly capacity factors
- Handle generation unit phases (start/end dates)
- Store processed data in `generation_data` table

## Checking Import Status

```bash
# Check what EIA data exists in database
poetry run python scripts/seeds/raw_generation_data/eia/check_import_status.py
```

This will show:
- Total records by source type (Excel vs API)
- Date range coverage
- Records per plant code
- Data completeness

## Troubleshooting

### Excel Import Issues

**Problem**: "No Excel files found"
- Solution: Ensure `.xlsx` files are in the `data/` subdirectory
- Check file naming matches EIA-923 format

**Problem**: "No configured plants found"
- Solution: Verify generation units with `source='EIA'` exist in database
- Check that `generation_unit.code` matches plant codes in Excel files

### API Import Issues

**Problem**: "API key not configured"
- Solution: Add `EIA_API_KEY` to `.env` file
- Verify key is valid at https://www.eia.gov/opendata/

**Problem**: "500 errors on many batches"
- **This is expected behavior** - the EIA API returns 500 errors for certain combinations of plant codes
- The smart retry logic automatically handles this by:
  - Splitting failing batches into smaller groups
  - Retrying until it finds valid combinations
  - Recovering all available data despite initial failures
- Common causes of 500 errors:
  - Plant codes without recent data (inactive plants)
  - Invalid plant code combinations that trigger API bugs
  - Truly invalid plant codes (like `66167_1`)
- The script successfully processes valid data and logs truly failed plant codes
- Check logs to see retry attempts and final success/failure for each batch

**Problem**: "Getting 0 records for requested date range"
- Check if the date range is too recent (1-2 month lag for EIA data)
- Check if the date range is too old (some plants don't have historical API data)
- Use `--dry-run` first to see what data is available
- For historical data (pre-2024), use Excel file imports instead

## Best Practices

1. **Initial Setup**: Use Excel import for historical data (2000-2024)
2. **Regular Updates**: Use API import monthly to fetch latest data
3. **Verification**: Always run aggregation after imports
4. **Monitoring**: Use `check_import_status.py` to verify data coverage

## Data Quality Notes

- EIA data has ~1-2 month publication lag
- Some plants report sporadically or stop reporting
- Zero generation values are valid (wind farms don't always generate)
- Negative values are filtered out during import
- Excel files are considered authoritative for historical data
- API data may be revised; re-running imports updates existing records

## File Structure

```
eia/
├── README.md                           # This file
├── import_from_api.py                  # API import script
├── import_parallel_optimized.py        # Excel import script (parallel)
├── check_import_status.py              # Status checker
├── EIA_DATA_ANALYSIS.md                # Historical analysis notes
└── data/                               # Place Excel files here
    ├── EIA923_Schedules_2_3_4_5_M_12_2024_Final_Revision.xlsx
    ├── EIA923_Schedules_2_3_4_5_M_12_2023_Final_Revision.xlsx
    └── ...
```

## Performance Tips

### Excel Imports
- Use more workers (`--workers 8`) on machines with more CPU cores
- First import will take 5-10 minutes for all files
- Subsequent imports are faster (updates existing records)

### API Imports
- Batch size of 10 plants is optimal (balances speed vs API limits)
- Whole import takes ~10-15 minutes for all 1,537 plants
- Can run multiple times safely (upserts existing records)

## Related Documentation

- [EIA-923 Data Guide](https://www.eia.gov/electricity/data/eia923/)
- [EIA Open Data API](https://www.eia.gov/opendata/)
- Main CLAUDE.md: Data Pipeline Architecture section
- `END_TO_END_FEATURE_DEVELOPMENT_GUIDE.md`: Aggregation process
