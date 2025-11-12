# ELEXON Data Import

This directory contains scripts for importing ELEXON (UK electricity generation) data into the `generation_data_raw` table.

## Data Summary

**Total ELEXON Data:** 22.9M records
- **CSV Files:** 22.9M records (Mar 2013 → Feb 15, 2024) - 99.9%
- **API:** 11,777 records (July 31 → Oct 6, 2025) - 0.1%
- **Gap:** Feb 2024 → July 2025 (need CSV files)

**Current Status:** Partial Sept-Oct 2025 data via API

## Import Methods

### Method 1: CSV File Import (For Historical Data)

**Use for:** Bulk historical data (2013-2024), large date ranges

**Source Files:**
- **Location**: `data/*.csv` (CSV files from ELEXON)
- **Format**: 30-minute settlement period data for UK BM Units
- **Coverage:** All UK wind BM Units

**Script:** `import_parallel_optimized.py`

**Usage:**
```bash
# Import single CSV file
poetry run python scripts/seeds/raw_generation_data/elexon/import_parallel_optimized.py \
  --file data/elexon_2024.csv

# Import all files in directory
poetry run python scripts/seeds/raw_generation_data/elexon/import_parallel_optimized.py \
  --directory data/
```

**Features:**
- Polars for fast CSV reading (5-10x faster than pandas)
- PostgreSQL COPY for bulk inserts
- Filters to only configured BM units
- Parallel processing
- Stores as `source_type='csv'`

### Method 2: API Import (For Recent Data)

**Use for:** Recent data updates (last few days/weeks), filling gaps

**Source:** ELEXON Insights API (B1610 dataset)

**Script:** `import_from_api.py` (NEW!)

**Usage:**
```bash
# Fetch single day
poetry run python scripts/seeds/raw_generation_data/elexon/import_from_api.py \
  --start 2024-02-11 --end 2025-10-11

# Fetch one week
poetry run python scripts/seeds/raw_generation_data/elexon/import_from_api.py \
  --start 2025-10-01 --end 2025-10-07

# Fetch multiple months (auto-chunks into 7-day batches)
poetry run python scripts/seeds/raw_generation_data/elexon/import_from_api.py \
  --start 2025-02-16 --end 2025-10-17

# Dry run (see what would be fetched)
poetry run python scripts/seeds/raw_generation_data/elexon/import_from_api.py \
  --start 2025-10-11 --end 2025-10-11 --dry-run
```

**Automatic Chunking:** The script automatically breaks large date ranges into 7-day chunks to avoid API/database limits. You can safely request months of data in one command!

**Features:**
- Fetches all configured BM units in ONE API call
- Bulk upsert (updates existing records)
- 30-minute settlement periods
- Stores as `source_type='api'`

**Requirements:**
- `ELEXON_API_KEY` must be set in `.env` file
- Register at: https://data.elexon.co.uk/

## Data Mapping (Both Methods)

### Database Fields
```
source           = 'ELEXON'
source_type      = 'csv' or 'api'
identifier       = BM Unit ID (e.g., 'T_WBURB-1')
period_start     = Settlement period start time (UTC)
period_end       = Settlement period end time (start + 30 min)
period_type      = 'PT30M' (always 30-minute periods)
value_extracted  = Generation output (MWh for 30-min period)
unit             = 'MWh'
data             = JSONB with full details
```

### Data JSONB Structure

**From CSV:**
```json
{
  "bmu_id": "T_WBURB-1",
  "settlement_date": "20240115",
  "settlement_period": 24,
  "metered_volume": 45.2
}
```

**From API:**
```json
{
  "bm_unit": "T_WBURB-1",
  "level_from": 45.2,
  "level_to": 45.8,
  "settlement_period": 24,
  "settlement_date": "2024-01-15T12:00:00",
  "import_metadata": {
    "import_timestamp": "2025-10-19T10:00:00Z",
    "import_method": "api_script",
    "import_script": "import_from_api.py"
  }
}
```

## After Import: Run Aggregation

Both import methods store data in `generation_data_raw`. After importing, process into hourly aggregates:

```bash
# Aggregate the imported data
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --source ELEXON \
  --start 2025-10-11 \
  --end 2025-10-11
```

## Performance Comparison

| Method | Use Case | Speed | API Calls | Best For |
|--------|----------|-------|-----------|----------|
| CSV Import | Historical (2013-2024) | Very Fast | 0 | Bulk data |
| API Import | Recent (last week) | Fast | 1 | Live updates |

## Key Notes

### Settlement Periods
- ELEXON uses 30-minute settlement periods
- 48 periods per day (Period 1-48)
- Period 1 = 00:00-00:30, Period 48 = 23:30-00:00

### BM Units
- Current database has 219 BM units configured
- API fetches all units in one call (efficient!)
- Only stores data for configured units

## Troubleshooting

### API Import Errors

**Missing API Key:**
```bash
# Add to .env file:
ELEXON_API_KEY=your_key_here
```

**No Data Returned:**
- ELEXON usually has same-day or next-day data
- Much faster publishing than ENTSOE
- Check if BM units are correctly configured

**Rate Limits:**
- ELEXON API has rate limits
- Script includes automatic rate limiting
- For large date ranges, consider chunking by week