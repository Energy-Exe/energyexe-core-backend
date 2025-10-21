# ENTSOE Data Import

This directory contains scripts for importing ENTSOE (European Network of Transmission System Operators for Electricity) generation data into the `generation_data_raw` table.

## Current Data Coverage

**Excel Files**: December 2014 → August 31, 2025 (4.4M records)
**API Imports**: September 1, 2025 → October 19, 2025 (16K records)

**Coverage is continuous with no gaps.**

## Import Methods

### Method 1: Excel File Import (Historical Data)

**Use for:**
- Bulk historical data (2014-2025)
- Large date ranges (months/years)
- Initial data load
- Control areas not supported by API (DE, NL, GB)

**Source Files:**
- **Location**: `data/*.xlsx` (Excel files from ENTSOE Transparency Platform)
- **Format**: European electricity generation data (15-min/hourly)
- **Coverage**: All European bidding zones and control areas

**Script:** `import_parallel_optimized.py`

**Usage:**
```bash
# Import single Excel file
poetry run python scripts/seeds/raw_generation_data/entsoe/import_parallel_optimized.py \
  --file data/entsoe_2025_09.xlsx

# Import all files in directory
poetry run python scripts/seeds/raw_generation_data/entsoe/import_parallel_optimized.py \
  --directory data/

# Faster import with 8 workers
poetry run python scripts/seeds/raw_generation_data/entsoe/import_parallel_optimized.py \
  --directory data/ --workers 8
```

**Features:**
- Excel → CSV conversion for speed (10x faster)
- Parallel processing of multiple files
- Filters to only configured units
- Handles both 15-min and hourly data
- Stores as `source_type='excel'`

---

### Method 2: API Import (Recent Data Updates)

**Use for:**
- Recent data updates (last few days/weeks)
- Daily/weekly automated imports
- Incremental updates after Excel baseline

**Source:** ENTSOE Transparency Platform API
**API Key Required**: Set `ENTSOE_API_KEY` in `.env`

**Script:** `import_from_api.py`

**Basic Usage:**
```bash
# Fetch single day (safest)
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-17 --end 2025-10-17

# Fetch one week
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-11 --end 2025-10-17

# Fetch multiple months (auto-chunks into 7-day batches)
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-09-01 --end 2025-10-17
```

**Advanced Options:**
```bash
# Fetch specific control areas only
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-17 --end 2025-10-17 --zones DK FR BE

# Dry run (see what would be fetched)
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-17 --end 2025-10-17 --dry-run

# Custom chunk size for large imports
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-09-01 --end 2025-10-17 --chunk-days 14
```

**Features:**
- ✅ **Control area grouping** (6 API calls for all units)
- ✅ **Bulk upsert** (updates existing records, no duplicates)
- ✅ **Automatic chunking** for large date ranges (7-day chunks by default)
- ✅ **Smart retry logic** with automatic backoff
- ✅ **Filters for configured units** only
- ✅ Stores as `source_type='api'`

**Important Notes:**
- ⚠️ **ENTSOE has 2-3 day publication delay** - don't request yesterday's data
- ⚠️ Use dates at least 3 days in the past for reliable results
- ⚠️ The script groups by **control area**, not bidding zone

---

## Supported Control Areas

The API groups windfarms by **control area** (not bidding zone). One API call fetches all units in a control area.

### ✅ Working Control Areas (API Supported)

**BE (Belgium)** - Control Area: `10YBE----------2`
- 10 offshore wind farms
- Coverage: Continuous, up to current -2 days
- Example: Belwind, Northwind, Nobelwind, C-Power

**DK (Denmark)** - Control Area: `10Y1001A1001A796`
- 11 offshore wind farms (Anholt, Horns Rev, Kriegers Flak, Rødsand, Vesterhav)
- Coverage: Continuous, up to current -2 days
- Note: 181 onshore farms use Energistyrelsen data source (file uploads)

**FR (France)** - Control Area: `10YFR-RTE------C`
- 32 offshore wind farms
- Coverage: Continuous, up to current -2 days
- Example: Saint-Brieuc, Fécamp, Banc de Guérande

### ❌ Not Supported via API

**National Grid (UK)** - Control Area: `10YGB----------A`
- Use **ELEXON API** instead (see `scripts/seeds/raw_generation_data/elexon/`)
- ENTSOE doesn't provide per-unit generation data for UK

**DE(TenneT GER) (Germany)**, **NL (Netherlands)**
- Per-unit generation data not available via ENTSOE API
- Use **Excel file imports** for these control areas

---

## Recommended Import Strategy

### Initial Setup (One-time)

1. **Import historical Excel files** (2014 through August 2025)
   ```bash
   poetry run python scripts/seeds/raw_generation_data/entsoe/import_parallel_optimized.py \
     --directory data/
   ```

2. **Backfill September-October 2025** via API (to bridge gap)
   ```bash
   poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
     --start 2025-09-01 --end 2025-10-17
   ```

### Daily Maintenance

Run daily to import yesterday's data (with 3-day safety buffer):

```bash
# Import data from 3 days ago
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-18 --end 2025-10-18
```

Or automate with cron:
```bash
# Run daily at 6 AM to import data from 3 days ago
0 6 * * * cd /path/to/project && poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py --start $(date -d "3 days ago" +\%Y-\%m-\%d) --end $(date -d "3 days ago" +\%Y-\%m-\%d)
```

---

## Data Mapping

### Database Fields
```
source           = 'ENTSOE'
source_type      = 'excel' or 'api'
identifier       = EIC code (e.g., '45W000000000046I' for Anholt)
period_start     = DateTime (UTC)
period_end       = Calculated based on resolution
period_type      = 'PT15M' or 'PT60M'
value_extracted  = Generation output (MW)
unit             = 'MW'
data             = JSONB with full details
```

### JSONB Data Structure

**From Excel:**
```json
{
  "area_code": "10Y1001A1001A796",
  "generation_unit_code": "45W000000000046I",
  "generation_unit_name": "Anholt",
  "actual_generation_output_mw": 119.67,
  "installed_capacity_mw": 400,
  "resolution_code": "PT60M"
}
```

**From API:**
```json
{
  "eic_code": "45W000000000046I",
  "area_code": "10Y1001A1001A796",
  "production_type": "wind",
  "resolution_code": "PT60M",
  "installed_capacity_mw": 400,
  "import_metadata": {
    "import_timestamp": "2025-10-19T10:00:00Z",
    "import_method": "api_script",
    "import_script": "import_from_api.py"
  }
}
```

---

## After Import: Run Aggregation

Both import methods store data in `generation_data_raw`. After importing, process into hourly aggregates:

```bash
# Aggregate the imported data
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --source ENTSOE \
  --start 2025-10-17 \
  --end 2025-10-17
```

---

## Performance

### Excel Import
- **Speed**: Very fast (parallel processing, CSV conversion)
- **API Calls**: 0
- **Best for**: Bulk historical data (months/years)
- **Time**: ~5-10 minutes for 1 year of data (all zones)

### API Import
- **Speed**: Fast (1-2 minutes per week)
- **API Calls**: 6 calls per day (one per control area)
- **Best for**: Recent incremental updates (days/weeks)
- **Time**: ~30 seconds per day (all working control areas)

---

## Troubleshooting

### API Import Errors

**"Delivered time interval is not valid for this Data item"**
- Date is too recent (ENTSOE has 2-3 day publication delay)
- **Solution**: Use dates at least 3 days in the past

**"InvalidBusinessParameterError"**
- Control area doesn't provide per-unit generation data via API
- **Solution**: Use Excel file imports instead

**"No matching units found"**
- EIC codes in database don't match API response
- **Solution**: Verify EIC codes are correct in database

**For UK windfarms:**
- ENTSOE doesn't provide UK per-unit data
- **Solution**: Use ELEXON API (see `scripts/seeds/raw_generation_data/elexon/`)

### Control Area vs Bidding Zone

**IMPORTANT**: The API uses **control area codes** in the `in_Domain` parameter, NOT bidding zone codes.

- ❌ Wrong: `in_Domain=10YDK-1--------W` (DK1 bidding zone)
- ✅ Correct: `in_Domain=10Y1001A1001A796` (DK control area)

Each windfarm has both:
- `windfarm.bidzone_id` → Bidding zone (for market/pricing)
- `windfarm.control_area_id` → Control area (for ENTSOE API)

---

## Example Workflows

### Scenario 1: New Project Setup
```bash
# 1. Import all historical data
poetry run python scripts/seeds/raw_generation_data/entsoe/import_parallel_optimized.py --directory data/

# 2. Bridge gap to current with API
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-09-01 --end 2025-10-17

# 3. Aggregate all data
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --source ENTSOE --start 2025-01-01 --end 2025-10-17
```

### Scenario 2: Weekly Update
```bash
# Import last week (with 3-day buffer)
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-11 --end 2025-10-17

# Aggregate new data
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --source ENTSOE --start 2025-10-11 --end 2025-10-17
```

### Scenario 3: Specific Control Area Update
```bash
# Update only Danish offshore wind farms
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-11 --end 2025-10-17 --zones DK

# Or Belgium and France only
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-11 --end 2025-10-17 --zones BE FR
```

---

## Key Learnings

1. **Control Areas vs Bidding Zones**: ENTSOE API requires control area codes
2. **Publication Lag**: Always use dates 3+ days in the past
3. **API Limitations**: Not all European zones provide per-unit data
4. **Danish Data**: Now works via API! (Fixed October 2025)
5. **UK Data**: Use ELEXON API, not ENTSOE

## Files in This Directory

- `import_from_api.py` - API import script (recent data)
- `import_parallel_optimized.py` - Excel import script (historical data)
- `check_import_status.py` - View current data coverage
- `clear_entsoe_data.py` - Clear all ENTSOE data (use with caution)
- `data/` - Directory for Excel files
- `README.md` - This file
