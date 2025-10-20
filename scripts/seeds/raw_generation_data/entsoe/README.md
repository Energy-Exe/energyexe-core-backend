# ENTSOE Data Import

This directory contains scripts for importing ENTSOE (European electricity generation) data into the `generation_data_raw` table.

## Import Methods

### Method 1: Excel File Import (For Historical Data)

**Use for:** Bulk historical data (2014-2025), large date ranges

**Source Files:**
- **Location**: `data/*.xlsx` (Excel files from ENTSOE Transparency Platform)
- **Format**: European electricity generation data (15-min/hourly)
- **Coverage**: All European bidding zones

**Script:** `import_parallel_optimized.py`

**Usage:**
```bash
# Import single Excel file
poetry run python scripts/seeds/raw_generation_data/entsoe/import_parallel_optimized.py \
  --file data/entsoe_2025_09.xlsx

# Import all files in directory
poetry run python scripts/seeds/raw_generation_data/entsoe/import_parallel_optimized.py \
  --directory data/
```

**Features:**
- Excel → CSV conversion for speed (10x faster)
- Parallel processing of multiple files
- Filters to only configured units
- Handles both 15-min and hourly data
- Stores as `source_type='excel'`

### Method 2: API Import (For Recent Data)

**Use for:** Recent data updates (last few days/weeks), small date ranges

**Source:** ENTSOE Transparency Platform API

**Script:** `import_from_api.py` (NEW!)

**Usage:**
```bash
# Fetch single day
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-11 --end 2025-10-11

# Fetch one week
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-01 --end 2025-10-07

# Fetch multiple months (auto-chunks into 7-day batches)
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-09-02 --end 2025-10-17 --zones BE FR

# Fetch specific bidding zones only
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-11 --end 2025-10-11 --zones BE FR

# Dry run (see what would be fetched)
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2025-10-11 --end 2025-10-11 --dry-run
```

**Automatic Chunking:** The script automatically breaks large date ranges into 7-day chunks to avoid API/database limits. You can safely request months of data in one command!

**Features:**
- Bidding zone grouping (7 API calls instead of 40+)
- Bulk upsert (updates existing records)
- Automatic rate limiting
- Only fetches configured units
- Stores as `source_type='api'`

**Important Notes:**
- ENTSOE has 1-2 day publication delay (don't request yesterday's data)
- Not all zones provide per-unit data via API
- Zones with working API: BE, FR (up to current), DK1, DK2 (up to Sept 2025)
- For UK data, use ELEXON API instead

## Data Mapping (Both Methods)

### Database Fields
```
source           = 'ENTSOE'
source_type      = 'excel' or 'api'
identifier       = GenerationUnitCode (e.g., '48W0000000000047')
period_start     = DateTime (UTC)
period_end       = Calculated based on resolution
period_type      = 'PT15M' or 'PT60M'
value_extracted  = Generation output (MW)
unit             = 'MW'
data             = JSONB with full details
```

### Data JSONB Structure

**From Excel:**
```json
{
  "area_code": "10YBE----------2",
  "generation_unit_code": "22WBELWIN1500271",
  "generation_unit_name": "Belwind Phase 1",
  "actual_generation_output_mw": 119.67,
  "installed_capacity_mw": 171,
  "resolution_code": "PT60M"
}
```

**From API:**
```json
{
  "eic_code": "22WBELWIN1500271",
  "area_code": "10YBE----------2",
  "production_type": "wind",
  "resolution_code": "PT60M",
  "installed_capacity_mw": 171,
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
  --source ENTSOE \
  --start 2025-10-11 \
  --end 2025-10-11
```

## Performance Comparison

| Method | Use Case | Speed | API Calls | Best For |
|--------|----------|-------|-----------|----------|
| Excel Import | Historical (2014-2025) | Very Fast | 0 | Bulk data, all zones |
| API Import | Recent (last week) | Fast | 7-10 | Live updates, BE/FR zones |

## Troubleshooting

### API Import Errors

**InvalidBusinessParameterError:**
- Date too recent (ENTSOE has 1-2 day delay)
- Zone doesn't provide per-unit data
- Solution: Use older date or Excel files

**NoMatchingDataError:**
- No data available for those units/dates
- Solution: Check if zone publishes per-unit data

**For UK windfarms:**
- Use ELEXON API instead (see `scripts/seeds/raw_generation_data/elexon/`)

### Which Zones Work via API?

**Working (Recent Data):**
- ✅ BE (Belgium): Up to current -2 days
- ✅ FR (France): Up to current -2 days

**Partially Working:**
- ⚠️ DK1, DK2 (Denmark): Up to Sept 2025 only

**Not Available via API:**
- ❌ GB (UK): Use ELEXON instead
- ❌ DE-LU, NL: Use Excel files only