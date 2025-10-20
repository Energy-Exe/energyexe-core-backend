# Generation Data Import

This directory contains scripts for importing raw generation data from ELEXON, ENTSOE, Taipower, NVE, Energistyrelsen, and EIA sources.

## Quick Start

### 1. Ensure Generation Units are Configured

First, make sure generation units are populated in the database:

```bash
cd energyexe-core-backend

# Add all generation units (ELEXON + ENTSOE)
poetry run python scripts/seeds/seed_generation_units.py

# Or add only missing ENTSOE units
poetry run python scripts/seeds/add_entsoe_units_only.py
```

### 2. Import ELEXON Data

Import raw ELEXON generation data from CSV files:

```bash
# Standard import with 4 workers
poetry run python scripts/seeds/raw_generation_data/elexon/import_parallel_optimized.py

# Faster import with 8 workers
poetry run python scripts/seeds/raw_generation_data/elexon/import_parallel_optimized.py --workers 8

# Check import status
poetry run python scripts/seeds/raw_generation_data/elexon/check_import_status.py
```

### 3. Import ENTSOE Data

Import raw ENTSOE generation data from Excel files:

poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
    --source ELEXON \
    --start 2024-02-15 \
    --end 2025-10-20

```bash
# Standard import with 4 workers
poetry run python scripts/seeds/raw_generation_data/entsoe/import_parallel_optimized.py

# Faster import with 8 workers
poetry run python scripts/seeds/raw_generation_data/entsoe/import_parallel_optimized.py --workers 8

# Check import status
poetry run python scripts/seeds/raw_generation_data/entsoe/check_import_status.py
```

### 4. Import Taipower Data

Import raw Taipower generation data from Excel files:

```bash
# Standard import with cleanup (removes existing data first)
poetry run python scripts/seeds/raw_generation_data/taipower/import_parallel_optimized.py

# Import without cleanup (append mode)
poetry run python scripts/seeds/raw_generation_data/taipower/import_parallel_optimized.py --no-clean

# Faster import with 8 workers
poetry run python scripts/seeds/raw_generation_data/taipower/import_parallel_optimized.py --workers 8

# Check import status
poetry run python scripts/seeds/raw_generation_data/taipower/check_import_status.py

# Clear Taipower data only
poetry run python scripts/seeds/raw_generation_data/taipower/clear_taipower_data.py
```

### 5. Import NVE Data

Import raw NVE (Norwegian Water Resources and Energy Directorate) generation data:

```bash
# Standard import with cleanup (removes existing data first)
poetry run python scripts/seeds/raw_generation_data/nve/import_parallel_optimized.py

# Import without cleanup (append mode)
poetry run python scripts/seeds/raw_generation_data/nve/import_parallel_optimized.py --no-clean

# Test with sample data (first 1000 rows)
poetry run python scripts/seeds/raw_generation_data/nve/import_parallel_optimized.py --sample 1000

# Faster import with 8 workers
poetry run python scripts/seeds/raw_generation_data/nve/import_parallel_optimized.py --workers 8

# Check import status
poetry run python scripts/seeds/raw_generation_data/nve/check_import_status.py
```

### 6. Import Energistyrelsen Data

Import raw Energistyrelsen (Danish Energy Agency) monthly generation data:

```bash
# Standard import with cleanup (removes existing data first)
poetry run python scripts/seeds/raw_generation_data/energistyrelsen/import_parallel_optimized.py

# Import without cleanup (append mode)
poetry run python scripts/seeds/raw_generation_data/energistyrelsen/import_parallel_optimized.py --no-clean

# Test with sample data (first 1000 rows)
poetry run python scripts/seeds/raw_generation_data/energistyrelsen/import_parallel_optimized.py --sample 1000

# Faster import with 8 workers
poetry run python scripts/seeds/raw_generation_data/energistyrelsen/import_parallel_optimized.py --workers 8

# Check import status
poetry run python scripts/seeds/raw_generation_data/energistyrelsen/check_import_status.py

# Check configured units
poetry run python scripts/seeds/raw_generation_data/energistyrelsen/check_energistyrelsen_units.py
```

### 7. Import EIA Data

Import raw EIA (U.S. Energy Information Administration) monthly wind generation data:

```bash
# Standard import with cleanup (removes existing data first)
poetry run python scripts/seeds/raw_generation_data/eia/import_parallel_optimized.py

# Import without cleanup (append mode)
poetry run python scripts/seeds/raw_generation_data/eia/import_parallel_optimized.py --no-clean

# Test with first 3 files
poetry run python scripts/seeds/raw_generation_data/eia/import_parallel_optimized.py --sample 3

# Faster import with 8 workers
poetry run python scripts/seeds/raw_generation_data/eia/import_parallel_optimized.py --workers 8

# Check import status
poetry run python scripts/seeds/raw_generation_data/eia/check_import_status.py
```

## Prerequisites

Install required dependencies:
```bash
poetry add polars pyarrow psutil asyncpg openpyxl
```

## Clear Data

To clear existing data before re-import:

```bash
# Clear all raw_generation_data_raw
poetry run python scripts/seeds/raw_generation_data/elexon/clear_raw_generation_data_raw.py

# Or clear only specific source
poetry run python scripts/seeds/raw_generation_data/clear_by_source.py --source ELEXON
poetry run python scripts/seeds/raw_generation_data/clear_by_source.py --source ENTSOE
poetry run python scripts/seeds/raw_generation_data/clear_by_source.py --source Taipower
poetry run python scripts/seeds/raw_generation_data/clear_by_source.py --source NVE
poetry run python scripts/seeds/raw_generation_data/clear_by_source.py --source ENERGISTYRELSEN

# Clear Taipower data specifically
poetry run python scripts/seeds/raw_generation_data/taipower/clear_taipower_data.py
```

## Data Details

### ELEXON Data
- **Source**: CSV files in `elexon_raw_data/data/`
- **Format**: Half-hourly generation data
- **Fields**: Settlement Date, Period, BMU ID, Generation Output
- **Coverage**: ~283 configured BMU units
- **Size**: ~100M+ records across 4 CSV files

### ENTSOE Data
- **Source**: Excel files in `entsoe/data/`
- **Format**: Hourly/15-min generation data
- **Fields**: DateTime, Unit Code, Area, Generation Output, Capacity
- **Coverage**: ~108 configured generation units
- **Size**: ~13M records across 129 monthly Excel files (2014-2025)

### Taipower Data
- **Source**: Excel files in `taipower/data/`
- **Format**: Hourly generation data
- **Fields**: Timestamp, Installed capacity, Power generation, Capacity factor
- **Coverage**: 33 configured wind farms (22 data files)
- **Size**: ~500K+ records across 22 Excel files
- **Units**: Uses Chinese unit codes (e.g., 彰工, 海洋竹南, 芳一風)
- **Period**: 2020-2025

### NVE Data
- **Source**: Single Excel file in `nve/data/`
- **Format**: Pivoted hourly generation data (columns are wind farms)
- **Fields**: Timestamp rows, wind farm columns with MWh values
- **Coverage**: 63 configured Norwegian wind farms
- **Size**: ~200K rows × 71 wind farm columns (14M+ data points)
- **Units**: Uses numeric codes mapped to wind farm names
- **Period**: 2002-2024
- **Special**: Data is in wide/pivoted format requiring special processing

### Energistyrelsen Data
- **Source**: Single Excel file in `energistyrelsen/data/`
- **Format**: Monthly generation data (pivoted format with months as columns)
- **Fields**: Turbine metadata rows, month columns with kWh values
- **Coverage**: 312 configured Danish wind turbines
- **Size**: ~10K turbine rows × 276 month columns (2.7M+ data points)
- **Units**: Uses GSRN (Grid System Registration Number) codes
- **Period**: 2002-2025 (monthly aggregation)
- **Special**: Data is monthly (not hourly), in kWh (converted to MWh), pivoted format

### EIA Data
- **Source**: Excel files in `eia/data/` (one file per year, 2001-2025)
- **Format**: Monthly wind generation data (pivoted format with months as columns)
- **Fields**: Plant ID, Plant Name, Fuel Type, monthly generation columns (Jan-Dec)
- **Coverage**: Configured U.S. wind plants (Plant ID = generation_unit.code)
- **Size**: ~25 files, 500-1,500 wind rows per file (~12K-37K total wind rows)
- **Units**: Uses Plant ID codes (direct mapping to generation_unit.code)
- **Period**: 2001-2025 (monthly aggregation, 25 years)
- **Special**: Filters for fuel_type='WND' (wind only), monthly in MWh, pivoted format

## Performance Optimizations

All import scripts include:
- **Parallel processing** (4-8 workers)
- **PostgreSQL COPY** for bulk inserts (10-50x faster)
- **Polars/Pandas** for fast data reading
- **Unit filtering** to only import configured units
- **Dynamic memory management**
- **Batch accumulation** for efficiency

Expected performance:
- ELEXON: ~12 minutes for 100M+ records
- ENTSOE: ~10 minutes for 13M records
- Taipower: ~3-5 minutes for 500K+ records
- NVE: ~10-15 minutes for 14M+ data points (full file)
- Energistyrelsen: ~5-10 minutes for 2.7M+ data points (monthly data)
- EIA: ~5-10 minutes for 300K records (25 files, monthly data)

## Monitoring Import Progress

All imports provide:
- Real-time progress bars per file
- Records/second throughput metrics
- Memory usage monitoring
- Final summary statistics

## Troubleshooting

If import fails:
1. Check database connection settings
2. Verify generation units are configured:
   - ELEXON: `source='ELEXON'`
   - ENTSOE: `source='ENTSOE'`
   - Taipower: `source='Taipower'` (note: capital T, lowercase rest)
   - NVE: `source='NVE'`
   - Energistyrelsen: `source='ENERGISTYRELSEN'`
   - EIA: `source='EIA'` (Plant ID must match generation_unit.code)
3. Ensure sufficient disk space and memory
4. Check logs for specific error messages

For debugging:
```bash
# Enable profiling
poetry run python scripts/seeds/raw_generation_data/elexon/import_parallel_optimized.py --profile
poetry run python scripts/seeds/raw_generation_data/entsoe/import_parallel_optimized.py --profile
poetry run python scripts/seeds/raw_generation_data/taipower/import_parallel_optimized.py --profile
```

## Important: Phase-Based Generation Units

### Multi-Phase Plants

Many wind farms have **multiple phases** representing expansions, repowering, or different stages of development. These should use the **same generation unit code** with different `start_date`/`end_date` periods, NOT different codes with suffixes.

**Correct Approach (Phase-Based):**
```
Code: 56291
- Phase 1: "Horse Hollow 1" | 2005-12-01 to 2017-12-30
- Phase 2: "Horse Hollow 1 RP" | 2017-12-31 to present
```

**Incorrect Approach (Auto-Generated Suffixes):**
```
Code: 56291    → "Horse Hollow 1"
Code: 56291_1  → "Horse Hollow 1 RP"  ❌ BREAKS API IMPORTS
```

### Why This Matters

**APIs report at the plant/facility level:**
- EIA API: Returns data for plant `56291` (all phases combined)
- API does NOT recognize `56291_1` → Returns 500 errors
- Taipower API: Same behavior for Chinese codes
- ELEXON API: Has its own format with underscores (e.g., `T_ACHRW-1`) - these are legitimate

**Database handles phases with dates:**
- Multiple generation units can share the same code
- `start_date` and `end_date` differentiate which phase was active when
- Aggregation scripts use these dates to select the correct phase for each time period

### Generation Unit Code Guidelines

**DO:**
- ✅ Use codes exactly as they appear in source data
- ✅ Allow duplicate codes for different phases (differentiated by dates)
- ✅ Set proper `start_date`/`end_date` for each phase
- ✅ Verify codes work with the data source's API (if applicable)

**DON'T:**
- ❌ Auto-generate suffixes (`_1`, `_2`, `_3`) for duplicate codes
- ❌ Modify codes from their original source format
- ❌ Use placeholder codes like `nan_1`, `nan_2`

**Exception: ELEXON**
- ELEXON BM unit codes naturally contain underscores and hyphens (e.g., `T_ACHRW-1`)
- This is the official format - keep these as-is
- Have verified data (100K+ records per unit)

### Fixed Issues (Oct 2025)

**EIA:** Fixed 182 auto-generated suffix codes
- `57874_1` → `57874` (now phase-based)
- Removed suffix generation from import script
- All EIA codes now compatible with EIA API

**Taipower:** Fixed 10 auto-generated suffix codes
- `彰工_1` → `彰工` (now phase-based)
- All Taipower codes now compatible with Taipower API

**ENERGISTYRELSEN:** All codes are `nan_X` placeholders
- No valid plant codes in source data
- Uses turbine-level data instead (acceptable)

## EIA API Import

In addition to Excel file imports, EIA data can be fetched via API for recent months:

```bash
# Fetch recent months via API (recommended for keeping data up-to-date)
poetry run python scripts/seeds/raw_generation_data/eia/import_from_api.py \
    --start-year 2025 --start-month 2 --end-year 2025 --end-month 7

# See full documentation
cat scripts/seeds/raw_generation_data/eia/README.md
```

**Key Features:**
- Fetches data for 1,537 units (1,355 unique plant codes after deduplication)
- Smart retry logic handles API errors automatically
- Bulk upsert prevents duplicates
- Takes ~10-15 minutes for full run
- Requires `EIA_API_KEY` in `.env` file

## Notes

- **Taipower, NVE, Energistyrelsen & EIA**: Automatically clear existing data before import (use `--no-clean` to append)
- **ELEXON & ENTSOE**: Append by default (manually clear if needed)
- **NVE, Energistyrelsen & EIA**: Data is pivoted - columns are units/months, rows are timestamps/turbines/plants
- **All sources**: Data stored in `raw_generation_data_raw` table with JSONB structure
- **NVE & Energistyrelsen**: Use `--sample N` to test with first N rows before full import
- **EIA**: Use `--sample N` to test with first N files before full import
- **Energistyrelsen & EIA**: Monthly data (not hourly), stored with `period_type='month'`
- **EIA**: Filters for wind data only (fuel_type='WND'), Plant ID maps to generation_unit.code