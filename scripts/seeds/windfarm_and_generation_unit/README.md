# Windfarm and Generation Unit Import System

This folder contains an optimized multi-step system for importing windfarms and generation units from Excel files into the database.

## 📁 Files

- `step1_preload_lookups.py` - Preloads all lookup data and creates a cache file
- `step2_bulk_import.py` - Performs bulk import using the cache
- `validate_data.py` - Validates Excel data quality before import
- `clean_database.py` - Removes all windfarm and generation unit data (use with caution!)
- `lookup_cache.json` - Cache file created by step 1 (auto-generated)

## 🚀 Quick Start

### Complete Import Process

```bash
# 1. Validate data quality (optional but recommended)
poetry run python scripts/seeds/windfarm_and_generation_unit/validate_data.py

# 2. Preload lookups and create cache
poetry run python scripts/seeds/windfarm_and_generation_unit/step1_preload_lookups.py

# 3. Run bulk import
poetry run python scripts/seeds/windfarm_and_generation_unit/step2_bulk_import.py

# Done! Check the summary for results
```

### Clean and Restart

```bash
# Remove all existing data (WARNING: This deletes data!)
poetry run python scripts/seeds/windfarm_and_generation_unit/clean_database.py

# Then run the import process above
```

## 📋 Command Reference

### Step 1: Preload Lookups

Creates a cache of all lookup data to avoid repeated database queries.

```bash
poetry run python scripts/seeds/windfarm_and_generation_unit/step1_preload_lookups.py \
    --excel scripts/seeds/generation_unit_seed.xlsx \
    --output scripts/seeds/windfarm_and_generation_unit/lookup_cache.json
```

**Options:**
- `--excel` - Path to Excel file (default: `scripts/seeds/generation_unit_seed.xlsx`)
- `--output` - Output cache file path (default: `lookup_cache.json` in this folder)

### Step 2: Bulk Import

Performs the actual import using bulk operations for maximum performance.

```bash
poetry run python scripts/seeds/windfarm_and_generation_unit/step2_bulk_import.py \
    --excel scripts/seeds/generation_unit_seed.xlsx \
    --cache scripts/seeds/windfarm_and_generation_unit/lookup_cache.json \
    --limit 100
```

**Options:**
- `--excel` - Path to Excel file
- `--cache` - Path to cache file from step 1
- `--limit` - Limit number of rows to process (for testing)
- `--skip-geography` - Skip creating geography entities
- `--skip-owners` - Skip processing owners

### Data Validation

Check data quality before import:

```bash
poetry run python scripts/seeds/windfarm_and_generation_unit/validate_data.py \
    --excel scripts/seeds/generation_unit_seed.xlsx \
    --limit 100
```

**Options:**
- `--excel` - Path to Excel file
- `--limit` - Limit rows to validate

### Clean Database

Remove all windfarm and generation unit data:

```bash
# Interactive (asks for confirmation)
poetry run python scripts/seeds/windfarm_and_generation_unit/clean_database.py

# Force clean (no confirmation)
poetry run python scripts/seeds/windfarm_and_generation_unit/clean_database.py --force
```

## 🎯 Performance

The optimized import system provides:
- **10-100x faster** than the original script
- Processes **2000+ rows in < 30 seconds**
- Bulk operations minimize database queries
- Preloaded cache eliminates redundant lookups
- Efficient memory usage with batch processing

## 📊 Data Mapping

### Status Mapping
- `Operational` → `operational`
- `Decommissioned` → `decommissioned`
- `Under Installation` → `under_installation`
- `Expanded` → `expanded`

### Technology Type
- Foundation type `fixed` or `floating` → `offshore_wind`
- All others → `onshore_wind`

### Location Type
- Foundation type `fixed` or `floating` → `offshore`
- All others → `onshore`

## ⚠️ Important Notes

1. **Always run Step 1 before Step 2** - The cache file is required
2. **Validate data first** - Use the validation script to check data quality
3. **Backup before cleaning** - The clean script permanently deletes data
4. **Check existing data** - The import adds to existing data, use clean if you need to start fresh

## 🐛 Troubleshooting

### "Cache file not found"
Run step 1 first to create the cache file.

### "Country not found"
The Excel file has missing or invalid country names. Check the validation output.

### Import is slow
- Make sure you're using the scripts in this folder, not the old ones
- Check that the cache file exists and is recent
- Consider using `--limit` for testing

### Database connection errors
- Check your `.env` file has correct database credentials
- Ensure the database is running
- Try restarting the backend server

## 🌊 NVE-Specific Considerations

### Phase-Based Generation Units

NVE windfarms have a **phase-based structure** where a single windfarm has multiple phases (expansion stages) over time. Each phase:
- Has its own `start_date` and `end_date`
- Represents the operational capacity during that time period
- **Shares the same code** with other phases of the same windfarm

**Example: Bessakerfjellet**
- Code: `20`
- Phase 1: 2007-09-10 to 2008-08-29 (capacity: 100 MW)
- Phase 2: 2008-08-30 to present (capacity: 150 MW)

### Important: Code Uniqueness

As of migration `7daf40c2a86e`, the **unique constraint on `generation_units.code` has been removed**. This allows multiple phases to share the same code, which is required for NVE data.

### Importing NVE Generation Units

When importing NVE generation units from Excel:

1. **All phases must have the same code** (e.g., all Bessakerfjellet phases use code `20`)
2. **Start and end dates are critical** - they determine which phase is active at any given time
3. **Generation data is matched by code + timestamp** - the import script finds the correct phase based on the data timestamp

### NVE Data Import Process

```bash
# 1. Import generation units (use this folder's scripts)
poetry run python scripts/seeds/windfarm_and_generation_unit/step1_preload_lookups.py
poetry run python scripts/seeds/windfarm_and_generation_unit/step2_bulk_import.py

# 2. Import NVE generation data (uses phase-aware matching)
poetry run python scripts/seeds/generation_data/nve/import_parallel_optimized.py

# 3. Aggregate NVE data (handles phase selection)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
    --start 2002-01-01 --end 2024-12-31 --source NVE
```

See `scripts/seeds/generation_data/nve/README.md` for detailed NVE import documentation.

## 📈 Example Output

```
============================================================
STEP 2: BULK IMPORT
============================================================

Reading Excel file: scripts/seeds/generation_unit_seed.xlsx
  Processing 2212 rows

Ensuring geography entities exist...
  Created 5 countries
  Created 12 states
  Created 8 bidzones

Bulk inserting 450 windfarms...
  ✓ Created 450 windfarms

Bulk inserting 2212 generation units...
  ✓ Created 2212 generation units

Processing owners...
  Created 127 owners
  Created 892 owner relationships

============================================================
IMPORT SUMMARY
============================================================
✓ Countries created: 5
✓ States created: 12
✓ Windfarms created: 450
✓ Generation units created: 2212
✓ Owners created: 127
✓ Owner relationships created: 892

⏱ Time elapsed: 28.3 seconds
📊 Processing rate: 78.2 rows/second
============================================================
```