# Turbine Units Import

Import turbine units from CSV file with automatic windfarm and turbine model matching.

## Overview

This script imports turbine units from `turbine_units.csv` and:
- Matches windfarms by name (case-insensitive, normalized)
- Matches turbine models by model name
- Generates unique codes in format: `{WINDFARM_CODE}-{SERIAL}`
- Imports start_date and end_date for operational tracking
- Performs bulk insert for optimal performance

## Quick Start

```bash
# Import all turbine units
poetry run python scripts/seeds/turbine_units/import_turbine_units.py

# Test with first 50 rows
poetry run python scripts/seeds/turbine_units/import_turbine_units.py --limit 50
```

## CSV Format

The `turbine_units.csv` file must have these columns:

| Column | Required | Description |
|--------|----------|-------------|
| `turbine_unit_id` | Yes | Original turbine ID from source |
| `windfarm_name` | Yes | Windfarm name (matched to existing windfarms) |
| `turbine_model` | Yes | Turbine model name (matched to turbine_models.model) |
| `turbine_status` | No | Status (operational, decommissioned, etc.) |
| `start_date` | Yes | Operational start date (format: M/D/YYYY) |
| `end_date` | No | Decommission date (if applicable) |

Example row:
```csv
turbine_unit_id,windfarm_name,turbine_model,turbine_status,start_date,end_date
571313174000828844,Aagaard Phase 1,SWT-3.2-113,Operational,12/16/2015,
```

## Code Generation

Turbine codes are automatically generated as: **`{WINDFARM_CODE}-{SERIAL}`**

Examples:
- `AALBORG_ØSTHAVN-001` (Aalborg Østhavn windfarm, turbine #1)
- `AALBORG_ØSTHAVN-002` (Aalborg Østhavn windfarm, turbine #2)
- `AAGAARD-001` (Aagaard windfarm, turbine #1)

Serial numbers:
- Start at 001 for each windfarm
- Auto-increment sequentially
- Preserved across re-imports (continues from highest existing)

## Matching Logic

### Windfarm Matching
- Normalizes both CSV windfarm_name and database Windfarm.name
- Case-insensitive comparison
- Strips whitespace
- Falls back to code matching if name doesn't match

### Turbine Model Matching
- Normalizes both CSV turbine_model and database TurbineModel.model
- Case-insensitive comparison
- Exact match required

## Date Parsing

Supports multiple date formats:
- `M/D/YYYY` (e.g., 12/16/2015)
- `YYYY-MM-DD` (e.g., 2015-12-16)
- `D/M/YYYY` (e.g., 16/12/2015)
- `YYYY/M/D` (e.g., 2015/12/16)
- `D.M.YYYY` (e.g., 16.12.2015)

Empty or missing dates are stored as NULL.

## Prerequisites

Before running this import:

1. **Windfarms must exist** - Run windfarm import first:
   ```bash
   poetry run python scripts/seeds/windfarm_and_generation_unit/step1_preload_lookups.py
   poetry run python scripts/seeds/windfarm_and_generation_unit/step2_bulk_import.py
   ```

2. **Turbine models must exist** - Check turbine_models table is populated

## Performance

- **Bulk operations** for maximum speed
- **In-memory lookups** avoid repeated database queries
- **Idempotent** - safe to re-run, skips existing turbine units
- **Expected rate**: ~100-500 rows/second (depending on data complexity)

For 2,006 rows: ~5-20 seconds

## Troubleshooting

### "Windfarms not found"
The CSV contains windfarm names that don't exist in the database.

**Solutions:**
1. Import windfarms first using the windfarm seed scripts
2. Check CSV for typos in windfarm_name column
3. Review the "Windfarms not found" list in the summary

### "Turbine models not found"
The CSV contains turbine model names that don't exist in turbine_models table.

**Solutions:**
1. Check turbine_models table: `SELECT model FROM turbine_models ORDER BY model;`
2. Add missing turbine models to the database
3. Review the "Turbine models not found" list in the summary

### Duplicate codes
Turbine codes must be unique. If you see errors about duplicate codes:

**Solutions:**
1. Script handles this automatically with ON CONFLICT DO NOTHING
2. Check for duplicate rows in CSV
3. Clear existing turbine units if doing a fresh import:
   ```sql
   DELETE FROM turbine_units WHERE windfarm_id IN (
       SELECT id FROM windfarms WHERE source = 'ENERGISTYRELSEN'
   );
   ```

## Command Reference

```bash
# Standard import
poetry run python scripts/seeds/turbine_units/import_turbine_units.py

# Custom CSV path
poetry run python scripts/seeds/turbine_units/import_turbine_units.py \
    --csv /path/to/custom_turbine_units.csv

# Test with limited rows
poetry run python scripts/seeds/turbine_units/import_turbine_units.py --limit 100

# Get help
poetry run python scripts/seeds/turbine_units/import_turbine_units.py --help
```

## Example Output

```
============================================================
TURBINE UNIT IMPORT
============================================================

Loading lookup data...
  Loaded 1589 windfarms
  Loaded 489 turbine models
  Found 0 existing turbine units

Reading CSV file: scripts/seeds/turbine_units/turbine_units.csv
  Total rows to process: 2006

Bulk inserting 2006 turbine units...
  ✓ Created 2006 turbine units

============================================================
TURBINE UNIT IMPORT SUMMARY
============================================================
✓ Rows processed: 2006
✓ Turbine units created: 2006
⚠ Turbine units skipped: 0

⏱ Time elapsed: 8.45 seconds
📊 Processing rate: 237.4 rows/second
============================================================
```

## Data Quality Notes

### Location Data (lat/lng)
Currently set to placeholder values (0.0, 0.0) because:
- CSV doesn't include individual turbine coordinates
- Turbine positions should be updated separately with actual GPS coordinates
- Consider using windfarm centroid as temporary fallback

### Hub Height
- Not included in CSV
- Left as NULL
- Can be populated later from turbine model specifications or separate data source

### Status Mapping
- `Operational` → `operational`
- `Decommissioned` → `decommissioned`
- Other values are stored as-is (lowercased)

## Next Steps

After importing turbine units:

1. **Verify import**: Check turbine_units table counts
   ```sql
   SELECT COUNT(*) FROM turbine_units;
   SELECT windfarm_id, COUNT(*) FROM turbine_units GROUP BY windfarm_id LIMIT 10;
   ```

2. **Update locations**: Add actual turbine GPS coordinates if available

3. **Import generation data**: Run ENERGISTYRELSEN data import
   ```bash
   poetry run python scripts/seeds/generation_data/energistyrelsen/import_parallel_optimized.py
   ```

4. **Aggregate data**: Process monthly generation data
   ```bash
   poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py \
       --start 2002-01 --end 2025-12 --source ENERGISTYRELSEN
   ```
