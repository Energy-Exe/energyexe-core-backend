# NVE Data Processing

## Import Summary (Jan 2026)

| Metric | Value |
|--------|-------|
| Raw records imported | 5,441,098 |
| Aggregated records | 5,441,099 |
| Data completeness | 99.5% of CSV |
| Date range | 2002-01-01 to 2024-12-31 |
| Windfarms linked | 82.4% (4.27M records) |

**Key fixes applied:**
- Pre-commercial data now imported using `first_power_date` (not `start_date`)
- Capacity factor set to NULL before `commercial_operational_date`
- Phase 1 generation units linked to parent windfarms (279 units fixed)
- Decommissioned windfarm data imported (60,166 records via `import_missing_decommissioned.py`)

## Source Files
- **Location**: `data/vindprod2002-2024_kraftverk.xlsx`
- **Format**: Norwegian wind farm hourly data (pivoted)
- **Period**: 2002-2024
- **Coverage**: 71 windfarms, 366 generation unit phases
- **Timezone**: **UTC** (verified by DST transition analysis)

### Timezone Verification
The source data is in UTC, confirmed by:
- All days have exactly 24 hours, including DST transition days
- Spring forward days (e.g., March 31, 2024) contain 02:00 timestamps, which don't exist in Europe/Oslo local time
- Fall back days (e.g., October 27, 2024) have only one 02:00 entry instead of two

## Data Structure - Pivoted Format
```
                    | Bessakerfjellet | Bjerkreim | Buheii | Øyfjellet | ...
Row 1 (codes)       | 20              | 72        | 1088   | 1086      | ...
Row 2 (windfarm)    | Bessakerfjellet | Bjerkreim | Buheii | Øyfjellet | ...
2002-01-01 00:00:00 | 0.3             | NaN       | NaN    | NaN       | ...
2002-01-01 01:00:00 | 0.5             | NaN       | NaN    | NaN       | ...
...
```

## Phase-Based Structure

### What are Phases?

NVE windfarms have **multiple phases** representing expansion stages over time. Each phase:
- Has its own operational period (`start_date` to `end_date`)
- Represents the capacity at that stage
- **Shares the same code** with other phases of the same windfarm

### Example: Bessakerfjellet (Code 20)

```
Phase 1: 2007-09-10 to 2008-08-29  (100 MW)
Phase 2: 2008-08-30 to present     (150 MW)
```

When importing generation data:
- Data from 2007-09-10 to 2008-08-29 → matched to Phase 1
- Data from 2008-08-30 onwards → matched to Phase 2

### Example: Øyfjellet (Code 1086)

This windfarm has **54 phases** with very granular expansion stages, some lasting just days:
```
Phase 1:  2021-08-25 to 2021-09-05  (400.00 MW)
Phase 2:  2021-09-06 to 2021-09-15  (405.00 MW)
...
Phase 54: 2022-09-28 to present     (11,122.70 MW)
```

## Import Process

### Script: `import_parallel_optimized.py`

**Phase-aware import** - automatically matches each data point to the correct phase:

```bash
# Full import (2002-2024)
poetry run python scripts/seeds/generation_data/nve/import_parallel_optimized.py

# Test with sample
poetry run python scripts/seeds/generation_data/nve/import_parallel_optimized.py --sample 1000

# Options
poetry run python scripts/seeds/generation_data/nve/import_parallel_optimized.py \
    --workers 4 \           # Parallel processing
    --no-clean \            # Keep existing data
    --sample 1000           # Process only first N rows
```

### Processing Logic

1. **Load generation units** - Groups by code (multiple phases per code)
2. **Read Excel file** - Row 1 contains windfarm codes
3. **Create code mapping** - Maps Excel columns to codes (e.g., column "Bessakerfjellet" → code "20")
4. **Process each row**:
   - Parse timestamp
   - For each windfarm column with data:
     - Get code (e.g., "20")
     - Find operational phase at that timestamp
     - Create record with correct phase's `generation_unit_id`
5. **Bulk insert** - Batch insert into `generation_data_raw`

### Phase Selection Algorithm

```python
def find_operational_unit(units_list, timestamp):
    """Find which phase was operational at the timestamp."""
    for unit in units_list:
        if unit.start_date and timestamp < unit.start_date:
            continue  # Too early
        if unit.end_date and timestamp > unit.end_date:
            continue  # Too late
        return unit  # This phase was operational
    return None  # No operational phase found
```

## Data Storage

### generation_data_raw Table

Each hourly record is stored as:

```json
{
  "period_start": "2022-07-02T00:00:00",
  "period_end": "2022-07-02T01:00:00",
  "period_type": "hour",
  "source": "NVE",
  "identifier": "1086",  // Code (shared by all phases)
  "value_extracted": 2500.5,  // MWh
  "data": {
    "generation_mwh": 2500.5,
    "unit_code": "1086",
    "unit_name": "Øyfjellet Phase 51",
    "generation_unit_id": "uuid-of-phase-51",
    "windfarm_id": "uuid-of-oyfjellet",
    "timestamp": "2022-07-02T00:00:00"
  }
}
```

### Key Points

- **identifier**: Stores the code (e.g., "1086"), NOT the phase-specific unit name
- **data.generation_unit_id**: References the specific phase that was operational
- **data.unit_name**: The phase name (e.g., "Øyfjellet Phase 51")

## Aggregation Process

See `scripts/seeds/aggregate_generation_data/README.md` for details.

```bash
# Aggregate all NVE data
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
    --start 2002-01-01 --end 2024-12-31 --source NVE
```

The aggregation script also uses phase-aware matching to link raw data to the correct generation unit.

## Coverage

- **71 windfarms** in Excel file
- **366 generation unit phases** in database
- **~8.3M+ records** (71 windfarms × ~200K hours)
- **Date range**: 2002-01-01 to 2024-12-31

## Important Notes

1. **Code Uniqueness**: As of migration `7daf40c2a86e`, the unique constraint on `generation_units.code` has been removed to support multiple phases sharing codes.

2. **Phase Dates are Critical**: Incorrect start/end dates will result in data being matched to the wrong phase or not matched at all.

3. **Missing Phases**: If a timestamp falls outside all phase date ranges, the data won't be imported (logged as debug warning).

4. **Excel Structure**: The script expects:
   - Row 1: Numeric codes
   - Row 2: Windfarm names (ignored)
   - Row 3+: Timestamps in first column, generation data in other columns