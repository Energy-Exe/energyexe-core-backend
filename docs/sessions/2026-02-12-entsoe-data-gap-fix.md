# ENTSOE Data Gap Fix — Session 2026-02-12

## Problem

ENTSOE data stopped after January 23, 2026. Import cron jobs reported "success" but stored 0 records from Jan 24 onwards. From Feb 6, aggregation started crashing with `log_dir.mkdir()` error.

## Root Cause Analysis

### 1. FR (France) — `CardinalityViolationError`

The Issue 4 fix (previous session) correctly added `data_direction` parsing to `entsoe_client.py` — French wind units return BOTH "Actual Aggregated" (generation) and "Actual Consumption" from the ENTSOE API. However, `import_from_api.py` was not updated to handle this. It stored all records with `source_type='api'`, causing duplicate keys in the same INSERT batch:

```
(ENTSOE, api, 17W100P100P3382R, 2026-01-24 01:00) → generation
(ENTSOE, api, 17W100P100P3382R, 2026-01-24 01:00) → consumption  ← DUPLICATE
```

PostgreSQL error: `ON CONFLICT DO UPDATE command cannot affect row a second time`

**Why it worked before**: Old `entsoe_client.py` didn't parse `col[2]`, so generation and consumption were merged into a single value — consumption was silently discarded.

### 2. GB (National Grid) — `entsoe-py` library bug

`entsoe-py` 0.7.1 crashes at `entsoe.py:2255` with `AttributeError: 'RangeIndex' object has no attribute 'set_levels'` when the ENTSOE API returns an `Acknowledgement_MarketDocument` (empty/error XML) instead of generation data. The parsed DataFrame has a `RangeIndex` instead of `MultiIndex` for columns.

### 3. NL (Netherlands) — No wind data available

Unit `48W00000HOWAO-1M` has no wind generation data in the ENTSOE per-unit API. Not a bug.

### 4. `area_data` NameError bug

In `import_from_api.py` exception handler (retry loop), the code referenced `area_data` but the loop variable was `zone_data`. Any API exception would trigger a `NameError`, crashing the entire script and masking the real error.

### 5. Docker `log_dir.mkdir()` crash (from previous session)

`process_generation_data_robust.py` line 92: `self.log_dir.mkdir(exist_ok=True)` fails in environments with restricted filesystem. Fixed in previous session with try/except fallback to `/tmp`.

## Fixes Applied

### File: `scripts/seeds/raw_generation_data/entsoe/import_from_api.py`

**Fix 1: Split generation/consumption records** (lines 186-290)
- Before: All records stored with `source_type='api'`
- After: Split `unit_df` by `data_direction` column, use `source_type='api_consumption'` for consumption records
- This leverages the existing 4-column unique constraint `(source, source_type, identifier, period_start)`

**Fix 2: `area_data` → `zone_data`** (lines 391-427)
- Fixed 6 references to `area_data` in the exception handler to use `zone_data` (the correct loop variable)

### File: `app/services/entsoe_client.py`

**Fix 3: Handle `entsoe-py` RangeIndex bug** (after line 305)
- Detect `"set_levels" in err_msg and "RangeIndex" in err_msg`
- Log warning and return empty DataFrame instead of crashing
- Prevents GB failures from blocking other control areas

## Data Backfill

### Step 1: Raw data import

| Control Area | Records | Status |
|---|---|---|
| DK (Denmark) | 12,096 | Success |
| BE (Belgium) | 1,680 | Success |
| FR (France) | 25,648 (gen + consumption) | Success (after fix) |
| GB (National Grid) | 0 | entsoe-py bug (handled gracefully) |
| NL (Netherlands) | 0 | No wind data in API |
| **Total** | **39,424** | |

### Step 2: Aggregation

```
Date range:          2026-01-24 to 2026-02-12
Total days:          20
Successful days:     20
Failed days:         0
Total raw records:   28,816
Total hourly records: 8,464
Processing time:     0:02:21
```

Note: Raw records count (28,816) differs from import count (39,424) because aggregation excludes `api_consumption` records and counts per-day, while import includes consumption and counts total.

## Remaining Issues

### GB (National Grid) — No data from ENTSOE API
- `entsoe-py` 0.7.1 has a bug when ENTSOE returns empty XML for per-unit queries
- Options: (a) upgrade entsoe-py when a fix is released, (b) monkey-patch the library, (c) use a different ENTSOE API endpoint for GB
- GB data was previously available — may be a temporary ENTSOE API issue

### NL — Unit not in API response
- `48W00000HOWAO-1M` (Hollandse Kust) not returned by ENTSOE per-unit API
- May need to use a different query method or register additional EIC codes

### Import job silent failure
- `import_from_api.py` exits with code 0 even when 0 records are stored
- `import_job_service.py` marks job as "success" with `records_imported=0`
- Should add validation: if total_records == 0 and no expected errors, exit with code 1

## Commands Used

```bash
# Diagnostic queries
poetry run python /tmp/diagnose_entsoe_gap.py

# Dry run test
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2026-02-10 --end 2026-02-10 --dry-run

# Backfill import (all zones except FR)
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2026-01-24 --end 2026-02-12

# Backfill FR after fix
poetry run python scripts/seeds/raw_generation_data/entsoe/import_from_api.py \
  --start 2026-01-24 --end 2026-02-12 --zones FR

# Aggregate
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --source ENTSOE --start 2026-01-24 --end 2026-02-12
```

## Key Learnings

1. **When adding consumption parsing to the client, update ALL consumers** — `entsoe_client.py`, `raw_data_storage_service.py`, AND `import_from_api.py` must all handle generation/consumption split
2. **PostgreSQL `ON CONFLICT DO UPDATE` rejects duplicate keys within the same INSERT batch** — must deduplicate or split before inserting
3. **`entsoe-py` doesn't handle empty API responses gracefully** — always wrap `query_generation_per_plant()` with error handling for `AttributeError`/`RangeIndex`
4. **Import scripts should fail loudly on 0 records** — silent success with 0 records is worse than a clear failure
