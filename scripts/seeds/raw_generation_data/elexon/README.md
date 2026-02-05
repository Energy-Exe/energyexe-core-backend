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

## After Import: Aggregation Pipeline

Both import methods store data in `generation_data_raw`. After importing, raw 30-minute records must be aggregated into hourly `generation_data` records.

### How Aggregation Works

Two processors exist:

| Processor | Location | Use case |
|-----------|----------|----------|
| `process_generation_data_daily.py` | `scripts/seeds/aggregate_generation_data/` | Multi-source daily cron, uses `settlement_date+SP` for UTC hour |
| `elexon_processor.py` | `scripts/seeds/` | ELEXON-only batch reprocessing with verify/debug modes |

For each day and each BM unit, the aggregation does:

1. **Fetch** B1610 raw records from `generation_data_raw` (excluding BOAV source types)
2. **Deduplicate** — if both API and CSV records exist for the same `(identifier, period_start)`, API wins
3. **Fetch** BOAV bid records separately (curtailment data)
4. **Calculate correct UTC hour** from `settlement_date` + `settlement_period` in the JSONB `data` column (not from `period_start` — see BST section below)
5. **Apply import/export sign** — `E` (export to grid) = positive, `I` (import from grid) = negative
6. **Group** by `(hour, identifier)` and sum:
   - `metered_mwh` = sum of signed B1610 values (what was delivered to grid)
   - `curtailed_mwh` = sum of abs(BOAV bid volumes) (what was curtailed)
   - `generation_mwh` = `metered_mwh` + `curtailed_mwh` (actual production capability)
7. **Clear** existing aggregated records for that day/source (idempotent — safe to re-run)
8. **Save** to `generation_data` with capacity factor, quality scores, and `raw_data_ids` linkage

### Running Aggregation

```bash
# Aggregate a date range (daily processor)
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_daily.py \
  --source ELEXON --start 2025-10-01 --end 2025-10-31

# Parallel re-aggregation by month (for large ranges)
poetry run python scripts/seeds/aggregate_generation_data/reprocess_year_parallel.py \
  --source ELEXON --year 2025

# ELEXON-specific processor with verification
poetry run python scripts/seeds/elexon_processor.py process --start 2025-10-01 --end 2025-10-31
poetry run python scripts/seeds/elexon_processor.py verify --windfarm-id 42
poetry run python scripts/seeds/elexon_processor.py debug --hour "2025-10-26T01:00:00" --windfarm-id 42
```

### Aggregated Data Schema (`generation_data`)

One row per `(hour, generation_unit_id, source)` — enforced by unique constraint.

| Field | Meaning |
|-------|---------|
| `metered_mwh` | What the grid received — **use this for validation against ELEXON** |
| `curtailed_mwh` | Energy lost to curtailment (from BOAV bids) |
| `generation_mwh` | `metered + curtailed` — actual production capability |
| `capacity_factor` | `generation_mwh / capacity_mw` (from `generation_units`) |
| `raw_data_ids` | Array of `generation_data_raw.id` values for traceability |
| `quality_flag` | HIGH/MEDIUM/LOW/POOR based on data completeness |

### BOAV-Only Hours

Some hours have curtailment data but zero metered output (fully curtailed). The pipeline creates records with `metered_mwh = 0`, `generation_mwh = curtailed_mwh`. About 2,900 hour/unit combinations have BOAV data with no corresponding aggregated record (65% at 23:00 UTC day boundary).

---

## Key Things to Keep in Mind

### BST / DST Timezone Handling (Critical)

UK settlement periods are relative to **local time** (`Europe/London`), not UTC. During BST (late March - late October), local time is UTC+1.

- Settlement period 1 starts at 00:00 UK time = **23:00 UTC the previous day** during BST
- Normal days have **48** settlement periods, spring-forward days have **46**, fall-back days have **50**
- The aggregation query window extends **+1 hour** beyond the UTC day boundary to capture BST-offset records
- The correct UTC hour **must** be derived from `settlement_date + settlement_period` in the JSONB `data` column — **not** from `period_start`, which was stored incorrectly for historical CSV imports
- `settlement_date` must be present in the JSONB `data` column for correct aggregation. If missing, the pipeline falls back to `period_start` which is wrong during BST.

See `docs/ELEXON_BST_FIX_LOG.md` for full details on the BST fixes applied.

### `metered_mwh` vs `generation_mwh`

- `metered_mwh` matches official ELEXON figures — always use this for validation
- `generation_mwh` includes curtailment and is ~2-5% higher

### Import/Export Indicator

The `import_export_ind` field in JSONB (`I`/`E`) determines the sign during aggregation. Most wind generation is `E` (positive). Some records are `I` (negative — unit consumed from grid).

### API vs CSV Deduplication

Both sources can coexist in `generation_data_raw`. Aggregation always **prefers API** over CSV when duplicates exist for the same `(identifier, period_start)`.

### `start_date` / `first_power_date` Gating

Aggregation skips raw data for dates before a unit's `first_power_date` (or `start_date` as fallback) in `generation_units`. If raw data exists but aggregated data is missing for early months, check these dates.

### Re-aggregation is Idempotent

Both processors clear existing `generation_data` records for the target day/source before inserting. Safe to re-run without creating duplicates. But re-aggregation also updates `raw_data_ids` — if raw data was re-imported (e.g., BST fix), you **must** re-aggregate to update these references.

### Settlement Periods
- ELEXON uses 30-minute settlement periods
- Normal days: 48 periods (Period 1 = 00:00-00:30, Period 48 = 23:30-00:00)
- Spring forward (March): 46 periods (clock skips 01:00-02:00)
- Fall back (October): 50 periods (clock repeats 01:00-02:00)

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