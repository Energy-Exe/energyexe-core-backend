# Taipower Data Import

This directory contains scripts for importing Taipower (Taiwan Power Company) wind generation data into the `generation_data_raw` table.

## Import Methods

### Method 1: Excel File Import (Historical Data)

**Use for:**
- Bulk historical data (2020-2025)
- Large date ranges
- Initial data load
- Specific wind farm historical data

**Source Files:**
- **Location**: `data/*.xlsx` (22 Excel files, one per wind farm)
- **Format**: Taiwan wind farm hourly generation data
- **Columns**: Timestamp, Installed capacity(MW), Power generation(MWh), Capacity factor(%)

**Script:** `import_parallel_optimized.py`

**Usage:**
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

**Features:**
- Parallel processing of Excel files
- English filename → Chinese code mapping
- Auto-cleanup before import
- Stores as `source_type='file'`

---

### Method 2: API Import (Live Data Snapshots)

**Use for:**
- Current/live generation data
- Hourly or daily snapshots
- Building historical data over time
- Monitoring real-time performance

**Source:** Taipower Open Data API
**Endpoint:** `https://service.taipower.com.tw/data/opendata/apply/file/d006001/001.json`

**Script:** `import_from_api.py` (NEW!)

**IMPORTANT**: Taipower API only provides **current/live data** (not historical by date range). Run this script periodically to build historical data.

**Usage:**
```bash
# Fetch and store current snapshot
poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py

# Dry run (see what would be imported)
poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py --dry-run

# Import specific units only (by Chinese code)
poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py --units "彰工" "海能風"
```

**Features:**
- ✅ **Live data snapshots** (updated every ~10 minutes by Taipower)
- ✅ **Bulk upsert** (updates existing records, no duplicates)
- ✅ **Filters for wind generation** only (風力)
- ✅ **Maps to configured units** automatically
- ✅ Stores as `source_type='api'`

**Automation with Cron:**
```bash
# Run hourly to capture snapshots
0 * * * * cd /path/to/project && poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py
```

---

## File Name → Unit Code Mapping

Each Excel file represents one wind farm:

| English File Name | Chinese Code | Database Name |
|------------------|--------------|---------------|
| Chang Kong | 彰工 | Chang Kong |
| Formosa 1 - HaiYang Zhunan | 海洋竹南 | Formosa 1 (HaiYang) |
| Formosa 2 - HaiNeng | 海能風 | Formosa 2 (HaiNeng) |
| Changfang-Xidao FangEr | 芳二風 | Changfang & Xidao 2 (FangEr) |
| Changfang-Xidao FangYi | 芳一風 | Changfang & Xidao 1 (FangYi) |
| Greater ChanghuaSE - WoEr | 沃二風 | Greater Changhua 2A (WoEr) |
| Greater ChanghuaSE - WoYi | 沃一風 | Greater Changhua 1 (WoYi) |
| Yunlin YunHu | 允湖(註10) | Yunlin (YunHu) |
| Yunlin YunSi | 允西(註10) | Yunlin (YunSi) |
| Zhongneng | 中能風(註10) | Zhongneng |
| ... and 13 more units |

---

## Data Mapping

### Database Fields
```
source           = 'Taipower'
source_type      = 'file' or 'api'
identifier       = Chinese unit code (e.g., '彰工', '海能風')
period_start     = DateTime (UTC, converted from Taiwan time UTC+8)
period_end       = period_start + 1 hour (for Excel) or same as start (for API snapshots)
period_type      = 'hour' (Excel) or 'snapshot' (API)
value_extracted  = Generation output (MW for API, MWh for Excel)
unit             = 'MW' (API) or 'MWh' (Excel)
data             = JSONB with full details
```

### JSONB Data Structure

**From Excel Files:**
```json
{
  "generation_mw": 119.67,
  "installed_capacity_mw": 400,
  "capacity_factor": 29.9,
  "unit_code": "海能風",
  "generation_unit_id": 123,
  "file_source": "Formosa 2 - HaiNeng.xlsx"
}
```

**From API:**
```json
{
  "generation_mw": 336.4,
  "installed_capacity_mw": 376,
  "capacity_factor": 89.5,
  "generation_type": "風力",
  "unit_code": "海能風",
  "unit_name": "Formosa 2 (HaiNeng)",
  "generation_unit_id": 123,
  "windfarm_id": 456,
  "notes": "",
  "api_timestamp": "2025-10-21T21:20:00",
  "import_metadata": {
    "import_timestamp": "2025-10-21T19:31:21Z",
    "import_method": "api_script",
    "import_script": "import_from_api.py"
  }
}
```

---

## API Import Details

### How Taipower API Works

**Endpoint**: `https://service.taipower.com.tw/data/opendata/apply/file/d006001/001.json`

**Response:**
- Returns **current snapshot** of all Taiwan generation units
- Updated every ~10 minutes by Taipower
- Includes all generation types (thermal, hydro, solar, wind, etc.)
- 210+ total units, ~28 wind farms

**Data Fields:**
- `DateTime`: Timestamp of snapshot
- `aaData`: Array of generation units
  - `unit_name`: Chinese unit code (機組名稱)
  - `generation_type`: 風力, 火力, 水力, etc.
  - `installed_capacity_mw`: Installed capacity
  - `net_generation_mw`: Current generation
  - `capacity_utilization_percent`: Capacity factor
  - `notes`: Additional notes

### Import Process

1. **Fetch** current snapshot from API (all 210+ units)
2. **Filter** for wind generation units (風力) only → ~28 units
3. **Match** to configured units in database (Chinese code mapping)
4. **Store** in `generation_data_raw` with `period_type='snapshot'`
5. **Upsert** - if timestamp already exists, update values

### Building Historical Data

Since the API only provides current snapshots, you need to run the script periodically:

**Hourly snapshots (recommended):**
```bash
# Cron: Every hour
0 * * * * cd /path/to/project && poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py
```

**Daily snapshots:**
```bash
# Cron: Once per day at noon
0 12 * * * cd /path/to/project && poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py
```

Over time, this builds a historical dataset of Taipower snapshots.

---

## After Import: Run Aggregation

Both import methods store data in `generation_data_raw`. After importing, process into hourly aggregates:

```bash
# Aggregate Taipower data
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --source Taipower \
  --start 2025-10-21 \
  --end 2025-10-21
```

Note: Use `--source Taipower` (capital T, lowercase rest), not `TAIPOWER`.

---

## Example Workflows

### Scenario 1: Initial Setup (Historical Excel + Start API Monitoring)

```bash
# 1. Import all historical Excel files
poetry run python scripts/seeds/raw_generation_data/taipower/import_parallel_optimized.py

# 2. Start capturing live snapshots
poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py

# 3. Set up cron for hourly snapshots
crontab -e
# Add: 0 * * * * cd /path/to/project && poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py
```

### Scenario 2: Daily API Snapshot

```bash
# Fetch current data
poetry run python scripts/seeds/raw_generation_data/taipower/import_from_api.py

# Aggregate
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --source Taipower --start 2025-10-21 --end 2025-10-21
```

### Scenario 3: Web UI File Upload (Alternative)

For historical data updates, use the web UI:
1. Navigate to `/raw-data-fetch` → "File Upload" tab
2. Select "Taipower (Taiwan)"
3. Select generation unit
4. Upload Excel file with date range filtering

---

## Comparison: Excel vs API

| Method | Data Type | Coverage | Frequency | Best For |
|--------|-----------|----------|-----------|----------|
| **Excel Import** | Historical hourly | 2020-2025 | One-time/monthly | Bulk historical data |
| **API Import** | Live snapshots | Current only | Hourly/daily | Real-time monitoring |
| **Web UI Upload** | Historical hourly | User-specified | As needed | Operations team updates |

---

## Configured Units

The database has 33 Taipower wind generation units configured. The API import script automatically:

1. Fetches all 210+ units from API
2. Filters for wind generation (風力) → ~28 units
3. Matches to configured 33 units by Chinese code
4. Stores ~23-25 matched units per snapshot

**Not matched units** (in API but not in database):
- 其它台電自有 (Other Taipower-owned)
- 其它購電風力 (Other purchased wind)
- 沃四風(註10), 沃南風(註10) (Not yet configured)
- 小計 (Subtotal - not a real unit)

---

## Troubleshooting

### "No Taipower units configured in database"
- Run generation unit import first
- Check units exist with `source='Taipower'` (capital T)

### API Returns Data But 0 Records Stored
- Unit codes in database don't match API response
- Verify Chinese codes match exactly (including special characters)

### API Connection Errors
- Check network connectivity
- Taipower API may be down temporarily
- Retry after a few minutes

---

## Files in This Directory

- `import_from_api.py` - API import script (live snapshots) **NEW!**
- `import_parallel_optimized.py` - Excel import script (historical data)
- `check_import_status.py` - View current data coverage
- `clear_taipower_data.py` - Clear all Taipower data
- `data/` - Directory for Excel files (22 files, one per wind farm)
- `README.md` - This file

---

## Key Differences from Other Sources

**ENTSOE/ELEXON/EIA**: Support date range queries in API → Can fetch historical data
**Taipower**: Only provides current snapshot → Must run periodically to build historical data

This is why Taipower has both:
- Excel files for historical data (2020-2025)
- API script for ongoing live data collection
- Web UI file upload for ad-hoc historical updates
