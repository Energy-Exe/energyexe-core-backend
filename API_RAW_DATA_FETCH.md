# API Raw Data Fetch - Implementation Guide

## Overview

This system allows you to fetch data from external APIs and store it in `generation_data_raw` table, matching the format of Excel file imports. The raw data can then be processed using existing aggregation scripts.

## Architecture

```
External APIs → Raw Data Storage → generation_data_raw → Aggregation Scripts → generation_data
```

**Key Principle**: Fetch and storage are completely separate from aggregation.

## Available Sources

### ✅ Implemented (with Public APIs)

1. **ENTSOE** (European Network)
   - Data Type: 15-minute or hourly
   - Format: MW values
   - Coverage: European wind farms with EIC codes
   - Endpoint: `POST /api/v1/raw-data/entsoe/fetch`

2. **ELEXON** (UK)
   - Data Type: 30-minute settlement periods
   - Format: MWh values
   - Coverage: UK wind farms with BM Units
   - Endpoint: `POST /api/v1/raw-data/elexon/fetch`

3. **EIA** (USA)
   - Data Type: Monthly totals
   - Format: MWh values
   - Coverage: US wind plants
   - Endpoint: `POST /api/v1/raw-data/eia/fetch`
   - Note: Uses mock data if API key not configured

4. **TAIPOWER** (Taiwan)
   - Data Type: Live/10-minute snapshots
   - Format: MW values
   - Coverage: All Taiwan wind units (API returns all units in one call)
   - Endpoint: `POST /api/v1/raw-data/taipower/fetch`
   - Special: API returns ALL units, we filter for selected windfarms

### ❌ Not Available (No Public APIs)

5. **NVE** (Norway)
   - Endpoint: `POST /api/v1/raw-data/nve/fetch`
   - Returns error: "No public API - use Excel import"

6. **ENERGISTYRELSEN** (Denmark)
   - Endpoint: `POST /api/v1/raw-data/energistyrelsen/fetch`
   - Returns error: "No public API - use Excel import"

## API Endpoints

### Base URL
```
http://127.0.0.1:8001/api/v1/raw-data
```

### Request Format (All Sources)

```json
POST /api/v1/raw-data/{source}/fetch

{
  "windfarm_ids": [123, 456],
  "start_date": "2024-01-01T00:00:00Z",
  "end_date": "2024-01-31T23:59:59Z"
}
```

### Response Format

```json
{
  "success": true,
  "source": "ENTSOE",
  "windfarm_ids": [123, 456],
  "windfarm_names": ["Anholt", "Horns Rev 1"],
  "date_range": {
    "start": "2024-01-01T00:00:00",
    "end": "2024-01-31T23:59:59"
  },
  "records_stored": 2880,
  "records_updated": 120,
  "generation_units_processed": [
    {
      "id": 789,
      "code": "48W000000ANHOLT1",
      "name": "Anholt Wind Farm",
      "records_stored": 1440,
      "records_updated": 60
    }
  ],
  "summary": {
    "total_api_calls": 1,
    "api_response_time_seconds": 2.5
  },
  "errors": []
}
```

## Data Transformation Rules

### ENTSOE
```python
GenerationDataRaw(
    source='ENTSOE',
    source_type='api',
    identifier='48W000000ANHOLT1',  # EIC code
    period_start='2024-01-01T00:00:00+00:00',
    period_end='2024-01-01T00:15:00+00:00',
    period_type='PT15M',
    value_extracted=125.5,  # MW
    unit='MW',
    data={
        'eic_code': '48W000000ANHOLT1',
        'area_code': 'DK_1',
        'production_type': 'wind',
        'resolution_code': 'PT15M',
        'installed_capacity_mw': 400.0,
        'fetch_metadata': {
            'fetched_by_user_id': 1,
            'fetch_timestamp': '2024-01-01T12:00:00Z',
            'fetch_method': 'api',
            'api_metadata': {...}
        }
    }
)
```

### ELEXON
```python
GenerationDataRaw(
    source='ELEXON',
    source_type='api',
    identifier='BARKB-1',  # BM Unit
    period_start='2024-01-01T00:00:00+00:00',
    period_end='2024-01-01T00:30:00+00:00',
    period_type='PT30M',
    value_extracted=45.2,  # MWh
    unit='MWh',
    data={
        'bm_unit': 'BARKB-1',
        'level_from': 45.2,
        'level_to': 45.8,
        'settlement_period': 1,
        'settlement_date': '2024-01-01',
        'fetch_metadata': {...}
    }
)
```

### EIA
```python
GenerationDataRaw(
    source='EIA',
    source_type='api',
    identifier='12345',  # Plant Code
    period_start='2024-01-01T00:00:00+00:00',
    period_end='2024-02-01T00:00:00+00:00',
    period_type='month',
    value_extracted=50000.0,  # MWh
    unit='MWh',
    data={
        'plant_code': '12345',
        'plant_name': 'Example Wind Farm',
        'state': 'TX',
        'fuel_type': 'WND',
        'period': '2024-01',
        'generation_mwh': 50000.0,
        'fetch_metadata': {...}
    }
)
```

### TAIPOWER
```python
GenerationDataRaw(
    source='TAIPOWER',
    source_type='api',
    identifier='彰工',  # Chinese unit name
    period_start='2024-01-01T12:00:00+00:00',
    period_end='2024-01-01T12:10:00+00:00',
    period_type='PT10M',
    value_extracted=25.5,  # MW
    unit='MW',
    data={
        'unit_name': '彰工',
        'generation_type': 'wind',
        'installed_capacity_mw': 50.0,
        'net_generation_mw': 25.5,
        'capacity_utilization_percent': 51.0,
        'notes': null,
        'fetch_metadata': {...}
    }
)
```

## Key Features

### ✅ Update Existing Records
- If a record exists (same source + identifier + period_start), it will be **updated**
- Tracks both `records_stored` (new) and `records_updated` (existing)

### ✅ Track Data Origin
- `source_type='api'` distinguishes from Excel imports (`source_type='excel'`)
- `fetch_metadata` includes:
  - `fetched_by_user_id`: Who triggered the fetch
  - `fetch_timestamp`: When it was fetched
  - `fetch_method`: Always 'api'
  - `api_metadata`: Response metadata from the API

### ✅ Multiple Windfarms
- Can fetch data for multiple windfarms in one request
- Each windfarm's generation units are processed separately

### ✅ Granular Date Range
- Day-level precision (not just years)
- Date range is flexible for each source

## Usage Examples

### Example 1: Fetch ENTSOE Data
```bash
curl -X POST http://127.0.0.1:8001/api/v1/raw-data/entsoe/fetch \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "windfarm_ids": [8767],
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-07T23:59:59Z"
  }'
```

### Example 2: Fetch EIA Monthly Data
```bash
curl -X POST http://127.0.0.1:8001/api/v1/raw-data/eia/fetch \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "windfarm_ids": [7204],
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-12-31T23:59:59Z"
  }'
```

### Example 3: Fetch TAIPOWER Live Data
```bash
curl -X POST http://127.0.0.1:8001/api/v1/raw-data/taipower/fetch \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "windfarm_ids": [1234, 5678],
    "start_date": "2024-01-01T00:00:00Z",
    "end_date": "2024-01-01T23:59:59Z"
  }'
```

## After Fetching: Run Aggregation

Once raw data is stored, process it using existing scripts:

### For Hourly Data (ENTSOE, ELEXON, TAIPOWER)
```bash
cd energyexe-core-backend

# Process specific date range
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --start 2024-01-01 \
  --end 2024-01-07 \
  --source ENTSOE
```

### For Monthly Data (EIA)
```bash
cd energyexe-core-backend

# Process specific month range
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_monthly.py \
  --start 2024-01 \
  --end 2024-12 \
  --source EIA
```

## Database Queries

### Check Raw Data
```sql
-- See what API data was fetched
SELECT
    source,
    source_type,
    identifier,
    COUNT(*) as records,
    MIN(period_start) as first_date,
    MAX(period_start) as last_date,
    data->'fetch_metadata'->>'fetched_by_user_id' as user_id,
    data->'fetch_metadata'->>'fetch_timestamp' as fetch_time
FROM generation_data_raw
WHERE source_type = 'api'
GROUP BY source, source_type, identifier,
         data->'fetch_metadata'->>'fetched_by_user_id',
         data->'fetch_metadata'->>'fetch_timestamp'
ORDER BY MAX(period_start) DESC;
```

### Check Aggregated Data
```sql
-- See if raw data was aggregated
SELECT
    source,
    source_resolution,
    COUNT(*) as records,
    MIN(hour) as first_hour,
    MAX(hour) as last_hour
FROM generation_data
WHERE source IN ('ENTSOE', 'ELEXON', 'EIA', 'TAIPOWER')
GROUP BY source, source_resolution
ORDER BY source;
```

## Frontend Access

Navigate to: **Raw Data Fetch** in the sidebar under "Data" section

URL: `http://localhost:3008/raw-data-fetch`

## Workflow

1. **Select Source**: Choose ENTSOE, ELEXON, EIA, or TAIPOWER
2. **Select Windfarms**: Multiple selection (filtered by source's country)
3. **Choose Date Range**: Day-level precision
4. **Click "Fetch & Store"**: Data is fetched and stored synchronously
5. **View Results**: See records stored/updated and processing details
6. **Run Aggregation**: Use CLI scripts to process raw data

## Source-Specific Notes

### ENTSOE
- Automatically detects bidding zones from windfarm configuration
- Extracts EIC codes from generation units
- Handles both 15-minute and hourly data
- Returns per-unit data (not aggregated by zone)

### ELEXON
- Fetches data for BM Units associated with windfarms
- 30-minute settlement periods
- Values are MWh for each period

### EIA
- Monthly data only (not daily/hourly)
- Requires valid API key (falls back to mock data for testing)
- Date range is rounded to months

### TAIPOWER
- Live data only (current snapshot)
- API returns ALL Taiwan wind units in one call
- We filter for selected windfarms after fetching
- Good for current status, not historical data

## Files Created

### Backend
- `app/schemas/raw_data_fetch.py` - Request/response schemas
- `app/services/raw_data_storage_service.py` - Service layer
- `app/api/v1/endpoints/raw_data_fetch.py` - API endpoints
- Updated `app/api/v1/router.py` - Registered routes

### Frontend
- `src/lib/raw-data-fetch-api.ts` - API hooks
- `src/routes/_protected/raw-data-fetch/index.tsx` - UI page
- Updated `src/components/layout/admin-layout.tsx` - Navigation

## Next Steps

After fetching raw data:
1. Verify data in `generation_data_raw` table
2. Run appropriate aggregation script
3. Verify processed data in `generation_data` table
4. Use in existing dashboards and visualizations
