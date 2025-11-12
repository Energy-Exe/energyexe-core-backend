# Weather Data System - Complete Guide

## Table of Contents
- [Overview](#overview)
- [Database Architecture](#database-architecture)
- [Data Flow](#data-flow)
- [Backend Implementation](#backend-implementation)
- [Frontend Implementation](#frontend-implementation)
- [Data Import Process](#data-import-process)
- [API Endpoints](#api-endpoints)
- [Usage Examples](#usage-examples)

---

## Overview

The Weather Data System integrates ERA5 climate reanalysis data from the Copernicus Climate Data Store into the EnergyExe platform, providing:
- Historical weather data for 1,591 windfarms worldwide
- Hourly wind speed, direction, and temperature measurements
- Weather-generation correlation analysis
- Advanced wind resource assessment tools
- Interactive visualizations

**Data Source:** ERA5 Climate Reanalysis (Copernicus CDS)
**Coverage:** 2021-2025 (expandable)
**Granularity:** Hourly
**Spatial Resolution:** Bilinear interpolation to exact windfarm coordinates

---

## Database Architecture

### Tables

#### 1. `weather_data` (Main Table)
Stores processed hourly weather data for each windfarm.

```sql
CREATE TABLE weather_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hour TIMESTAMPTZ NOT NULL,                    -- Hour timestamp (UTC)
    windfarm_id INTEGER NOT NULL REFERENCES windfarms(id),
    wind_speed_100m NUMERIC(8,3),                 -- Wind speed at 100m (m/s)
    wind_direction_deg NUMERIC(6,2),              -- Direction (0-360°)
    temperature_2m_k NUMERIC(8,3),                -- Temperature at 2m (Kelvin)
    temperature_2m_c NUMERIC(8,3),                -- Temperature at 2m (Celsius)
    source VARCHAR(50) DEFAULT 'ERA5',
    raw_data_id BIGINT,                           -- Reference to raw data
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_weather_data_hour ON weather_data(hour);
CREATE INDEX idx_weather_data_windfarm ON weather_data(windfarm_id);
CREATE INDEX idx_weather_data_windfarm_hour ON weather_data(windfarm_id, hour);
```

**Expected Records:** 1,591 windfarms × 24 hours × 365 days = ~14M records/year

#### 2. `weather_data_raw` (Raw Data Table)
Stores raw ERA5 GRIB data before processing.

```sql
CREATE TABLE weather_data_raw (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50),
    timestamp TIMESTAMPTZ,
    latitude NUMERIC(8,5),
    longitude NUMERIC(8,5),
    data JSONB,                                   -- Raw GRIB variables
    metadata JSONB,                               -- Fetch metadata
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Data Flow

```
┌─────────────────┐
│  ERA5 CDS API   │
│  (Copernicus)   │
└────────┬────────┘
         │ GRIB Format
         ▼
┌─────────────────────────────────────────┐
│  fetch_daily_all_windfarms.py           │
│  • Downloads daily GRIB file            │
│  • Covers all windfarm locations        │
│  • Bilinear interpolation to coords     │
└────────┬────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  weather_data Table                     │
│  • 1 record per windfarm per hour       │
│  • 38,184 records per day               │
│    (1,591 windfarms × 24 hours)         │
└────────┬────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  API Endpoints (17 endpoints)           │
│  • Statistics & Timeseries              │
│  • Wind Rose & Distribution             │
│  • Power Curve & Correlation            │
│  • Advanced Analytics                   │
└────────┬────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  Frontend Visualizations                │
│  • Weather Overview Card                │
│  • Weather Analysis Tab (13 charts)    │
│  • Windfarm Details Integration         │
└─────────────────────────────────────────┘
```

---

## Backend Implementation

### File Structure

```
energyexe-core-backend/
├── app/
│   ├── models/
│   │   └── weather_data.py                     # SQLAlchemy models
│   ├── schemas/
│   │   └── weather_data.py                     # Pydantic schemas
│   ├── services/
│   │   ├── weather_data_service.py             # Core CRUD operations
│   │   ├── weather_analytics_service.py        # Wind statistics & analytics
│   │   └── weather_correlation_service.py      # Weather-generation analysis
│   └── api/v1/endpoints/
│       └── weather_data.py                     # 17 API endpoints
├── scripts/seeds/weather_data/
│   ├── fetch_daily_all_windfarms.py            # Main import script
│   └── WEATHER_DATA_COMPLETE_GUIDE.md          # This file
└── alembic/versions/
    └── *_add_weather_data_tables.py            # Database migration
```

### Key Components

#### 1. Models (`app/models/weather_data.py`)
- `WeatherData` - Main weather records
- `WeatherDataRaw` - Raw GRIB data storage

#### 2. Services

**WeatherDataService:**
- `get_availability_calendar()` - Data coverage by date
- `get_missing_dates()` - Identify gaps
- `trigger_fetch_for_date()` - Background job spawning

**WeatherAnalyticsService:**
- `get_weather_timeseries()` - Hourly/daily/monthly aggregation
- `get_wind_statistics()` - Mean, median, percentiles, Weibull
- `get_wind_rose_data()` - Direction × speed frequency
- `get_wind_speed_distribution()` - Histogram with Weibull fit
- `get_diurnal_patterns()` - 24-hour cycle
- `get_seasonal_patterns()` - Monthly trends
- `get_wind_speed_duration_curve()` - Cumulative hours at speeds

**WeatherCorrelationService:**
- `get_weather_generation_correlation()` - Wind vs generation
- `get_power_curve_actual()` - Actual performance curve
- `get_capacity_factor_by_wind()` - CF by speed bins
- `get_energy_rose_data()` - Generation by direction
- `get_temperature_impact()` - Air density effects
- `get_weather_generation_heatmap()` - Hour × month patterns

#### 3. Import Script

**`fetch_daily_all_windfarms.py`**

Features:
- Fetches single ERA5 GRIB file covering all windfarm locations
- Uses bilinear interpolation for exact coordinates
- Processes 1,591 windfarms × 24 hours = 38,184 records/day
- Skips already-complete days (checks for 38,184 records)
- Handles timezone conversion (Oslo → UTC)
- Efficient batch inserts (14 batches of 2,900 records)

Usage:
```bash
# Single day
poetry run python scripts/seeds/weather_data/fetch_daily_all_windfarms.py \
  --date 2024-10-15

# Date range (slow - processes one day at a time)
poetry run python scripts/seeds/weather_data/fetch_daily_all_windfarms.py \
  --start 2024-10-01 --end 2024-10-31
```

Performance:
- GRIB download: ~30-60 seconds
- Processing 1,591 windfarms: ~3-4 minutes
- Database insert: ~10 seconds
- **Total: ~5 minutes per day**

---

## Frontend Implementation

### File Structure

```
energyexe-admin-ui/
├── src/
│   ├── lib/
│   │   └── weather-data-api.ts                 # API hooks & types
│   ├── components/
│   │   ├── weather/
│   │   │   ├── WindRoseChart.tsx               # 13 visualization
│   │   │   ├── WindSpeedDistributionChart.tsx  # components
│   │   │   ├── PowerCurveChart.tsx
│   │   │   ├── DiurnalPatternChart.tsx
│   │   │   ├── SeasonalPatternChart.tsx
│   │   │   ├── WeatherHeatmapChart.tsx
│   │   │   ├── TemperatureImpactChart.tsx
│   │   │   ├── EnergyRoseChart.tsx
│   │   │   ├── CapacityFactorByWindChart.tsx
│   │   │   ├── CorrelationScatterPlot.tsx
│   │   │   ├── WindStatisticsCard.tsx
│   │   │   ├── WindDurationCurveChart.tsx
│   │   │   └── WeatherTimeSeriesChart.tsx
│   │   └── windfarms/
│   │       ├── weather-overview-card.tsx       # Summary on main page
│   │       └── weather-tab.tsx                 # Full analysis tab
│   └── routes/_protected/
│       └── weather-data/
│           ├── analytics.tsx                   # Standalone analytics
│           └── calendar.tsx                    # Data availability
```

### Integration Points

#### 1. Windfarm Details Page (`windfarms/$windfarmId.tsx`)

**Weather Overview Card** (Auto-loaded):
- Last 365 days statistics
- Average wind speed (P50, P90)
- Prevailing direction
- Average temperature
- Data coverage percentage
- Link to full analysis

**Weather Analysis Tab** (On-demand):
- Date range selector (30/90/365 days quick options)
- 4 sub-tabs:
  1. **Wind Resource** - Rose, distribution, duration curve
  2. **Power Performance** - Power curve, correlation, capacity factor
  3. **Temporal Patterns** - Diurnal, seasonal, heatmap
  4. **Advanced Analysis** - Energy rose, temperature impact

#### 2. Standalone Analytics Page (`/weather-data/analytics`)
- Windfarm selector
- All visualizations available
- Date range controls
- Shareable URLs

### API Hooks (`weather-data-api.ts`)

All hooks use TanStack Query for caching and state management:

```typescript
// Statistics
useWeatherStatistics(windfarmId, startDate, endDate)

// Charts
useWindRose(windfarmId, startDate, endDate)
useWindSpeedDistribution(windfarmId, startDate, endDate)
usePowerCurve(windfarmId, startDate, endDate)
useDiurnalPattern(windfarmId, startDate, endDate)
useSeasonalPattern(windfarmId, startDate, endDate)
useEnergyRose(windfarmId, startDate, endDate)
useTemperatureImpact(windfarmId, startDate, endDate, refSpeed)
useWeatherHeatmap(windfarmId, year, metric)
useWindSpeedDurationCurve(windfarmId, startDate, endDate)

// Correlation
useWeatherGenerationCorrelation(windfarmId, startDate, endDate)
useCapacityFactorByWind(windfarmId, startDate, endDate)

// Data management
useWeatherAvailability(startDate, endDate, windfarmId?)
useWeatherMissingDates(startDate, endDate)
useWeatherFetchMutation()
```

---

## Data Import Process

### Initial Setup

1. **CDS API Key Configuration**
   ```bash
   # Create ~/.cdsapirc
   echo "url: https://cds.climate.copernicus.eu/api" > ~/.cdsapirc
   echo "key: YOUR_KEY_HERE" >> ~/.cdsapirc
   ```

2. **Install Dependencies**
   ```bash
   poetry add cdsapi xarray cfgrib scipy
   ```

3. **Run Database Migration**
   ```bash
   poetry run alembic upgrade head
   ```

### Parallel Import Strategy

For bulk historical imports, use parallel processing:

**1. Generate Missing Dates**
```sql
-- Example: Find missing dates for 2024
WITH all_dates AS (
    SELECT generate_series(
        '2024-01-01'::date,
        '2024-12-31'::date,
        '1 day'::interval
    )::date AS date
),
existing_complete_dates AS (
    SELECT DISTINCT DATE(hour) as date
    FROM weather_data
    WHERE EXTRACT(YEAR FROM hour) = 2024
    GROUP BY DATE(hour)
    HAVING COUNT(*) >= 38184  -- 1,591 windfarms × 24 hours
)
SELECT to_char(ad.date, 'YYYY-MM-DD')
FROM all_dates ad
LEFT JOIN existing_complete_dates ecd ON ad.date = ecd.date
WHERE ecd.date IS NULL
ORDER BY ad.date;
```

**2. Split Into Chunks**
```bash
# Save missing dates to file
psql $DATABASE_URL -t -c "SQL_QUERY" | sed 's/^[[:space:]]*//' > /tmp/missing_2024.txt

# Split into 3 chunks for parallel processing
lines=$(wc -l < /tmp/missing_2024.txt)
chunk=$((lines / 3 + 1))
split -l $chunk /tmp/missing_2024.txt /tmp/missing_2024_part_
```

**3. Launch Parallel Processes**
```bash
#!/bin/bash
cd /path/to/energyexe-core-backend

process_chunk() {
    local file=$1
    local part=$2
    local logfile="/tmp/backfill_2024_$part.log"

    while IFS= read -r date; do
        poetry run python scripts/seeds/weather_data/fetch_daily_all_windfarms.py \
            --date "$date" >> "$logfile" 2>&1
    done < "$file"
}

# Launch 3 processes in parallel
process_chunk /tmp/missing_2024_part_aa aa &
process_chunk /tmp/missing_2024_part_ab ab &
process_chunk /tmp/missing_2024_part_ac ac &

wait
echo "Import complete!"
```

**Performance:**
- 3 parallel processes: ~180 days/hour
- Single process: ~60 days/hour
- Full year (365 days): ~2 hours with 3 processes

### Import Progress Monitoring

```sql
-- Check coverage by year
SELECT
    EXTRACT(YEAR FROM hour) as year,
    COUNT(DISTINCT DATE(hour)) as complete_days,
    CASE
        WHEN EXTRACT(YEAR FROM hour) = 2024 THEN 366
        ELSE 365
    END as total_days,
    ROUND(COUNT(DISTINCT DATE(hour))::numeric /
        CASE WHEN EXTRACT(YEAR FROM hour) = 2024 THEN 366 ELSE 365 END * 100, 1
    ) as pct
FROM weather_data
WHERE EXTRACT(YEAR FROM hour) IN (2021, 2022, 2023, 2024, 2025)
GROUP BY EXTRACT(YEAR FROM hour)
ORDER BY year;
```

---

## API Endpoints

### Base URL: `/api/v1/weather-data`

#### Data Management

**GET `/availability`**
- Returns: Date-by-date availability with record counts
- Query params: `start_date`, `end_date`, `windfarm_id` (optional)

**GET `/missing-dates`**
- Returns: List of dates with incomplete data
- Query params: `start_date`, `end_date`

**POST `/fetch`**
- Triggers background fetch job for a date
- Body: `{ "date": "2024-10-15", "forceRefetch": false }`
- Returns: Job ID for tracking

**GET `/fetch-jobs/{job_id}`**
- Returns: Status of fetch job

#### Wind Analytics

**GET `/windfarms/{id}/timeseries`**
- Returns: Time series data
- Query params: `start_date`, `end_date`, `aggregation` (hourly/daily/monthly)

**GET `/windfarms/{id}/statistics`**
- Returns: Comprehensive wind statistics
- Fields: mean, median, mode, P10, P50, P90, max, min, stdDev, Weibull params

**GET `/windfarms/{id}/wind-rose`**
- Returns: Wind frequency by direction (16 bins) and speed (5 bins)

**GET `/windfarms/{id}/distribution`**
- Returns: Wind speed histogram with Weibull fit

**GET `/windfarms/{id}/diurnal-pattern`**
- Returns: Average wind by hour of day (0-23)

**GET `/windfarms/{id}/seasonal-pattern`**
- Returns: Average wind and temperature by month (1-12)

**GET `/windfarms/{id}/duration-curve`**
- Returns: Cumulative hours at different wind speeds

#### Weather-Generation Correlation

**GET `/windfarms/{id}/correlation`**
- Returns: Wind speed vs generation correlation with R²

**GET `/windfarms/{id}/power-curve`**
- Returns: Actual power curve with cut-in, rated, cut-out speeds

**GET `/windfarms/{id}/capacity-factor-by-wind`**
- Returns: Capacity factor grouped by wind speed bins

**GET `/windfarms/{id}/energy-rose`**
- Returns: Generation contribution by wind direction

**GET `/windfarms/{id}/temperature-impact`**
- Returns: Temperature effect on generation at constant wind speed
- Query params: `reference_wind_speed` (default: 10 m/s)

**GET `/windfarms/{id}/heatmap`**
- Returns: Hour × month heatmap data
- Query params: `year`, `metric` (wind_speed/temperature/generation)

---

## Usage Examples

### Backend (Python)

```python
from app.services.weather_analytics_service import WeatherAnalyticsService
from app.services.weather_correlation_service import WeatherCorrelationService

# Get wind statistics
stats = await WeatherAnalyticsService.get_wind_statistics(
    db=db,
    windfarm_id=123,
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31)
)

# Get power curve
power_curve = await WeatherCorrelationService.get_power_curve_actual(
    db=db,
    windfarm_id=123,
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31)
)
```

### Frontend (TypeScript/React)

```typescript
import { useWeatherStatistics, useWindRose } from '@/lib/weather-data-api'

function WindfarmWeatherDashboard({ windfarmId }: { windfarmId: number }) {
  const startDate = '2024-01-01'
  const endDate = '2024-12-31'

  const { data: stats, isLoading } = useWeatherStatistics(
    windfarmId,
    startDate,
    endDate
  )

  const { data: windRose } = useWindRose(windfarmId, startDate, endDate)

  if (isLoading) return <Skeleton />

  return (
    <div>
      <WindStatisticsCard data={stats} />
      <WindRoseChart data={windRose} />
    </div>
  )
}
```

---

## Key Features & Optimizations

### 1. Skip Complete Days
The import script checks if a day is already complete (38,184 records) before fetching:
```python
async def check_day_complete(date: datetime) -> bool:
    query = text("""
        SELECT COUNT(*) FROM weather_data
        WHERE DATE(hour) = :date
    """)
    result = await db.execute(query, {"date": date.date()})
    count = result.scalar()
    return count >= 38184  # 1,591 windfarms × 24 hours
```

### 2. Bilinear Interpolation
Instead of nearest-grid-point, uses bilinear interpolation for accuracy:
```python
ds_interp = ds.interp(
    latitude=xr.DataArray(lats, dims='points'),
    longitude=xr.DataArray(lons, dims='points'),
    method='linear'
)
```

### 3. Batch Inserts
Uses SQLAlchemy bulk insert for performance (14 batches × 2,900 records):
```python
batch_size = 2900
for i in range(0, len(records), batch_size):
    batch = records[i:i + batch_size]
    db.add_all(batch)
    await db.commit()
```

### 4. Efficient GRIB Processing
Single GRIB file covers all windfarms (vs. 1,591 separate API calls):
```python
# Bounding box for all windfarms
N = max(lats)  # 71.5°N (Norway)
S = min(lats)  # 18.5°N (Taiwan)
W = min(lons)  # -165.9°W (USA)
E = max(lons)  # 121.6°E (Taiwan)
```

### 5. Caching with TanStack Query
Frontend queries are cached for 5 minutes, reducing API load:
```typescript
queryKey: ['weather-data', 'statistics', windfarmId, startDate, endDate]
staleTime: 5 * 60 * 1000  // 5 minutes
```

---

## Troubleshooting

### Common Issues

**1. "No module named 'structlog'"**
```bash
poetry install  # Reinstall dependencies
```

**2. "Connection to CDS API failed"**
- Check ~/.cdsapirc exists with valid key
- Verify network connectivity to cds.climate.copernicus.eu

**3. "No variable named 't2m' in GRIB file"**
- Corrupted GRIB file, delete and re-download:
```bash
rm grib_files/daily/era5_YYYYMMDD.grib
```

**4. Slow imports**
- Use parallel processing (3-6 processes)
- Check CPU usage with `top`
- Verify database connection pool size

**5. Duplicate records**
- Currently no unique constraint on (windfarm_id, hour)
- Future: Add constraint and use ON CONFLICT DO UPDATE

---

## Future Enhancements

1. **Real-time Updates**
   - Automated daily imports via cron job
   - Webhook notifications for data gaps

2. **Additional Variables**
   - Pressure, humidity, precipitation
   - Cloud cover, solar radiation

3. **Forecast Data**
   - Integrate ECMWF forecasts
   - 10-day wind predictions

4. **Machine Learning**
   - Wind-to-power ML models
   - Anomaly detection for sensors

5. **Data Quality**
   - Automated validation rules
   - Outlier detection and flagging

---

## References

- **ERA5 Documentation:** https://confluence.ecmwf.int/display/CKB/ERA5
- **CDS API Guide:** https://cds.climate.copernicus.eu/api-how-to
- **xarray Documentation:** https://docs.xarray.dev/
- **cfgrib Documentation:** https://github.com/ecmwf/cfgrib

---

**Last Updated:** November 10, 2025
**Version:** 1.0
**Maintainer:** EnergyExe Development Team
