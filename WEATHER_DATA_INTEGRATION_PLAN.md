# Weather Data Integration Plan - ERA5 Copernicus

**Created**: 2025-11-03
**Updated**: 2025-11-03
**Purpose**: Fetch and store ERA5 weather data for windfarms (data foundation only)

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Scope & Requirements](#scope--requirements)
3. [Data Architecture](#data-architecture)
4. [Data Fetching Strategy](#data-fetching-strategy)
5. [Data Processing Pipeline](#data-processing-pipeline)
6. [Implementation Phases](#implementation-phases)
7. [Detailed TODO List](#detailed-todo-list)
8. [Scripts Overview](#scripts-overview)
9. [Storage Estimates](#storage-estimates)
10. [Future Phases](#future-phases)

---

## Executive Summary

### Primary Use Case
**Long-term wind resource assessment and inter-annual variability analysis** - 30 years of data (1995-2025) provides:
- Multiple El Niño/La Niña cycles
- Long-term climate patterns and trends
- Robust statistical baseline for wind resource evaluation
- Inter-annual variability analysis for risk assessment

### Goals (Phase 1: Data Foundation)
- Fetch hourly weather data from ERA5 Copernicus (1995-present)
- Store raw ERA5 data (all available parameters for future use)
- Process basic weather metrics (wind speed, direction, temperature) at windfarm level
- Build reusable scripts for daily updates and historical backfill
- **NO analytics, correlation, or frontend yet** - just data pipeline

### Key Constraints
- ERA5 data has **5-day latency** (don't fetch data from last 5 days)
- Data in **GRIB format** (requires xarray + cfgrib libraries)
- **0.25° resolution** grid (~25-30km spacing)
- API prefers chunked requests (monthly batches recommended)

### Success Criteria
- All windfarms have hourly weather data from 1995 to (today - 5 days)
- 30 years of data for long-term wind resource assessment and inter-annual variability analysis
- Scripts can fetch and process data for any date range
- Data models support future expansion (additional parameters, analytics)

---

## Scope & Requirements

### In Scope (Phase 1: Data Seeding)
✅ Windfarm-level weather data (no turbine-level)
✅ Historical backfill (1995 to present - 30 years)
✅ Long-term wind resource assessment and inter-annual variability analysis
✅ Raw data storage (ALL ERA5 parameters in JSONB)
✅ Processed data (wind speed, direction, temperature only)
✅ Script-based approach (runnable per day/date range)
✅ Data models flexible for future expansion

### Out of Scope (Future Phases)
❌ Turbine-level weather data
❌ Hub height wind speed calculations
❌ Roughness length modeling
❌ Correlation analysis
❌ Weibull distributions
❌ Power curve analysis
❌ Performance metrics
❌ Forecast integration
❌ Backend API endpoints
❌ Frontend UI
❌ Data retention/cleanup (keep all data)

### Requirements Summary
1. **Windfarm-level only** - One weather record per windfarm per hour
2. **30 years of data** - 1995 to present for long-term wind resource assessment
3. **Flexible raw storage** - Store all ERA5 parameters (not just wind/temp)
4. **Simple processed data** - Only wind speed, direction, temperature for now
5. **No ERA5 coupling** - Windfarms don't store ERA5 grid references
6. **Reusable scripts** - Can run per day/month for incremental updates

---

## Data Architecture

### Database Models (Simplified)

Follow existing `generation_data_raw` → `generation_data` pattern.

#### 1. `weather_data_raw` Table

Store raw ERA5 data per grid point (source-agnostic, no windfarm coupling).

```python
"""app/models/weather_data.py"""

from datetime import datetime
from sqlalchemy import (
    BigInteger,
    DateTime,
    Numeric,
    String,
    Index,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class WeatherDataRaw(Base):
    """Raw weather data from ERA5 Copernicus (grid point level)."""

    __tablename__ = "weather_data_raw"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Source identification
    source: Mapped[str] = mapped_column(String(20), default="ERA5", nullable=False, index=True)
    source_type: Mapped[str] = mapped_column(String(20), default="api", nullable=False)

    # Temporal fields (hourly UTC)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    # Spatial fields (ERA5 grid point - 0.25° resolution)
    latitude: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)   # e.g., 55.7500
    longitude: Mapped[float] = mapped_column(Numeric(7, 4), nullable=False)  # e.g., 12.5000

    # Raw data storage (ALL ERA5 parameters in JSONB for flexibility)
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    # Example structure:
    # {
    #   "u100": 5.23,        # 100m u-component of wind (m/s)
    #   "v100": -2.45,       # 100m v-component of wind (m/s)
    #   "t2m": 283.15,       # 2m temperature (Kelvin)
    #   "sp": 101325,        # Surface pressure (Pa) - optional, for future
    #   "tp": 0.002,         # Total precipitation (m) - optional, for future
    #   "ssrd": 1500000,     # Solar radiation (J/m²) - optional, for future
    #   "fetch_metadata": {
    #     "fetch_date": "2025-11-03T10:00:00Z",
    #     "grib_file": "era5_202501.grib",
    #     "grid_resolution": "0.25"
    #   }
    # }

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        # Prevent duplicate fetches for same grid point + time
        UniqueConstraint('source', 'latitude', 'longitude', 'timestamp',
                        name='uq_weather_raw_grid_time'),
        # Efficient queries by time and location
        Index('idx_weather_raw_timestamp', 'timestamp'),
        Index('idx_weather_raw_location', 'latitude', 'longitude'),
    )

    def __repr__(self) -> str:
        return f"<WeatherDataRaw(id={self.id}, lat={self.latitude}, lon={self.longitude}, time={self.timestamp})>"
```

**Key Design Decisions:**
- ✅ Store ALL ERA5 parameters in JSONB (not just wind/temp) for future flexibility
- ✅ No windfarm relationship (raw data is source-independent)
- ✅ Grid point level (0.25° resolution)
- ✅ Unique constraint prevents duplicate API fetches
- ✅ Can store data from other sources later (e.g., forecast models)

---

#### 2. `weather_data` Table

Processed hourly weather data linked to windfarms.

```python
"""app/models/weather_data.py (continued)"""

from uuid import uuid4
from sqlalchemy import ForeignKey, Integer
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.orm import relationship


class WeatherData(Base):
    """Processed hourly weather data for windfarms."""

    __tablename__ = "weather_data"

    id: Mapped[str] = mapped_column(
        PostgresUUID(as_uuid=True), primary_key=True, default=uuid4
    )

    # Temporal (fixed hourly period)
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    # Windfarm relationship
    windfarm_id: Mapped[int] = mapped_column(Integer, ForeignKey("windfarms.id"), nullable=False)

    # Calculated wind metrics (at 100m height)
    wind_speed_100m: Mapped[float] = mapped_column(Numeric(8, 3), nullable=False)  # m/s
    wind_direction_deg: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)  # 0-360°

    # Temperature
    temperature_2m_k: Mapped[float] = mapped_column(Numeric(6, 2), nullable=False)  # Kelvin
    temperature_2m_c: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)  # Celsius

    # Source tracking
    source: Mapped[str] = mapped_column(String(20), default="ERA5", nullable=False)
    raw_data_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("weather_data_raw.id"), nullable=True
    )

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    windfarm = relationship("Windfarm", back_populates="weather_data")

    __table_args__ = (
        # One weather record per windfarm per hour
        UniqueConstraint('hour', 'windfarm_id', 'source',
                        name='uq_weather_hour_windfarm_source'),
        # Fast queries by windfarm + time range
        Index('idx_weather_windfarm_hour', 'windfarm_id', 'hour'),
    )

    def __repr__(self) -> str:
        return f"<WeatherData(windfarm_id={self.windfarm_id}, hour={self.hour}, wind_speed={self.wind_speed_100m})>"
```

**Key Design Decisions:**
- ✅ Windfarm-level only (no turbine relationship)
- ✅ Simple metrics: wind speed, direction, temperature
- ✅ No hub height calculations (future phase)
- ✅ No ERA5 grid references stored (keeps windfarm model clean)
- ✅ Links to raw_data_id for traceability

---

#### 3. `windfarm` Table Extensions

**NO ERA5-specific fields added to windfarm model.**

```python
"""app/models/windfarm.py"""

# Add relationship only (no new columns)
class Windfarm(Base):
    # ... existing fields (lat, lng, etc.) ...

    # Relationships
    weather_data = relationship("WeatherData", back_populates="windfarm")
```

**Why no ERA5 fields?**
- Source independence: Windfarm doesn't care if data comes from ERA5, forecasts, or other sources
- Keeps model clean and focused
- Grid point mapping happens in processing scripts, not data model

---

### Data Flow Diagram

```
ERA5 API (GRIB files)
    ↓
[Fetch Script] - extract grid points for windfarm locations
    ↓
weather_data_raw (grid point + all parameters in JSONB)
    ↓
[Processing Script] - calculate wind speed, direction, temp
    ↓
weather_data (windfarm + processed metrics)
    ↓
[Future] Analytics, Correlation, API endpoints
```

---

## Data Fetching Strategy

### Geographic Optimization

**Goal:** Minimize API calls by fetching only unique ERA5 grid points.

**Approach:**

```python
# Step 1: Calculate unique ERA5 grid points for all windfarms
def calculate_era5_grid_points(windfarms: List[Windfarm]) -> Set[Tuple[float, float]]:
    """
    Convert windfarm lat/lon to nearest 0.25° ERA5 grid points.

    Example:
        Windfarm at (55.759, 12.583)
        → ERA5 grid: (55.75, 12.50)  [rounds to nearest 0.25]
    """
    grid_points = set()

    for wf in windfarms:
        # Round to nearest 0.25 degree
        grid_lat = round(wf.lat * 4) / 4  # 55.759 → 55.75
        grid_lon = round(wf.lng * 4) / 4  # 12.583 → 12.50
        grid_points.add((grid_lat, grid_lon))

    return grid_points
```

**Expected Efficiency:**
- 50 windfarms → ~20-30 unique grid points (due to clustering)
- Reduces API calls by 40-60%

### ERA5 CDS API Configuration

**Setup `~/.cdsapirc`:**

```
url: https://cds.climate.copernicus.eu/api
key: fea40a33-7b81-4f9a-a145-a1556b25c940
```

**Install library:**

```bash
poetry add cdsapi
poetry add xarray
poetry add cfgrib
```

### Fetching Strategy

**Monthly chunking** (recommended by ERA5 to avoid request limits):

```python
"""scripts/seeds/weather_data/era5/import_from_api.py"""

import cdsapi
from datetime import datetime, timezone
from typing import List, Tuple, Dict

def fetch_era5_monthly(
    grid_points: List[Tuple[float, float]],
    year: int,
    month: int
) -> str:
    """
    Fetch ERA5 data for all grid points for one month.

    Returns path to downloaded GRIB file.
    """
    c = cdsapi.Client()

    # Create bounding box from grid points
    lats = [p[0] for p in grid_points]
    lons = [p[1] for p in grid_points]

    north = max(lats) + 0.25  # Add buffer
    south = min(lats) - 0.25
    east = max(lons) + 0.25
    west = min(lons) - 0.25

    # Determine days in month
    if month == 12:
        days_in_month = 31
    else:
        next_month = datetime(year, month + 1, 1)
        last_day = next_month - timedelta(days=1)
        days_in_month = last_day.day

    request = {
        'product_type': 'reanalysis',
        'format': 'grib',
        'variable': [
            '100m_u_component_of_wind',
            '100m_v_component_of_wind',
            '2m_temperature',
            # Optional - store for future use:
            'surface_pressure',
            'total_precipitation',
            'surface_solar_radiation_downwards',
        ],
        'year': str(year),
        'month': f'{month:02d}',
        'day': [f'{d:02d}' for d in range(1, days_in_month + 1)],
        'time': [f'{h:02d}:00' for h in range(24)],  # All 24 hours
        'area': [north, west, south, east],  # [N, W, S, E]
    }

    output_file = f'/tmp/era5_{year}{month:02d}.grib'
    c.retrieve('reanalysis-era5-single-levels', request, output_file)

    return output_file
```

### GRIB File Parsing

```python
"""scripts/seeds/weather_data/era5/parse_grib.py"""

import xarray as xr
import pandas as pd
from typing import List, Dict, Set, Tuple

def parse_grib_file(
    grib_file_path: str,
    target_grid_points: Set[Tuple[float, float]]
) -> List[Dict]:
    """
    Parse GRIB file and extract data for target grid points.

    Returns list of records ready for insertion into weather_data_raw.
    """
    # Open GRIB with xarray (using cfgrib engine)
    ds = xr.open_dataset(grib_file_path, engine='cfgrib')

    # Dataset contains:
    # - coordinates: time, latitude, longitude
    # - variables: u100, v100, t2m, sp, tp, ssrd, etc.

    records = []

    for (target_lat, target_lon) in target_grid_points:
        # Select nearest grid point
        point_data = ds.sel(
            latitude=target_lat,
            longitude=target_lon,
            method='nearest'
        )

        # Extract all timestamps for this location
        for time_idx in range(len(point_data.time)):
            timestamp = pd.Timestamp(point_data.time.values[time_idx]).to_pydatetime()

            # Build JSONB data dict with all available parameters
            data_dict = {
                'u100': float(point_data.u100.values[time_idx]),
                'v100': float(point_data.v100.values[time_idx]),
                't2m': float(point_data.t2m.values[time_idx]),
            }

            # Add optional parameters if available
            if 'sp' in point_data:
                data_dict['sp'] = float(point_data.sp.values[time_idx])
            if 'tp' in point_data:
                data_dict['tp'] = float(point_data.tp.values[time_idx])
            if 'ssrd' in point_data:
                data_dict['ssrd'] = float(point_data.ssrd.values[time_idx])

            # Add fetch metadata
            data_dict['fetch_metadata'] = {
                'grib_file': grib_file_path,
                'fetch_date': datetime.utcnow().isoformat(),
                'grid_resolution': '0.25',
            }

            record = {
                'source': 'ERA5',
                'source_type': 'api',
                'timestamp': timestamp,
                'latitude': float(target_lat),
                'longitude': float(target_lon),
                'data': data_dict,
            }
            records.append(record)

    ds.close()
    return records
```

### Bulk Insert Pattern

Follow `generation_data_raw` bulk upsert pattern:

```python
"""scripts/seeds/weather_data/era5/bulk_insert.py"""

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.weather_data import WeatherDataRaw

async def bulk_insert_weather_raw(records: List[Dict], db: AsyncSession):
    """
    Bulk insert raw weather data with conflict resolution.
    """
    stmt = insert(WeatherDataRaw).values(records)

    # On conflict (duplicate timestamp + location), update data
    stmt = stmt.on_conflict_do_update(
        constraint='uq_weather_raw_grid_time',
        set_={
            'data': stmt.excluded.data,
            'updated_at': datetime.utcnow(),
        }
    )

    await db.execute(stmt)
    await db.commit()

    logger.info(f"Inserted/updated {len(records)} raw weather records")
```

---

## Data Processing Pipeline

### Processing Script

Similar to `process_generation_data_robust.py`:

```python
"""scripts/seeds/weather_data/process_weather_data.py"""

import asyncio
import math
from datetime import datetime, timezone
from typing import List, Dict, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.models.weather_data import WeatherDataRaw, WeatherData
from app.models.windfarm import Windfarm


def calculate_nearest_grid_point(lat: float, lon: float) -> Tuple[float, float]:
    """Round to nearest 0.25° ERA5 grid point."""
    grid_lat = round(lat * 4) / 4
    grid_lon = round(lon * 4) / 4
    return (grid_lat, grid_lon)


def calculate_wind_metrics(u100: float, v100: float) -> Dict:
    """
    Calculate wind speed and direction from u/v components.

    Returns: {wind_speed_100m, wind_direction_deg}
    """
    # Wind speed: sqrt(u² + v²)
    wind_speed = math.sqrt(u100**2 + v100**2)

    # Wind direction (meteorological convention: direction FROM which wind blows)
    # atan2(v, u) gives mathematical angle
    math_angle = math.atan2(v100, u100)
    # Convert to degrees
    angle_deg = math.degrees(math_angle)
    # Convert to meteorological direction (0° = North, 90° = East, clockwise)
    wind_direction = (270 - angle_deg) % 360

    return {
        'wind_speed_100m': round(wind_speed, 3),
        'wind_direction_deg': round(wind_direction, 2),
    }


async def process_weather_data(
    start_date: datetime,
    end_date: datetime,
    windfarm_ids: Optional[List[int]] = None
):
    """
    Process raw weather data into weather_data table.

    Args:
        start_date: Start of date range (inclusive)
        end_date: End of date range (inclusive)
        windfarm_ids: Optional list of windfarm IDs to process
    """
    AsyncSessionLocal = get_session_factory()

    async with AsyncSessionLocal() as db:
        # 1. Get windfarms
        query = select(Windfarm)
        if windfarm_ids:
            query = query.where(Windfarm.id.in_(windfarm_ids))
        result = await db.execute(query)
        windfarms = result.scalars().all()

        logger.info(f"Processing weather data for {len(windfarms)} windfarms")

        # 2. Process each windfarm
        for windfarm in windfarms:
            # Find nearest ERA5 grid point
            grid_lat, grid_lon = calculate_nearest_grid_point(windfarm.lat, windfarm.lng)

            # Get raw data for this grid point and time range
            raw_query = select(WeatherDataRaw).where(
                WeatherDataRaw.source == 'ERA5',
                WeatherDataRaw.latitude == grid_lat,
                WeatherDataRaw.longitude == grid_lon,
                WeatherDataRaw.timestamp >= start_date,
                WeatherDataRaw.timestamp <= end_date,
            ).order_by(WeatherDataRaw.timestamp)

            raw_result = await db.execute(raw_query)
            raw_records = raw_result.scalars().all()

            if not raw_records:
                logger.warning(f"No raw data found for windfarm {windfarm.id} at grid ({grid_lat}, {grid_lon})")
                continue

            # 3. Process each raw record
            processed_records = []

            for raw in raw_records:
                # Extract parameters from JSONB
                u100 = raw.data.get('u100')
                v100 = raw.data.get('v100')
                t2m_k = raw.data.get('t2m')

                if u100 is None or v100 is None or t2m_k is None:
                    logger.warning(f"Missing required parameters in raw record {raw.id}")
                    continue

                # Calculate wind metrics
                wind_metrics = calculate_wind_metrics(u100, v100)

                # Convert temperature to Celsius
                t2m_c = t2m_k - 273.15

                # Create processed record
                processed = {
                    'hour': raw.timestamp,
                    'windfarm_id': windfarm.id,
                    'wind_speed_100m': wind_metrics['wind_speed_100m'],
                    'wind_direction_deg': wind_metrics['wind_direction_deg'],
                    'temperature_2m_k': round(t2m_k, 2),
                    'temperature_2m_c': round(t2m_c, 2),
                    'source': 'ERA5',
                    'raw_data_id': raw.id,
                }
                processed_records.append(processed)

            # 4. Bulk insert processed records
            if processed_records:
                await bulk_insert_weather_data(processed_records, db)
                logger.info(f"Processed {len(processed_records)} records for windfarm {windfarm.id}")


async def bulk_insert_weather_data(records: List[Dict], db: AsyncSession):
    """Bulk insert processed weather data with conflict resolution."""
    from sqlalchemy.dialects.postgresql import insert

    stmt = insert(WeatherData).values(records)

    # On conflict, update metrics
    stmt = stmt.on_conflict_do_update(
        constraint='uq_weather_hour_windfarm_source',
        set_={
            'wind_speed_100m': stmt.excluded.wind_speed_100m,
            'wind_direction_deg': stmt.excluded.wind_direction_deg,
            'temperature_2m_k': stmt.excluded.temperature_2m_k,
            'temperature_2m_c': stmt.excluded.temperature_2m_c,
            'raw_data_id': stmt.excluded.raw_data_id,
            'updated_at': datetime.utcnow(),
        }
    )

    await db.execute(stmt)
    await db.commit()


# CLI execution
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Process weather data')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--windfarms', nargs='+', type=int, help='Windfarm IDs (optional)')

    args = parser.parse_args()

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)

    asyncio.run(process_weather_data(start, end, args.windfarms))
```

---

## Implementation Phases

### Phase 1: Database Foundation (Days 1-2)

**Goal:** Database models created and migrated.

**Tasks:**
1. Create `app/models/weather_data.py` with `WeatherDataRaw` and `WeatherData` models
2. Update `app/models/windfarm.py` with relationship
3. Create Alembic migration
4. Run migration on dev database
5. Test models with sample data

**Deliverables:**
- ✅ Models created
- ✅ Migration applied
- ✅ Can insert/query records

---

### Phase 2: Data Fetching Scripts (Days 3-5)

**Goal:** Scripts can fetch ERA5 data and store in `weather_data_raw`.

**Tasks:**
1. Install dependencies (cdsapi, xarray, cfgrib)
2. Set up ERA5 API credentials
3. Create helper functions:
   - `calculate_era5_grid_points()`
   - `fetch_era5_monthly()`
   - `parse_grib_file()`
   - `bulk_insert_weather_raw()`
4. Create main fetch script: `scripts/seeds/weather_data/era5/import_from_api.py`
5. Create status check script: `scripts/seeds/weather_data/era5/check_import_status.py`
6. Test with single month

**Deliverables:**
- ✅ Can fetch data for specific month
- ✅ Raw data stored in database
- ✅ GRIB parsing working correctly

---

### Phase 3: Data Processing Scripts (Days 6-8)

**Goal:** Scripts can process raw data into `weather_data` table.

**Tasks:**
1. Create wind calculation functions
2. Create grid point matching logic
3. Create processing script: `scripts/seeds/weather_data/process_weather_data.py`
4. Add bulk insert with conflict resolution
5. Test with processed test data

**Deliverables:**
- ✅ Can process raw data for date range
- ✅ Processed data linked to windfarms
- ✅ Wind calculations verified correct

---

### Phase 4: Historical Backfill (Days 9-20)

**Goal:** Complete historical data (1995-present) imported.

**Tasks:**
1. Create backfill orchestration script
2. Run month-by-month import (1995-2025)
3. Monitor progress and handle errors
4. Validate data quality
5. Process all raw data

**Deliverables:**
- ✅ 30 years of raw data imported (~7.9M records)
- ✅ All windfarms have processed weather data (~13.1M records)
- ✅ Data quality validated

---

## Detailed TODO List

### Database Setup

- [ ] **TODO 1.1:** Create `app/models/weather_data.py`
  - [ ] Define `WeatherDataRaw` class with all fields
  - [ ] Define `WeatherData` class with all fields
  - [ ] Add `__repr__` methods
  - [ ] Add to `app/models/__init__.py`

- [ ] **TODO 1.2:** Update `app/models/windfarm.py`
  - [ ] Add `weather_data` relationship
  - [ ] No new columns needed

- [ ] **TODO 1.3:** Create Alembic migration
  ```bash
  cd energyexe-core-backend
  poetry run alembic revision --autogenerate -m "Add weather data tables"
  ```
  - [ ] Review generated migration
  - [ ] Test migration up/down

- [ ] **TODO 1.4:** Apply migration
  ```bash
  poetry run alembic upgrade head
  ```

- [ ] **TODO 1.5:** Test models
  - [ ] Write test script to insert sample records
  - [ ] Verify relationships work
  - [ ] Check indexes created

---

### Dependencies Installation

- [ ] **TODO 2.1:** Update `pyproject.toml`
  ```toml
  cdsapi = "^0.7.7"
  xarray = "^2024.1.0"
  cfgrib = "^0.9.12"
  ```

- [ ] **TODO 2.2:** Install dependencies
  ```bash
  poetry install
  ```

- [ ] **TODO 2.3:** Configure ERA5 API
  - [ ] Create `~/.cdsapirc` with credentials
  - [ ] Test API connection

---

### Fetch Scripts

- [ ] **TODO 3.1:** Create directory structure
  ```
  scripts/seeds/weather_data/
  ├── era5/
  │   ├── __init__.py
  │   ├── import_from_api.py
  │   ├── parse_grib.py
  │   ├── check_import_status.py
  │   └── helpers.py
  └── process_weather_data.py
  ```

- [ ] **TODO 3.2:** Create `helpers.py`
  - [ ] `calculate_era5_grid_points(windfarms)` function
  - [ ] `haversine_distance(lat1, lon1, lat2, lon2)` function
  - [ ] `create_date_chunks(start, end)` function

- [ ] **TODO 3.3:** Create `parse_grib.py`
  - [ ] `parse_grib_file(file_path, grid_points)` function
  - [ ] Handle missing parameters gracefully
  - [ ] Return list of dicts for bulk insert

- [ ] **TODO 3.4:** Create `import_from_api.py`
  - [ ] `fetch_era5_monthly(grid_points, year, month)` function
  - [ ] `get_active_windfarms()` helper
  - [ ] `bulk_insert_weather_raw(records)` function
  - [ ] Main CLI with argparse
  - [ ] Error handling and retry logic

- [ ] **TODO 3.5:** Create `check_import_status.py`
  - [ ] Show coverage per grid point
  - [ ] Show missing date ranges
  - [ ] Summary statistics

- [ ] **TODO 3.6:** Test fetch script
  ```bash
  # Test with single month
  poetry run python scripts/seeds/weather_data/era5/import_from_api.py \
    --start 2025-01-01 --end 2025-01-31
  ```

---

### Processing Scripts

- [ ] **TODO 4.1:** Create `process_weather_data.py`
  - [ ] `calculate_nearest_grid_point(lat, lon)` function
  - [ ] `calculate_wind_metrics(u100, v100)` function
  - [ ] `process_weather_data(start, end, windfarm_ids)` main function
  - [ ] `bulk_insert_weather_data(records)` function
  - [ ] CLI with argparse

- [ ] **TODO 4.2:** Add logging
  - [ ] Use structlog (consistent with project)
  - [ ] Log progress, errors, warnings
  - [ ] Track processing time

- [ ] **TODO 4.3:** Test processing script
  ```bash
  # Process January 2025
  poetry run python scripts/seeds/weather_data/process_weather_data.py \
    --start 2025-01-01 --end 2025-01-31
  ```

- [ ] **TODO 4.4:** Verify calculations
  - [ ] Manually verify wind speed calculation
  - [ ] Verify wind direction (check known wind events)
  - [ ] Verify temperature conversion

---

### Historical Backfill

- [ ] **TODO 5.1:** Create backfill orchestration script
  ```python
  # scripts/seeds/weather_data/era5/backfill_historical.py
  ```
  - [ ] Loop through years 1995-2025
  - [ ] Loop through months
  - [ ] Fetch + Process for each month
  - [ ] Progress tracking
  - [ ] Resume from last successful month

- [ ] **TODO 5.2:** Run backfill (estimated 20-25 hours for 30 years)
  ```bash
  poetry run python scripts/seeds/weather_data/era5/backfill_historical.py \
    --start-year 1995 --end-year 2025
  ```

- [ ] **TODO 5.3:** Monitor progress
  - [ ] Check logs for errors
  - [ ] Verify data being inserted
  - [ ] Check disk space usage

- [ ] **TODO 5.4:** Data quality checks
  - [ ] Run check_import_status.py
  - [ ] Verify no missing months
  - [ ] Spot-check calculations
  - [ ] Compare with known weather events

---

## Scripts Overview

### Script: `import_from_api.py`

**Purpose:** Fetch ERA5 data for date range and store in `weather_data_raw`.

**Usage:**
```bash
# Fetch single day
poetry run python scripts/seeds/weather_data/era5/import_from_api.py \
  --start 2025-01-15 --end 2025-01-15

# Fetch month
poetry run python scripts/seeds/weather_data/era5/import_from_api.py \
  --start 2025-01-01 --end 2025-01-31

# Fetch year
poetry run python scripts/seeds/weather_data/era5/import_from_api.py \
  --start 2025-01-01 --end 2025-12-31
```

**Flow:**
1. Get all active windfarms
2. Calculate unique ERA5 grid points
3. For each month in date range:
   - Call ERA5 API (downloads GRIB file)
   - Parse GRIB file
   - Bulk insert into `weather_data_raw`
   - Delete GRIB file
4. Log summary

---

### Script: `process_weather_data.py`

**Purpose:** Process raw data into `weather_data` table.

**Usage:**
```bash
# Process single day
poetry run python scripts/seeds/weather_data/process_weather_data.py \
  --start 2025-01-15 --end 2025-01-15

# Process specific windfarms
poetry run python scripts/seeds/weather_data/process_weather_data.py \
  --start 2025-01-01 --end 2025-01-31 \
  --windfarms 1 2 3

# Process all windfarms for month
poetry run python scripts/seeds/weather_data/process_weather_data.py \
  --start 2025-01-01 --end 2025-01-31
```

**Flow:**
1. Get windfarms (all or filtered)
2. For each windfarm:
   - Find nearest ERA5 grid point
   - Query raw data for grid point + date range
   - Calculate wind speed, direction, temperature
   - Bulk insert into `weather_data`
3. Log summary

---

### Script: `check_import_status.py`

**Purpose:** Check what data has been imported.

**Usage:**
```bash
poetry run python scripts/seeds/weather_data/era5/check_import_status.py
```

**Output:**
```
ERA5 Weather Data Import Status
================================

Grid Points:
  (55.75, 12.50): 1995-01-01 to 2025-10-28 [OK] (30 years)
  (56.00, 10.25): 1995-01-01 to 2025-10-28 [OK] (30 years)
  (57.25, 11.75): 1995-01-01 to 2025-09-30 [INCOMPLETE]

Windfarms Processed:
  Windfarm 1 (Horns Rev 1): 1995-01-01 to 2025-10-28 [OK] (30 years)
  Windfarm 2 (Anholt): 1995-01-01 to 2025-10-28 [OK] (30 years)
  Windfarm 5 (Nysted): 1995-01-01 to 2025-09-30 [INCOMPLETE]

Total Raw Records: 7,884,000
Total Processed Records: 13,140,000
Coverage: 30 years (1995-2025)
```

---

### Script: `backfill_historical.py`

**Purpose:** Orchestrate full historical backfill.

**Usage:**
```bash
# Full backfill (1995-2025, 30 years)
poetry run python scripts/seeds/weather_data/era5/backfill_historical.py

# Partial backfill (2000-2025)
poetry run python scripts/seeds/weather_data/era5/backfill_historical.py \
  --start-year 2000
```

**Flow:**
1. For each year from start to end:
   2. For each month (1-12):
      - Check if data already exists (skip if yes)
      - Fetch raw data (call import_from_api)
      - Process data (call process_weather_data)
      - Sleep 60s (rate limiting)
   3. Log yearly summary
4. Final summary

**Performance:**
- Estimated time: ~20-25 hours for 30 years (1995-2025)
- Can run incrementally (resumes from last successful month)
- Recommended: Run overnight or over weekend

---

## Storage Estimates

### Raw Data (`weather_data_raw`)

**Calculation:**
- Grid points: 30 unique locations
- Time range: 30 years (1995-2025) = 262,800 hours
- Records: 30 × 262,800 = **7,884,000 rows**
- Storage per row: ~200 bytes (JSONB compressed)
- **Total: ~1.5 GB**

### Processed Data (`weather_data`)

**Calculation:**
- Windfarms: 50 locations
- Time range: 30 years = 262,800 hours
- Records: 50 × 262,800 = **13,140,000 rows**
- Storage per row: ~150 bytes
- **Total: ~1.9 GB**

### Combined Storage

**Total: ~3.4 GB for 30 years**

### Indexes

- Estimated 20-30% overhead for indexes
- **Total with indexes: ~4.4 GB**

**Conclusion:** Storage is not a concern - 4.4 GB is very manageable for 30 years of hourly weather data across all windfarm locations.

---

## Future Phases

### Phase 5: Backend API Endpoints (Future)

**Out of scope for Phase 1.** Will include:

- `GET /api/v1/weather/windfarm/{id}` - Get weather data for windfarm
- `POST /api/v1/weather/fetch` - Trigger data fetch
- `GET /api/v1/weather/status` - Check import status

### Phase 6: Frontend UI (Future)

**Out of scope for Phase 1.** Will include:

- Weather data management page
- Import status dashboard
- Basic charts/graphs

### Phase 7: Analytics & Correlation (Future)

**Out of scope for Phase 1.** Will include:

- Correlation analysis with generation data
- Power curve calculation
- Performance metrics
- Weibull distributions
- Hub height wind speed calculations

---

## Summary

This plan provides a **focused, step-by-step approach** to building the weather data foundation:

1. ✅ Simple data models (no over-engineering)
2. ✅ Script-based approach (reusable, testable)
3. ✅ Follows existing patterns (generation_data_raw → generation_data)
4. ✅ Separates concerns (fetch → store → process)
5. ✅ No premature optimization (analytics can come later)

**Estimated Timeline:** 15-20 days for Phase 1 (data foundation with 30-year backfill)

**Next Steps:**
1. Review and approve this plan
2. Start with TODO 1.1 (create models)
3. Work through todos sequentially
4. Test each phase before moving to next

---

**End of Plan**