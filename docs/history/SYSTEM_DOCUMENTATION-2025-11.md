> **Historical snapshot (November 2025).** This was the original whole-platform system documentation, written before the AWS migration. Deployment, scheduling (crontab) and dependency details are out of date; see the repository README, ARCHITECTURE.md and docs/README.md for the current state. Moved here from the workspace root on 2026-10-04.

# EnergyExe System Documentation

This document provides comprehensive technical documentation for the EnergyExe energy data analytics platform.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Database Schema](#2-database-schema)
3. [Seed Scripts & Data Initialization](#3-seed-scripts--data-initialization)
4. [Calculation Logic](#4-calculation-logic)
5. [Weather Data Processing](#5-weather-data-processing)
6. [Cron Jobs & Scheduling](#6-cron-jobs--scheduling)
7. [Data Availability & Quality](#7-data-availability--quality)

---

## 1. System Overview

### Architecture

EnergyExe is a full-stack energy data analytics platform consisting of:

```
┌─────────────────────────────────────────────────────────────────┐
│                    energyexe-admin-ui                           │
│     React 19 + TanStack Router/Query + Tailwind + Shadcn        │
└─────────────────────────────────────────────────────────────────┘
                              ↕ REST API
┌─────────────────────────────────────────────────────────────────┐
│                  energyexe-core-backend                         │
│         FastAPI + SQLAlchemy 2.0 (async) + PostgreSQL           │
└─────────────────────────────────────────────────────────────────┘
                              ↕
┌─────────────────────────────────────────────────────────────────┐
│                      External Data Sources                       │
│   ENTSOE | ELEXON | EIA | Taipower | NVE | ERA5 (Weather)       │
└─────────────────────────────────────────────────────────────────┘
```

### Technology Stack

**Backend:**
- FastAPI with async/await patterns
- PostgreSQL with SQLAlchemy 2.0 (async ORM)
- Poetry for dependency management
- Alembic for database migrations
- Structured logging with structlog

**Frontend:**
- React 19 with TypeScript
- TanStack Router (file-based routing)
- TanStack Query for data fetching
- Vite for bundling
- Tailwind CSS + Shadcn UI components
- Recharts and Plotly for visualizations

---

## 2. Database Schema

### Entity Relationship Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────┐
│  countries  │────<│   states    │     │     regions     │
└─────────────┘     └─────────────┘     └─────────────────┘
      │                   │                     │
      │    ┌──────────────┼─────────────────────┘
      │    │              │
      ↓    ↓              ↓
┌─────────────────────────────────────────────────────────┐
│                      windfarms                           │
│  (country_id, state_id, region_id, bidzone_id, etc.)    │
└─────────────────────────────────────────────────────────┘
      │              │                    │
      │              │                    │
      ↓              ↓                    ↓
┌──────────┐  ┌──────────────┐  ┌─────────────────┐
│ turbine  │  │ generation   │  │  weather_data   │
│  units   │  │    units     │  │                 │
└──────────┘  └──────────────┘  └─────────────────┘
      │              │
      ↓              ↓
┌─────────────────────────────────────────────────────────┐
│                   generation_data                        │
│        (hour, windfarm_id, generation_mwh, etc.)        │
└─────────────────────────────────────────────────────────┘
```

### Core Tables

#### Authentication & Users

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `users` | User accounts | email, username, hashed_password, is_superuser |
| `audit_logs` | Action tracking | user_id, action, resource_type, old_values, new_values |

#### Geography & Grid Infrastructure

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `countries` | Country data | code (ISO3), name, lat, lng, polygon_wkt |
| `states` | States/provinces | code, name, country_id |
| `regions` | Geographic regions | code, name, location_type (sea/land) |
| `bidzones` | Electricity market zones | code, name, bidzone_type |
| `control_areas` | TSO control areas | code, name, country_id |
| `market_balance_areas` | Energy balance zones | code, name, country_id |

#### Wind Farm Infrastructure

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `windfarms` | Wind farm sites | code, name, nameplate_capacity_mw, lat, lng, status |
| `projects` | Project groupings | code, name, fuel_type |
| `turbine_models` | Turbine specifications | model, supplier, rated_power_kw, rotor_diameter_m |
| `turbine_units` | Individual turbines | code, windfarm_id, turbine_model_id, lat, lng |
| `owners` | Asset owners | code, name, type |
| `windfarm_owners` | Ownership relationships | windfarm_id, owner_id, ownership_percentage |

#### Generation Data

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `generation_units` | Power generation units | code, name, source, fuel_type, capacity_mw |
| `generation_data_raw` | Raw source data | source, period_start, period_end, data (JSONB) |
| `generation_data` | Processed hourly data | hour, generation_mwh, capacity_factor, completeness |
| `generation_unit_mapping` | Source ID mappings | source, source_identifier, generation_unit_id |

#### Weather Data

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `weather_data_raw` | Raw ERA5 grid data | latitude, longitude, timestamp, data (JSONB) |
| `weather_data` | Processed windfarm weather | hour, windfarm_id, wind_speed_100m, temperature_2m_c |

#### Job Tracking

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `import_job_executions` | Import job tracking | job_name, source, status, records_imported |
| `weather_import_jobs` | Weather import tracking | job_name, status, files_downloaded |
| `data_anomalies` | Data quality issues | anomaly_type, severity, status, period_start |

### Key Indexes

```sql
-- Generation data performance indexes
CREATE INDEX idx_gen_unit_hour ON generation_data (generation_unit_id, hour);
CREATE INDEX idx_gen_windfarm_hour ON generation_data (windfarm_id, hour);

-- Weather data indexes
CREATE INDEX idx_weather_windfarm_hour ON weather_data (windfarm_id, hour);

-- Import job indexes
CREATE INDEX ix_import_jobs_source_status ON import_job_executions (source, status);
CREATE INDEX ix_import_jobs_recent ON import_job_executions (created_at);
```

### Unique Constraints

- `generation_data`: (hour, generation_unit_id, source)
- `weather_data`: (hour, windfarm_id, source)
- `generation_unit_mapping`: (source, source_identifier)

---

## 3. Seed Scripts & Data Initialization

### Execution Order

The seed scripts must be executed in a specific order to respect foreign key constraints:

```
┌────────────────────────────────────────────────────────────────┐
│  TIER 1: Foundation (No Dependencies)                          │
│  ├── seed_countries.py     (57 countries)                      │
│  ├── seed_regions.py       (14 offshore regions)               │
│  ├── seed_owners.py        (1,786+ owners)                     │
│  └── seed_turbine_models.py (1,000+ models)                    │
├────────────────────────────────────────────────────────────────┤
│  TIER 2: Geography (Depends on: Countries)                     │
│  ├── seed_states.py        (150+ states)                       │
│  ├── seed_bidzones.py      (71 bidzones)                       │
│  ├── seed_control_areas.py (46 control areas)                  │
│  └── seed_market_balance_areas.py (60+ MBAs)                   │
├────────────────────────────────────────────────────────────────┤
│  TIER 3: Infrastructure (Depends on: All Above)                │
│  └── seed_windfarms.py     (500+ windfarms)                    │
├────────────────────────────────────────────────────────────────┤
│  TIER 4: Equipment (Depends on: Windfarms, Turbine Models)     │
│  ├── seed_generation_units.py (500+ units)                     │
│  └── seed_turbine_units.py    (turbine installations)          │
└────────────────────────────────────────────────────────────────┘
```

### Running Seeds

```bash
cd energyexe-core-backend

# Run all seeds in order (with confirmation prompt)
poetry run python scripts/seed_data.py

# Run with auto-confirmation
poetry run python scripts/seed_data.py -y
```

### Data Import Scripts

After initial seeding, data imports populate generation and weather data:

| Script Location | Purpose |
|-----------------|---------|
| `scripts/seeds/raw_generation_data/entsoe/` | European energy data |
| `scripts/seeds/raw_generation_data/elexon/` | UK energy data |
| `scripts/seeds/raw_generation_data/eia/` | US energy data |
| `scripts/seeds/raw_generation_data/taipower/` | Taiwan energy data |
| `scripts/seeds/weather_data/era5/` | ERA5 weather data |

---

## 4. Calculation Logic

### Capacity Factor

**Definition:** The ratio of actual energy output to the maximum possible output.

**Formula:**
```
Capacity Factor (%) = (Generation MWh / (Capacity MW × Hours)) × 100
```

**Implementation** (`weather_correlation_service.py`):
```python
# Per-hour capacity factor
cf = (avg_gen / float(capacity_mw) * 100) if capacity_mw > 0 else 0

# Overall capacity factor for a period
total_possible = capacity_mw * total_hours
overall_cf = (total_generation / total_possible * 100) if total_possible > 0 else 0
```

### Generation Data Aggregation

**30-minute to hourly:**
```python
# Two 30-minute periods per hour
generation_mwh = sum(period_value * 0.5 for period in periods)
completeness = len(periods) / 2  # 0.5 or 1.0
```

**15-minute to hourly:**
```python
# Four 15-minute periods per hour
generation_mwh = sum(period_value * 0.25 for period in periods)
completeness = len(periods) / 4  # 0.25, 0.5, 0.75, or 1.0
```

**Monthly data distribution:**
```python
hours_in_month = (period_end - period_start).total_seconds() / 3600
hourly_generation = monthly_total / hours_in_month
```

### Quality Score Assignments

| Data Type | Expected Records | Quality Score |
|-----------|------------------|---------------|
| Hourly (1 record) | 1 | 1.0 |
| 30-min (2 records) | 2 | 0.95 |
| 30-min (1 record) | 2 | 0.7 |
| 15-min (4 records) | 4 | 0.95 |
| 15-min (partial) | 4 | 0.6 + (0.1 × N) |
| Monthly (interpolated) | - | 0.5 |

### Statistical Metrics

**Coefficient of Variation:**
```python
cv = (std_dev / mean * 100) if mean != 0 else 0.0
```

**Percentile Ranking:**
```python
percentile = (1 - (rank - 1) / total) * 100
```

**Weibull Distribution Fitting:**
```python
shape_k, loc, scale_lambda = stats.weibull_min.fit(wind_speeds, floc=0)
```

### Power Curve Fitting (Gompertz)

```
y = A × exp(-B × exp(-C × x))

where:
  A = Installed nameplate capacity (fixed)
  B, C = Fitted parameters
  x = Wind speed (m/s)
  y = Generation output (MW)
```

---

## 5. Weather Data Processing

### Data Source: ERA5 Copernicus

**Configuration:**
- API: `https://cds.climate.copernicus.eu/api`
- Coverage: Europe (N: 71°, W: -11°, S: 35°, E: 32°)
- Resolution: ~31km grid, hourly UTC

**Variables Collected:**

| Variable | Height | Units |
|----------|--------|-------|
| U/V Component Wind | 100m | m/s |
| U/V Component Wind | 10m | m/s |
| Temperature | 2m | K → °C |
| Surface Pressure | Surface | Pa |

### Processing Pipeline

```
1. CDS API Request
   ↓
2. Download GRIB file to /tmp/grib_files/
   ↓
3. Parse with xarray (engine='cfgrib')
   ↓
4. Bilinear interpolation to windfarm locations
   ↓
5. Calculate wind speed: sqrt(u100² + v100²)
   ↓
6. Calculate direction: (270 - atan2(v100, u100)°) % 360
   ↓
7. Convert temperature: K - 273.15 = °C
   ↓
8. Bulk insert to weather_data table
```

### API Endpoints

```
GET  /api/v1/weather-data/availability
GET  /api/v1/weather-data/windfarms/{id}/timeseries
GET  /api/v1/weather-data/windfarms/{id}/statistics
GET  /api/v1/weather-data/windfarms/{id}/wind-rose
GET  /api/v1/weather-data/windfarms/{id}/distribution
GET  /api/v1/weather-data/windfarms/{id}/correlation
GET  /api/v1/weather-data/windfarms/{id}/power-curve
POST /api/v1/weather/imports
```

---

## 6. Cron Jobs & Scheduling

### Scheduled Import Jobs

| Job Name | Source | Schedule | Data Lag | Records/Run |
|----------|--------|----------|----------|-------------|
| `entsoe-daily` | ENTSOE | 06:00 UTC daily | 3 days | ~1,872 |
| `elexon-daily` | ELEXON | 07:00 UTC daily | 10 days | ~3,000+ |
| `taipower-hourly` | Taipower | :05 hourly | 0 days | ~23 |
| `eia-monthly` | EIA | 02:00 UTC 1st | 2 months | ~81,000 |

### Cron Configuration

```bash
# Install cron jobs
crontab scripts/jobs/crontab.txt

# Or manually:
crontab -e
```

**Crontab entries:**
```bash
PROJECT_DIR=/path/to/energyexe-core-backend

# ENTSOE - Daily at 6 AM UTC
0 6 * * * cd $PROJECT_DIR && poetry run python scripts/jobs/run_import_with_tracking.py entsoe-daily

# ELEXON - Daily at 7 AM UTC
0 7 * * * cd $PROJECT_DIR && poetry run python scripts/jobs/run_import_with_tracking.py elexon-daily

# Taipower - Every hour at :05
5 * * * * cd $PROJECT_DIR && poetry run python scripts/jobs/run_import_with_tracking.py taipower-hourly

# EIA - Monthly on 1st at 2 AM UTC
0 2 1 * * cd $PROJECT_DIR && poetry run python scripts/jobs/run_import_with_tracking.py eia-monthly
```

### Job Execution Flow

```
Cron Trigger
    ↓
run_import_with_tracking.py
    ↓
Creates ImportJobExecution (status=pending)
    ↓
ImportJobService.execute_job()
    ↓
Updates status to "running"
    ↓
Executes source-specific import script (subprocess)
    ↓
Parses output for metrics
    ↓
Updates job with results (success/failed)
    ↓
Web UI displays status at /import-jobs
```

### API Endpoints

```
POST /api/v1/import-jobs/                    Create manual job
GET  /api/v1/import-jobs/                    List jobs
POST /api/v1/import-jobs/{id}/execute        Execute job
POST /api/v1/import-jobs/{id}/retry          Retry failed job
GET  /api/v1/import-jobs/latest/status       Dashboard status
GET  /api/v1/import-jobs/health/status       System health
POST /api/v1/import-jobs/trigger/{job_name}  Public trigger
```

---

## 7. Data Availability & Quality

### Import Job Tracking

Every import is tracked in `import_job_executions`:

| Status | Description |
|--------|-------------|
| `pending` | Job created, not yet started |
| `running` | Currently executing |
| `success` | Completed successfully |
| `failed` | Execution failed |
| `retrying` | Being retried after failure |

**Tracked Metrics:**
- `records_imported`: New records inserted
- `records_updated`: Existing records modified
- `api_calls_made`: API requests made
- `duration_seconds`: Execution time
- `error_message`: Failure details

### Data Quality Monitoring

**Anomaly Types:**
- `CAPACITY_FACTOR_OVER_LIMIT`: CF > 120% (physical impossibility)
- `NEGATIVE_GENERATION`: Negative power values
- `MISSING_DATA`: Expected data not present
- `DATA_SPIKE`: Unusual data jumps
- `DATA_GAP`: Time periods without data
- `INVALID_CAPACITY`: Capacity inconsistencies

**Severity Levels:**
- `LOW`: < 1.3x threshold
- `MEDIUM`: 1.3-1.5x threshold
- `HIGH`: 1.5-2.0x threshold
- `CRITICAL`: >= 2.0x threshold

**Anomaly Status Workflow:**
```
PENDING → INVESTIGATING → RESOLVED
                       → IGNORED
                       → FALSE_POSITIVE
```

### Completeness Tracking

**Per-record completeness** (`generation_data.completeness`):
```
completeness = actual_data_points / expected_data_points
```

**Example:**
- Hourly data with 4 expected 15-min records but only 2 present: completeness = 0.5
- Full hourly data: completeness = 1.0

### Health Monitoring

**System Health Calculation:**
```python
if recent_failures == 0 and running_jobs < 5:
    health = "healthy"
elif recent_failures <= 3:
    health = "degraded"
else:
    health = "critical"
```

### Re-aggregation Process

When data issues are fixed, re-aggregation recalculates affected periods:

```python
async def reaggregate_period(start_date, end_date, sources, windfarm_id):
    # 1. Delete existing aggregated data for period
    # 2. Re-process raw data through aggregation pipeline
    # 3. Recalculate capacity factors and completeness
    # 4. Day-by-day processing with per-day commits
```

### API Endpoints

```
POST /api/v1/data-anomalies/detect           Detect anomalies
GET  /api/v1/data-anomalies                  List anomalies
PATCH /api/v1/data-anomalies/{id}/status     Update status
POST /api/v1/data-anomalies/{id}/reaggregate Re-aggregate period
```

---

## Appendix: File Structure

```
energyexe/
├── energyexe-core-backend/
│   ├── app/
│   │   ├── api/v1/endpoints/           # REST API endpoints
│   │   ├── core/                       # Config, security, deps
│   │   ├── models/                     # SQLAlchemy models
│   │   ├── schemas/                    # Pydantic schemas
│   │   └── services/                   # Business logic
│   ├── alembic/versions/               # Database migrations
│   ├── scripts/
│   │   ├── seeds/                      # Seed data scripts
│   │   └── jobs/                       # Cron job scripts
│   └── tests/                          # Test suite
│
├── energyexe-admin-ui/
│   ├── src/
│   │   ├── components/                 # React components
│   │   ├── lib/                        # API clients
│   │   └── routes/                     # TanStack Router routes
│   └── tests/                          # Frontend tests
│
├── CLAUDE.md                           # Development guide
└── SYSTEM_DOCUMENTATION.md             # This file
```

---

## Quick Reference Commands

```bash
# Backend
cd energyexe-core-backend
poetry install --with dev,test          # Install dependencies
poetry run alembic upgrade head         # Run migrations
poetry run python scripts/seed_data.py  # Seed database
poetry run python scripts/start.py      # Start API server
poetry run pytest                       # Run tests

# Frontend
cd energyexe-admin-ui
pnpm install                            # Install dependencies
pnpm dev                                # Start dev server
pnpm build                              # Production build
pnpm test                               # Run tests
```
