# Weather Data Scripts

## Quick Links

📚 **[Complete Documentation](./WEATHER_DATA_COMPLETE_GUIDE.md)** - Full system guide

---

## Quick Start

### 1. Setup CDS API Key

Create `~/.cdsapirc`:
```bash
url: https://cds.climate.copernicus.eu/api
key: fea40a33-7b81-4f9a-a145-a1556b25c940
```

### 2. Install Dependencies

```bash
poetry add cdsapi xarray cfgrib scipy
```

### 3. Run Database Migration

```bash
cd /path/to/energyexe-core-backend
poetry run alembic upgrade head
```

### 4. Import Weather Data

**Single day:**
```bash
poetry run python scripts/seeds/weather_data/fetch_daily_all_windfarms.py \
  --date 2024-10-15
```

**Date range:**
```bash
poetry run python scripts/seeds/weather_data/fetch_daily_all_windfarms.py \
  --start 2024-10-01 \
  --end 2024-10-31
```

---

## What This Does

- Downloads ERA5 weather data from Copernicus Climate Data Store
- Processes data for **1,591 windfarms worldwide**
- Stores **hourly** wind speed, direction, and temperature
- Creates **38,184 records per day** (1,591 windfarms × 24 hours)
- Uses **bilinear interpolation** for exact coordinates
- **Skips already-complete days** automatically

---

## Performance

- **Single day import:** ~5 minutes
  - GRIB download: 30-60 seconds
  - Processing: 3-4 minutes
  - Database insert: 10 seconds

- **Parallel import (3 processes):** ~180 days/hour
- **Full year (365 days):** ~2 hours with parallel processing

---

## File Structure

```
weather_data/
├── README.md                           # This file
├── WEATHER_DATA_COMPLETE_GUIDE.md      # Complete documentation
├── fetch_daily_all_windfarms.py        # Main import script
└── grib_files/                         # Cached GRIB files (auto-created)
    └── daily/
        └── era5_YYYYMMDD.grib
```

---

## Check Data Coverage

```sql
SELECT
    EXTRACT(YEAR FROM hour) as year,
    COUNT(DISTINCT DATE(hour)) as complete_days,
    365 as total_days,
    ROUND(COUNT(DISTINCT DATE(hour))::numeric / 365 * 100, 1) as pct
FROM weather_data
GROUP BY EXTRACT(YEAR FROM hour)
ORDER BY year;
```

---

## For Complete Documentation

See **[WEATHER_DATA_COMPLETE_GUIDE.md](./WEATHER_DATA_COMPLETE_GUIDE.md)** for:
- Database architecture
- API endpoints (17 endpoints)
- Frontend integration
- Parallel import strategies
- Troubleshooting guide
- Advanced usage examples

---

**Last Updated:** November 10, 2025
