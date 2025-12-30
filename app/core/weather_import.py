"""Core weather import functionality for ERA5 data fetching and processing."""
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import structlog

logger = structlog.get_logger()


class WeatherImportCore:
    """Core functionality for importing ERA5 weather data."""

    def __init__(self):
        """Initialize weather import core."""
        from app.core.config import get_settings

        settings = get_settings()
        self.cdsapi_url = settings.CDSAPI_URL
        self.cdsapi_key = settings.CDSAPI_KEY

        if not self.cdsapi_url or not self.cdsapi_key:
            logger.warning(
                "CDS API credentials not configured",
                has_url=bool(self.cdsapi_url),
                has_key=bool(self.cdsapi_key)
            )

    async def fetch_and_process_date_range(
        self,
        start_date: date,
        end_date: date,
        job_id: Optional[int] = None,
        force_refresh: bool = False,
    ) -> Dict[str, any]:
        """
        Fetch and process ERA5 data for a date range.

        This is the main entry point that orchestrates the entire import process.
        By default, it skips dates that already have complete data. Use force_refresh=True
        to re-fetch and update data for all dates in the range.

        Args:
            start_date: Start date for import
            end_date: End date for import
            job_id: Optional job ID for progress tracking
            force_refresh: If True, re-fetch data even for days that already have complete data

        Returns:
            Dict with statistics:
                - records: Total records imported
                - files_downloaded: Number of GRIB files downloaded
                - files_deleted: Number of GRIB files cleaned up
                - api_calls: Number of API calls made
                - dates_processed: Number of dates successfully processed
                - dates_skipped: Number of dates skipped (already complete)
                - errors: List of error messages
        """
        # Check dependencies
        if not self._check_dependencies():
            raise RuntimeError(
                "Missing required dependencies for weather import. "
                "Ensure cdsapi, xarray, and cfgrib are installed."
            )

        # Check credentials
        if not self.cdsapi_url or not self.cdsapi_key:
            raise RuntimeError(
                "CDS API credentials not configured. "
                "Set CDSAPI_URL and CDSAPI_KEY environment variables."
            )

        stats = {
            'records': 0,
            'files_downloaded': 0,
            'files_deleted': 0,
            'api_calls': 0,
            'dates_processed': 0,
            'dates_skipped': 0,
            'errors': []
        }

        logger.info(
            "Starting weather import",
            start_date=str(start_date),
            end_date=str(end_date),
            job_id=job_id,
            force_refresh=force_refresh
        )

        # Process each date in range
        current_date = start_date
        while current_date <= end_date:
            try:
                date_stats = await self._process_single_date(current_date, job_id, force_refresh)

                # Aggregate stats
                stats['records'] += date_stats.get('records', 0)
                stats['files_downloaded'] += date_stats.get('files_downloaded', 0)
                stats['files_deleted'] += date_stats.get('files_deleted', 0)
                stats['api_calls'] += date_stats.get('api_calls', 0)

                if date_stats.get('skipped', False):
                    stats['dates_skipped'] += 1
                else:
                    stats['dates_processed'] += 1

                logger.info(
                    "Date processed successfully",
                    date=str(current_date),
                    records=date_stats.get('records', 0),
                    skipped=date_stats.get('skipped', False)
                )

            except Exception as e:
                error_msg = f"Failed to process {current_date}: {str(e)}"
                stats['errors'].append(error_msg)
                logger.error("Date processing failed", date=str(current_date), error=str(e))

            current_date += timedelta(days=1)

        logger.info("Weather import completed", **stats)
        return stats

    async def _process_single_date(
        self,
        target_date: date,
        job_id: Optional[int] = None,
        force_refresh: bool = False
    ) -> Dict[str, int]:
        """
        Process a single date: fetch GRIB, extract data, insert to DB, cleanup.

        Args:
            target_date: Date to process
            job_id: Optional job ID for progress tracking
            force_refresh: If True, re-fetch data even if date already has complete data

        Returns:
            Dict with date statistics including 'skipped' flag
        """
        # Import here to avoid issues if dependencies not available
        import cdsapi
        import xarray as xr
        from sqlalchemy import select

        from app.core.database import get_session_factory
        from app.models.windfarm import Windfarm
        from app.models.weather_data import WeatherData

        stats = {
            'records': 0,
            'files_downloaded': 0,
            'files_deleted': 0,
            'api_calls': 0,
            'skipped': False
        }

        # Get all windfarms
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Windfarm).where(
                    Windfarm.lat.isnot(None),
                    Windfarm.lng.isnot(None)
                )
            )
            windfarms = list(result.scalars().all())

        if not windfarms:
            logger.warning("No active windfarms found")
            return stats

        logger.info(f"Processing date {target_date} for {len(windfarms)} windfarms")

        # Check if date already complete (skip if complete and not forcing refresh)
        if not force_refresh and await self._is_date_complete(target_date, len(windfarms)):
            logger.info(f"Date {target_date} already complete, skipping (use force_refresh=True to re-fetch)")
            stats['skipped'] = True
            return stats

        # Setup GRIB directory
        grib_dir = Path("/tmp/grib_files") / "daily"
        grib_dir.mkdir(parents=True, exist_ok=True)
        grib_file = grib_dir / f"era5_{target_date.strftime('%Y%m%d')}.grib"

        # Check if GRIB already exists
        if grib_file.exists():
            logger.info(f"Using existing GRIB file: {grib_file}")
        else:
            # Download from CDS API
            logger.info(f"Downloading ERA5 data for {target_date}")
            await self._download_era5_grib(target_date, grib_file)
            stats['files_downloaded'] = 1
            stats['api_calls'] = 1

        # Parse GRIB and extract data
        logger.info("Parsing GRIB file and interpolating data")

        # Open GRIB file
        ds = xr.open_dataset(str(grib_file), engine='cfgrib')

        # Extract weather data for each windfarm using bilinear interpolation
        records = self._extract_windfarm_data(ds, windfarms, target_date)
        ds.close()

        # Insert to database
        await self._bulk_insert_weather_data(records)
        stats['records'] = len(records)

        # Cleanup GRIB file
        if grib_file.exists():
            grib_file.unlink()
            stats['files_deleted'] = 1
            logger.info(f"Deleted GRIB file: {grib_file}")

        # Update job progress if job_id provided
        if job_id:
            await self._update_job_progress(job_id, target_date, len(records))

        return stats

    async def _download_era5_grib(self, target_date: date, output_path: Path):
        """Download ERA5 GRIB file from CDS API."""
        import cdsapi

        # Configure client with explicit credentials for production reliability
        # cdsapi.Client() can read from env vars, but explicit is more reliable
        c = cdsapi.Client(url=self.cdsapi_url, key=self.cdsapi_key)

        # ERA5 request parameters
        # Using 100m wind components and 2m temperature (standard single-levels)
        # Note: Excluding total_precipitation as it's accumulated and causes time dimension conflicts
        request_params = {
            'product_type': 'reanalysis',
            'format': 'grib',
            'variable': [
                '100m_u_component_of_wind',
                '100m_v_component_of_wind',
                '10m_u_component_of_wind',
                '10m_v_component_of_wind',
                '2m_temperature',
                'surface_pressure',
            ],
            'year': target_date.year,
            'month': f'{target_date.month:02d}',
            'day': f'{target_date.day:02d}',
            'time': [f'{h:02d}:00' for h in range(24)],
            'area': [71, -11, 35, 32],  # N, W, S, E - covers Europe
        }

        logger.info("Submitting CDS API request", date=str(target_date))

        # Download (this will block until download completes)
        c.retrieve('reanalysis-era5-single-levels', request_params, str(output_path))

        logger.info("Download complete", file=str(output_path))

    def _extract_windfarm_data(
        self,
        ds,  # xarray.Dataset
        windfarms: List,
        target_date: date
    ) -> List[Dict]:
        """
        Extract weather data for all windfarms using bilinear interpolation.

        Uses xarray's built-in interpolation which is faster than scipy.

        Args:
            ds: xarray Dataset with ERA5 data
            windfarms: List of Windfarm models
            target_date: Date being processed

        Returns:
            List of weather data records ready for database insertion
        """
        import math
        import pandas as pd

        records = []

        logger.info(
            "GRIB grid info",
            grid_size=f"{len(ds.latitude)} x {len(ds.longitude)}",
            time_points=len(ds.time)
        )

        # Process each windfarm
        for i, wf in enumerate(windfarms):
            if i % 100 == 0:
                logger.info(f"Processing windfarm {i+1}/{len(windfarms)}")

            wf_lat = float(wf.lat)
            wf_lng = float(wf.lng)

            try:
                # Interpolate ALL time points at once for this windfarm (MUCH faster!)
                u100_all = ds['u100'].interp(latitude=wf_lat, longitude=wf_lng, method='linear').values
                v100_all = ds['v100'].interp(latitude=wf_lat, longitude=wf_lng, method='linear').values
                t2m_all = ds['t2m'].interp(latitude=wf_lat, longitude=wf_lng, method='linear').values

                # Process each time point
                for time_idx in range(len(ds.time)):
                    # ERA5 timestamps are in UTC - explicitly specify timezone
                    timestamp = pd.Timestamp(ds.time.values[time_idx], tz='UTC').to_pydatetime()

                    u100 = float(u100_all[time_idx])
                    v100 = float(v100_all[time_idx])
                    t2m = float(t2m_all[time_idx])

                    # Calculate wind speed and direction
                    wind_speed = math.sqrt(u100**2 + v100**2)
                    math_angle = math.atan2(v100, u100)
                    wind_direction = (270 - math.degrees(math_angle)) % 360

                    # Create record
                    record = {
                        'hour': timestamp,
                        'windfarm_id': wf.id,
                        'wind_speed_100m': round(wind_speed, 3),
                        'wind_direction_deg': round(wind_direction, 2),
                        'temperature_2m_k': round(t2m, 2),
                        'temperature_2m_c': round(t2m - 273.15, 2),
                        'source': 'ERA5',
                        'raw_data_id': None,
                    }

                    records.append(record)

            except Exception as e:
                logger.warning(f"Failed to interpolate for windfarm {wf.id}: {e}")
                continue

        logger.info(f"Extracted {len(records)} records")
        return records

    async def _bulk_insert_weather_data(self, records: List[Dict]):
        """Bulk insert weather data records to database in batches."""
        from datetime import datetime
        from sqlalchemy.dialects.postgresql import insert
        from app.core.database import get_session_factory
        from app.models.weather_data import WeatherData

        if not records:
            return

        AsyncSessionLocal = get_session_factory()
        BATCH_SIZE = 2900  # Match working script

        async with AsyncSessionLocal() as db:
            total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE

            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i:i + BATCH_SIZE]
                batch_num = (i // BATCH_SIZE) + 1

                logger.info(f"Inserting batch {batch_num}/{total_batches}: {len(batch)} records")

                stmt = insert(WeatherData).values(batch)
                stmt = stmt.on_conflict_do_update(
                    constraint='uq_weather_hour_windfarm_source',
                    set_={
                        'wind_speed_100m': stmt.excluded.wind_speed_100m,
                        'wind_direction_deg': stmt.excluded.wind_direction_deg,
                        'temperature_2m_k': stmt.excluded.temperature_2m_k,
                        'temperature_2m_c': stmt.excluded.temperature_2m_c,
                        'updated_at': datetime.utcnow(),
                    }
                )

                await db.execute(stmt)

            await db.commit()

        logger.info(f"Bulk insert complete: {len(records)} records in {total_batches} batches")

    async def _is_date_complete(self, target_date: date, expected_windfarms: int) -> bool:
        """Check if date already has complete data."""
        from sqlalchemy import select, func
        from app.core.database import get_session_factory
        from app.models.weather_data import WeatherData

        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            # Count records for this date
            query = select(func.count(WeatherData.id)).where(
                func.date(WeatherData.hour) == target_date,
                WeatherData.source == 'ERA5'
            )
            result = await db.execute(query)
            count = result.scalar()

            # Complete if we have 24 hours * windfarm count
            expected = expected_windfarms * 24
            return count >= expected

    async def _update_job_progress(self, job_id: int, completed_date: date, records_count: int):
        """Update job metadata with progress."""
        from app.core.database import get_session_factory
        from app.models.weather_import_job import WeatherImportJob

        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            job = await db.get(WeatherImportJob, job_id)
            if job and job.job_metadata:
                current_completed = job.job_metadata.get('dates_completed', 0)
                job.update_progress(
                    dates_completed=current_completed + 1,
                    current_date=completed_date.strftime('%Y-%m-%d'),
                    current_phase='processing',
                    records_processed=records_count
                )
                await db.commit()

    def _check_dependencies(self) -> bool:
        """Check if required dependencies are available."""
        try:
            import cdsapi
            import xarray
            import cfgrib
            import scipy
            return True
        except ImportError as e:
            logger.error(f"Missing dependency: {e}")
            return False
