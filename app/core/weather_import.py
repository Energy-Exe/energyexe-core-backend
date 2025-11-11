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
        self.cdsapi_url = os.getenv("CDSAPI_URL")
        self.cdsapi_key = os.getenv("CDSAPI_KEY")

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
    ) -> Dict[str, any]:
        """
        Fetch and process ERA5 data for a date range.

        This is the main entry point that orchestrates the entire import process.

        Args:
            start_date: Start date for import
            end_date: End date for import
            job_id: Optional job ID for progress tracking

        Returns:
            Dict with statistics:
                - records: Total records imported
                - files_downloaded: Number of GRIB files downloaded
                - files_deleted: Number of GRIB files cleaned up
                - api_calls: Number of API calls made
                - dates_processed: Number of dates successfully processed
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
            'errors': []
        }

        logger.info(
            "Starting weather import",
            start_date=str(start_date),
            end_date=str(end_date),
            job_id=job_id
        )

        # Process each date in range
        current_date = start_date
        while current_date <= end_date:
            try:
                date_stats = await self._process_single_date(current_date, job_id)

                # Aggregate stats
                stats['records'] += date_stats.get('records', 0)
                stats['files_downloaded'] += date_stats.get('files_downloaded', 0)
                stats['files_deleted'] += date_stats.get('files_deleted', 0)
                stats['api_calls'] += date_stats.get('api_calls', 0)
                stats['dates_processed'] += 1

                logger.info(
                    "Date processed successfully",
                    date=str(current_date),
                    records=date_stats.get('records', 0)
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
        job_id: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Process a single date: fetch GRIB, extract data, insert to DB, cleanup.

        Args:
            target_date: Date to process
            job_id: Optional job ID for progress tracking

        Returns:
            Dict with date statistics
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
            'api_calls': 0
        }

        # Get all windfarms
        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Windfarm).where(Windfarm.is_active == True)
            )
            windfarms = list(result.scalars().all())

        if not windfarms:
            logger.warning("No active windfarms found")
            return stats

        logger.info(f"Processing date {target_date} for {len(windfarms)} windfarms")

        # Check if date already complete
        if await self._is_date_complete(target_date, len(windfarms)):
            logger.info(f"Date {target_date} already complete, skipping")
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

        # Configure client
        c = cdsapi.Client(url=self.cdsapi_url, key=self.cdsapi_key)

        # ERA5 request parameters
        request_params = {
            'product_type': 'reanalysis',
            'format': 'grib',
            'variable': [
                'temperature_at_100m',
                'temperature_at_10m',
                'u_component_of_wind_at_100m',
                'u_component_of_wind_at_10m',
                'v_component_of_wind_at_100m',
                'v_component_of_wind_at_10m',
                'surface_pressure',
                'total_precipitation',
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

        Args:
            ds: xarray Dataset with ERA5 data
            windfarms: List of Windfarm models
            target_date: Date being processed

        Returns:
            List of weather data records ready for database insertion
        """
        import numpy as np
        from scipy.interpolate import RegularGridInterpolator

        records = []

        # Get grid coordinates
        lats = ds.latitude.values
        lons = ds.longitude.values
        times = ds.time.values

        logger.info(
            "GRIB grid info",
            grid_size=f"{len(lats)} x {len(lons)}",
            time_points=len(times)
        )

        # Process each time point
        for time_idx, time_val in enumerate(times):
            hour_dt = np.datetime64(time_val, 'ns').astype(datetime)

            # Create interpolators for each variable
            interpolators = {}
            variables = {
                'u100': 'u_component_of_wind_at_100m',
                'v100': 'v_component_of_wind_at_100m',
                'u10': 'u_component_of_wind_at_10m',
                'v10': 'v_component_of_wind_at_10m',
                't2m': 'temperature_at_10m',
                't100': 'temperature_at_100m',
                'sp': 'surface_pressure',
                'tp': 'total_precipitation',
            }

            for key, var_name in variables.items():
                if var_name in ds:
                    data = ds[var_name].isel(time=time_idx).values
                    # Create interpolator (lats must be increasing for scipy)
                    if lats[0] > lats[-1]:
                        # Reverse if decreasing
                        interpolators[key] = RegularGridInterpolator(
                            (lats[::-1], lons),
                            data[::-1, :],
                            method='linear',
                            bounds_error=False,
                            fill_value=None
                        )
                    else:
                        interpolators[key] = RegularGridInterpolator(
                            (lats, lons),
                            data,
                            method='linear',
                            bounds_error=False,
                            fill_value=None
                        )

            # Interpolate for each windfarm
            for wf in windfarms:
                point = (wf.latitude, wf.longitude)

                # Extract interpolated values
                data_dict = {}
                for key, interpolator in interpolators.items():
                    value = float(interpolator(point))
                    data_dict[key] = value

                # Calculate wind speed and direction at 100m
                if 'u100' in data_dict and 'v100' in data_dict:
                    u100 = data_dict['u100']
                    v100 = data_dict['v100']
                    wind_speed_100 = np.sqrt(u100**2 + v100**2)
                    wind_direction_100 = (np.degrees(np.arctan2(-u100, -v100)) + 180) % 360
                else:
                    wind_speed_100 = None
                    wind_direction_100 = None

                # Create record
                record = {
                    'windfarm_id': wf.id,
                    'hour': hour_dt.replace(tzinfo=None),  # Store as naive UTC
                    'source': 'ERA5',
                    'wind_speed_100m': wind_speed_100,
                    'wind_direction_100m': wind_direction_100,
                    'temperature_2m': data_dict.get('t2m'),
                    'surface_pressure': data_dict.get('sp'),
                    'data': data_dict,  # Store all raw data in JSONB
                }

                records.append(record)

        return records

    async def _bulk_insert_weather_data(self, records: List[Dict]):
        """Bulk insert weather data records to database."""
        from sqlalchemy.dialects.postgresql import insert
        from app.core.database import get_session_factory
        from app.models.weather_data import WeatherData

        if not records:
            return

        AsyncSessionLocal = get_session_factory()
        async with AsyncSessionLocal() as db:
            # Use PostgreSQL upsert to handle duplicates
            stmt = insert(WeatherData).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=['windfarm_id', 'hour', 'source'],
                set_={
                    'wind_speed_100m': stmt.excluded.wind_speed_100m,
                    'wind_direction_100m': stmt.excluded.wind_direction_100m,
                    'temperature_2m': stmt.excluded.temperature_2m,
                    'surface_pressure': stmt.excluded.surface_pressure,
                    'data': stmt.excluded.data,
                }
            )

            await db.execute(stmt)
            await db.commit()

        logger.info(f"Inserted {len(records)} weather records")

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
