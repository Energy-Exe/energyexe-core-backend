"""GRIB file parsing for ERA5 weather data."""

import xarray as xr
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple
import structlog

logger = structlog.get_logger()


def parse_grib_file(
    grib_file_path: str,
    target_coordinates: List[Tuple[float, float, int]]
) -> List[Dict]:
    """
    Parse GRIB file and extract data for target windfarm coordinates.

    Uses nearest neighbor selection - ERA5 will return data from the
    nearest grid point to each requested coordinate.

    Args:
        grib_file_path: Path to GRIB file
        target_coordinates: List of (lat, lon, windfarm_id) tuples

    Returns:
        List of records ready for insertion into weather_data_raw
    """
    logger.info(f"Parsing GRIB file: {grib_file_path}")
    logger.info(f"Target coordinates: {len(target_coordinates)} locations")

    try:
        # Open GRIB with xarray (using cfgrib engine)
        ds = xr.open_dataset(grib_file_path, engine='cfgrib')

        logger.info(f"GRIB file opened successfully")
        logger.info(f"Variables available: {list(ds.data_vars.keys())}")
        logger.info(f"Time points: {len(ds.time)}")

    except Exception as e:
        logger.error(f"Failed to open GRIB file: {e}")
        raise

    records = []
    processed_coords = set()

    for (target_lat, target_lon, windfarm_id) in target_coordinates:
        try:
            # Select nearest grid point (ERA5 will snap to actual grid)
            point_data = ds.sel(
                latitude=target_lat,
                longitude=target_lon,
                method='nearest'
            )

            # Get the actual grid point that was selected
            actual_lat = float(point_data.latitude.values)
            actual_lon = float(point_data.longitude.values)

            # Track to avoid duplicates in same file
            grid_key = (actual_lat, actual_lon)
            if grid_key in processed_coords:
                logger.debug(f"Skipping duplicate grid point {grid_key} for windfarm {windfarm_id}")
                continue

            processed_coords.add(grid_key)

            # Extract all timestamps for this location
            for time_idx in range(len(point_data.time)):
                timestamp = pd.Timestamp(point_data.time.values[time_idx]).to_pydatetime()

                # Build JSONB data dict with all available parameters
                data_dict = {}

                # Required parameters
                if 'u100' in point_data:
                    data_dict['u100'] = float(point_data.u100.values[time_idx])
                if 'v100' in point_data:
                    data_dict['v100'] = float(point_data.v100.values[time_idx])
                if 't2m' in point_data:
                    data_dict['t2m'] = float(point_data.t2m.values[time_idx])

                # Optional parameters (store for future use)
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
                    'requested_lat': target_lat,
                    'requested_lon': target_lon,
                    'actual_grid_lat': actual_lat,
                    'actual_grid_lon': actual_lon,
                    'windfarm_id': windfarm_id,
                }

                record = {
                    'source': 'ERA5',
                    'source_type': 'api',
                    'timestamp': timestamp,
                    'latitude': actual_lat,  # Store actual grid point ERA5 returned
                    'longitude': actual_lon,
                    'data': data_dict,
                }
                records.append(record)

            logger.debug(f"Extracted data for windfarm {windfarm_id} at grid point ({actual_lat}, {actual_lon})")

        except Exception as e:
            logger.error(f"Failed to extract data for coordinate ({target_lat}, {target_lon}): {e}")
            continue

    ds.close()

    logger.info(f"Parsed {len(records)} records from GRIB file")
    logger.info(f"Unique grid points: {len(processed_coords)}")

    return records
