"""Helper functions for ERA5 weather data fetching and processing."""

import math
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
from app.models.windfarm import Windfarm


def get_windfarm_coordinates(windfarms: List[Windfarm]) -> List[Tuple[float, float, int]]:
    """
    Get exact coordinates for all windfarms.

    Returns:
        List of (latitude, longitude, windfarm_id) tuples
    """
    coordinates = []
    for wf in windfarms:
        if wf.lat is not None and wf.lng is not None:
            coordinates.append((float(wf.lat), float(wf.lng), wf.id))
    return coordinates


def create_bounding_box(coordinates: List[Tuple[float, float, int]]) -> Dict[str, float]:
    """
    Create bounding box from list of coordinates.

    Args:
        coordinates: List of (lat, lon, windfarm_id) tuples

    Returns:
        Dict with north, south, east, west boundaries
    """
    if not coordinates:
        raise ValueError("No coordinates provided")

    lats = [coord[0] for coord in coordinates]
    lons = [coord[1] for coord in coordinates]

    # Add small buffer (0.1 degrees) to ensure we capture all points
    north = max(lats) + 0.1
    south = min(lats) - 0.1
    east = max(lons) + 0.1
    west = min(lons) - 0.1

    return {
        'north': north,
        'south': south,
        'east': east,
        'west': west,
    }


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two points on Earth using Haversine formula.

    Args:
        lat1, lon1: First point (degrees)
        lat2, lon2: Second point (degrees)

    Returns:
        Distance in kilometers
    """
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    lon1_rad = math.radians(lon1)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))

    # Earth radius in kilometers
    R = 6371.0

    return R * c


def create_monthly_chunks(start_date: datetime, end_date: datetime) -> List[Tuple[datetime, datetime]]:
    """
    Split date range into monthly chunks.

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        List of (month_start, month_end) tuples
    """
    chunks = []
    current = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current <= end_date:
        # Start of month
        month_start = current

        # End of month
        if current.month == 12:
            month_end = current.replace(day=31, hour=23, minute=59, second=59)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
            month_end = next_month - timedelta(seconds=1)

        # Don't exceed end_date
        if month_end > end_date:
            month_end = end_date

        chunks.append((month_start, month_end))

        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return chunks


def create_daily_chunks(start_date: datetime, end_date: datetime) -> List[datetime]:
    """
    Split date range into daily chunks.

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        List of dates (one per day)
    """
    days = []
    current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    while current.date() <= end_date.date():
        days.append(current)
        current = current + timedelta(days=1)

    return days


def calculate_wind_speed(u_component: float, v_component: float) -> float:
    """
    Calculate wind speed from u and v components.

    Args:
        u_component: Eastward wind component (m/s)
        v_component: Northward wind component (m/s)

    Returns:
        Wind speed in m/s
    """
    return math.sqrt(u_component**2 + v_component**2)


def calculate_wind_direction(u_component: float, v_component: float) -> float:
    """
    Calculate wind direction from u and v components.

    Meteorological convention: direction FROM which wind blows.
    0° = North, 90° = East, 180° = South, 270° = West

    Args:
        u_component: Eastward wind component (m/s)
        v_component: Northward wind component (m/s)

    Returns:
        Wind direction in degrees (0-360)
    """
    # atan2(v, u) gives mathematical angle
    math_angle = math.atan2(v_component, u_component)

    # Convert to degrees
    angle_deg = math.degrees(math_angle)

    # Convert to meteorological direction
    # Mathematical: 0° = East, counter-clockwise
    # Meteorological: 0° = North, clockwise (direction FROM)
    wind_direction = (270 - angle_deg) % 360

    return wind_direction


def kelvin_to_celsius(kelvin: float) -> float:
    """Convert temperature from Kelvin to Celsius."""
    return kelvin - 273.15
