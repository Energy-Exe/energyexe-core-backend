"""Weather summary service for historical wind analysis by year/month."""
from calendar import monthrange
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.weather_data import WeatherData
from app.models.windfarm import Windfarm
from app.schemas.weather_summary import (
    DirectionBin,
    PeriodSummary,
    WeatherSummaryResponse,
)


# 16 compass points with their center degrees
COMPASS_POINTS = [
    ("N", 0.0),
    ("NNE", 22.5),
    ("NE", 45.0),
    ("ENE", 67.5),
    ("E", 90.0),
    ("ESE", 112.5),
    ("SE", 135.0),
    ("SSE", 157.5),
    ("S", 180.0),
    ("SSW", 202.5),
    ("SW", 225.0),
    ("WSW", 247.5),
    ("W", 270.0),
    ("WNW", 292.5),
    ("NW", 315.0),
    ("NNW", 337.5),
]

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


class WeatherSummaryService:
    """Service for computing historical weather summaries by year or month."""

    @staticmethod
    def _get_direction_name(degrees: float) -> str:
        """Convert degrees to compass direction name."""
        index = int((degrees + 11.25) / 22.5) % 16
        return COMPASS_POINTS[index][0]

    @staticmethod
    def _calculate_prevailing_direction(directions: List[float]) -> Tuple[float, float]:
        """
        Calculate prevailing wind direction using vector averaging.

        Wind direction is circular (0°=360°), so simple averaging doesn't work.
        We convert to unit vectors, average them, and convert back.

        Args:
            directions: List of wind directions in degrees

        Returns:
            Tuple of (prevailing_direction_degrees, consistency)
            - consistency is 0-1 where 0=completely random, 1=all same direction
        """
        if not directions:
            return 0.0, 0.0

        # Convert to radians
        rad = np.radians(directions)

        # Calculate mean of unit vectors
        u = np.mean(np.sin(rad))  # East-West component
        v = np.mean(np.cos(rad))  # North-South component

        # Convert back to degrees (0-360)
        avg_direction = np.degrees(np.arctan2(u, v)) % 360

        # Consistency is the length of the mean vector
        # 0 = uniformly distributed, 1 = all same direction
        consistency = np.sqrt(u**2 + v**2)

        return round(float(avg_direction), 2), round(float(consistency), 4)

    @staticmethod
    def _build_direction_histogram(directions: List[float]) -> List[DirectionBin]:
        """
        Bin wind directions into 16 compass points.

        Args:
            directions: List of wind directions in degrees

        Returns:
            List of DirectionBin objects for each compass point
        """
        bins = {name: 0 for name, _ in COMPASS_POINTS}
        total = len(directions) if directions else 1

        for deg in directions:
            # Find closest compass point (each bin is 22.5° wide)
            index = int(((deg + 11.25) % 360) / 22.5)
            name = COMPASS_POINTS[index][0]
            bins[name] += 1

        return [
            DirectionBin(
                direction=name,
                degrees=degrees,
                count=bins[name],
                percentage=round(bins[name] / total * 100, 2),
            )
            for name, degrees in COMPASS_POINTS
        ]

    @staticmethod
    def _format_period_label(year: int, month: Optional[int]) -> str:
        """Format period label for display."""
        if month is None:
            return str(year)
        return f"{MONTH_NAMES[month - 1]} {year}"

    @staticmethod
    def _expected_hours_in_period(year: int, month: Optional[int]) -> int:
        """Calculate expected hours in a period."""
        if month is None:
            # Yearly: account for leap years
            days = 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365
            return days * 24
        else:
            # Monthly
            _, days = monthrange(year, month)
            return days * 24

    async def get_period_summaries(
        self,
        db: AsyncSession,
        windfarm_id: int,
        period_type: str = "monthly",
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
    ) -> WeatherSummaryResponse:
        """
        Compute wind speed and direction summaries grouped by year or month.

        Args:
            db: Database session
            windfarm_id: Windfarm ID to analyze
            period_type: "monthly" or "yearly"
            start_year: Filter start year (optional)
            end_year: Filter end year (optional)

        Returns:
            WeatherSummaryResponse with list of period summaries
        """
        # Get windfarm info
        windfarm_query = select(Windfarm).where(Windfarm.id == windfarm_id)
        windfarm_result = await db.execute(windfarm_query)
        windfarm = windfarm_result.scalar_one_or_none()

        if not windfarm:
            raise ValueError(f"Windfarm {windfarm_id} not found")

        # Build query for weather data
        query = select(
            WeatherData.hour,
            WeatherData.wind_speed_100m,
            WeatherData.wind_direction_deg,
        ).where(
            and_(
                WeatherData.windfarm_id == windfarm_id,
                WeatherData.source == "ERA5",
                WeatherData.wind_speed_100m.isnot(None),
                WeatherData.wind_direction_deg.isnot(None),
            )
        )

        # Apply year filters if provided
        if start_year:
            start_dt = datetime(start_year, 1, 1)
            query = query.where(WeatherData.hour >= start_dt)
        if end_year:
            end_dt = datetime(end_year, 12, 31, 23, 59, 59)
            query = query.where(WeatherData.hour <= end_dt)

        query = query.order_by(WeatherData.hour)

        result = await db.execute(query)
        rows = result.fetchall()

        # Group by period
        groups: Dict[Tuple[int, Optional[int]], List[Tuple[float, float]]] = defaultdict(list)

        for row in rows:
            hour: datetime = row[0]
            wind_speed: float = float(row[1])
            wind_direction: float = float(row[2])

            year = hour.year
            if period_type == "yearly":
                key = (year, None)
            else:
                key = (year, hour.month)

            groups[key].append((wind_speed, wind_direction))

        # Calculate summary for each period
        summaries = []
        for (year, month), data in groups.items():
            wind_speeds = [d[0] for d in data]
            directions = [d[1] for d in data]

            # Wind speed statistics
            avg_speed = float(np.mean(wind_speeds)) if wind_speeds else 0.0
            min_speed = float(np.min(wind_speeds)) if wind_speeds else 0.0
            max_speed = float(np.max(wind_speeds)) if wind_speeds else 0.0
            std_speed = float(np.std(wind_speeds)) if wind_speeds else 0.0

            # Direction analysis (vector averaging for circular data)
            prev_dir, consistency = self._calculate_prevailing_direction(directions)
            histogram = self._build_direction_histogram(directions)

            # Data quality
            hours_with_data = len(data)
            expected_hours = self._expected_hours_in_period(year, month)
            data_completeness = hours_with_data / expected_hours if expected_hours > 0 else 0.0

            summary = PeriodSummary(
                year=year,
                month=month,
                period_label=self._format_period_label(year, month),
                avg_wind_speed_ms=round(avg_speed, 2),
                min_wind_speed_ms=round(min_speed, 2),
                max_wind_speed_ms=round(max_speed, 2),
                std_wind_speed_ms=round(std_speed, 2),
                prevailing_direction_deg=prev_dir,
                prevailing_direction_name=self._get_direction_name(prev_dir),
                direction_consistency=consistency,
                direction_histogram=histogram,
                hours_with_data=hours_with_data,
                data_completeness=round(data_completeness, 4),
            )
            summaries.append(summary)

        # Sort chronologically
        summaries.sort(key=lambda x: (x.year, x.month or 0))

        return WeatherSummaryResponse(
            windfarm_id=windfarm_id,
            windfarm_name=windfarm.name or "",
            windfarm_code=windfarm.code or "",
            period_type=period_type,
            summaries=summaries,
        )
