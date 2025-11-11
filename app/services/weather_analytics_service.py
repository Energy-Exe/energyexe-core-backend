"""Weather analytics service for wind analysis and visualization data."""
from datetime import datetime
from typing import List, Optional, Tuple
from sqlalchemy import select, func, and_, text, case
from sqlalchemy.ext.asyncio import AsyncSession
import math
from scipy import stats
import numpy as np

from app.models.weather_data import WeatherData
from app.schemas.weather_data import (
    WeatherTimeseries,
    WindRoseData,
    WindSpeedDistribution,
    DiurnalPattern,
    SeasonalPattern,
    WindStatistics,
    WindSpeedDurationCurve,
)


class WeatherAnalyticsService:
    """Service for weather data analytics and visualization."""

    @staticmethod
    def _get_direction_name(degrees: float) -> str:
        """Convert degrees to compass direction name."""
        directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                     "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        index = int((degrees + 11.25) / 22.5) % 16
        return directions[index]

    async def get_weather_timeseries(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        aggregation: str = "hourly",
    ) -> WeatherTimeseries:
        """
        Get weather time series data.

        Args:
            db: Database session
            windfarm_id: Windfarm ID
            start_date: Start datetime
            end_date: End datetime
            aggregation: "hourly", "daily", or "monthly"

        Returns:
            Weather timeseries data
        """
        if aggregation == "hourly":
            query = select(
                WeatherData.hour,
                WeatherData.wind_speed_100m,
                WeatherData.wind_direction_deg,
                WeatherData.temperature_2m_c,
            ).where(
                WeatherData.windfarm_id == windfarm_id,
                WeatherData.hour >= start_date,
                WeatherData.hour <= end_date,
                WeatherData.source == 'ERA5'
            ).order_by(WeatherData.hour)

            result = await db.execute(query)
            rows = result.fetchall()

            return WeatherTimeseries(
                timestamps=[row[0] for row in rows],
                wind_speed=[float(row[1]) for row in rows],
                wind_direction=[float(row[2]) for row in rows],
                temperature=[float(row[3]) for row in rows],
                aggregation="hourly",
            )

        elif aggregation == "daily":
            query = text("""
                SELECT
                    DATE(hour AT TIME ZONE 'UTC') as date,
                    AVG(wind_speed_100m) as avg_wind_speed,
                    AVG(wind_direction_deg) as avg_wind_direction,
                    AVG(temperature_2m_c) as avg_temperature
                FROM weather_data
                WHERE windfarm_id = :windfarm_id
                  AND hour >= :start_date
                  AND hour <= :end_date
                  AND source = 'ERA5'
                GROUP BY DATE(hour AT TIME ZONE 'UTC')
                ORDER BY date;
            """)

            result = await db.execute(query, {
                "windfarm_id": windfarm_id,
                "start_date": start_date,
                "end_date": end_date
            })
            rows = result.fetchall()

            return WeatherTimeseries(
                timestamps=[datetime.combine(row[0], datetime.min.time()) for row in rows],
                wind_speed=[float(row[1]) for row in rows],
                wind_direction=[float(row[2]) for row in rows],
                temperature=[float(row[3]) for row in rows],
                aggregation="daily",
            )

        else:  # monthly
            query = text("""
                SELECT
                    DATE_TRUNC('month', hour) as month,
                    AVG(wind_speed_100m) as avg_wind_speed,
                    AVG(wind_direction_deg) as avg_wind_direction,
                    AVG(temperature_2m_c) as avg_temperature
                FROM weather_data
                WHERE windfarm_id = :windfarm_id
                  AND hour >= :start_date
                  AND hour <= :end_date
                  AND source = 'ERA5'
                GROUP BY DATE_TRUNC('month', hour)
                ORDER BY month;
            """)

            result = await db.execute(query, {
                "windfarm_id": windfarm_id,
                "start_date": start_date,
                "end_date": end_date
            })
            rows = result.fetchall()

            return WeatherTimeseries(
                timestamps=[row[0] for row in rows],
                wind_speed=[float(row[1]) for row in rows],
                wind_direction=[float(row[2]) for row in rows],
                temperature=[float(row[3]) for row in rows],
                aggregation="monthly",
            )

    async def get_wind_rose_data(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> WindRoseData:
        """
        Get wind rose data (frequency by direction and speed bins).

        Returns 16 direction bins × 5 speed bins.
        """
        query = text("""
            SELECT
                FLOOR(wind_direction_deg / 22.5) * 22.5 as direction_bin,
                CASE
                    WHEN wind_speed_100m < 5 THEN 0
                    WHEN wind_speed_100m < 10 THEN 1
                    WHEN wind_speed_100m < 15 THEN 2
                    WHEN wind_speed_100m < 20 THEN 3
                    ELSE 4
                END as speed_bin_idx,
                COUNT(*) as frequency
            FROM weather_data
            WHERE windfarm_id = :windfarm_id
              AND hour >= :start_date
              AND hour <= :end_date
              AND source = 'ERA5'
            GROUP BY direction_bin, speed_bin_idx
            ORDER BY direction_bin, speed_bin_idx;
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rows = result.fetchall()

        # Get total hours for calm percentage
        total_query = text("""
            SELECT
                COUNT(*) as total_hours,
                COUNT(*) FILTER (WHERE wind_speed_100m < 0.5) as calm_hours
            FROM weather_data
            WHERE windfarm_id = :windfarm_id
              AND hour >= :start_date
              AND hour <= :end_date
              AND source = 'ERA5';
        """)

        total_result = await db.execute(total_query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        total_row = total_result.fetchone()
        total_hours = total_row[0]
        calm_hours = total_row[1]

        # Build 2D frequency array
        # 16 direction bins × 5 speed bins
        direction_bins = [i * 22.5 for i in range(16)]
        speed_bins = [
            {"min": 0, "max": 5, "label": "0-5 m/s"},
            {"min": 5, "max": 10, "label": "5-10 m/s"},
            {"min": 10, "max": 15, "label": "10-15 m/s"},
            {"min": 15, "max": 20, "label": "15-20 m/s"},
            {"min": 20, "max": 100, "label": "20+ m/s"},
        ]

        # Initialize 2D array
        frequency = [[0.0 for _ in range(5)] for _ in range(16)]

        # Fill with data
        for row in rows:
            dir_bin = float(row[0])  # Convert Decimal to float
            speed_idx = int(row[1])
            count = int(row[2])

            dir_idx = int(dir_bin / 22.5) % 16
            frequency[dir_idx][speed_idx] = (count / total_hours) * 100 if total_hours > 0 else 0

        calm_percentage = (calm_hours / total_hours * 100) if total_hours > 0 else 0

        return WindRoseData(
            direction_bins=direction_bins,
            speed_bins=speed_bins,
            frequency=frequency,
            total_hours=total_hours,
            calm_percentage=round(calm_percentage, 2),
        )

    async def get_wind_speed_distribution(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> WindSpeedDistribution:
        """
        Get wind speed distribution with Weibull fit.

        Uses 1 m/s bins and fits Weibull distribution.
        """
        # Get wind speed data
        query = text("""
            SELECT wind_speed_100m
            FROM weather_data
            WHERE windfarm_id = :windfarm_id
              AND hour >= :start_date
              AND hour <= :end_date
              AND source = 'ERA5'
            ORDER BY wind_speed_100m;
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        wind_speeds = [float(row[0]) for row in result.fetchall()]

        if not wind_speeds:
            # Return empty distribution
            return WindSpeedDistribution(
                speed_bins=[],
                frequency=[],
                frequency_percentage=[],
                weibull_k=0,
                weibull_c=0,
                weibull_fit=[],
                mean_speed=0,
                median_speed=0,
                mode_speed=0,
                std_dev=0,
            )

        # Calculate statistics
        mean_speed = np.mean(wind_speeds)
        median_speed = np.median(wind_speeds)
        std_dev = np.std(wind_speeds)

        # Create histogram (1 m/s bins)
        max_speed = int(np.ceil(max(wind_speeds)))
        bins = list(range(0, max_speed + 1))
        hist, _ = np.histogram(wind_speeds, bins=bins)

        # Fit Weibull distribution
        # Weibull parameters: k (shape), loc (location, usually 0), c (scale)
        try:
            shape, loc, scale = stats.weibull_min.fit(wind_speeds, floc=0)
            weibull_k = shape
            weibull_c = scale

            # Generate fitted curve
            x = np.linspace(0, max_speed, 100)
            weibull_fit = stats.weibull_min.pdf(x, shape, loc, scale) * len(wind_speeds)
        except:
            weibull_k = 2.0  # Default Rayleigh
            weibull_c = mean_speed / 0.886  # Approximate
            weibull_fit = []

        # Mode (most common bin)
        mode_idx = np.argmax(hist) if len(hist) > 0 else 0
        mode_speed = bins[mode_idx] if mode_idx < len(bins) else 0

        # Convert frequency to percentage
        total = len(wind_speeds)
        frequency_percentage = [(count / total * 100) if total > 0 else 0 for count in hist]

        return WindSpeedDistribution(
            speed_bins=bins[:-1],  # Bin edges
            frequency=hist.tolist(),
            frequency_percentage=[round(p, 2) for p in frequency_percentage],
            weibull_k=round(weibull_k, 3),
            weibull_c=round(weibull_c, 3),
            weibull_fit=[round(f, 2) for f in weibull_fit] if len(weibull_fit) > 0 else [],
            mean_speed=round(mean_speed, 2),
            median_speed=round(median_speed, 2),
            mode_speed=round(mode_speed, 2),
            std_dev=round(std_dev, 2),
        )

    async def get_diurnal_patterns(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> DiurnalPattern:
        """Get average wind pattern by hour of day."""
        query = text("""
            SELECT
                EXTRACT(HOUR FROM hour) as hour_of_day,
                AVG(wind_speed_100m) as avg_wind_speed,
                MIN(wind_speed_100m) as min_wind_speed,
                MAX(wind_speed_100m) as max_wind_speed,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wind_speed_100m) as median_wind_speed,
                STDDEV(wind_speed_100m) as std_dev
            FROM weather_data
            WHERE windfarm_id = :windfarm_id
              AND hour >= :start_date
              AND hour <= :end_date
              AND source = 'ERA5'
            GROUP BY hour_of_day
            ORDER BY hour_of_day;
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rows = result.fetchall()

        return DiurnalPattern(
            hours=[int(row[0]) for row in rows],
            avg_wind_speed=[round(float(row[1]), 2) for row in rows],
            min_wind_speed=[round(float(row[2]), 2) for row in rows],
            max_wind_speed=[round(float(row[3]), 2) for row in rows],
            median_wind_speed=[round(float(row[4]), 2) for row in rows],
            std_dev=[round(float(row[5]), 2) if row[5] else 0 for row in rows],
        )

    async def get_seasonal_patterns(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> SeasonalPattern:
        """Get average wind pattern by month."""
        query = text("""
            SELECT
                EXTRACT(MONTH FROM hour) as month_num,
                AVG(wind_speed_100m) as avg_wind_speed,
                MIN(wind_speed_100m) as min_wind_speed,
                MAX(wind_speed_100m) as max_wind_speed,
                AVG(temperature_2m_c) as avg_temperature
            FROM weather_data
            WHERE windfarm_id = :windfarm_id
              AND hour >= :start_date
              AND hour <= :end_date
              AND source = 'ERA5'
            GROUP BY month_num
            ORDER BY month_num;
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rows = result.fetchall()

        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        return SeasonalPattern(
            months=[month_names[int(row[0]) - 1] for row in rows],
            month_numbers=[int(row[0]) for row in rows],
            avg_wind_speed=[round(float(row[1]), 2) for row in rows],
            min_wind_speed=[round(float(row[2]), 2) for row in rows],
            max_wind_speed=[round(float(row[3]), 2) for row in rows],
            avg_temperature=[round(float(row[4]), 2) for row in rows],
        )

    async def get_wind_statistics(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> WindStatistics:
        """Calculate comprehensive wind statistics."""
        # Get all wind data for statistics
        query = text("""
            SELECT
                wind_speed_100m,
                wind_direction_deg,
                temperature_2m_c
            FROM weather_data
            WHERE windfarm_id = :windfarm_id
              AND hour >= :start_date
              AND hour <= :end_date
              AND source = 'ERA5';
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rows = result.fetchall()

        if not rows:
            # Return zero statistics
            return WindStatistics(
                mean_speed=0, median_speed=0, mode_speed=0,
                p10_speed=0, p50_speed=0, p90_speed=0,
                max_speed=0, min_speed=0, std_dev=0, variance=0,
                mean_temperature=0, max_temperature=0, min_temperature=0,
                prevailing_direction=0, prevailing_direction_name="N",
                capacity_factor_estimate=0, total_hours=0,
                calm_hours=0, calm_percentage=0
            )

        wind_speeds = np.array([float(row[0]) for row in rows])
        wind_directions = [float(row[1]) for row in rows]
        temperatures = np.array([float(row[2]) for row in rows])

        # Wind speed statistics
        mean_speed = np.mean(wind_speeds)
        median_speed = np.median(wind_speeds)
        p10_speed = np.percentile(wind_speeds, 10)
        p50_speed = np.percentile(wind_speeds, 50)
        p90_speed = np.percentile(wind_speeds, 90)
        max_speed = np.max(wind_speeds)
        min_speed = np.min(wind_speeds)
        std_dev = np.std(wind_speeds)
        variance = np.var(wind_speeds)

        # Temperature statistics
        mean_temp = np.mean(temperatures)
        max_temp = np.max(temperatures)
        min_temp = np.min(temperatures)

        # Prevailing direction (most common 22.5° bin)
        direction_bins = [int(d / 22.5) * 22.5 for d in wind_directions]
        prevailing_bin = max(set(direction_bins), key=direction_bins.count)
        prevailing_name = self._get_direction_name(prevailing_bin)

        # Calm hours (< 3 m/s cut-in speed)
        calm_hours = int(np.sum(wind_speeds < 3.0))
        total_hours = len(wind_speeds)
        calm_percentage = (calm_hours / total_hours * 100) if total_hours > 0 else 0

        # Rough capacity factor estimate (simplified power curve)
        # Assumes: Cut-in 3 m/s, Rated 12 m/s, Cut-out 25 m/s, Cubic relationship
        cf_values = []
        for ws in wind_speeds:
            if ws < 3 or ws > 25:
                cf = 0
            elif ws < 12:
                cf = ((ws - 3) / (12 - 3)) ** 3  # Cubic up to rated
            else:
                cf = 1.0  # Rated power
            cf_values.append(cf)

        capacity_factor_estimate = np.mean(cf_values) * 100 if cf_values else 0

        return WindStatistics(
            mean_speed=round(mean_speed, 2),
            median_speed=round(median_speed, 2),
            mode_speed=round(p50_speed, 2),  # Use median as mode approximation
            p10_speed=round(p10_speed, 2),
            p50_speed=round(p50_speed, 2),
            p90_speed=round(p90_speed, 2),
            max_speed=round(max_speed, 2),
            min_speed=round(min_speed, 2),
            std_dev=round(std_dev, 2),
            variance=round(variance, 2),
            mean_temperature=round(mean_temp, 2),
            max_temperature=round(max_temp, 2),
            min_temperature=round(min_temp, 2),
            prevailing_direction=float(prevailing_bin),
            prevailing_direction_name=prevailing_name,
            capacity_factor_estimate=round(capacity_factor_estimate, 2),
            total_hours=total_hours,
            calm_hours=calm_hours,
            calm_percentage=round(calm_percentage, 2),
        )

    async def get_wind_speed_duration_curve(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> WindSpeedDurationCurve:
        """
        Get wind speed duration curve (sorted wind speeds).

        Shows how many hours have wind >= certain speed.
        """
        query = text("""
            SELECT wind_speed_100m
            FROM weather_data
            WHERE windfarm_id = :windfarm_id
              AND hour >= :start_date
              AND hour <= :end_date
              AND source = 'ERA5'
            ORDER BY wind_speed_100m DESC;
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        wind_speeds = [float(row[0]) for row in result.fetchall()]

        if not wind_speeds:
            return WindSpeedDurationCurve(
                hours=[],
                wind_speed=[],
                cumulative_percentage=[],
            )

        total = len(wind_speeds)
        hours = list(range(total))
        cumulative_percentage = [(i / total * 100) for i in range(total)]

        return WindSpeedDurationCurve(
            hours=hours,
            wind_speed=wind_speeds,
            cumulative_percentage=[round(p, 2) for p in cumulative_percentage],
        )
