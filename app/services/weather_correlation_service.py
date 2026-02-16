"""Weather-generation correlation service."""
from datetime import datetime
from typing import List, Optional
from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession
import numpy as np
from scipy import stats

from app.models.weather_data import WeatherData
from app.models.generation_data import GenerationData
from app.schemas.weather_data import (
    CorrelationData,
    PowerCurveData,
    CapacityFactorData,
    CapacityFactorBin,
    EnergyRoseData,
    TemperatureImpactData,
    HeatmapData,
)


class WeatherCorrelationService:
    """Service for analyzing weather-generation correlations."""

    async def get_weather_generation_correlation(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> CorrelationData:
        """
        Calculate correlation between wind speed and generation.

        Groups by 1 m/s wind speed bins.
        """
        # First, get the correlation coefficient separately
        corr_query = text("""
            SELECT CORR(wd.wind_speed_100m, gd.generation_mwh) as correlation
            FROM weather_data wd
            JOIN generation_data gd ON
                gd.windfarm_id = wd.windfarm_id
                AND gd.hour = wd.hour
            WHERE wd.windfarm_id = :windfarm_id
              AND wd.hour >= :start_date
              AND wd.hour <= :end_date
              AND wd.source = 'ERA5'
              AND gd.generation_mwh IS NOT NULL;
        """)

        corr_result = await db.execute(corr_query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        correlation = corr_result.scalar() or 0

        # Then get the binned data
        query = text("""
            SELECT
                FLOOR(wd.wind_speed_100m) as wind_speed_bin,
                AVG(gd.generation_mwh) as avg_generation,
                MIN(gd.generation_mwh) as min_generation,
                MAX(gd.generation_mwh) as max_generation,
                STDDEV(gd.generation_mwh) as std_dev,
                COUNT(*) as record_count
            FROM weather_data wd
            JOIN generation_data gd ON
                gd.windfarm_id = wd.windfarm_id
                AND gd.hour = wd.hour
            WHERE wd.windfarm_id = :windfarm_id
              AND wd.hour >= :start_date
              AND wd.hour <= :end_date
              AND wd.source = 'ERA5'
              AND gd.generation_mwh IS NOT NULL
            GROUP BY wind_speed_bin
            HAVING COUNT(*) >= 5  -- Minimum samples for reliability
            ORDER BY wind_speed_bin;
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rows = result.fetchall()

        if not rows:
            return CorrelationData(
                wind_speed_bins=[],
                avg_generation_mw=[],
                min_generation_mw=[],
                max_generation_mw=[],
                std_dev_generation=[],
                record_count=[],
                correlation_coefficient=0,
                r_squared=0,
            )

        r_squared = float(correlation) ** 2

        return CorrelationData(
            wind_speed_bins=[float(row[0]) for row in rows],
            avg_generation_mw=[round(float(row[1]), 3) for row in rows],
            min_generation_mw=[round(float(row[2]), 3) for row in rows],
            max_generation_mw=[round(float(row[3]), 3) for row in rows],
            std_dev_generation=[round(float(row[4]), 3) if row[4] else 0 for row in rows],
            record_count=[int(row[5]) for row in rows],
            correlation_coefficient=round(float(correlation), 3),
            r_squared=round(r_squared, 3),
        )

    async def get_power_curve_actual(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> PowerCurveData:
        """
        Calculate actual power curve from data.

        Uses 0.5 m/s wind speed bins for smooth curve.
        """
        query = text("""
            SELECT
                FLOOR(wd.wind_speed_100m * 2) / 2 as wind_speed_bin,
                AVG(gd.generation_mwh) as avg_generation,
                COUNT(*) as sample_count,
                STDDEV(gd.generation_mwh) as std_dev
            FROM weather_data wd
            JOIN generation_data gd ON
                gd.windfarm_id = wd.windfarm_id
                AND gd.hour = wd.hour
            WHERE wd.windfarm_id = :windfarm_id
              AND wd.hour >= :start_date
              AND wd.hour <= :end_date
              AND wd.source = 'ERA5'
              AND gd.generation_mwh IS NOT NULL
            GROUP BY wind_speed_bin
            HAVING COUNT(*) >= 10  -- Minimum samples
            ORDER BY wind_speed_bin;
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rows = result.fetchall()

        if not rows:
            return PowerCurveData(
                wind_speed=[],
                generation_mw=[],
                sample_count=[],
                std_dev=[],
                cut_in_speed=None,
                rated_speed=None,
                cut_out_speed=None,
                rated_power=None,
                correlation_coefficient=0,
                r_squared=0,
            )

        wind_speeds = [float(row[0]) for row in rows]
        generation = [float(row[1]) for row in rows]
        sample_counts = [int(row[2]) for row in rows]
        std_devs = [float(row[3]) if row[3] else 0 for row in rows]

        # Detect curve parameters
        gen_array = np.array(generation)
        non_zero_gen = gen_array[gen_array > 0.01]

        cut_in_speed = None
        rated_speed = None
        rated_power = None

        if len(non_zero_gen) > 0:
            # Cut-in: first wind speed with generation > 0
            cut_in_idx = np.where(gen_array > 0.01)[0][0] if len(np.where(gen_array > 0.01)[0]) > 0 else None
            if cut_in_idx is not None:
                cut_in_speed = float(wind_speeds[cut_in_idx])

            # Rated: max generation
            rated_power = float(np.max(gen_array))
            rated_idx = np.argmax(gen_array)
            rated_speed = float(wind_speeds[rated_idx])

        cut_out_speed = 25.0  # Standard assumption

        # Calculate correlation
        if len(wind_speeds) >= 3:
            correlation, _ = stats.pearsonr(wind_speeds, generation)
            r_squared = correlation ** 2
        else:
            correlation = 0
            r_squared = 0

        return PowerCurveData(
            wind_speed=[round(ws, 1) for ws in wind_speeds],
            generation_mw=[round(g, 3) for g in generation],
            sample_count=sample_counts,
            std_dev=[round(sd, 3) for sd in std_devs],
            cut_in_speed=round(cut_in_speed, 1) if cut_in_speed else None,
            rated_speed=round(rated_speed, 1) if rated_speed else None,
            cut_out_speed=cut_out_speed,
            rated_power=round(rated_power, 2) if rated_power else None,
            correlation_coefficient=round(correlation, 3),
            r_squared=round(r_squared, 3),
        )

    async def get_capacity_factor_by_wind(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> CapacityFactorData:
        """
        Calculate capacity factor grouped by wind speed bins.

        Shows which wind speeds contribute most to generation.
        """
        # Get windfarm capacity
        from app.models.windfarm import Windfarm
        windfarm_query = select(Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
        wf_result = await db.execute(windfarm_query)
        capacity_mw = wf_result.scalar()

        if not capacity_mw:
            capacity_mw = 1000  # Default if not set

        query = text("""
            SELECT
                CASE
                    WHEN wd.wind_speed_100m < 5 THEN '0-5'
                    WHEN wd.wind_speed_100m < 10 THEN '5-10'
                    WHEN wd.wind_speed_100m < 15 THEN '10-15'
                    WHEN wd.wind_speed_100m < 20 THEN '15-20'
                    ELSE '20+'
                END as wind_bin,
                CASE
                    WHEN wd.wind_speed_100m < 5 THEN 2.5
                    WHEN wd.wind_speed_100m < 10 THEN 7.5
                    WHEN wd.wind_speed_100m < 15 THEN 12.5
                    WHEN wd.wind_speed_100m < 20 THEN 17.5
                    ELSE 22.5
                END as wind_center,
                AVG(gd.generation_mwh - COALESCE(gd.consumption_mwh, 0)) as avg_generation,
                COUNT(*) as frequency,
                SUM(gd.generation_mwh - COALESCE(gd.consumption_mwh, 0)) as total_generation
            FROM weather_data wd
            JOIN generation_data gd ON
                gd.windfarm_id = wd.windfarm_id
                AND gd.hour = wd.hour
            WHERE wd.windfarm_id = :windfarm_id
              AND wd.hour >= :start_date
              AND wd.hour <= :end_date
              AND wd.source = 'ERA5'
              AND gd.generation_mwh IS NOT NULL
            GROUP BY wind_bin, wind_center
            ORDER BY wind_center;
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rows = result.fetchall()

        if not rows:
            return CapacityFactorData(
                wind_speed_bins=[],
                wind_speed_centers=[],
                capacity_factors=[],
                frequencies=[],
                generation_contributions=[],
                overall_capacity_factor=0,
            )

        total_hours = sum(int(row[3]) for row in rows)
        total_generation = sum(float(row[4]) for row in rows)

        bins = []
        centers = []
        cfs = []
        freqs = []
        contributions = []

        for row in rows:
            wind_bin = row[0]
            wind_center = float(row[1])
            avg_gen = float(row[2])
            count = int(row[3])
            bin_total_gen = float(row[4])

            # Capacity factor for this bin
            cf = (avg_gen / float(capacity_mw) * 100) if capacity_mw > 0 else 0

            # Frequency (% of time)
            freq = (count / total_hours * 100) if total_hours > 0 else 0

            # Generation contribution (% of total generation)
            contribution = (bin_total_gen / total_generation * 100) if total_generation > 0 else 0

            bins.append(wind_bin)
            centers.append(wind_center)
            cfs.append(round(cf, 2))
            freqs.append(round(freq, 2))
            contributions.append(round(contribution, 2))

        # Overall CF
        total_possible = capacity_mw * total_hours
        overall_cf = (total_generation / total_possible * 100) if total_possible > 0 else 0

        return CapacityFactorData(
            wind_speed_bins=bins,
            wind_speed_centers=centers,
            capacity_factors=cfs,
            frequencies=freqs,
            generation_contributions=contributions,
            overall_capacity_factor=round(overall_cf, 2),
        )

    async def get_energy_rose_data(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> EnergyRoseData:
        """
        Get energy rose - generation by wind direction.

        Shows which directions contribute most energy (not just frequency).
        """
        query = text("""
            SELECT
                FLOOR(wd.wind_direction_deg / 22.5) * 22.5 as direction_bin,
                SUM(gd.generation_mwh - COALESCE(gd.consumption_mwh, 0)) as total_generation,
                COUNT(*) as frequency
            FROM weather_data wd
            JOIN generation_data gd ON
                gd.windfarm_id = wd.windfarm_id
                AND gd.hour = wd.hour
            WHERE wd.windfarm_id = :windfarm_id
              AND wd.hour >= :start_date
              AND wd.hour <= :end_date
              AND wd.source = 'ERA5'
              AND gd.generation_mwh IS NOT NULL
            GROUP BY direction_bin
            ORDER BY direction_bin;
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rows = result.fetchall()

        if not rows:
            return EnergyRoseData(
                direction_bins=[],
                generation_by_direction=[],
                percentage_by_direction=[],
                frequency_by_direction=[],
            )

        total_generation = sum(float(row[1]) for row in rows)
        total_frequency = sum(int(row[2]) for row in rows)

        direction_bins = [float(row[0]) for row in rows]
        generation = [float(row[1]) for row in rows]
        frequencies = [int(row[2]) for row in rows]

        percentages = [(g / total_generation * 100) if total_generation > 0 else 0 for g in generation]
        freq_percentages = [(f / total_frequency * 100) if total_frequency > 0 else 0 for f in frequencies]

        return EnergyRoseData(
            direction_bins=direction_bins,
            generation_by_direction=[round(g, 2) for g in generation],
            percentage_by_direction=[round(p, 2) for p in percentages],
            frequency_by_direction=[round(f, 2) for f in freq_percentages],
        )

    async def get_temperature_impact(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        reference_wind_speed: float = 10.0,
    ) -> TemperatureImpactData:
        """
        Analyze temperature impact on generation at constant wind speed.

        Args:
            reference_wind_speed: Wind speed to analyze (default 10 m/s)
        """
        # Get data for wind speeds within ±1 m/s of reference
        query = text("""
            SELECT
                FLOOR(wd.temperature_2m_c / 5) * 5 as temp_bin,
                AVG(gd.generation_mwh) as avg_generation,
                COUNT(*) as sample_count
            FROM weather_data wd
            JOIN generation_data gd ON
                gd.windfarm_id = wd.windfarm_id
                AND gd.hour = wd.hour
            WHERE wd.windfarm_id = :windfarm_id
              AND wd.hour >= :start_date
              AND wd.hour <= :end_date
              AND wd.wind_speed_100m >= :min_wind
              AND wd.wind_speed_100m <= :max_wind
              AND wd.source = 'ERA5'
              AND gd.generation_mwh IS NOT NULL
            GROUP BY temp_bin
            HAVING COUNT(*) >= 10
            ORDER BY temp_bin;
        """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date,
            "min_wind": reference_wind_speed - 1,
            "max_wind": reference_wind_speed + 1,
        })
        rows = result.fetchall()

        if not rows or len(rows) < 2:
            return TemperatureImpactData(
                reference_wind_speed=reference_wind_speed,
                temperature_bins=[],
                avg_generation=[],
                sample_count=[],
                impact_percentage=0,
            )

        temp_bins = [float(row[0]) for row in rows]
        avg_gen = [float(row[1]) for row in rows]
        sample_counts = [int(row[2]) for row in rows]

        # Calculate impact: % change per 10°C
        # Linear regression: generation vs temperature
        if len(temp_bins) >= 2:
            slope, intercept, _, _, _ = stats.linregress(temp_bins, avg_gen)
            # Impact per 10°C change
            base_gen = avg_gen[0] if avg_gen[0] > 0 else np.mean(avg_gen)
            impact_percentage = (slope * 10 / base_gen * 100) if base_gen > 0 else 0
        else:
            impact_percentage = 0

        return TemperatureImpactData(
            reference_wind_speed=reference_wind_speed,
            temperature_bins=[round(t, 1) for t in temp_bins],
            avg_generation=[round(g, 3) for g in avg_gen],
            sample_count=sample_counts,
            impact_percentage=round(impact_percentage, 2),
        )

    async def get_weather_generation_heatmap(
        self,
        db: AsyncSession,
        windfarm_id: int,
        year: int,
        metric: str = "wind_speed",
    ) -> HeatmapData:
        """
        Get hour × month heatmap data.

        Args:
            metric: "wind_speed", "temperature", or "generation"
        """
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31, 23, 59, 59)

        if metric == "generation":
            query = text("""
                SELECT
                    EXTRACT(HOUR FROM gd.hour) as hour,
                    EXTRACT(MONTH FROM gd.hour) as month,
                    AVG(gd.generation_mwh) as avg_value
                FROM generation_data gd
                WHERE gd.windfarm_id = :windfarm_id
                  AND gd.hour >= :start_date
                  AND gd.hour <= :end_date
                  AND gd.generation_mwh IS NOT NULL
                GROUP BY hour, month
                ORDER BY hour, month;
            """)
        else:
            value_col = "wind_speed_100m" if metric == "wind_speed" else "temperature_2m_c"
            query = text(f"""
                SELECT
                    EXTRACT(HOUR FROM hour) as hour,
                    EXTRACT(MONTH FROM hour) as month,
                    AVG({value_col}) as avg_value
                FROM weather_data
                WHERE windfarm_id = :windfarm_id
                  AND hour >= :start_date
                  AND hour <= :end_date
                  AND source = 'ERA5'
                GROUP BY hour, month
                ORDER BY hour, month;
            """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rows = result.fetchall()

        # Build 2D array: 24 hours × 12 months
        values = [[0.0 for _ in range(12)] for _ in range(24)]

        for row in rows:
            hour = int(row[0])
            month = int(row[1])
            value = float(row[2])
            values[hour][month - 1] = round(value, 2)

        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        unit = "m/s" if metric == "wind_speed" else ("°C" if metric == "temperature" else "MW")

        return HeatmapData(
            hours=list(range(24)),
            months=month_names,
            values=values,
            metric=metric,
            unit=unit,
        )

    async def get_weather_generation_heatmap_daterange(
        self,
        db: AsyncSession,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        metric: str = "wind_speed",
    ) -> HeatmapData:
        """
        Get hour × month heatmap data for a date range.

        Args:
            metric: "wind_speed", "temperature", or "generation"
        """
        if metric == "generation":
            query = text("""
                SELECT
                    EXTRACT(HOUR FROM gd.hour) as hour,
                    EXTRACT(MONTH FROM gd.hour) as month,
                    AVG(gd.generation_mwh) as avg_value
                FROM generation_data gd
                WHERE gd.windfarm_id = :windfarm_id
                  AND gd.hour >= :start_date
                  AND gd.hour <= :end_date
                  AND gd.generation_mwh IS NOT NULL
                GROUP BY hour, month
                ORDER BY hour, month;
            """)
        else:
            value_col = "wind_speed_100m" if metric == "wind_speed" else "temperature_2m_c"
            query = text(f"""
                SELECT
                    EXTRACT(HOUR FROM hour) as hour,
                    EXTRACT(MONTH FROM hour) as month,
                    AVG({value_col}) as avg_value
                FROM weather_data
                WHERE windfarm_id = :windfarm_id
                  AND hour >= :start_date
                  AND hour <= :end_date
                  AND source = 'ERA5'
                GROUP BY hour, month
                ORDER BY hour, month;
            """)

        result = await db.execute(query, {
            "windfarm_id": windfarm_id,
            "start_date": start_date,
            "end_date": end_date
        })
        rows = result.fetchall()

        # Build 2D array: 24 hours × 12 months
        values = [[0.0 for _ in range(12)] for _ in range(24)]

        for row in rows:
            hour = int(row[0])
            month = int(row[1])
            value = float(row[2])
            values[hour][month - 1] = round(value, 2)

        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        unit = "m/s" if metric == "wind_speed" else ("°C" if metric == "temperature" else "MW")

        return HeatmapData(
            hours=list(range(24)),
            months=month_names,
            values=values,
            metric=metric,
            unit=unit,
        )
