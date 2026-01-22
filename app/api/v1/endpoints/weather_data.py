"""Weather data API endpoints."""
from datetime import datetime, date
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_db
from app.services.weather_data_service import WeatherDataService
from app.services.weather_analytics_service import WeatherAnalyticsService
from app.services.weather_correlation_service import WeatherCorrelationService
from app.services.weather_summary_service import WeatherSummaryService
from app.schemas.weather_data import (
    DateAvailability,
    WeatherFetchRequest,
    WeatherFetchJobResponse,
    WeatherTimeseries,
    WindRoseData,
    WindSpeedDistribution,
    DiurnalPattern,
    SeasonalPattern,
    WindStatistics,
    CorrelationData,
    PowerCurveData,
    CapacityFactorData,
    EnergyRoseData,
    TemperatureImpactData,
    HeatmapData,
    WindSpeedDurationCurve,
)
from app.schemas.weather_summary import WeatherSummaryResponse

router = APIRouter(prefix="/weather-data", tags=["Weather Data"])


# ============================================================================
# AVAILABILITY & FETCH ENDPOINTS
# ============================================================================


@router.get("/availability", response_model=List[DateAvailability])
async def get_weather_availability(
    start_date: date = Query(..., description="Start date"),
    end_date: date = Query(..., description="End date"),
    windfarm_id: Optional[int] = Query(None, description="Filter by windfarm"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get weather data availability calendar for date range.

    Returns availability status for each date including:
    - Whether data exists
    - Record count
    - Completion percentage
    """
    service = WeatherDataService()
    return await service.get_availability_calendar(db, start_date, end_date, windfarm_id)


@router.get("/missing-dates", response_model=List[date])
async def get_missing_dates(
    start_date: date = Query(..., description="Start date"),
    end_date: date = Query(..., description="End date"),
    db: AsyncSession = Depends(get_db),
):
    """Get list of dates with missing or incomplete weather data."""
    service = WeatherDataService()
    return await service.get_missing_dates(db, start_date, end_date)


@router.post("/fetch", response_model=WeatherFetchJobResponse)
async def trigger_weather_fetch(
    request: WeatherFetchRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger ERA5 weather data fetch for a specific date.

    Spawns background job to fetch and process data.
    Returns job ID for status tracking.
    """
    service = WeatherDataService()
    return await service.trigger_fetch_for_date(db, request)


@router.get("/fetch-jobs/{job_id}", response_model=WeatherFetchJobResponse)
async def get_fetch_job_status(
    job_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Get status of a weather fetch job."""
    service = WeatherDataService()
    result = await service.get_fetch_job_status(db, job_id)
    if not result:
        raise HTTPException(status_code=404, detail="Job not found")
    return result


# ============================================================================
# BASIC ANALYTICS ENDPOINTS
# ============================================================================


@router.get("/windfarms/{windfarm_id}/timeseries", response_model=WeatherTimeseries)
async def get_weather_timeseries(
    windfarm_id: int,
    start_date: datetime = Query(..., description="Start datetime"),
    end_date: datetime = Query(..., description="End datetime"),
    aggregation: str = Query("daily", regex="^(hourly|daily|monthly)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get weather time series data for a windfarm.

    Supports hourly, daily, or monthly aggregation.
    """
    service = WeatherAnalyticsService()
    return await service.get_weather_timeseries(db, windfarm_id, start_date, end_date, aggregation)


@router.get("/windfarms/{windfarm_id}/statistics", response_model=WindStatistics)
async def get_wind_statistics(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Get comprehensive wind statistics for a windfarm.

    Includes mean, median, percentiles, Weibull parameters, etc.
    """
    service = WeatherAnalyticsService()
    return await service.get_wind_statistics(db, windfarm_id, start_date, end_date)


# ============================================================================
# WIND ANALYSIS ENDPOINTS
# ============================================================================


@router.get("/windfarms/{windfarm_id}/wind-rose", response_model=WindRoseData)
async def get_wind_rose(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Get wind rose data (frequency by direction and speed).

    Returns 16 direction bins × 5 speed bins for polar chart.
    """
    service = WeatherAnalyticsService()
    return await service.get_wind_rose_data(db, windfarm_id, start_date, end_date)


@router.get("/windfarms/{windfarm_id}/distribution", response_model=WindSpeedDistribution)
async def get_wind_distribution(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Get wind speed distribution with Weibull fit.

    Useful for resource assessment and energy forecasting.
    """
    service = WeatherAnalyticsService()
    return await service.get_wind_speed_distribution(db, windfarm_id, start_date, end_date)


@router.get("/windfarms/{windfarm_id}/diurnal-pattern", response_model=DiurnalPattern)
async def get_diurnal_pattern(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Get diurnal wind pattern (average by hour of day).

    Shows daily wind cycle and peak hours.
    """
    service = WeatherAnalyticsService()
    return await service.get_diurnal_patterns(db, windfarm_id, start_date, end_date)


@router.get("/windfarms/{windfarm_id}/seasonal-pattern", response_model=SeasonalPattern)
async def get_seasonal_pattern(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Get seasonal wind pattern (average by month).

    Shows seasonal trends and inter-annual variability.
    """
    service = WeatherAnalyticsService()
    return await service.get_seasonal_patterns(db, windfarm_id, start_date, end_date)


@router.get("/windfarms/{windfarm_id}/duration-curve", response_model=WindSpeedDurationCurve)
async def get_duration_curve(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Get wind speed duration curve.

    Shows cumulative hours at different wind speeds.
    """
    service = WeatherAnalyticsService()
    return await service.get_wind_speed_duration_curve(db, windfarm_id, start_date, end_date)


# ============================================================================
# CORRELATION ENDPOINTS
# ============================================================================


@router.get("/windfarms/{windfarm_id}/correlation", response_model=CorrelationData)
async def get_weather_generation_correlation(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Get correlation between wind speed and generation.

    Returns binned averages and correlation coefficient.
    """
    service = WeatherCorrelationService()
    return await service.get_weather_generation_correlation(db, windfarm_id, start_date, end_date)


@router.get("/windfarms/{windfarm_id}/power-curve", response_model=PowerCurveData)
async def get_power_curve(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Get actual power curve (wind speed vs generation).

    Returns empirical power curve with cut-in/rated/cut-out speeds.
    """
    service = WeatherCorrelationService()
    return await service.get_power_curve_actual(db, windfarm_id, start_date, end_date)


@router.get("/windfarms/{windfarm_id}/capacity-factor-by-wind", response_model=CapacityFactorData)
async def get_capacity_factor_by_wind(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Get capacity factor grouped by wind speed bins.

    Shows which wind speeds contribute most to generation.
    """
    service = WeatherCorrelationService()
    return await service.get_capacity_factor_by_wind(db, windfarm_id, start_date, end_date)


@router.get("/windfarms/{windfarm_id}/energy-rose", response_model=EnergyRoseData)
async def get_energy_rose(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Get energy rose (generation by wind direction).

    Shows which directions contribute most energy production.
    """
    service = WeatherCorrelationService()
    return await service.get_energy_rose_data(db, windfarm_id, start_date, end_date)


@router.get("/windfarms/{windfarm_id}/temperature-impact", response_model=TemperatureImpactData)
async def get_temperature_impact(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    reference_wind_speed: float = Query(10.0, description="Reference wind speed (m/s)"),
    db: AsyncSession = Depends(get_db),
):
    """
    Analyze temperature impact on generation at constant wind speed.

    Shows how air density (temperature) affects power output.
    """
    service = WeatherCorrelationService()
    return await service.get_temperature_impact(
        db, windfarm_id, start_date, end_date, reference_wind_speed
    )


@router.get("/windfarms/{windfarm_id}/heatmap", response_model=HeatmapData)
async def get_weather_heatmap(
    windfarm_id: int,
    start_date: datetime = Query(..., description="Start datetime"),
    end_date: datetime = Query(..., description="End datetime"),
    metric: str = Query("wind_speed", regex="^(wind_speed|temperature|generation)$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get hour × month heatmap data for date range.

    Metric options: wind_speed, temperature, generation
    """
    service = WeatherCorrelationService()
    return await service.get_weather_generation_heatmap_daterange(
        db, windfarm_id, start_date, end_date, metric
    )


# ============================================================================
# HISTORICAL SUMMARY ENDPOINTS
# ============================================================================


@router.get("/windfarms/{windfarm_id}/weather-summary", response_model=WeatherSummaryResponse)
async def get_weather_summary(
    windfarm_id: int,
    period_type: str = Query(
        "monthly", regex="^(monthly|yearly)$", description="Aggregation period"
    ),
    start_year: Optional[int] = Query(None, ge=2000, le=2100, description="Filter start year"),
    end_year: Optional[int] = Query(None, ge=2000, le=2100, description="Filter end year"),
    db: AsyncSession = Depends(get_db),
):
    """
    Get historical wind speed and direction summaries grouped by year or month.

    Returns for each period:
    - Average, min, max, std wind speed
    - Prevailing wind direction (vector-averaged for circular data)
    - Direction distribution histogram (16 compass bins)
    - Data completeness metrics

    Useful for analyzing year-over-year or seasonal trends in wind patterns.
    """
    service = WeatherSummaryService()
    try:
        return await service.get_period_summaries(
            db,
            windfarm_id=windfarm_id,
            period_type=period_type,
            start_year=start_year,
            end_year=end_year,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ============================================================================
# PORTFOLIO-LEVEL WEATHER ENDPOINTS
# ============================================================================

from typing import Dict, Any, Literal
from datetime import timedelta
from app.core.deps import get_current_user
from app.models.user import User


@router.get("/portfolio/summary")
async def get_portfolio_weather_summary(
    start_date: datetime = Query(..., description="Start date for analysis"),
    end_date: datetime = Query(..., description="End date for analysis"),
    portfolio_id: Optional[int] = Query(None, description="Filter by portfolio ID"),
    country_id: Optional[int] = Query(None, description="Filter by country ID"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Dict[str, Any]:
    """
    Get aggregated weather summary across all accessible wind farms.

    Returns:
    - Portfolio-wide average wind speed and temperature
    - Wind conditions breakdown by country
    - Correlation summary (best/worst performers)
    - Seasonal patterns comparison
    """
    from sqlalchemy import select, func, text
    from app.models.weather_data import WeatherData
    from app.models.generation_data import GenerationData
    from app.models.windfarm import Windfarm
    from app.models.geography import Country
    from app.models.portfolio import PortfolioItem

    # Build windfarm filter based on portfolio or country
    windfarm_filter_ids = None
    if portfolio_id:
        windfarm_ids_query = select(PortfolioItem.windfarm_id).where(
            PortfolioItem.portfolio_id == portfolio_id
        )
        windfarm_ids_result = await db.execute(windfarm_ids_query)
        windfarm_filter_ids = [row[0] for row in windfarm_ids_result.fetchall()]
        if not windfarm_filter_ids:
            return {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "avg_wind_speed": 0,
                "min_wind_speed": 0,
                "max_wind_speed": 0,
                "avg_temperature": 0,
                "farm_count": 0,
                "total_hours": 0,
                "by_country": [],
                "correlation_summary": [],
                "seasonal_patterns": [],
            }

    # Portfolio-wide weather statistics
    portfolio_stats_query = text("""
        SELECT
            AVG(w.wind_speed_100m) as avg_wind_speed,
            MIN(w.wind_speed_100m) as min_wind_speed,
            MAX(w.wind_speed_100m) as max_wind_speed,
            AVG(w.temperature_2m_c) as avg_temperature,
            COUNT(DISTINCT w.windfarm_id) as farm_count,
            COUNT(*) as total_hours
        FROM weather_data w
        JOIN windfarms wf ON w.windfarm_id = wf.id
        WHERE w.hour >= :start_date
          AND w.hour < :end_date
          AND w.wind_speed_100m IS NOT NULL
    """ + (" AND w.windfarm_id = ANY(:windfarm_ids)" if windfarm_filter_ids else "") +
    (" AND wf.country_id = :country_id" if country_id else ""))

    params = {
        'start_date': start_date,
        'end_date': end_date + timedelta(days=1),
    }
    if windfarm_filter_ids:
        params['windfarm_ids'] = windfarm_filter_ids
    if country_id:
        params['country_id'] = country_id

    stats_result = await db.execute(portfolio_stats_query, params)
    stats_row = stats_result.fetchone()

    # Weather by country breakdown
    by_country_query = text("""
        SELECT
            c.id as country_id,
            c.name as country_name,
            c.code as country_code,
            AVG(w.wind_speed_100m) as avg_wind_speed,
            AVG(w.temperature_2m_c) as avg_temperature,
            COUNT(DISTINCT w.windfarm_id) as farm_count,
            COUNT(*) as data_points
        FROM weather_data w
        JOIN windfarms wf ON w.windfarm_id = wf.id
        JOIN countries c ON wf.country_id = c.id
        WHERE w.hour >= :start_date
          AND w.hour < :end_date
          AND w.wind_speed_100m IS NOT NULL
    """ + (" AND w.windfarm_id = ANY(:windfarm_ids)" if windfarm_filter_ids else "") +
    (" AND wf.country_id = :country_id" if country_id else "") + """
        GROUP BY c.id, c.name, c.code
        ORDER BY avg_wind_speed DESC
    """)

    by_country_result = await db.execute(by_country_query, params)
    by_country_rows = by_country_result.fetchall()

    by_country = [
        {
            "country_id": row.country_id,
            "country_name": row.country_name,
            "country_code": row.country_code,
            "avg_wind_speed": round(float(row.avg_wind_speed or 0), 2),
            "avg_temperature": round(float(row.avg_temperature or 0), 1),
            "farm_count": row.farm_count,
            "data_points": row.data_points,
        }
        for row in by_country_rows
    ]

    # Correlation summary - best and worst performers (wind-generation correlation)
    correlation_query = text("""
        WITH farm_correlations AS (
            SELECT
                wf.id as windfarm_id,
                wf.name as windfarm_name,
                wf.code as windfarm_code,
                c.name as country_name,
                AVG(w.wind_speed_100m) as avg_wind_speed,
                AVG(g.generation_mwh) as avg_generation,
                wf.nameplate_capacity_mw,
                CASE
                    WHEN wf.nameplate_capacity_mw > 0
                    THEN AVG(g.generation_mwh) / wf.nameplate_capacity_mw * 100
                    ELSE 0
                END as capacity_factor,
                CORR(w.wind_speed_100m, g.generation_mwh) as wind_gen_correlation,
                COUNT(*) as data_points
            FROM weather_data w
            JOIN generation_data g ON w.windfarm_id = g.windfarm_id AND w.hour = g.hour
            JOIN windfarms wf ON w.windfarm_id = wf.id
            JOIN countries c ON wf.country_id = c.id
            WHERE w.hour >= :start_date
              AND w.hour < :end_date
              AND w.wind_speed_100m IS NOT NULL
              AND g.generation_mwh IS NOT NULL
    """ + (" AND w.windfarm_id = ANY(:windfarm_ids)" if windfarm_filter_ids else "") +
    (" AND wf.country_id = :country_id" if country_id else "") + """
            GROUP BY wf.id, wf.name, wf.code, c.name, wf.nameplate_capacity_mw
            HAVING COUNT(*) > 24
        )
        SELECT * FROM farm_correlations
        ORDER BY wind_gen_correlation DESC NULLS LAST
    """)

    correlation_result = await db.execute(correlation_query, params)
    correlation_rows = correlation_result.fetchall()

    correlation_summary = [
        {
            "windfarm_id": row.windfarm_id,
            "windfarm_name": row.windfarm_name,
            "windfarm_code": row.windfarm_code,
            "country_name": row.country_name,
            "avg_wind_speed": round(float(row.avg_wind_speed or 0), 2),
            "avg_generation_mwh": round(float(row.avg_generation or 0), 2),
            "capacity_factor": round(float(row.capacity_factor or 0), 1),
            "wind_gen_correlation": round(float(row.wind_gen_correlation or 0), 3) if row.wind_gen_correlation else None,
            "data_points": row.data_points,
        }
        for row in correlation_rows
    ]

    # Seasonal patterns (monthly averages across portfolio)
    seasonal_query = text("""
        SELECT
            EXTRACT(MONTH FROM w.hour) as month,
            AVG(w.wind_speed_100m) as avg_wind_speed,
            AVG(w.temperature_2m_c) as avg_temperature,
            COUNT(DISTINCT w.windfarm_id) as farm_count,
            COUNT(*) as data_points
        FROM weather_data w
        JOIN windfarms wf ON w.windfarm_id = wf.id
        WHERE w.hour >= :start_date
          AND w.hour < :end_date
          AND w.wind_speed_100m IS NOT NULL
    """ + (" AND w.windfarm_id = ANY(:windfarm_ids)" if windfarm_filter_ids else "") +
    (" AND wf.country_id = :country_id" if country_id else "") + """
        GROUP BY EXTRACT(MONTH FROM w.hour)
        ORDER BY month
    """)

    seasonal_result = await db.execute(seasonal_query, params)
    seasonal_rows = seasonal_result.fetchall()

    month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]

    seasonal_patterns = [
        {
            "month": int(row.month),
            "month_name": month_names[int(row.month) - 1],
            "avg_wind_speed": round(float(row.avg_wind_speed or 0), 2),
            "avg_temperature": round(float(row.avg_temperature or 0), 1),
            "farm_count": row.farm_count,
            "data_points": row.data_points,
        }
        for row in seasonal_rows
    ]

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "avg_wind_speed": round(float(stats_row.avg_wind_speed or 0), 2) if stats_row else 0,
        "min_wind_speed": round(float(stats_row.min_wind_speed or 0), 2) if stats_row else 0,
        "max_wind_speed": round(float(stats_row.max_wind_speed or 0), 2) if stats_row else 0,
        "avg_temperature": round(float(stats_row.avg_temperature or 0), 1) if stats_row else 0,
        "farm_count": stats_row.farm_count if stats_row else 0,
        "total_hours": stats_row.total_hours if stats_row else 0,
        "by_country": by_country,
        "correlation_summary": correlation_summary,
        "seasonal_patterns": seasonal_patterns,
    }
