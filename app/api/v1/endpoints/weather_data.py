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
