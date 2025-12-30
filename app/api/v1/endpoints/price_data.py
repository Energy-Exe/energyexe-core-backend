"""API endpoints for price data management and analytics."""

from datetime import datetime
from typing import List, Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_db, get_current_user
from app.models.user import User
from app.services.price_data_storage_service import PriceDataStorageService
from app.services.price_processing_service import PriceProcessingService
from app.services.price_analytics_service import PriceAnalyticsService
from app.schemas.price_data import (
    PriceFetchRequest,
    PriceFetchResponse,
    PriceProcessRequest,
    PriceProcessResponse,
    PriceDataRawResponse,
    PriceDataRawListResponse,
    PriceDataResponse,
    PriceDataListResponse,
    BidzoneListResponse,
    BidzoneAvailabilityResponse,
    PriceStatisticsResponse,
    PriceCoverageResponse,
    CaptureRateRequest,
    CaptureRateResponse,
    CaptureRateCompareRequest,
    CaptureRateCompareResponse,
    RevenueMetricsRequest,
    RevenueMetricsResponse,
    PriceProfileRequest,
    PriceProfileResponse,
    CorrelationResponse,
    PriceAvailabilityResponse,
    PriceFetchDayRequest,
    PriceFetchDayResponse,
)

router = APIRouter(prefix="/prices", tags=["Price Data"])


# ============================================================
# Raw Price Data Endpoints
# ============================================================

@router.post("/fetch", response_model=PriceFetchResponse)
async def fetch_prices_from_entsoe(
    request: PriceFetchRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Fetch price data from ENTSOE API and store in database.

    This endpoint fetches day-ahead and/or intraday prices for specified
    bidzones and stores them in the price_data_raw table.
    """
    service = PriceDataStorageService(db)
    result = await service.fetch_and_store_prices(
        bidzone_codes=request.bidzone_codes,
        start_date=request.start_date,
        end_date=request.end_date,
        price_types=request.price_types,
        user_id=current_user.id,
    )
    return PriceFetchResponse(**result)


@router.get("/raw", response_model=PriceDataRawListResponse)
async def get_raw_prices(
    bidzone_codes: Optional[List[str]] = Query(None, description="Filter by bidzone codes"),
    start_date: Optional[datetime] = Query(None, description="Start date filter"),
    end_date: Optional[datetime] = Query(None, description="End date filter"),
    price_type: Optional[str] = Query(None, description="Filter by price type"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get raw price data from price_data_raw table.

    Returns bidzone-level raw price records.
    """
    service = PriceDataStorageService(db)
    records = await service.get_raw_prices(
        bidzone_codes=bidzone_codes,
        start_date=start_date,
        end_date=end_date,
        price_type=price_type,
        limit=limit,
        offset=offset,
    )
    return PriceDataRawListResponse(
        items=[PriceDataRawResponse.model_validate(r) for r in records],
        total=len(records),
        limit=limit,
        offset=offset,
    )


@router.get("/bidzones", response_model=BidzoneListResponse)
async def get_available_bidzones(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get list of bidzones that have price data available.

    Returns bidzones with data availability information.
    """
    service = PriceDataStorageService(db)
    bidzones = await service.get_available_bidzones()
    return BidzoneListResponse(
        items=[BidzoneAvailabilityResponse(**b) for b in bidzones],
        total=len(bidzones),
    )


@router.get("/availability", response_model=PriceAvailabilityResponse)
async def get_price_availability(
    year: Optional[int] = Query(None, description="Year for availability check"),
    month: Optional[int] = Query(None, description="Month for availability check (1-12)"),
    bidzone_codes: Optional[str] = Query(None, description="Comma-separated list of bidzone codes"),
    price_type: Optional[str] = Query(None, description="Filter by price type: day_ahead or intraday"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get price data availability for a specified month.

    Shows which days have price data for selected bidzones,
    including record count and price types available per day.

    Similar to /generation/availability endpoint pattern.
    """
    from datetime import datetime as dt

    # Default to current month if not provided
    now = dt.utcnow()
    year = year or now.year
    month = month or now.month

    # Validate month range
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="Month must be between 1 and 12")

    # Parse bidzone_codes from comma-separated string
    bidzone_list = None
    if bidzone_codes:
        bidzone_list = [b.strip() for b in bidzone_codes.split(',') if b.strip()]

    service = PriceDataStorageService(db)
    result = await service.get_price_availability(
        year=year,
        month=month,
        bidzone_codes=bidzone_list,
        price_type=price_type,
    )

    return PriceAvailabilityResponse(**result)


@router.post("/fetch-day", response_model=PriceFetchDayResponse)
async def fetch_prices_for_specific_dates(
    request: PriceFetchDayRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Fetch price data from ENTSOE API for specific dates and bidzones.

    This endpoint allows fetching day-ahead and/or intraday prices for
    specific dates rather than a date range. Useful for filling gaps
    in historical data.

    Supports all Nordic areas:
    - Norway: NO_1, NO_2, NO_3, NO_4, NO_5
    - Sweden: SE_1, SE_2, SE_3, SE_4
    - Denmark: DK_1, DK_2
    - Finland: FI

    Returns detailed status of import for each date and bidzone.
    """
    service = PriceDataStorageService(db)
    result = await service.fetch_and_store_prices_for_dates(
        dates=request.dates,
        bidzone_codes=request.bidzone_codes,
        price_types=request.price_types,
        user_id=current_user.id,
    )

    return PriceFetchDayResponse(**result)


# ============================================================
# Processed Price Data Endpoints
# ============================================================

@router.post("/process", response_model=PriceProcessResponse)
async def process_raw_to_hourly(
    request: PriceProcessRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Process raw price data to windfarm-level hourly data.

    Maps bidzone prices to individual windfarms based on their bidzone_id
    and stores in the price_data table.
    """
    service = PriceProcessingService(db)
    result = await service.process_raw_to_hourly(
        windfarm_ids=request.windfarm_ids,
        bidzone_codes=request.bidzone_codes,
        start_date=request.start_date,
        end_date=request.end_date,
        force_reprocess=request.force_reprocess,
    )
    return PriceProcessResponse(**result)


@router.get("/processed", response_model=PriceDataListResponse)
async def get_processed_prices(
    windfarm_ids: Optional[List[int]] = Query(None, description="Filter by windfarm IDs"),
    bidzone_ids: Optional[List[int]] = Query(None, description="Filter by bidzone IDs"),
    start_date: Optional[datetime] = Query(None, description="Start date filter"),
    end_date: Optional[datetime] = Query(None, description="End date filter"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get processed price data from price_data table.

    Returns windfarm-level hourly price records.
    """
    service = PriceProcessingService(db)
    records = await service.get_processed_prices(
        windfarm_ids=windfarm_ids,
        bidzone_ids=bidzone_ids,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )
    return PriceDataListResponse(
        items=[PriceDataResponse.model_validate(r) for r in records],
        total=len(records),
        limit=limit,
        offset=offset,
    )


@router.get("/windfarms/{windfarm_id}/statistics", response_model=PriceStatisticsResponse)
async def get_windfarm_price_statistics(
    windfarm_id: int,
    start_date: datetime,
    end_date: datetime,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get price statistics for a windfarm.

    Returns average, min, max prices for the specified period.
    """
    service = PriceProcessingService(db)
    stats = await service.get_price_statistics(
        windfarm_id=windfarm_id,
        start_date=start_date,
        end_date=end_date,
    )
    if not stats:
        raise HTTPException(status_code=404, detail="No price data found for windfarm")
    return PriceStatisticsResponse(**stats)


@router.get("/windfarms/{windfarm_id}/coverage", response_model=PriceCoverageResponse)
async def get_windfarm_price_coverage(
    windfarm_id: int,
    start_date: datetime,
    end_date: datetime,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get price data coverage for a windfarm.

    Returns information about data completeness.
    """
    service = PriceProcessingService(db)
    coverage = await service.get_windfarm_coverage(
        windfarm_id=windfarm_id,
        start_date=start_date,
        end_date=end_date,
    )
    return PriceCoverageResponse(**coverage)


# ============================================================
# Analytics Endpoints
# ============================================================

@router.post("/analytics/capture-rate", response_model=CaptureRateResponse)
async def calculate_capture_rate(
    request: CaptureRateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Calculate capture rate for a windfarm.

    Capture Rate = Achieved Price / Market Average Price
    - Achieved Price = Revenue / Total Generation (revenue-weighted)
    - Market Average Price = Simple time-weighted average
    """
    service = PriceAnalyticsService(db)
    result = await service.calculate_capture_rate(
        windfarm_id=request.windfarm_id,
        start_date=request.start_date,
        end_date=request.end_date,
        aggregation=request.aggregation,
        price_type=request.price_type,
    )
    return CaptureRateResponse(**result)


@router.get("/analytics/capture-rate/{windfarm_id}")
async def get_capture_rate(
    windfarm_id: int,
    start_date: datetime,
    end_date: datetime,
    aggregation: Literal["hour", "day", "week", "month", "year"] = Query("month"),
    price_type: Literal["day_ahead", "intraday"] = Query("day_ahead"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get capture rate for a windfarm (GET version).
    """
    service = PriceAnalyticsService(db)
    result = await service.calculate_capture_rate(
        windfarm_id=windfarm_id,
        start_date=start_date,
        end_date=end_date,
        aggregation=aggregation,
        price_type=price_type,
    )
    return result


@router.post("/analytics/capture-rate/compare", response_model=CaptureRateCompareResponse)
async def compare_capture_rates(
    request: CaptureRateCompareRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Compare capture rates across multiple windfarms.

    Returns capture rates for each windfarm sorted by performance.
    """
    service = PriceAnalyticsService(db)
    result = await service.compare_capture_rates(
        windfarm_ids=request.windfarm_ids,
        start_date=request.start_date,
        end_date=request.end_date,
        aggregation=request.aggregation,
    )
    return CaptureRateCompareResponse(**result)


@router.post("/analytics/revenue", response_model=RevenueMetricsResponse)
async def calculate_revenue_metrics(
    request: RevenueMetricsRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Calculate revenue metrics for a windfarm.

    Returns generation, revenue, and average prices by period.
    """
    service = PriceAnalyticsService(db)
    result = await service.calculate_revenue_metrics(
        windfarm_id=request.windfarm_id,
        start_date=request.start_date,
        end_date=request.end_date,
        aggregation=request.aggregation,
    )
    return RevenueMetricsResponse(**result)


@router.get("/analytics/revenue/{windfarm_id}")
async def get_revenue_metrics(
    windfarm_id: int,
    start_date: datetime,
    end_date: datetime,
    aggregation: Literal["hour", "day", "week", "month", "year"] = Query("month"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get revenue metrics for a windfarm (GET version).
    """
    service = PriceAnalyticsService(db)
    result = await service.calculate_revenue_metrics(
        windfarm_id=windfarm_id,
        start_date=start_date,
        end_date=end_date,
        aggregation=aggregation,
    )
    return result


@router.post("/analytics/price-profile", response_model=PriceProfileResponse)
async def get_price_profile(
    request: PriceProfileRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get price profile for a bidzone.

    Shows average prices by hour of day or day of week.
    """
    service = PriceAnalyticsService(db)
    result = await service.get_price_profile(
        bidzone_id=request.bidzone_id,
        start_date=request.start_date,
        end_date=request.end_date,
        aggregation=request.aggregation,
    )
    return PriceProfileResponse(**result)


@router.get("/analytics/correlation/{windfarm_id}", response_model=CorrelationResponse)
async def get_generation_price_correlation(
    windfarm_id: int,
    start_date: datetime,
    end_date: datetime,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get correlation between generation and prices for a windfarm.

    Helps understand if generation tends to be high/low when prices are high/low.
    """
    service = PriceAnalyticsService(db)
    result = await service.get_generation_price_correlation(
        windfarm_id=windfarm_id,
        start_date=start_date,
        end_date=end_date,
    )
    return CorrelationResponse(**result)
