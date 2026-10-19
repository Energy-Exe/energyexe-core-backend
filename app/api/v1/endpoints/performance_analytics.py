"""Performance Analytics section of the client portal (EPR-186)."""

from datetime import date
from typing import Literal, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.core.redis import cache_report, get_cached_report
from app.models.user import User
from app.services.comparison_analytics_service import ComparisonAnalyticsService
from app.services.financial_comparison_service import FinancialComparisonService
from app.services.generation_analytics_service import (
    MAX_DAY_WINDOW_DAYS,
    MAX_FARMS,
    GenerationAnalyticsService,
)
from app.services.performance_analytics_service import (
    Month,
    PerformanceAnalyticsService,
    add_months,
    month_key,
    parse_month,
)

router = APIRouter()

# The inputs only change with the nightly pipeline / view refresh.
CACHE_TTL_SECONDS = 6 * 3600
MAX_WINDOW_MONTHS = 360


def _month_window(start: str, end: str) -> Tuple[Month, Month]:
    """Parse and check a YYYY-MM window; raises 400 on bad input."""
    try:
        start_m, end_m = parse_month(start), parse_month(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="start and end must be YYYY-MM")
    if end_m < start_m:
        raise HTTPException(status_code=400, detail="end must be on or after start")
    if add_months(start_m, MAX_WINDOW_MONTHS) <= end_m:
        raise HTTPException(
            status_code=400, detail=f"The window covers at most {MAX_WINDOW_MONTHS} months"
        )
    return start_m, end_m


@router.get("/windfarms/{windfarm_id}/performance")
async def get_windfarm_performance(
    windfarm_id: int,
    start: str = Query(..., description="First month, YYYY-MM"),
    end: str = Query(..., description="Last month (inclusive), YYYY-MM"),
    benchmark: Optional[Literal["zone", "country"]] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Monthly wind-normalised index, spot price, benchmark and P50 analysis."""
    start_m, end_m = _month_window(start, end)
    cache_key = (
        f"perf-analytics:v3:{windfarm_id}:{month_key(*start_m)}:{month_key(*end_m)}:"
        f"{benchmark or '-'}"
    )
    cached = await get_cached_report(cache_key)
    if cached is not None:
        return cached

    data = await PerformanceAnalyticsService(db).get_performance(
        windfarm_id, start_m, end_m, benchmark
    )
    if data is None:
        raise HTTPException(status_code=404, detail="Wind farm not found")

    await cache_report(cache_key, data, ttl_seconds=CACHE_TTL_SECONDS)
    return data


@router.get("/generation")
async def get_generation(
    windfarm_ids: str = Query(..., description="Comma-separated wind farm ids (1-6)"),
    granularity: Literal["day", "month", "year"] = Query("month"),
    start: str = Query(..., description="First day, YYYY-MM-DD"),
    end: str = Query(..., description="Last day (inclusive), YYYY-MM-DD"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Generation, capacity factor and zone spot price per period (EPR-187)."""
    try:
        ids = list(dict.fromkeys(int(x) for x in windfarm_ids.split(",") if x.strip()))
    except ValueError:
        raise HTTPException(status_code=400, detail="windfarm_ids must be integers")
    if not 1 <= len(ids) <= MAX_FARMS:
        raise HTTPException(status_code=400, detail=f"Pick 1 to {MAX_FARMS} wind farms")
    try:
        start_d, end_d = date.fromisoformat(start), date.fromisoformat(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="start and end must be YYYY-MM-DD")
    span_days = (end_d - start_d).days + 1
    if span_days < 1:
        raise HTTPException(status_code=400, detail="end must be on or after start")
    if span_days > MAX_WINDOW_MONTHS * 31:
        raise HTTPException(
            status_code=400, detail=f"The window covers at most {MAX_WINDOW_MONTHS} months"
        )
    if granularity == "day" and span_days > MAX_DAY_WINDOW_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"The day view covers at most {MAX_DAY_WINDOW_DAYS} days",
        )

    cache_key = f"perf-analytics:gen:v2:{','.join(map(str, ids))}:{granularity}:{start_d}:{end_d}"
    cached = await get_cached_report(cache_key)
    if cached is not None:
        return cached

    data = await GenerationAnalyticsService(db).get_generation(ids, granularity, start_d, end_d)
    await cache_report(cache_key, data, ttl_seconds=CACHE_TTL_SECONDS)
    return data


@router.get("/comparison")
async def get_comparison(
    start: str = Query(..., description="First month, YYYY-MM"),
    end: str = Query(..., description="Last month (inclusive), YYYY-MM"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Monthly capacity factor of every compared wind farm (EPR-188)."""
    start_m, end_m = _month_window(start, end)
    cache_key = f"perf-analytics:cmp:v2:{month_key(*start_m)}:{month_key(*end_m)}"
    cached = await get_cached_report(cache_key)
    if cached is not None:
        return cached

    # The monthly view is keyed by each month's first day.
    data = await ComparisonAnalyticsService(db).get_comparison(
        date(start_m[0], start_m[1], 1), date(end_m[0], end_m[1], 1)
    )
    await cache_report(cache_key, data, ttl_seconds=CACHE_TTL_SECONDS)
    return data


@router.get("/financial")
async def get_financial(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Annual revenue / OPEX per MWh and EBITDA margin per financial entity (EPR-189)."""
    cache_key = "perf-analytics:fin:v1"
    cached = await get_cached_report(cache_key)
    if cached is not None:
        return cached

    data = await FinancialComparisonService(db).get_financial()
    await cache_report(cache_key, data, ttl_seconds=CACHE_TTL_SECONDS)
    return data
