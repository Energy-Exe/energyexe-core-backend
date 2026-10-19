"""Performance Analytics section of the client portal (EPR-186)."""

import calendar
from datetime import date
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.core.redis import cache_report, get_cached_report
from app.models.user import User
from app.services.comparison_analytics_service import ComparisonAnalyticsService
from app.services.generation_analytics_service import (
    MAX_DAY_WINDOW_DAYS,
    MAX_FARMS,
    GenerationAnalyticsService,
)
from app.services.performance_analytics_service import PerformanceAnalyticsService, parse_month

router = APIRouter()

# The inputs only change with the nightly pipeline / view refresh.
CACHE_TTL_SECONDS = 6 * 3600
MAX_WINDOW_MONTHS = 360


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
    try:
        start_m, end_m = parse_month(start), parse_month(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="start and end must be YYYY-MM")
    span = (end_m[0] - start_m[0]) * 12 + (end_m[1] - start_m[1]) + 1
    if span < 1 or span > MAX_WINDOW_MONTHS:
        raise HTTPException(
            status_code=400,
            detail=f"end must be on or after start, at most {MAX_WINDOW_MONTHS} months",
        )

    cache_key = f"perf-analytics:v2:{windfarm_id}:{start}:{end}:{benchmark or '-'}"
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
    if span_days < 1 or span_days > MAX_WINDOW_MONTHS * 31:
        raise HTTPException(status_code=400, detail="end must be on or after start")
    if granularity == "day" and span_days > MAX_DAY_WINDOW_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"The day view covers at most {MAX_DAY_WINDOW_DAYS} days",
        )

    cache_key = f"perf-analytics:gen:v1:{','.join(map(str, ids))}:{granularity}:{start_d}:{end_d}"
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
    try:
        start_m, end_m = parse_month(start), parse_month(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="start and end must be YYYY-MM")
    span = (end_m[0] - start_m[0]) * 12 + (end_m[1] - start_m[1]) + 1
    if span < 1 or span > MAX_WINDOW_MONTHS:
        raise HTTPException(
            status_code=400,
            detail=f"end must be on or after start, at most {MAX_WINDOW_MONTHS} months",
        )

    cache_key = f"perf-analytics:cmp:v1:{start}:{end}"
    cached = await get_cached_report(cache_key)
    if cached is not None:
        return cached

    last_day = calendar.monthrange(end_m[0], end_m[1])[1]
    data = await ComparisonAnalyticsService(db).get_comparison(
        date(start_m[0], start_m[1], 1), date(end_m[0], end_m[1], last_day)
    )
    await cache_report(cache_key, data, ttl_seconds=CACHE_TTL_SECONDS)
    return data
