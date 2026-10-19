"""Performance Analytics section of the client portal (EPR-186)."""

from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.core.redis import cache_report, get_cached_report
from app.models.user import User
from app.services.performance_analytics_service import (
    PerformanceAnalyticsService,
    parse_month,
)

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

    cache_key = f"perf-analytics:v1:{windfarm_id}:{start}:{end}:{benchmark or '-'}"
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
