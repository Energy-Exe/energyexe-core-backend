"""
API endpoints for windfarm comparison and analytics.
"""

from typing import List, Optional
from datetime import date, datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import exclude_deleted, get_current_user_optional
from app.models.user import User
from app.services.comparison_service import ComparisonService

router = APIRouter()


@router.get("/windfarms")
async def get_available_windfarms(
    include_deleted: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    """Get list of windfarms available for comparison.

    Soft-deleted windfarms are excluded unless an admin requests
    include_deleted=true (admin panel only).
    """
    service = ComparisonService(db)
    return await service.get_available_windfarms(
        visible_only=exclude_deleted(current_user, include_deleted)
    )


@router.get("/compare")
async def compare_windfarms(
    windfarm_ids: List[int] = Query(..., description="List of windfarm IDs to compare"),
    start_date: date = Query(..., description="Start date for comparison"),
    end_date: date = Query(..., description="End date for comparison"),
    granularity: str = Query("daily", description="Aggregation granularity: hourly, daily, weekly, monthly"),
    exclude_ramp_up: bool = Query(True, description="Exclude ramp-up period records from results"),
    db: AsyncSession = Depends(get_db),
):
    """Compare generation data across multiple windfarms."""

    if not windfarm_ids:
        return {
            "error": "At least one windfarm ID is required"
        }

    service = ComparisonService(db)
    return await service.get_windfarm_comparison(
        windfarm_ids=windfarm_ids,
        start_date=start_date,
        end_date=end_date,
        granularity=granularity,
        exclude_ramp_up=exclude_ramp_up
    )


@router.get("/statistics")
async def get_windfarm_statistics(
    windfarm_ids: List[int] = Query(..., description="List of windfarm IDs"),
    period_days: int = Query(30, description="Number of days for statistics (fallback when no explicit date range is given)"),
    start_date: Optional[date] = Query(None, description="Explicit window start (overrides period_days when paired with end_date)"),
    end_date: Optional[date] = Query(None, description="Explicit window end (overrides period_days when paired with start_date)"),
    exclude_ramp_up: bool = Query(True, description="Exclude ramp-up period records from results"),
    db: AsyncSession = Depends(get_db),
):
    """Get detailed statistics for selected windfarms."""

    if not windfarm_ids:
        return {
            "error": "At least one windfarm ID is required"
        }

    service = ComparisonService(db)
    return await service.get_windfarm_statistics(
        windfarm_ids=windfarm_ids,
        period_days=period_days,
        start_date=start_date,
        end_date=end_date,
        exclude_ramp_up=exclude_ramp_up
    )