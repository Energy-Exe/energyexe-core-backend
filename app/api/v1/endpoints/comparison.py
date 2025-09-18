"""
API endpoints for windfarm comparison and analytics.
"""

from typing import List, Optional
from datetime import date, datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.services.comparison_service import ComparisonService

router = APIRouter()


@router.get("/windfarms")
async def get_available_windfarms(
    db: AsyncSession = Depends(get_db),
):
    """Get list of windfarms available for comparison."""
    service = ComparisonService(db)
    return await service.get_available_windfarms()


@router.get("/compare")
async def compare_windfarms(
    windfarm_ids: List[int] = Query(..., description="List of windfarm IDs to compare"),
    start_date: date = Query(..., description="Start date for comparison"),
    end_date: date = Query(..., description="End date for comparison"),
    granularity: str = Query("daily", description="Aggregation granularity: hourly, daily, weekly, monthly"),
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
        granularity=granularity
    )


@router.get("/statistics")
async def get_windfarm_statistics(
    windfarm_ids: List[int] = Query(..., description="List of windfarm IDs"),
    period_days: int = Query(30, description="Number of days for statistics"),
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
        period_days=period_days
    )