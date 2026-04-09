"""API endpoints for P50 target management and analysis."""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.p50_target import (
    P50AnalysisResult,
    P50TargetCreate,
    P50TargetResponse,
    P50TargetUpdate,
)
from app.services.p50_target_service import P50TargetService

router = APIRouter()


@router.get("/windfarms/{windfarm_id}/p50-targets", response_model=List[P50TargetResponse])
async def get_p50_targets(
    windfarm_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get all P50 targets for a windfarm, ordered by start date."""
    service = P50TargetService(db)
    targets = await service.get_targets(windfarm_id)
    return [service._to_target_response(t) for t in targets]


@router.get("/windfarms/{windfarm_id}/p50-targets/active", response_model=P50TargetResponse)
async def get_active_p50_target(
    windfarm_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get the currently active P50 target for a windfarm."""
    service = P50TargetService(db)
    target = await service.get_active_target(windfarm_id)
    if target is None:
        raise HTTPException(status_code=404, detail="No active P50 target found for this windfarm")
    return service._to_target_response(target)


@router.post("/windfarms/{windfarm_id}/p50-targets", response_model=P50TargetResponse, status_code=201)
async def create_p50_target(
    windfarm_id: int,
    data: P50TargetCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new P50 target for a windfarm.

    If p50_target_start_date is not provided, it defaults to the windfarm's
    commercial operational date month + 2 months, rounded up to the 1st.
    """
    service = P50TargetService(db)
    try:
        target = await service.create_target(windfarm_id, data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return service._to_target_response(target)


@router.put("/windfarms/{windfarm_id}/p50-targets/{target_id}", response_model=P50TargetResponse)
async def update_p50_target(
    windfarm_id: int,
    target_id: int,
    data: P50TargetUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a P50 target."""
    service = P50TargetService(db)
    try:
        target = await service.update_target(target_id, data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if target is None:
        raise HTTPException(status_code=404, detail="P50 target not found")
    return service._to_target_response(target)


@router.delete("/windfarms/{windfarm_id}/p50-targets/{target_id}")
async def delete_p50_target(
    windfarm_id: int,
    target_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Delete a P50 target."""
    service = P50TargetService(db)
    deleted = await service.delete_target(target_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="P50 target not found")
    return {"ok": True}


@router.get("/windfarms/{windfarm_id}/p50-analysis", response_model=P50AnalysisResult)
async def get_p50_analysis(
    windfarm_id: int,
    target_id: Optional[int] = Query(None, description="Specific target ID, defaults to active target"),
    db: AsyncSession = Depends(get_db),
):
    """Get full P50 analysis: cumulative actual vs target comparison with gap metrics."""
    service = P50TargetService(db)
    result = await service.get_p50_analysis(windfarm_id, target_id=target_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="No P50 target found for this windfarm. Create one first.",
        )
    return result
