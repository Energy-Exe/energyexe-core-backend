"""Structural-constraint flags read API (Module 1b).

Read-only listing for now. Review actions (confirm/dismiss/notes) come
in a follow-up milestone once the analyst-review UI lands.
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_db
from app.models.structural_constraint_flag import StructuralConstraintFlag
from app.models.windfarm import Windfarm

router = APIRouter()


class StructuralConstraintResponse(BaseModel):
    id: int
    windfarm_id: int
    period_start: datetime
    period_end: datetime
    duration_hours: int
    wind_bins_affected: Optional[int] = None
    mean_q90_ratio: Optional[float] = None
    mean_q50_ratio: Optional[float] = None
    flag_trigger: str
    flag_source: str
    review_status: str
    analyst_notes: Optional[str] = None
    reviewed_by: Optional[int] = None
    reviewed_at: Optional[datetime] = None
    pipeline_run_id: Optional[int] = None
    created_at: datetime

    class Config:
        from_attributes = True


@router.get("/", response_model=List[StructuralConstraintResponse])
async def list_structural_constraints(
    windfarm_id: Optional[int] = Query(None, description="Filter to one windfarm"),
    review_status: str = Query(
        "pending_review",
        description="Filter by review status; use '' (empty) to return all",
    ),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> List[StructuralConstraintResponse]:
    """List detected structural-constraint runs.

    Defaults to ``review_status='pending_review'`` — the analyst queue.
    Pass an empty string to list all statuses.
    """
    if windfarm_id is not None:
        # Validate windfarm exists for clearer 404 instead of empty list
        result = await db.execute(select(Windfarm.id).where(Windfarm.id == windfarm_id))
        if result.scalar_one_or_none() is None:
            raise HTTPException(status_code=404, detail="Windfarm not found")

    stmt = select(StructuralConstraintFlag)
    if windfarm_id is not None:
        stmt = stmt.where(StructuralConstraintFlag.windfarm_id == windfarm_id)
    if review_status:
        stmt = stmt.where(StructuralConstraintFlag.review_status == review_status)
    stmt = (
        stmt.order_by(
            StructuralConstraintFlag.windfarm_id,
            StructuralConstraintFlag.period_start,
        )
        .limit(limit)
        .offset(offset)
    )

    result = await db.execute(stmt)
    rows = result.scalars().all()
    return [StructuralConstraintResponse.model_validate(r) for r in rows]
