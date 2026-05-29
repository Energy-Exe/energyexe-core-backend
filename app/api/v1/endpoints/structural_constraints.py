"""Structural-constraint flags API (Module 1b).

Listing + analyst review actions. Auto-detected runs land as
``pending_review``; an analyst confirms (it was a real structural constraint
— masked from Modules 3/4/5) or dismisses (false positive). Only ``confirmed``
flags affect published analytics.
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, get_db
from app.models.structural_constraint_flag import StructuralConstraintFlag
from app.models.user import User
from app.models.windfarm import Windfarm
from app.services.structural_constraint_detection_service import (
    ALLOWED_REVIEW_STATUSES,
    StructuralConstraintDetectionService,
)

router = APIRouter()


class StructuralConstraintResponse(BaseModel):
    id: int
    windfarm_id: int
    windfarm_name: Optional[str] = None
    windfarm_code: Optional[str] = None
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


class ReviewUpdateRequest(BaseModel):
    """Analyst confirm/dismiss/re-open action."""

    review_status: str
    analyst_notes: Optional[str] = None


class ReviewSummaryResponse(BaseModel):
    pending_review: int = 0
    confirmed: int = 0
    dismissed: int = 0
    windfarms_with_pending: int = 0


def _to_response(
    flag: StructuralConstraintFlag,
    windfarm_name: Optional[str] = None,
    windfarm_code: Optional[str] = None,
) -> StructuralConstraintResponse:
    resp = StructuralConstraintResponse.model_validate(flag)
    resp.windfarm_name = windfarm_name
    resp.windfarm_code = windfarm_code
    return resp


@router.get("/summary", response_model=ReviewSummaryResponse)
async def review_summary(
    db: AsyncSession = Depends(get_db),
) -> ReviewSummaryResponse:
    """Counts per review_status + how many windfarms still have pending flags."""
    rows = (
        await db.execute(
            select(
                StructuralConstraintFlag.review_status,
                func.count().label("n"),
            ).group_by(StructuralConstraintFlag.review_status)
        )
    ).all()
    counts = {status: n for status, n in rows}

    pending_wfs = (
        await db.execute(
            select(func.count(func.distinct(StructuralConstraintFlag.windfarm_id))).where(
                StructuralConstraintFlag.review_status == "pending_review"
            )
        )
    ).scalar() or 0

    return ReviewSummaryResponse(
        pending_review=counts.get("pending_review", 0),
        confirmed=counts.get("confirmed", 0),
        dismissed=counts.get("dismissed", 0),
        windfarms_with_pending=int(pending_wfs),
    )


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
    """List detected structural-constraint runs (with windfarm name/code).

    Defaults to ``review_status='pending_review'`` — the analyst queue. Pass an
    empty string to list all statuses.
    """
    if windfarm_id is not None:
        result = await db.execute(select(Windfarm.id).where(Windfarm.id == windfarm_id))
        if result.scalar_one_or_none() is None:
            raise HTTPException(status_code=404, detail="Windfarm not found")

    stmt = select(StructuralConstraintFlag, Windfarm.name, Windfarm.code).join(
        Windfarm, StructuralConstraintFlag.windfarm_id == Windfarm.id
    )
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

    rows = (await db.execute(stmt)).all()
    return [_to_response(flag, name, code) for flag, name, code in rows]


@router.patch("/{flag_id}", response_model=StructuralConstraintResponse)
async def review_structural_constraint(
    flag_id: int,
    body: ReviewUpdateRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> StructuralConstraintResponse:
    """Confirm / dismiss / re-open a flag. Stamps reviewer + timestamp.

    Only ``confirmed`` flags mask hours from Modules 3/4/5 on the next pipeline
    run, so confirming/dismissing here changes which periods affect published
    analytics.
    """
    if body.review_status not in ALLOWED_REVIEW_STATUSES:
        raise HTTPException(
            status_code=422,
            detail=f"review_status must be one of {list(ALLOWED_REVIEW_STATUSES)}",
        )

    service = StructuralConstraintDetectionService(db)
    flag = await service.set_review_status(
        flag_id,
        review_status=body.review_status,
        analyst_notes=body.analyst_notes,
        reviewed_by=current_user.id,
    )
    if flag is None:
        raise HTTPException(status_code=404, detail="Constraint flag not found")

    wf = (
        await db.execute(
            select(Windfarm.name, Windfarm.code).where(Windfarm.id == flag.windfarm_id)
        )
    ).first()
    name, code = (wf[0], wf[1]) if wf else (None, None)
    return _to_response(flag, name, code)
