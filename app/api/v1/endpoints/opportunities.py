"""API endpoints for opportunity detection and management."""

from datetime import datetime, timezone
from typing import List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.opportunity import Opportunity, OpportunityStatus
from app.models.user import User
from app.models.windfarm import Windfarm
from app.schemas.opportunity import (
    OpportunityDetectRequest,
    OpportunityListResponse,
    OpportunityResponse,
    OpportunityStatusUpdate,
)

logger = structlog.get_logger()
router = APIRouter()


def _to_response(opp: Opportunity, windfarm_name: Optional[str] = None) -> OpportunityResponse:
    """Convert Opportunity model to response schema."""
    return OpportunityResponse(
        id=opp.id,
        windfarm_id=opp.windfarm_id,
        windfarm_name=windfarm_name,
        schema_code=opp.schema_code,
        severity=opp.severity,
        branch=opp.branch,
        status=opp.status,
        data_slots=opp.data_slots or {},
        missing_slots=opp.missing_slots or [],
        triggered_by_id=opp.triggered_by_id,
        detection_period_start=opp.detection_period_start,
        detection_period_end=opp.detection_period_end,
        detection_run_id=opp.detection_run_id,
        suppression_reason=opp.suppression_reason,
        created_at=opp.created_at,
        updated_at=opp.updated_at,
        acknowledged_at=opp.acknowledged_at,
        resolved_at=opp.resolved_at,
    )


@router.get("/", response_model=OpportunityListResponse)
async def list_opportunities(
    windfarm_id: Optional[int] = Query(None),
    schema_code: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="Filter by status. Default: ACTIVE"),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """List opportunities with filters."""
    conditions = []
    if windfarm_id:
        conditions.append(Opportunity.windfarm_id == windfarm_id)
    if schema_code:
        conditions.append(Opportunity.schema_code == schema_code)
    if severity:
        conditions.append(Opportunity.severity == severity)
    if status:
        conditions.append(Opportunity.status == status)
    else:
        # Default: exclude SUPERSEDED
        conditions.append(Opportunity.status != OpportunityStatus.SUPERSEDED)

    # Get total count
    count_q = select(func.count(Opportunity.id))
    if conditions:
        count_q = count_q.where(and_(*conditions))
    total = (await db.execute(count_q)).scalar() or 0

    # Get items with windfarm name
    query = (
        select(Opportunity, Windfarm.name.label("windfarm_name"))
        .join(Windfarm, Opportunity.windfarm_id == Windfarm.id)
    )
    if conditions:
        query = query.where(and_(*conditions))
    query = query.order_by(Opportunity.created_at.desc()).offset(offset).limit(limit)

    result = await db.execute(query)
    rows = result.all()
    items = [_to_response(r.Opportunity, r.windfarm_name) for r in rows]

    # Summary counts by severity (across all matching, not just this page)
    summary_q = (
        select(Opportunity.severity, func.count(Opportunity.id))
        .where(Opportunity.status != OpportunityStatus.SUPERSEDED)
    )
    if windfarm_id:
        summary_q = summary_q.where(Opportunity.windfarm_id == windfarm_id)
    summary_q = summary_q.group_by(Opportunity.severity)
    summary_result = await db.execute(summary_q)
    summary = {r[0]: r[1] for r in summary_result.fetchall()}

    return OpportunityListResponse(items=items, total=total, summary=summary)


@router.get("/{opportunity_id}", response_model=OpportunityResponse)
async def get_opportunity(
    opportunity_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get opportunity detail."""
    result = await db.execute(
        select(Opportunity, Windfarm.name.label("windfarm_name"))
        .join(Windfarm, Opportunity.windfarm_id == Windfarm.id)
        .where(Opportunity.id == opportunity_id)
    )
    row = result.first()
    if not row:
        raise HTTPException(status_code=404, detail="Opportunity not found")
    return _to_response(row.Opportunity, row.windfarm_name)


@router.patch("/{opportunity_id}", response_model=OpportunityResponse)
async def update_opportunity_status(
    opportunity_id: int,
    request: OpportunityStatusUpdate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Acknowledge or resolve an opportunity."""
    result = await db.execute(select(Opportunity).where(Opportunity.id == opportunity_id))
    opp = result.scalar_one_or_none()
    if not opp:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if request.status == OpportunityStatus.ACKNOWLEDGED:
        opp.status = OpportunityStatus.ACKNOWLEDGED
        opp.acknowledged_at = now
    elif request.status == OpportunityStatus.RESOLVED:
        opp.status = OpportunityStatus.RESOLVED
        opp.resolved_at = now
    else:
        raise HTTPException(status_code=400, detail="Status must be ACKNOWLEDGED or RESOLVED")

    opp.updated_at = now
    await db.commit()
    await db.refresh(opp)

    # Get windfarm name for response
    wf = await db.execute(select(Windfarm.name).where(Windfarm.id == opp.windfarm_id))
    wf_name = wf.scalar_one_or_none()
    return _to_response(opp, wf_name)


@router.post("/detect")
async def trigger_detection(
    request: OpportunityDetectRequest = OpportunityDetectRequest(),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Manually trigger opportunity detection."""
    from app.services.opportunity_detection_service import OpportunityDetectionService

    service = OpportunityDetectionService(db)
    result = await service.run_detection_job(
        windfarm_ids=request.windfarm_ids,
        period_months=request.period_months,
    )
    return result
