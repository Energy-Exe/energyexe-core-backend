"""API endpoints for opportunity detection and management."""

from datetime import datetime, timedelta, timezone
from typing import List, Optional

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.core.deps import get_current_active_user, get_db
from app.models.import_job_execution import ImportJobExecution, ImportJobStatus
from app.models.opportunity import Opportunity, OpportunityStatus, SchemaCode
from app.models.portfolio import PortfolioItem
from app.models.user import User
from app.models.windfarm import Windfarm
from app.schemas.opportunity import (
    DetectionTriggerResponse,
    OpportunityDetectRequest,
    OpportunityListResponse,
    OpportunityResponse,
    OpportunityStatusUpdate,
)
from app.services.opportunity_schemas.schema_names import get_schema_name
from app.services.portfolio_service import PortfolioService

logger = structlog.get_logger()
router = APIRouter()


def _parse_schema_codes(codes: Optional[List[str]]) -> Optional[List[SchemaCode]]:
    """Coerce request schema-code strings into ``SchemaCode`` enum members.

    Returns ``None`` (run-all) when the filter is null/empty. Raises HTTP 422
    on an unknown code so the caller gets a clear error instead of silently
    running everything.
    """
    if not codes:
        return None
    parsed: List[SchemaCode] = []
    for code in codes:
        try:
            parsed.append(SchemaCode(code))
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Unknown schema_code: {code}")
    return parsed


async def _run_detection_in_background(
    job_id: int,
    period_months: int,
    schema_codes: Optional[List[SchemaCode]],
) -> None:
    """Background entrypoint for a fleet-wide detection run (#114).

    Opens its OWN session (the request-scoped one is already closed by the time
    this runs) and drives the LIVE detection path via
    ``OpportunityDetectionService.run_detection_job`` over all operational
    windfarms. The ``import_job_executions`` row created synchronously by the
    endpoint (``job_id``) is the handle callers poll; this run reports into it.
    """
    from app.services.opportunity_detection_service import OpportunityDetectionService

    session_factory = get_session_factory()
    try:
        async with session_factory() as db:
            service = OpportunityDetectionService(db)
            # Reuse the polled row (job_id) instead of creating a duplicate, so the
            # caller's handle is the one driven RUNNING→SUCCESS/FAILED.
            await service.run_detection_job(
                windfarm_ids=None,
                period_months=period_months,
                schema_codes=schema_codes,
                job_id=job_id,
            )
    except Exception as exc:  # pragma: no cover - defensive background guard
        logger.error("opportunity_detection_background_failed", job_id=job_id, error=str(exc))
        # Backstop: ensure the polled row never hangs in RUNNING/PENDING if the
        # run blew up before it could mark itself FAILED.
        try:
            async with session_factory() as db:
                job = await db.get(ImportJobExecution, job_id)
                if job is not None and job.status not in (
                    ImportJobStatus.SUCCESS,
                    ImportJobStatus.FAILED,
                ):
                    job.mark_failed(str(exc))
                    await db.commit()
        except Exception as mark_exc:  # pragma: no cover - defensive
            logger.error(
                "opportunity_detection_background_mark_failed_error",
                job_id=job_id,
                error=str(mark_exc),
            )


def _to_response(opp: Opportunity, windfarm_name: Optional[str] = None) -> OpportunityResponse:
    """Convert Opportunity model to response schema.

    ``schema_name`` is resolved from ``SCHEMA_NAMES`` via ``get_schema_name``,
    which returns ``None`` for an unknown/legacy ``schema_code`` (no crash); the
    raw code remains available on ``schema_code`` for that fallback case.
    """
    return OpportunityResponse(
        id=opp.id,
        windfarm_id=opp.windfarm_id,
        windfarm_name=windfarm_name,
        schema_code=opp.schema_code,
        schema_name=get_schema_name(opp.schema_code),
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
    portfolio_id: Optional[int] = Query(
        None,
        description="Filter by portfolio ID (only opportunities for windfarms in this portfolio)",
    ),
    schema_code: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="Filter by status. Default: ACTIVE"),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """List opportunities with filters."""
    if portfolio_id is not None:
        portfolio = await PortfolioService(db).get_portfolio(portfolio_id, current_user.id)
        if not portfolio:
            raise HTTPException(status_code=404, detail=f"Portfolio {portfolio_id} not found")

    conditions = []
    if windfarm_id:
        conditions.append(Opportunity.windfarm_id == windfarm_id)
    if portfolio_id is not None:
        portfolio_windfarms = select(PortfolioItem.windfarm_id).where(
            PortfolioItem.portfolio_id == portfolio_id
        )
        conditions.append(Opportunity.windfarm_id.in_(portfolio_windfarms))
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
    query = select(Opportunity, Windfarm.name.label("windfarm_name")).join(
        Windfarm, Opportunity.windfarm_id == Windfarm.id
    )
    if conditions:
        query = query.where(and_(*conditions))
    query = query.order_by(Opportunity.created_at.desc()).offset(offset).limit(limit)

    result = await db.execute(query)
    rows = result.all()
    items = [_to_response(r.Opportunity, r.windfarm_name) for r in rows]

    # Summary counts by severity (across all matching, not just this page)
    summary_q = select(Opportunity.severity, func.count(Opportunity.id)).where(
        Opportunity.status != OpportunityStatus.SUPERSEDED
    )
    if windfarm_id:
        summary_q = summary_q.where(Opportunity.windfarm_id == windfarm_id)
    if portfolio_id is not None:
        summary_q = summary_q.where(
            Opportunity.windfarm_id.in_(
                select(PortfolioItem.windfarm_id).where(PortfolioItem.portfolio_id == portfolio_id)
            )
        )
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
    background_tasks: BackgroundTasks,
    request: OpportunityDetectRequest = OpportunityDetectRequest(),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Manually trigger opportunity detection.

    Two branches (#114):

    * **Scoped / SYNCHRONOUS** — when ``windfarm_ids`` is provided the run is
      fast and bounded, so detection executes inline and the per-run summary
      (``{job_id, windfarms_scanned, opportunities_created}``) is returned
      directly. Intended for single-asset debugging.
    * **Fleet-wide / BACKGROUND** — when ``windfarm_ids`` is empty/null running
      all 18 schemas over every operational windfarm can take minutes, so we
      DON'T hold the request (and a DB transaction) open. We create + commit an
      ``import_job_executions`` row up front, schedule the detection as a
      FastAPI ``BackgroundTasks`` job, and return a ``DetectionTriggerResponse``
      with the ``job_id`` immediately. Poll progress via that job row / GET
      ``/opportunities``.

    ``schema_codes`` (optional) restricts either branch to the listed schemas.
    """
    schema_codes = _parse_schema_codes(request.schema_codes)

    # ── Scoped synchronous path ──
    if request.windfarm_ids:
        from app.services.opportunity_detection_service import OpportunityDetectionService

        service = OpportunityDetectionService(db)
        return await service.run_detection_job(
            windfarm_ids=request.windfarm_ids,
            period_months=request.period_months,
            schema_codes=schema_codes,
        )

    # ── Fleet-wide background path ──
    # Create + commit the tracking row synchronously so the returned job_id is
    # real and immediately pollable; the background task REUSES this exact row
    # (passed as job_id) and drives it RUNNING→SUCCESS/FAILED, so the handle the
    # caller polls is the one that actually reports the run's outcome.
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    job = ImportJobExecution(
        job_name="opportunity-detection",
        source="SYSTEM",
        job_type="manual",
        import_start_date=now - timedelta(days=request.period_months * 30),
        import_end_date=now,
        status=ImportJobStatus.PENDING,
        created_by_id=current_user.id,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    background_tasks.add_task(
        _run_detection_in_background,
        job_id=job.id,
        period_months=request.period_months,
        schema_codes=schema_codes,
    )

    logger.info("opportunity_detection_scheduled", job_id=job.id, schema_codes=request.schema_codes)
    return DetectionTriggerResponse(
        job_id=job.id,
        status="scheduled",
        mode="background",
        message="Fleet-wide detection scheduled; poll job_id for results.",
    )
