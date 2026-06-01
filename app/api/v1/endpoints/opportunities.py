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
    TriggeredBySummary,
)
from app.services.opportunity_schemas.schema_names import get_schema_name, get_schema_one_liner
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


def _to_response(
    opp: Opportunity,
    windfarm_name: Optional[str] = None,
    parent: Optional[Opportunity] = None,
) -> OpportunityResponse:
    """Convert Opportunity model to response schema.

    ``schema_name`` / ``schema_one_liner`` are resolved from the SCHEMA_NAMES /
    SCHEMA_ONE_LINERS registries (both return ``None`` for an unknown/legacy
    ``schema_code`` — no crash; the raw code remains on ``schema_code``).

    ``parent`` is the resolved parent Opportunity for a dependent finding (i.e.
    the row whose id equals ``opp.triggered_by_id``); when supplied it is
    summarised into ``triggered_by`` so the UI can render the parent's name and
    severity instead of a bare id.
    """
    triggered_by = None
    if parent is not None:
        triggered_by = TriggeredBySummary(
            id=parent.id,
            schema_code=parent.schema_code,
            schema_name=get_schema_name(parent.schema_code),
            severity=parent.severity,
            status=parent.status,
        )
    return OpportunityResponse(
        id=opp.id,
        windfarm_id=opp.windfarm_id,
        windfarm_name=windfarm_name,
        schema_code=opp.schema_code,
        schema_name=get_schema_name(opp.schema_code),
        schema_one_liner=get_schema_one_liner(opp.schema_code),
        severity=opp.severity,
        branch=opp.branch,
        status=opp.status,
        data_slots=opp.data_slots or {},
        missing_slots=opp.missing_slots or [],
        triggered_by_id=opp.triggered_by_id,
        triggered_by=triggered_by,
        detection_period_start=opp.detection_period_start,
        detection_period_end=opp.detection_period_end,
        detection_run_id=opp.detection_run_id,
        suppression_reason=opp.suppression_reason,
        created_at=opp.created_at,
        updated_at=opp.updated_at,
        acknowledged_at=opp.acknowledged_at,
        resolved_at=opp.resolved_at,
    )


async def _fetch_parents(
    db: AsyncSession, opps: List[Opportunity]
) -> dict[int, Opportunity]:
    """Batch-resolve parent opportunities for a set of (possibly dependent) rows.

    Collects every non-null ``triggered_by_id`` and fetches the parents in ONE
    query (avoids N+1), returning an ``{id: Opportunity}`` map for ``_to_response``.
    """
    parent_ids = {o.triggered_by_id for o in opps if o.triggered_by_id is not None}
    if not parent_ids:
        return {}
    result = await db.execute(select(Opportunity).where(Opportunity.id.in_(parent_ids)))
    return {p.id: p for p in result.scalars().all()}


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
    parents = await _fetch_parents(db, [r.Opportunity for r in rows])
    items = [
        _to_response(
            r.Opportunity,
            r.windfarm_name,
            parents.get(r.Opportunity.triggered_by_id),
        )
        for r in rows
    ]

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
    parent = None
    if row.Opportunity.triggered_by_id is not None:
        parent = await db.get(Opportunity, row.Opportunity.triggered_by_id)
    return _to_response(row.Opportunity, row.windfarm_name, parent)


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
    parent = None
    if opp.triggered_by_id is not None:
        parent = await db.get(Opportunity, opp.triggered_by_id)
    return _to_response(opp, wf_name, parent)


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
