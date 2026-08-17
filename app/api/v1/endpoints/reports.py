"""Reports platform API (EPR-81/82/83) — shared by the client and admin portals.

Generation is async: POST returns 202 immediately and the frontend polls
``GET /reports/{id}/status`` (lightweight) or ``GET /reports/{id}`` (full).
Collection routes need the trailing slash (``redirect_slashes=False``).
"""

import asyncio
from typing import List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.responses import Response as RawResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.report import Report, ReportStatus, SectionStatus
from app.models.user import User
from app.schemas.report import (
    ReportCreate,
    ReportListResponse,
    ReportResponse,
    ReportSectionResponse,
    ReportStatusResponse,
    ReportSummary,
    ReportTypeMeta,
    SectionSpecMeta,
    SectionStatusItem,
)
from app.services import s3_service
from app.services.reports import orchestrator
from app.services.reports.registry import REPORT_TYPE_REGISTRY, get_report_type
from app.services.reports.service import ReportInFlightError, ReportService

logger = structlog.get_logger()
router = APIRouter()

# Retain-on-export: once a version's PDF was downloaded (or the version was
# locked), its content is immutable — regeneration must go through a new version.
_FROZEN_MESSAGE = (
    "This version has been exported and is frozen — use Re-run to create a new version"
)


# ── serialization helpers ───────────────────────────────────────────────


def _pdf_available(report: Report) -> bool:
    # SUPERSEDED rows only survive retain-on-export pruning when they were
    # exported — retained history is always downloadable.
    return report.pdf_s3_key is not None or report.status in (
        ReportStatus.COMPLETE,
        ReportStatus.PARTIAL,
        ReportStatus.SUPERSEDED,
    )


def _to_summary(service: ReportService, report: Report) -> ReportSummary:
    return ReportSummary(
        id=report.id,
        report_type=report.report_type,
        scope_type=report.scope_type,
        windfarm_id=report.windfarm_id,
        portfolio_id=report.portfolio_id,
        scope_meta=service.scope_meta(report),
        period_start=report.period_start,
        period_end=report.period_end,
        version=report.version,
        status=report.status,
        title=report.title,
        pdf_available=_pdf_available(report),
        locked=report.locked,
        frozen=report.is_frozen,
        pdf_downloaded_at=report.pdf_downloaded_at,
        requested_by_id=report.requested_by_id,
        created_at=report.created_at,
    )


def _to_response(service: ReportService, report: Report) -> ReportResponse:
    spec = get_report_type(report.report_type)
    sections = []
    for row in sorted(report.sections, key=lambda s: s.display_order):
        section_spec = spec.section(row.section_key) if spec else None
        sections.append(
            ReportSectionResponse(
                section_key=row.section_key,
                title=section_spec.title if section_spec else row.section_key,
                kind=section_spec.kind if section_spec else "narrative",
                ai_enabled=section_spec.ai_enabled if section_spec else False,
                pass_number=row.pass_number,
                display_order=row.display_order,
                layout=row.layout,
                status=row.status,
                data=row.data,
                narrative_json=row.narrative_json,
                narrative_text=row.narrative_text,
                error=row.error,
                generated_at=row.generated_at,
                cost_usd=float(row.cost_usd) if row.cost_usd is not None else None,
            )
        )
    requested_by_name = None
    if "requested_by" not in report.__dict__:
        pass  # relationship not loaded (list path) — leave name empty
    elif report.requested_by is not None:
        requested_by_name = report.requested_by.username
    return ReportResponse(
        id=report.id,
        report_type=report.report_type,
        scope_type=report.scope_type,
        windfarm_id=report.windfarm_id,
        portfolio_id=report.portfolio_id,
        scope_meta=service.scope_meta(report),
        period_start=report.period_start,
        period_end=report.period_end,
        version=report.version,
        status=report.status,
        title=report.title,
        pdf_available=_pdf_available(report),
        locked=report.locked,
        frozen=report.is_frozen,
        pdf_downloaded_at=report.pdf_downloaded_at,
        requested_by_id=report.requested_by_id,
        requested_by_name=requested_by_name,
        total_cost_usd=float(report.total_cost_usd or 0),
        generation_started_at=report.generation_started_at,
        generation_completed_at=report.generation_completed_at,
        error=report.error,
        created_at=report.created_at,
        sections=sections,
    )


# ── endpoints ───────────────────────────────────────────────────────────


@router.get("/types", response_model=List[ReportTypeMeta])
async def list_report_types(current_user: User = Depends(get_current_active_user)):
    """Registry metadata — lets the frontends render section shells generically."""
    return [
        ReportTypeMeta(
            code=spec.code,
            title=spec.title,
            scope=spec.scope,
            sections=[
                SectionSpecMeta(
                    key=s.key,
                    title=s.title,
                    kind=s.kind,
                    pass_number=s.pass_number,
                    layout=s.layout,
                    ai_enabled=s.ai_enabled,
                )
                for s in spec.display_ordered()
            ],
        )
        for spec in REPORT_TYPE_REGISTRY.values()
    ]


@router.post("/", response_model=ReportResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_report(
    payload: ReportCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a report and start generating it in the background."""
    service = ReportService(db)
    try:
        report = await service.create_report(payload, current_user)
    except ReportInFlightError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={"message": str(exc.message), "report_id": exc.existing_id},
        )
    orchestrator.start(report.id)
    report = await service.get_report(report.id, current_user)
    return _to_response(service, report)


@router.get("/", response_model=ReportListResponse)
async def list_reports(
    report_type: Optional[str] = None,
    windfarm_id: Optional[int] = None,
    portfolio_id: Optional[int] = None,
    status_filter: Optional[str] = None,
    include_superseded: bool = False,
    skip: int = 0,
    limit: int = 50,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """The reports library — everything generated anywhere on the platform."""
    service = ReportService(db)
    reports, total = await service.list_reports(
        current_user,
        report_type=report_type,
        windfarm_id=windfarm_id,
        portfolio_id=portfolio_id,
        status=status_filter,
        include_superseded=include_superseded,
        skip=skip,
        limit=min(limit, 200),
    )
    return ReportListResponse(items=[_to_summary(service, r) for r in reports], total=total)


@router.get("/{report_id}", response_model=ReportResponse)
async def get_report(
    report_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    service = ReportService(db)
    report = await service.get_report(report_id, current_user)
    return _to_response(service, report)


@router.get("/{report_id}/status", response_model=ReportStatusResponse)
async def get_report_status(
    report_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Lightweight poll target while a report generates."""
    service = ReportService(db)
    report = await service.get_report(report_id, current_user)
    spec = get_report_type(report.report_type)
    runnable_keys = {
        s.key
        for s in (spec.sections if spec else ())
        if s.data_builder is not None or s.narrative is not None
    }
    runnable = [s for s in report.sections if s.section_key in runnable_keys]
    done = [
        s
        for s in runnable
        if s.status in (SectionStatus.GENERATED, SectionStatus.FAILED, SectionStatus.SKIPPED)
    ]
    progress = int(100 * len(done) / len(runnable)) if runnable else 100
    return ReportStatusResponse(
        id=report.id,
        status=report.status,
        progress_pct=progress if report.status == ReportStatus.GENERATING else 100,
        sections=[
            SectionStatusItem(key=s.section_key, status=s.status, error=s.error)
            for s in sorted(report.sections, key=lambda s: s.display_order)
        ],
        total_cost_usd=float(report.total_cost_usd or 0),
    )


@router.get("/{report_id}/versions", response_model=List[ReportSummary])
async def get_report_versions(
    report_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """The retained version chain, newest first (retain-on-export): the live
    version plus every superseded version whose PDF left the platform."""
    service = ReportService(db)
    chain = await service.version_history(report_id, current_user)
    return [_to_summary(service, r) for r in chain]


@router.post("/{report_id}/generate", response_model=ReportResponse, status_code=202)
async def generate_all_sections(
    report_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Re-run the full pipeline: data sections, AI narratives, exec summary."""
    service = ReportService(db)
    report = await service.get_report(report_id, current_user)
    if report.status in (ReportStatus.PENDING, ReportStatus.GENERATING):
        raise HTTPException(status.HTTP_409_CONFLICT, "Report is already generating")
    if report.is_frozen:
        raise HTTPException(status.HTTP_409_CONFLICT, _FROZEN_MESSAGE)
    report.status = ReportStatus.GENERATING
    await db.commit()
    orchestrator.start(report.id)
    report = await service.get_report(report_id, current_user)
    return _to_response(service, report)


@router.post("/{report_id}/sections/{section_key}/generate", status_code=202)
async def generate_section(
    report_id: int,
    section_key: str,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """(Re)generate a single section."""
    service = ReportService(db)
    report = await service.get_report(report_id, current_user)
    spec = get_report_type(report.report_type)
    section_spec = spec.section(section_key) if spec else None
    if section_spec is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Unknown section")
    row = next((s for s in report.sections if s.section_key == section_key), None)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Section not part of this report")
    if row.status == SectionStatus.GENERATING:
        raise HTTPException(status.HTTP_409_CONFLICT, "Section is already generating")
    if report.is_frozen:
        raise HTTPException(status.HTTP_409_CONFLICT, _FROZEN_MESSAGE)
    if section_spec.pass_number == 2:
        # EPR-84: the executive summary unlocks only once Pass 1 is complete.
        missing = [
            s.section_key
            for s in report.sections
            if s.pass_number == 1 and s.status not in (SectionStatus.GENERATED, SectionStatus.STALE)
        ]
        if missing:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"Executive summary unlocks when all Pass-1 sections are generated "
                f"(missing: {', '.join(missing)})",
            )

    async def _run_and_finalize():
        if section_spec.data_builder is not None:
            await orchestrator.run_section(report_id, section_spec)
        else:
            await orchestrator.run_narrative_section(report_id, section_spec)
        if section_spec.pass_number == 1:
            # The summary must not silently outlive the sections it cites.
            await orchestrator.mark_pass2_stale(report_id)
        await orchestrator._finalize(report_id)

    asyncio.create_task(_run_and_finalize())
    return {"report_id": report_id, "section_key": section_key, "status": "GENERATING"}


@router.post("/{report_id}/regenerate", response_model=ReportResponse, status_code=202)
async def regenerate_report(
    report_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """New immutable version; the unexported predecessor is pruned (retain-on-export)."""
    service = ReportService(db)
    try:
        new = await service.regenerate(report_id, current_user)
    except ReportInFlightError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={"message": str(exc.message), "report_id": exc.existing_id},
        )
    orchestrator.start(new.id)
    new = await service.get_report(new.id, current_user)
    return _to_response(service, new)


@router.get("/{report_id}/pdf")
async def download_report_pdf(
    report_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Stream the stored PDF (renders on demand if no artifact exists yet).

    A live version whose sections changed since the artifact was stored is
    re-rendered before its bytes leave the platform; a frozen version always
    serves the exact bytes that were exported.
    """
    service = ReportService(db)
    report = await service.get_report(report_id, current_user)

    if not report.is_frozen and orchestrator.pdf_is_stale(report) and report.pdf_s3_key:
        await orchestrator._render_and_store_pdf(db, report)

    content: Optional[bytes] = None
    if report.pdf_s3_key:
        content = await s3_service.download_file(report.pdf_s3_key)
    if content is None:
        # SUPERSEDED versions reaching this endpoint are retained (exported)
        # history — they must stay downloadable even without a stored artifact.
        if report.status not in (
            ReportStatus.COMPLETE,
            ReportStatus.PARTIAL,
            ReportStatus.SUPERSEDED,
        ):
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Report has no content to render yet")
        content = await orchestrator.render_pdf_on_demand(report)
    if content is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "PDF unavailable")

    await service.mark_pdf_downloaded(report)
    scope = report.windfarm.name if report.windfarm is not None else report.title
    filename = f"{scope.replace(' ', '_')}_{report.report_type}_v{report.version}.pdf"
    return RawResponse(
        content=content,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.delete("/{report_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_report(
    report_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Soft-delete the whole version chain (library Delete, confirmed client-side)."""
    service = ReportService(db)
    await service.soft_delete(report_id, current_user)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
