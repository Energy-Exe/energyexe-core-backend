"""Two-pass report generation orchestrator.

Runs as a fire-and-forget asyncio task (the ``weather_imports`` pattern):
``POST /reports`` inserts rows and calls :func:`start`; the frontend polls
``GET /reports/{id}/status``. Every concurrently-running section opens its own
session — an AsyncSession must never be shared across tasks.

Phase 1 scope: deterministic data sections only. Sections that are
narrative-only (AI) stay UNGENERATED until the Phase-2 narrative service
lands; the report still completes.
"""

import asyncio
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import structlog
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from app.core.database import get_session_factory
from app.models.report import Report, ReportSection, ReportStatus, SectionStatus
from app.models.windfarm import Windfarm
from app.services import s3_service
from app.services.reports.context import ReportContext
from app.services.reports.registry import SectionSpec, get_report_type

logger = structlog.get_logger()

# Bound concurrent section builders — protects the 2vCPU Fargate task and,
# from Phase 2 on, caps concurrent LLM calls.
_SECTION_CONCURRENCY = 3

# Reports stuck in-flight longer than this are orphans of a task restart.
_STUCK_AFTER = timedelta(minutes=20)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def start(report_id: int) -> None:
    """Fire-and-forget entry point called by the API layer."""
    asyncio.create_task(run(report_id))


async def run(report_id: int) -> None:
    """Execute one full generation run. All errors are contained here."""
    factory = get_session_factory()
    try:
        async with factory() as db:
            report = await db.get(Report, report_id)
            if report is None:
                logger.error("report_run_missing", report_id=report_id)
                return
            spec = get_report_type(report.report_type)
            if spec is None:
                report.status = ReportStatus.FAILED
                report.error = f"Unknown report type {report.report_type}"
                await db.commit()
                return
            report.status = ReportStatus.GENERATING
            report.generation_started_at = _utcnow()
            await db.commit()

        # Pass 1 — deterministic data sections, concurrently, each in its own session.
        pass1 = [s for s in spec.sections if s.pass_number == 1 and s.data_builder is not None]
        semaphore = asyncio.Semaphore(_SECTION_CONCURRENCY)

        async def run_bounded(section_spec: SectionSpec) -> None:
            async with semaphore:
                await run_section(report_id, section_spec)

        await asyncio.gather(*(run_bounded(s) for s in pass1), return_exceptions=True)

        # Pass 2 (executive summary) is narrative-only — Phase 2 wires the
        # narrative service here, after the Pass-1 gather.

        await _finalize(report_id)
    except Exception as exc:
        logger.error("report_run_failed", report_id=report_id, error=str(exc), exc_info=True)
        async with factory() as db:
            report = await db.get(Report, report_id)
            if report is not None:
                report.status = ReportStatus.FAILED
                report.error = str(exc)
                report.generation_completed_at = _utcnow()
                await db.commit()


async def run_section(report_id: int, section_spec: SectionSpec) -> None:
    """Build one section's data slice; own session, own transaction, contained errors."""
    factory = get_session_factory()
    async with factory() as db:
        result = await db.execute(
            select(ReportSection).where(
                ReportSection.report_id == report_id,
                ReportSection.section_key == section_spec.key,
            )
        )
        section = result.scalar_one_or_none()
        if section is None:  # not part of this run (sections whitelist)
            return

        section.status = SectionStatus.GENERATING
        section.error = None
        await db.commit()

        started = _utcnow()
        try:
            ctx = await _build_context(db, report_id)
            data = await section_spec.data_builder(ctx)
            section.data = data
            section.status = SectionStatus.GENERATED
            section.generated_at = _utcnow()
            section.duration_ms = int((_utcnow() - started).total_seconds() * 1000)
        except Exception as exc:
            logger.error(
                "report_section_failed",
                report_id=report_id,
                section=section_spec.key,
                error=str(exc),
                exc_info=True,
            )
            section.status = SectionStatus.FAILED
            section.error = str(exc)[:2000]
        await db.commit()


async def _build_context(db, report_id: int) -> ReportContext:
    result = await db.execute(
        select(Report)
        .options(
            selectinload(Report.windfarm).selectinload(Windfarm.bidzone),
            selectinload(Report.windfarm).selectinload(Windfarm.country),
            selectinload(Report.portfolio),
        )
        .where(Report.id == report_id)
    )
    report = result.scalar_one()
    return ReportContext(
        db=db,
        report_id=report.id,
        scope_type=report.scope_type,
        period_start=report.period_start,
        period_end=report.period_end,
        windfarm=report.windfarm,
        portfolio=report.portfolio,
        params=report.params or {},
    )


async def _finalize(report_id: int) -> None:
    """Set the terminal report status and render/upload the PDF artifact."""
    factory = get_session_factory()
    async with factory() as db:
        result = await db.execute(
            select(Report)
            .options(
                selectinload(Report.sections),
                selectinload(Report.windfarm).selectinload(Windfarm.bidzone),
                selectinload(Report.windfarm).selectinload(Windfarm.country),
                selectinload(Report.portfolio),
            )
            .where(Report.id == report_id)
        )
        report = result.scalar_one_or_none()
        if report is None:
            return

        ran = [s for s in report.sections if s.pass_number == 1]
        failed = [s for s in ran if s.status == SectionStatus.FAILED]
        generated = [s for s in ran if s.status == SectionStatus.GENERATED]
        if not generated:
            report.status = ReportStatus.FAILED
            report.error = "All sections failed"
        elif failed:
            report.status = ReportStatus.PARTIAL
        else:
            report.status = ReportStatus.COMPLETE
        report.generation_completed_at = _utcnow()
        await db.commit()

        if generated:
            await _render_and_store_pdf(db, report)


async def _render_and_store_pdf(db, report: Report) -> None:
    """Render the PDF from the stored sections and upload it to S3.

    A PDF failure never fails the report — the download endpoint falls back
    to an on-demand render.
    """
    from app.services.reports.pdf import render_report_pdf

    try:
        with tempfile.TemporaryDirectory(prefix="report-pdf-") as tmp:
            pdf_path = await asyncio.to_thread(render_report_pdf, report, Path(tmp))
            slug = f"{report.report_type}-{report.windfarm_id or report.portfolio_id}"
            key = f"reports/{report.id}/v{report.version}/{slug}.pdf"
            await s3_service.upload_file(key, pdf_path, content_type="application/pdf")
        report.pdf_s3_key = key
        report.pdf_generated_at = _utcnow()
        await db.commit()
    except Exception as exc:
        logger.error("report_pdf_render_failed", report_id=report.id, error=str(exc), exc_info=True)


async def sweep_stuck_reports() -> int:
    """Startup sweeper: fail reports orphaned by a task restart.

    ``asyncio.create_task`` state dies with the process; anything still
    in-flight after _STUCK_AFTER at boot can never finish. Failing it also
    releases the in-flight unique index so users can retry.
    """
    factory = get_session_factory()
    cutoff = _utcnow() - _STUCK_AFTER
    async with factory() as db:
        result = await db.execute(
            update(Report)
            .where(
                Report.status.in_((ReportStatus.PENDING, ReportStatus.GENERATING)),
                Report.created_at < cutoff,
            )
            .values(
                status=ReportStatus.FAILED,
                error="Orphaned by service restart",
                generation_completed_at=_utcnow(),
            )
        )
        await db.execute(
            update(ReportSection)
            .where(ReportSection.status == SectionStatus.GENERATING)
            .values(status=SectionStatus.FAILED, error="Orphaned by service restart")
        )
        await db.commit()
        count = result.rowcount or 0
        if count:
            logger.warning("report_sweeper_failed_orphans", count=count)
        return count


async def render_pdf_on_demand(report: Report) -> Optional[bytes]:
    """Fallback for GET /reports/{id}/pdf when no stored artifact exists."""
    from app.services.reports.pdf import render_report_pdf

    with tempfile.TemporaryDirectory(prefix="report-pdf-") as tmp:
        pdf_path = await asyncio.to_thread(render_report_pdf, report, Path(tmp))
        return pdf_path.read_bytes()
