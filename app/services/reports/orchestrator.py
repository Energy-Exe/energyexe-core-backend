"""Two-pass report generation orchestrator.

Runs as a fire-and-forget asyncio task (the ``weather_imports`` pattern):
``POST /reports`` inserts rows and calls :func:`start`; the frontend polls
``GET /reports/{id}/status``. Every concurrently-running section opens its own
session — an AsyncSession must never be shared across tasks.

A run is three waves: Pass-1 data sections concurrently, then Pass-1 narrative
sections (they consume the fresh data sections), then the Pass-2 executive
summary (synthesises everything, renders first). Narrative sections respect
the REPORTS_MAX_COST_USD budget — once exceeded they are SKIPPED and the
report lands PARTIAL.
"""

import asyncio
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

import structlog
from sqlalchemy import func, select, update
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
        pass1_data = [s for s in spec.sections if s.pass_number == 1 and s.data_builder is not None]
        semaphore = asyncio.Semaphore(_SECTION_CONCURRENCY)

        async def run_bounded(section_spec: SectionSpec) -> None:
            async with semaphore:
                await run_section(report_id, section_spec)

        # Sections that declare ``after`` read another data section's persisted
        # payload (EPR-117) — they run once the independent wave has landed.
        for wave in section_waves(pass1_data):
            await asyncio.gather(*(run_bounded(s) for s in wave), return_exceptions=True)

        # Pass 1 narratives — consume the data sections just built.
        pass1_ai = [
            s
            for s in spec.sections
            if s.pass_number == 1 and s.data_builder is None and s.narrative is not None
        ]

        async def run_ai_bounded(section_spec: SectionSpec) -> None:
            async with semaphore:
                await run_narrative_section(report_id, section_spec)

        await asyncio.gather(*(run_ai_bounded(s) for s in pass1_ai), return_exceptions=True)

        # Pass 2 — the executive summary synthesises all Pass-1 outputs.
        for section_spec in (s for s in spec.sections if s.pass_number == 2):
            await run_narrative_section(report_id, section_spec)

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


def section_waves(sections: list) -> list:
    """Split Pass-1 data sections into execution waves.

    Wave 1 = sections with no ``after``; wave 2 = sections that read another
    section's persisted output. Two waves are enough for the current specs — a
    dependent-of-a-dependent would need a topological sort, so refuse it loudly.
    """
    independent = [s for s in sections if not s.after]
    dependent = [s for s in sections if s.after]
    independent_keys = {s.key for s in independent}
    for s in dependent:
        missing = [k for k in s.after if k not in independent_keys]
        if missing:
            raise ValueError(
                f"section {s.key!r} declares after={s.after!r} but {missing!r} "
                "is not an independent Pass-1 data section of this run"
            )
    return [w for w in (independent, dependent) if w]


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


async def run_narrative_section(report_id: int, section_spec: SectionSpec) -> None:
    """Generate one AI narrative section; own session, contained errors.

    Skips (never fails) when the API key is missing or the report has hit the
    REPORTS_MAX_COST_USD budget, so a data-complete report still ships.
    """
    from app.core.config import get_settings
    from app.services.reports import narrative_service

    factory = get_session_factory()
    async with factory() as db:
        result = await db.execute(
            select(ReportSection).where(
                ReportSection.report_id == report_id,
                ReportSection.section_key == section_spec.key,
            )
        )
        section = result.scalar_one_or_none()
        if section is None:
            return

        settings = get_settings()
        report = await _load_report_for_narrative(db, report_id)
        if not settings.ANTHROPIC_API_KEY:
            section.status = SectionStatus.SKIPPED
            section.error = "AI narratives disabled: ANTHROPIC_API_KEY is not configured"
            await db.commit()
            return
        spent = float(report.total_cost_usd or 0)
        if spent >= settings.REPORTS_MAX_COST_USD:
            section.status = SectionStatus.SKIPPED
            section.error = (
                f"Budget exhausted: ${spent:.2f} spent of "
                f"${settings.REPORTS_MAX_COST_USD:.2f} allowed"
            )
            await db.commit()
            return

        section.status = SectionStatus.GENERATING
        section.error = None
        await db.commit()

        started = _utcnow()
        try:
            spec = get_report_type(report.report_type)
            result_obj = await narrative_service.generate(report, spec, section_spec)
            section.narrative_json = result_obj.narrative_json
            section.narrative_text = result_obj.narrative_text
            if result_obj.data is not None:
                section.data = result_obj.data
            section.llm_model = result_obj.model
            section.prompt_version = result_obj.prompt_version
            section.input_tokens = result_obj.input_tokens
            section.output_tokens = result_obj.output_tokens
            cost = Decimal(str(round(result_obj.cost_usd, 6)))
            section.cost_usd = cost
            section.status = SectionStatus.GENERATED
            section.generated_at = _utcnow()
            section.duration_ms = int((_utcnow() - started).total_seconds() * 1000)
            # SQL-side accumulation: safe even if narratives ever run concurrently.
            await db.execute(
                update(Report)
                .where(Report.id == report_id)
                .values(
                    total_cost_usd=func.coalesce(Report.total_cost_usd, 0) + cost,
                    total_input_tokens=func.coalesce(Report.total_input_tokens, 0)
                    + result_obj.input_tokens,
                    total_output_tokens=func.coalesce(Report.total_output_tokens, 0)
                    + result_obj.output_tokens,
                )
            )
        except Exception as exc:
            logger.error(
                "report_narrative_failed",
                report_id=report_id,
                section=section_spec.key,
                error=str(exc),
                exc_info=True,
            )
            section.status = SectionStatus.FAILED
            section.error = str(exc)[:2000]
        await db.commit()


async def _load_report_for_narrative(db, report_id: int) -> Report:
    result = await db.execute(
        select(Report)
        .options(
            selectinload(Report.sections),
            selectinload(Report.windfarm),
            selectinload(Report.portfolio),
        )
        .where(Report.id == report_id)
    )
    return result.scalar_one()


async def mark_pass2_stale(report_id: int) -> None:
    """A Pass-1 section changed — flag generated Pass-2 sections as STALE."""
    factory = get_session_factory()
    async with factory() as db:
        await db.execute(
            update(ReportSection)
            .where(
                ReportSection.report_id == report_id,
                ReportSection.pass_number == 2,
                ReportSection.status == SectionStatus.GENERATED,
            )
            .values(status=SectionStatus.STALE)
        )
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

        # STALE still carries generated content — it only awaits a refresh.
        generated = [
            s for s in report.sections if s.status in (SectionStatus.GENERATED, SectionStatus.STALE)
        ]
        incomplete = [
            s
            for s in report.sections
            if s.status in (SectionStatus.FAILED, SectionStatus.SKIPPED, SectionStatus.UNGENERATED)
        ]
        if not generated:
            report.status = ReportStatus.FAILED
            report.error = "All sections failed"
        elif incomplete:
            report.status = ReportStatus.PARTIAL
        else:
            report.status = ReportStatus.COMPLETE
        report.generation_completed_at = _utcnow()
        await db.commit()

        if generated:
            await _render_and_store_pdf(db, report)


def pdf_is_stale(report: Report) -> bool:
    """The stored artifact predates a section change (or doesn't exist)."""
    if report.pdf_s3_key is None or report.pdf_generated_at is None:
        return True
    newest = max(
        (
            s.generated_at
            for s in report.sections
            if s.generated_at is not None
            and s.status in (SectionStatus.GENERATED, SectionStatus.STALE)
        ),
        default=None,
    )
    return newest is not None and report.pdf_generated_at < newest


async def _render_and_store_pdf(db, report: Report) -> None:
    """Render the PDF from the stored sections and upload it to S3.

    A PDF failure never fails the report — the download endpoint falls back
    to an on-demand render. A frozen version's stored artifact is never
    overwritten (retain-on-export: exported bytes stay exported).
    """
    from app.services.reports.pdf import render_report_pdf

    if report.is_frozen and report.pdf_s3_key is not None:
        logger.info("report_pdf_frozen_skip", report_id=report.id, version=report.version)
        return
    try:
        with tempfile.TemporaryDirectory(prefix="report-pdf-") as tmp:
            pdf_path = await asyncio.to_thread(render_report_pdf, report, Path(tmp))
            slug = f"{report.report_type}-{report.windfarm_id or report.portfolio_id}"
            key = f"reports/{report.id}/v{report.version}/{slug}.pdf"
            stored = await s3_service.upload_file(key, pdf_path, content_type="application/pdf")
        if stored:
            report.pdf_s3_key = stored
            report.pdf_generated_at = _utcnow()
            await db.commit()
        else:
            # S3 disabled (local dev) — recording a key nothing was stored
            # under would poison downloads; the endpoint renders on demand.
            logger.info("report_pdf_not_stored_s3_disabled", report_id=report.id)
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
