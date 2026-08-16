"""ReportService — CRUD, scoping, and retain-on-export versioning.

Generation itself lives in ``orchestrator.py``; this service owns rows.
"""

from datetime import datetime, timezone
from typing import List, Optional

import structlog
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.deps import is_client_request
from app.core.exceptions import NotFoundException, ValidationException
from app.models.portfolio import Portfolio
from app.models.report import Report, ReportSection, ReportStatus, SectionStatus
from app.models.user import User
from app.models.windfarm import Windfarm
from app.schemas.report import ReportCreate, ScopeMeta
from app.services import s3_service
from app.services.reports.registry import get_report_type

logger = structlog.get_logger()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class ReportInFlightError(ValidationException):
    """A generation for this target is already running (maps to HTTP 409)."""

    def __init__(self, existing_id: Optional[int]):
        super().__init__("A report for this target is already generating")
        self.existing_id = existing_id


class ReportService:
    def __init__(self, db: AsyncSession):
        self.db = db

    # ── creation ────────────────────────────────────────────────────────

    async def create_report(self, payload: ReportCreate, user: User) -> Report:
        spec = get_report_type(payload.report_type)
        if spec is None:
            raise ValidationException(f"Unknown report type: {payload.report_type}")

        scope_type = "windfarm" if payload.windfarm_id is not None else "portfolio"
        if spec.scope not in (scope_type, "both"):
            raise ValidationException(
                f"Report type '{spec.code}' does not support {scope_type} scope"
            )

        title = spec.title
        if scope_type == "windfarm":
            windfarm = await self.db.get(Windfarm, payload.windfarm_id)
            if windfarm is None or getattr(windfarm, "is_deleted", False):
                raise NotFoundException("Windfarm not found")
            title = f"{spec.title} — {windfarm.name}"
        else:
            portfolio = await self.db.get(Portfolio, payload.portfolio_id)
            if portfolio is None:
                raise NotFoundException("Portfolio not found")
            if is_client_request(user) and portfolio.user_id != user.id:
                raise NotFoundException("Portfolio not found")
            title = f"{spec.title} — {portfolio.name}"

        section_keys = payload.sections
        report = Report(
            report_type=spec.code,
            scope_type=scope_type,
            windfarm_id=payload.windfarm_id,
            portfolio_id=payload.portfolio_id,
            period_start=payload.period_start,
            period_end=payload.period_end,
            title=title,
            params={
                "sections": section_keys,
                "generate_narratives": payload.generate_narratives,
            },
            requested_by_id=user.id,
            status=ReportStatus.PENDING,
        )
        self.db.add(report)
        try:
            await self.db.flush()
        except IntegrityError:
            await self.db.rollback()
            existing = await self.db.execute(
                select(Report.id).where(
                    Report.report_type == spec.code,
                    Report.windfarm_id == payload.windfarm_id,
                    Report.portfolio_id == payload.portfolio_id,
                    Report.status.in_((ReportStatus.PENDING, ReportStatus.GENERATING)),
                )
            )
            raise ReportInFlightError(existing.scalar_one_or_none())

        self._create_section_rows(report, section_keys)
        await self.db.commit()
        await self.db.refresh(report, attribute_names=["sections"])
        return report

    def _create_section_rows(self, report: Report, section_keys: Optional[List[str]]) -> None:
        spec = get_report_type(report.report_type)
        for order, section in enumerate(spec.display_ordered()):
            if section_keys and section.key not in section_keys:
                continue
            self.db.add(
                ReportSection(
                    report_id=report.id,
                    section_key=section.key,
                    pass_number=section.pass_number,
                    display_order=order,
                    layout=section.layout,
                    status=SectionStatus.UNGENERATED,
                )
            )

    # ── reads ───────────────────────────────────────────────────────────

    async def get_report(self, report_id: int, user: User) -> Report:
        result = await self.db.execute(
            select(Report)
            .options(
                selectinload(Report.sections),
                selectinload(Report.windfarm).selectinload(Windfarm.bidzone),
                selectinload(Report.windfarm).selectinload(Windfarm.country),
                selectinload(Report.portfolio),
                selectinload(Report.requested_by),
            )
            .where(Report.id == report_id, Report.deleted_at.is_(None))
        )
        report = result.scalar_one_or_none()
        if report is None:
            raise NotFoundException("Report not found")
        if is_client_request(user) and report.requested_by_id != user.id:
            # v1 scoping: no org model yet — clients see their own reports only.
            raise NotFoundException("Report not found")
        return report

    async def list_reports(
        self,
        user: User,
        report_type: Optional[str] = None,
        windfarm_id: Optional[int] = None,
        portfolio_id: Optional[int] = None,
        status: Optional[str] = None,
        include_superseded: bool = False,
        skip: int = 0,
        limit: int = 50,
    ) -> tuple[List[Report], int]:
        query = (
            select(Report)
            .options(
                selectinload(Report.windfarm).selectinload(Windfarm.bidzone),
                selectinload(Report.windfarm).selectinload(Windfarm.country),
                selectinload(Report.portfolio),
            )
            .where(Report.deleted_at.is_(None))
        )
        if is_client_request(user):
            query = query.where(Report.requested_by_id == user.id)
        if not include_superseded:
            query = query.where(Report.status != ReportStatus.SUPERSEDED)
        if report_type:
            query = query.where(Report.report_type == report_type)
        if windfarm_id:
            query = query.where(Report.windfarm_id == windfarm_id)
        if portfolio_id:
            query = query.where(Report.portfolio_id == portfolio_id)
        if status:
            query = query.where(Report.status == status)

        count_result = await self.db.execute(
            select(func.count()).select_from(query.order_by(None).subquery())
        )
        total = count_result.scalar_one()

        result = await self.db.execute(
            query.order_by(Report.created_at.desc()).offset(skip).limit(limit)
        )
        return list(result.scalars().all()), total

    @staticmethod
    def scope_meta(report: Report) -> Optional[ScopeMeta]:
        if report.windfarm is not None:
            wf = report.windfarm
            return ScopeMeta(
                name=wf.name,
                capacity_mw=wf.nameplate_capacity_mw,
                bidzone=wf.bidzone.code if wf.bidzone is not None else None,
                country=wf.country.name if wf.country is not None else None,
            )
        if report.portfolio is not None:
            return ScopeMeta(name=report.portfolio.name)
        return None

    # ── versioning (retain-on-export) ───────────────────────────────────

    async def regenerate(self, report_id: int, user: User) -> Report:
        """Create version N+1 and supersede the current version.

        The superseded version is pruned (row + S3 PDF) unless it was ever
        exported (``pdf_downloaded_at``) or explicitly locked.
        """
        old = await self.get_report(report_id, user)
        if old.status in (ReportStatus.PENDING, ReportStatus.GENERATING):
            raise ReportInFlightError(old.id)

        new = Report(
            report_type=old.report_type,
            scope_type=old.scope_type,
            windfarm_id=old.windfarm_id,
            portfolio_id=old.portfolio_id,
            period_start=old.period_start,
            period_end=old.period_end,
            version=old.version + 1,
            title=old.title,
            params=old.params,
            requested_by_id=user.id,
            status=ReportStatus.PENDING,
        )
        # Free the in-flight slot for the new version before flushing it.
        keep_old = old.pdf_downloaded_at is not None or old.locked
        old.status = ReportStatus.SUPERSEDED
        self.db.add(new)
        await self.db.flush()

        old.superseded_by_id = new.id
        self._create_section_rows(new, (old.params or {}).get("sections"))

        if not keep_old:
            pdf_key = old.pdf_s3_key
            await self.db.delete(old)
            if pdf_key:
                try:
                    await s3_service.delete_by_prefix(pdf_key)
                except Exception as exc:
                    logger.warning("report_pdf_prune_failed", key=pdf_key, error=str(exc))

        await self.db.commit()
        await self.db.refresh(new, attribute_names=["sections"])
        return new

    # ── delete / download bookkeeping ───────────────────────────────────

    async def soft_delete(self, report_id: int, user: User) -> None:
        """Soft-delete the whole version chain of a report."""
        report = await self.get_report(report_id, user)
        now = _utcnow()
        chain = await self._version_chain(report)
        for r in chain:
            r.deleted_at = now
        await self.db.commit()

    async def _version_chain(self, report: Report) -> List[Report]:
        """All retained versions for the same target + type + period."""
        result = await self.db.execute(
            select(Report).where(
                Report.report_type == report.report_type,
                Report.windfarm_id == report.windfarm_id,
                Report.portfolio_id == report.portfolio_id,
                Report.period_start == report.period_start,
                Report.period_end == report.period_end,
                Report.deleted_at.is_(None),
            )
        )
        return list(result.scalars().all())

    async def mark_pdf_downloaded(self, report: Report) -> None:
        if report.pdf_downloaded_at is None:
            report.pdf_downloaded_at = _utcnow()
            await self.db.commit()
