"""SCADA finding-lifecycle service (Phase 7b).

Writes the human lifecycle state of a Predict finding into ``scada_finding_action`` (a public core
table), keyed by the stable natural key ``(farm, trigger, scope, cls)``. Mirrors the DataAnomaly
write pattern: load-or-create the ORM row, mutate, ``commit``/``refresh``. The register itself
(``scada.opportunity_register``) is read via schema-qualified raw SQL only to validate that the
natural key names a real finding before recording an action against it.
"""

from datetime import datetime, timezone
from typing import Optional

import structlog
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.scada_finding_action import ScadaFindingAction, ScadaFindingStatus

logger = structlog.get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class ScadaFindingService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _register_row_exists(self, farm: str, trigger: str, scope: str, cls: str) -> bool:
        """True when a register row carries this natural key (guards orphan actions)."""
        result = await self.db.execute(
            text(
                "SELECT 1 FROM scada.opportunity_register "
                "WHERE farm = :farm AND COALESCE(trigger, '') = :trigger "
                "AND scope = :scope AND cls = :cls LIMIT 1"
            ),
            {"farm": farm, "trigger": trigger, "scope": scope, "cls": cls},
        )
        return result.scalar() is not None

    async def set_action(
        self,
        *,
        farm: str,
        trigger: Optional[str],
        scope: str,
        cls: str,
        status: str,
        notes: Optional[str],
        user_id: int,
    ) -> Optional[ScadaFindingAction]:
        """Upsert the lifecycle state for one finding. Returns None if the finding doesn't exist."""
        trig = (trigger or "").strip()
        if not await self._register_row_exists(farm, trig, scope, cls):
            return None

        existing = (
            await self.db.execute(
                select(ScadaFindingAction).where(
                    ScadaFindingAction.farm == farm,
                    ScadaFindingAction.trigger == trig,
                    ScadaFindingAction.scope == scope,
                    ScadaFindingAction.cls == cls,
                )
            )
        ).scalar_one_or_none()

        now = _utcnow()
        if existing is None:
            existing = ScadaFindingAction(
                farm=farm, trigger=trig, scope=scope, cls=cls, status=status
            )
            self.db.add(existing)

        existing.status = status
        existing.updated_by = user_id
        if notes is not None:
            existing.notes = notes
        # First acknowledgement stamps acknowledged_at and keeps it thereafter.
        if existing.acknowledged_at is None:
            existing.acknowledged_at = now
        # resolved_at tracks the current state — set on resolve, cleared on reopen.
        existing.resolved_at = now if status == ScadaFindingStatus.RESOLVED else None

        await self.db.commit()
        await self.db.refresh(existing)
        logger.info(
            "scada_finding_action_set",
            farm=farm,
            trigger=trig,
            scope=scope,
            cls=cls,
            status=status,
            user_id=user_id,
        )
        return existing
