"""SCADA finding-lifecycle service (Phase 7b).

Writes the human lifecycle state of a Predict finding into ``scada_finding_action`` (a public core
table), keyed by the stable natural key ``(farm, asset_code, trigger, scope, cls)`` (asset since D-081). Mirrors the DataAnomaly
write pattern: load-or-create the ORM row, mutate, ``commit``/``refresh``. The register itself
(``scada.opportunity_register``) is read via schema-qualified raw SQL only to validate that the
natural key names a real finding before recording an action against it.

A value-ledger status key (D7; scope ``VL:…``) is the second existence check, for internal staff with the ledger
switched on: the asset's current (not stale) ledger run has a cause with that status key in its cause list and a
row for that cause.
"""

from datetime import datetime, timezone
from typing import Optional

import structlog
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.scada_finding_action import ScadaFindingAction, ScadaFindingStatus
from app.services.scada_ledger_service import ScadaLedgerService, scada_ledger_present

logger = structlog.get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class ScadaFindingService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _register_row_exists(
        self, farm: str, asset_code: str, trigger: str, scope: str, cls: str
    ) -> bool:
        """True when a register row carries this natural key (guards orphan actions)."""
        result = await self.db.execute(
            text(
                "SELECT 1 FROM scada.opportunity_register "
                "WHERE farm = :farm AND asset_code = :asset_code AND COALESCE(trigger, '') = :trigger "
                "AND scope = :scope AND cls = :cls LIMIT 1"
            ),
            {
                "farm": farm,
                "asset_code": asset_code,
                "trigger": trigger,
                "scope": scope,
                "cls": cls,
            },
        )
        return result.scalar() is not None

    async def _ledger_key_exists(
        self, farm: str, asset_code: str, trigger: str, scope: str, cls: str
    ) -> bool:
        """True when the asset's current ledger run has a row for a cause with this status key. False when the
        tables are absent or the switch is off, so such an environment keeps the register's 404."""
        if not get_settings().scada_ledger_enabled or not await scada_ledger_present(self.db):
            return False
        run = await ScadaLedgerService(self.db).current(farm, asset_code)
        if run is None:
            return False
        key = {"trigger": trigger, "scope": scope, "cls": cls}
        codes = [c["code"] for c in run["catalog"]["causes"] if c["status_key"] == key]
        if not codes:
            return False
        result = await self.db.execute(
            text(
                "SELECT 1 FROM scada.opportunity_ledger_cause WHERE farm = :farm AND asset_code = :asset_code "
                "AND run_id = :run_id AND cause = ANY(:codes) LIMIT 1"
            ),
            {"farm": farm, "asset_code": asset_code, "run_id": run["run_id"], "codes": codes},
        )
        return result.scalar() is not None

    async def set_action(
        self,
        *,
        farm: str,
        asset_code: str,
        trigger: Optional[str],
        scope: str,
        cls: str,
        status: str,
        notes: Optional[str],
        user_id: int,
        internal_staff: bool = False,
    ) -> Optional[ScadaFindingAction]:
        """Upsert the lifecycle state for one finding. Returns None if the finding doesn't exist. Ledger status
        keys are checked only for internal staff."""
        trig = (trigger or "").strip()
        if not await self._register_row_exists(farm, asset_code, trig, scope, cls) and not (
            internal_staff and await self._ledger_key_exists(farm, asset_code, trig, scope, cls)
        ):
            return None

        existing = (
            await self.db.execute(
                select(ScadaFindingAction).where(
                    ScadaFindingAction.farm == farm,
                    ScadaFindingAction.asset_code == asset_code,
                    ScadaFindingAction.trigger == trig,
                    ScadaFindingAction.scope == scope,
                    ScadaFindingAction.cls == cls,
                )
            )
        ).scalar_one_or_none()

        now = _utcnow()
        if existing is None:
            existing = ScadaFindingAction(
                farm=farm,
                asset_code=asset_code,
                trigger=trig,
                scope=scope,
                cls=cls,
                status=status,
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
            asset_code=asset_code,
            trigger=trig,
            scope=scope,
            cls=cls,
            status=status,
            user_id=user_id,
        )
        return existing
