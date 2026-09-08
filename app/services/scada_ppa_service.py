"""CRUD service for the SCADA PPA structure (EPR-97).

Instance-based (``ScadaPpaService(db)``), matching ScadaFindingService / ScadaOpportunityService.
Plain ORM ``select()`` throughout — the raw-SQL style elsewhere in SCADA is only for the
pipeline-owned ``scada.*`` tables, and this one is an ordinary public core table.

Never raises HTTP: "not found" is ``None`` and the endpoint turns that into a 404.
"""

from typing import List, Optional, Sequence, Tuple

import structlog
from sqlalchemy import delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.scada_ppa import ScadaPpa
from app.models.windfarm import Windfarm
from app.schemas.scada_ppa import ScadaPpaCreate, ScadaPpaUpdate

logger = structlog.get_logger()

# Terms shared across every farm leg of a multi-farm PPA — everything on the create payload except
# the asset list, which is what we fan out over.
_FANOUT_EXCLUDED = {"windfarm_ids"}


class ScadaPpaService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def list_ppas(
        self,
        *,
        windfarm_id: Optional[int] = None,
        ppa_code: Optional[str] = None,
        ppa_status: Optional[str] = None,
        q: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[ScadaPpa], int]:
        """Filtered page of rows plus the total matching count (before limit/offset)."""
        filters = []
        if windfarm_id is not None:
            filters.append(ScadaPpa.windfarm_id == windfarm_id)
        if ppa_code:
            filters.append(ScadaPpa.ppa_code == ppa_code)
        if ppa_status:
            filters.append(ScadaPpa.ppa_status == ppa_status)
        if q:
            like = f"%{q}%"
            filters.append(or_(ScadaPpa.ppa_buyer.ilike(like), ScadaPpa.ppa_code.ilike(like)))

        count_stmt = select(func.count(ScadaPpa.id))
        if filters:
            count_stmt = count_stmt.where(*filters)
        total = (await self.db.execute(count_stmt)).scalar() or 0

        stmt = select(ScadaPpa).options(selectinload(ScadaPpa.windfarm))
        if filters:
            stmt = stmt.where(*filters)
        # Group a multi-farm PPA together, newest contract first.
        stmt = stmt.order_by(ScadaPpa.ppa_code.asc(), ScadaPpa.windfarm_id.asc()).offset(offset).limit(limit)
        rows = list((await self.db.execute(stmt)).scalars().all())
        return rows, int(total)

    async def get_ppa(self, ppa_id: int) -> Optional[ScadaPpa]:
        stmt = (
            select(ScadaPpa)
            .options(selectinload(ScadaPpa.windfarm))
            .where(ScadaPpa.id == ppa_id)
        )
        return (await self.db.execute(stmt)).scalar_one_or_none()

    async def get_by_code(self, ppa_code: str) -> List[ScadaPpa]:
        """Every farm leg of one contract."""
        stmt = (
            select(ScadaPpa)
            .options(selectinload(ScadaPpa.windfarm))
            .where(ScadaPpa.ppa_code == ppa_code)
            .order_by(ScadaPpa.windfarm_id.asc())
        )
        return list((await self.db.execute(stmt)).scalars().all())

    async def missing_windfarm_ids(self, windfarm_ids: Sequence[int]) -> List[int]:
        """Which of these ids have no windfarms row — so the endpoint can 400 with the specific ids
        instead of letting the FK raise an opaque IntegrityError."""
        if not windfarm_ids:
            return []
        stmt = select(Windfarm.id).where(Windfarm.id.in_(list(windfarm_ids)))
        found = {row for row in (await self.db.execute(stmt)).scalars().all()}
        return [wf_id for wf_id in windfarm_ids if wf_id not in found]

    async def create_ppas(self, payload: ScadaPpaCreate) -> List[ScadaPpa]:
        """Fan the shared terms out into one row per windfarm, all sharing ``ppa_code``."""
        terms = payload.model_dump(exclude=_FANOUT_EXCLUDED)
        rows = [ScadaPpa(windfarm_id=wf_id, **terms) for wf_id in payload.windfarm_ids]
        self.db.add_all(rows)
        await self.db.commit()
        for row in rows:
            await self.db.refresh(row)
        logger.info(
            "scada_ppa_created",
            ppa_code=payload.ppa_code,
            windfarm_ids=payload.windfarm_ids,
            rows=len(rows),
        )
        # Re-read so the windfarm relationship is loaded for the response.
        return await self.get_by_code(payload.ppa_code)

    async def update_ppa(self, ppa_id: int, patch: ScadaPpaUpdate) -> Optional[ScadaPpa]:
        """Edit one farm's leg. Deliberately does not touch the rest of the ppa_code group."""
        row = (
            await self.db.execute(select(ScadaPpa).where(ScadaPpa.id == ppa_id))
        ).scalar_one_or_none()
        if row is None:
            return None

        for field, value in patch.model_dump(exclude_unset=True).items():
            setattr(row, field, value)

        await self.db.commit()
        await self.db.refresh(row)
        logger.info("scada_ppa_updated", id=ppa_id, ppa_code=row.ppa_code)
        return await self.get_ppa(ppa_id)

    async def delete_ppa(self, ppa_id: int) -> Optional[ScadaPpa]:
        row = (
            await self.db.execute(select(ScadaPpa).where(ScadaPpa.id == ppa_id))
        ).scalar_one_or_none()
        if row is None:
            return None
        await self.db.delete(row)
        await self.db.commit()
        logger.info("scada_ppa_deleted", id=ppa_id, ppa_code=row.ppa_code)
        return row

    async def delete_by_code(self, ppa_code: str) -> int:
        """Drop every farm leg of one contract. Returns the number of rows removed (0 = unknown code)."""
        result = await self.db.execute(delete(ScadaPpa).where(ScadaPpa.ppa_code == ppa_code))
        await self.db.commit()
        deleted = int(result.rowcount or 0)
        logger.info("scada_ppa_group_deleted", ppa_code=ppa_code, rows=deleted)
        return deleted
