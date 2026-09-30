"""CRUD service for the SCADA PPA structure (EPR-97), shared per wind farm (EPR-143).

Instance-based (``ScadaPpaService(db)``), matching ScadaFindingService / ScadaOpportunityService.
Plain ORM ``select()`` throughout — the raw-SQL style elsewhere in SCADA is only for the
pipeline-owned ``scada.*`` tables, and this one is an ordinary public core table.

History: EPR-136 made the register private per user (every statement carried
``created_by_id = :user``). Aje's 2026-09-25 decision — "one official set of terms per farm" —
reverses that: the register is ONE shared table for EnergyExe staff, visibility is enforced at the
API boundary by ``get_current_internal_user`` (users.is_internal AND is_superuser), and
``created_by_id`` is "entered by" provenance only. No method filters on it; ``create_ppas`` stamps it
from the authenticated caller, never from the payload.

Overlap refusal (EPR-143, Aje 2026-09-28, D-041): two ACTIVE rows on one farm whose date ranges
share a calendar day are "conflicting terms" and are refused at entry. The suite's own reader
resolves such a pair to UNKNOWN; the register must simply never hold one. Every write therefore
runs inside ONE transaction in a fixed order — (1) on update, lock and re-read the PPA row itself,
(2) merge the patch and re-validate the merged row, (3) lock the involved farms in ascending id
order, (4) query the overlaps against the merged row, (5) write, (6) commit — so two concurrent
writers on the same farm serialise on the farm lock and the loser sees the winner's row. Dates are
compared as closed ranges: an end date of 30 June and a start of 1 July do not overlap; a
NULL start or end is unbounded on that side. Draft / Superseded / Terminated / Expired rows never
take part.

Never raises HTTP: "not found" is ``None`` and the endpoint turns that into a 404; a broken
cross-field rule is :class:`ScadaPpaTermsError` (400) and an overlap is
:class:`ActiveTermsConflict` (409). After either the endpoint rolls the session back, which is
what releases the row/farm locks.
"""

from datetime import date
from decimal import Decimal
from typing import Any, List, Mapping, Optional, Sequence, Tuple

import structlog
from sqlalchemy import delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.scada_ppa import PpaStatus, ScadaPpa
from app.models.windfarm import Windfarm
from app.schemas.scada_ppa import ScadaPpaCreate, ScadaPpaUpdate

logger = structlog.get_logger()

# Terms shared across every farm leg of a multi-farm PPA — everything on the create payload except
# the asset list, which is what we fan out over. Provenance is not a term: it is set explicitly from
# the authenticated user, never from the payload.
_FANOUT_EXCLUDED = {"windfarm_ids"}

# The columns a PATCH may touch. The merged row is judged on these, read from the LOCKED row.
_PATCHABLE = tuple(ScadaPpaUpdate.model_fields)


class ScadaPpaTermsError(ValueError):
    """A cross-field rule fails on the row as it would be AFTER the write (endpoint -> 400)."""


class ActiveTermsConflict(Exception):
    """The merged row is Active and another Active row on the same farm overlaps it (endpoint -> 409)."""

    def __init__(self, windfarm_id: int, conflicts: Sequence[ScadaPpa]):
        self.windfarm_id = windfarm_id
        self.conflicts = list(conflicts)
        first = self.conflicts[0]
        span = f"{first.effective_date or 'open'} to {first.expiration_date or 'open'}"
        super().__init__(
            f"conflicting terms: an Active PPA ({first.ppa_code}, {span}) already covers "
            f"windfarm {windfarm_id}"
        )


def is_active_status(status: Optional[str]) -> bool:
    """Only ``Active`` is in force — the same rule as the suite's ``site_config`` and the private
    exporter (EPR-143, 78d082c): trimmed, case-insensitive, blank is not active."""
    return (status or "").strip().casefold() == PpaStatus.ACTIVE.value.casefold()


def validate_merged_terms(merged: Mapping[str, Any]) -> None:
    """The cross-field rules, applied to a row as it will be stored.

    ``ScadaPpaCreate`` / ``ScadaPpaUpdate`` can only check pairs the caller supplied together; a
    patch that sets just ``expiration_date`` has to be judged against the stored
    ``effective_date``, and — because two concurrent patches can each pass that check against the
    same stale row — the service applies it again to the LOCKED row before writing.
    """
    effective: Optional[date] = merged.get("effective_date")
    expiration: Optional[date] = merged.get("expiration_date")
    if effective is not None and expiration is not None and expiration <= effective:
        raise ScadaPpaTermsError("expiration_date must be after effective_date")

    floor = merged.get("floor_price")
    cap = merged.get("cap_price")
    if floor is not None and cap is not None and Decimal(floor) > Decimal(cap):
        raise ScadaPpaTermsError("floor_price must not exceed cap_price")

    rate = merged.get("indexation_rate_pct")
    if rate is not None and not (Decimal(-100) <= Decimal(rate) <= Decimal(100)):
        raise ScadaPpaTermsError(
            "indexation_rate_pct must be between -100 and 100 (percentage points)"
        )


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
        """Filtered page of the shared register plus the total matching count (before limit/offset)."""
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

        count_stmt = select(func.count(ScadaPpa.id)).where(*filters)
        total = (await self.db.execute(count_stmt)).scalar() or 0

        stmt = select(ScadaPpa).options(selectinload(ScadaPpa.windfarm)).where(*filters)
        # Group a multi-farm PPA together, newest contract first.
        stmt = (
            stmt.order_by(ScadaPpa.ppa_code.asc(), ScadaPpa.windfarm_id.asc())
            .offset(offset)
            .limit(limit)
        )
        rows = list((await self.db.execute(stmt)).scalars().all())
        return rows, int(total)

    async def get_ppa(self, ppa_id: int) -> Optional[ScadaPpa]:
        stmt = (
            select(ScadaPpa).options(selectinload(ScadaPpa.windfarm)).where(ScadaPpa.id == ppa_id)
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

    async def existing_pairs(self, ppa_code: str, windfarm_ids: Sequence[int]) -> List[int]:
        """Which of these farms already carry a row for ``ppa_code`` — a pre-check so the endpoint
        can name the colliding farms instead of surfacing only the unique-constraint error."""
        if not windfarm_ids:
            return []
        stmt = select(ScadaPpa.windfarm_id).where(
            ScadaPpa.ppa_code == ppa_code, ScadaPpa.windfarm_id.in_(list(windfarm_ids))
        )
        return sorted({row for row in (await self.db.execute(stmt)).scalars().all()})

    # ── overlap refusal (D-041) ────────────────────────────────────────────────

    async def lock_windfarms(self, windfarm_ids: Sequence[int]) -> List[int]:
        """``SELECT id FROM windfarms WHERE id IN (...) ORDER BY id FOR UPDATE``.

        Every writer that touches a farm's Active rows takes the farm's row lock first, always in
        ascending id order, so two writers on overlapping farm sets cannot deadlock and the second
        one's overlap query runs only after the first has committed. Returns the ids actually
        locked (a missing farm is simply absent). SQLite renders no FOR UPDATE, which is fine:
        the unit tests there cover semantics, the isolated-Postgres tests cover the race.
        """
        ids = sorted({int(i) for i in windfarm_ids})
        if not ids:
            return []
        stmt = (
            select(Windfarm.id)
            .where(Windfarm.id.in_(ids))
            .order_by(Windfarm.id.asc())
            .with_for_update()
        )
        return list((await self.db.execute(stmt)).scalars().all())

    async def active_overlaps(
        self,
        windfarm_id: int,
        effective: Optional[date],
        expiration: Optional[date],
        *,
        exclude_id: Optional[int] = None,
    ) -> List[ScadaPpa]:
        """Active rows on ``windfarm_id`` whose closed date range shares at least one day with
        ``[effective, expiration]``. A NULL bound is unbounded on that side; the day a contract
        ends is still covered by it, so 30 June / 1 July is NOT an overlap but 30 June / 30 June is.
        """
        stmt = select(ScadaPpa).where(
            ScadaPpa.windfarm_id == windfarm_id,
            ScadaPpa.ppa_status == PpaStatus.ACTIVE.value,
        )
        if exclude_id is not None:
            stmt = stmt.where(ScadaPpa.id != exclude_id)
        if effective is not None:
            # the other row must not have ended before we start
            stmt = stmt.where(
                or_(ScadaPpa.expiration_date.is_(None), ScadaPpa.expiration_date >= effective)
            )
        if expiration is not None:
            # the other row must not start after we end
            stmt = stmt.where(
                or_(ScadaPpa.effective_date.is_(None), ScadaPpa.effective_date <= expiration)
            )
        stmt = stmt.order_by(ScadaPpa.effective_date.asc().nulls_first(), ScadaPpa.id.asc())
        return list((await self.db.execute(stmt)).scalars().all())

    async def _refuse_overlaps(
        self,
        windfarm_id: int,
        effective: Optional[date],
        expiration: Optional[date],
        *,
        exclude_id: Optional[int] = None,
    ) -> None:
        clashes = await self.active_overlaps(
            windfarm_id, effective, expiration, exclude_id=exclude_id
        )
        if clashes:
            raise ActiveTermsConflict(windfarm_id, clashes)

    # ── writes ─────────────────────────────────────────────────────────────────

    async def create_ppas(self, payload: ScadaPpaCreate, *, user_id: int) -> List[ScadaPpa]:
        """Fan the shared terms out into one row per windfarm, all sharing ``ppa_code`` and all
        stamped with the caller as "entered by". All-or-nothing: one conflicting farm and no row
        is written."""
        farms = sorted(set(payload.windfarm_ids))
        locked = await self.lock_windfarms(farms)
        unknown = [wf for wf in farms if wf not in locked]
        if unknown:
            raise ScadaPpaTermsError(f"Unknown windfarm_id(s): {unknown}")
        if is_active_status(payload.ppa_status):
            for wf_id in farms:
                await self._refuse_overlaps(wf_id, payload.effective_date, payload.expiration_date)

        terms = payload.model_dump(exclude=_FANOUT_EXCLUDED)
        rows = [
            ScadaPpa(windfarm_id=wf_id, created_by_id=user_id, **terms)
            for wf_id in payload.windfarm_ids
        ]
        self.db.add_all(rows)
        await self.db.commit()
        for row in rows:
            await self.db.refresh(row)
        logger.info(
            "scada_ppa_created",
            ppa_code=payload.ppa_code,
            windfarm_ids=payload.windfarm_ids,
            rows=len(rows),
            user_id=user_id,
        )
        # Re-read so the windfarm relationship is loaded for the response.
        return await self.get_by_code(payload.ppa_code)

    async def update_ppa(self, ppa_id: int, patch: ScadaPpaUpdate) -> Optional[ScadaPpa]:
        """Edit one farm's leg. Deliberately does not touch the rest of the ppa_code group.

        The row the endpoint read to 404-check is stale by now; ``populate_existing`` makes the
        locked ``SELECT ... FOR UPDATE`` overwrite the identity-map copy with the values under the
        lock (the same effect as ``refresh()`` after the lock, in one round trip), and everything
        below — the merged terms, the farms to lock, the overlap query — is derived from THAT row.
        """
        stmt = (
            select(ScadaPpa)
            .where(ScadaPpa.id == ppa_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        row = (await self.db.execute(stmt)).scalar_one_or_none()
        if row is None:
            return None

        supplied = patch.model_dump(exclude_unset=True)
        merged = {field: supplied.get(field, getattr(row, field)) for field in _PATCHABLE}
        validate_merged_terms(merged)

        target_farm = int(merged["windfarm_id"])
        locked = await self.lock_windfarms({row.windfarm_id, target_farm})
        if target_farm not in locked:
            raise ScadaPpaTermsError(f"Unknown windfarm_id(s): [{target_farm}]")
        if is_active_status(merged["ppa_status"]):
            await self._refuse_overlaps(
                target_farm,
                merged["effective_date"],
                merged["expiration_date"],
                exclude_id=ppa_id,
            )

        for field, value in supplied.items():
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
