"""SCADA PPA endpoints (EPR-97) — the detailed, SCADA-only offtake contract structure.

Completely separate from ``/api/v1/ppas`` (the Perform PPA); the two share no models, schemas,
services or routes, and no Perform code path reads this table.

Two departures from the rest of ``/scada``, both deliberate:

* **Auth is ``get_current_superuser``, not ``get_current_active_user``.** Every other SCADA endpoint
  relies on the frontend's ``canAccessScada`` gate; this one writes commercial contract terms, so the
  backend enforces the same ``is_superuser`` check the portal does.
* **Writes are audited.** ``@audit_action`` is not used on SCADA endpoints today; contract data is
  worth the audit trail. The decorator resolves ``db`` / ``request`` / ``current_user`` by kwarg name,
  so those names are load-bearing in the handler signatures below.

``get_db`` comes from ``app.core.deps`` (the SCADA convention) — the copy in ``app.core.database`` is
a different function object, and test ``dependency_overrides`` only match the one actually imported.
"""

from datetime import date
from decimal import Decimal
from typing import Any, List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.audit import audit_action
from app.core.deps import get_current_superuser, get_db
from app.models.audit_log import AuditAction
from app.models.scada_ppa import ScadaPpa as ScadaPpaModel
from app.models.user import User
from app.schemas.scada_ppa import (
    ScadaPpa,
    ScadaPpaCreate,
    ScadaPpaDeleteResult,
    ScadaPpaListResponse,
    ScadaPpaUpdate,
)
from app.services.scada_ppa_service import ScadaPpaService

logger = structlog.get_logger()

router = APIRouter()

_DUPLICATE_DETAIL = (
    "A PPA with this code already exists for one of those windfarms "
    "(ppa_code + windfarm must be unique)."
)


def _validate_merged_row(row: ScadaPpaModel, patch: ScadaPpaUpdate) -> None:
    """Re-check the cross-field rules against the row as it will be AFTER the patch.

    ``ScadaPpaUpdate`` can only validate pairs where the caller supplied both halves; a patch that
    sets just ``expiration_date`` has to be judged against the stored ``effective_date``.
    """
    supplied = patch.model_dump(exclude_unset=True)

    def merged(field: str):
        return supplied.get(field, getattr(row, field))

    effective: Optional[date] = merged("effective_date")
    expiration: Optional[date] = merged("expiration_date")
    if effective is not None and expiration is not None and expiration <= effective:
        raise HTTPException(status_code=400, detail="expiration_date must be after effective_date")

    floor: Optional[Decimal] = merged("floor_price")
    cap: Optional[Decimal] = merged("cap_price")
    if floor is not None and cap is not None and Decimal(floor) > Decimal(cap):
        raise HTTPException(status_code=400, detail="floor_price must not exceed cap_price")


@router.get("", response_model=ScadaPpaListResponse)
@audit_action(AuditAction.ACCESS, "scada_ppa", description="Listed SCADA PPAs")
async def list_scada_ppas(
    windfarm_id: Optional[int] = Query(None, description="Only this windfarm's contract legs"),
    ppa_code: Optional[str] = Query(None, description="Exact contract code"),
    ppa_status: Optional[str] = Query(None, description="Draft/Active/Superseded/Terminated/Expired"),
    q: Optional[str] = Query(None, description="Substring match on buyer or code"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
) -> Any:
    """The SCADA PPA register. One row per windfarm; a multi-farm PPA is several rows sharing a code."""
    items, total = await ScadaPpaService(db).list_ppas(
        windfarm_id=windfarm_id,
        ppa_code=ppa_code,
        ppa_status=ppa_status,
        q=q,
        limit=limit,
        offset=offset,
    )
    return ScadaPpaListResponse(items=items, total=total)


# Declared above /{ppa_id} so "by-code" is not parsed as an id.
@router.get("/by-code/{ppa_code}", response_model=List[ScadaPpa])
@audit_action(AuditAction.ACCESS, "scada_ppa", description="Viewed a SCADA PPA group")
async def get_scada_ppa_group(
    ppa_code: str,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
) -> Any:
    """Every farm leg of one contract."""
    rows = await ScadaPpaService(db).get_by_code(ppa_code)
    if not rows:
        raise HTTPException(status_code=404, detail="SCADA PPA not found")
    return rows


@router.delete("/by-code/{ppa_code}", response_model=ScadaPpaDeleteResult)
@audit_action(
    AuditAction.DELETE,
    "scada_ppa",
    lambda result, *args, **kwargs: str(kwargs.get("ppa_code", "unknown")),
    description="Deleted a whole SCADA PPA group",
)
async def delete_scada_ppa_group(
    ppa_code: str,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
) -> Any:
    """Delete the contract across every farm it covers."""
    deleted = await ScadaPpaService(db).delete_by_code(ppa_code)
    if deleted == 0:
        raise HTTPException(status_code=404, detail="SCADA PPA not found")
    return ScadaPpaDeleteResult(ppa_code=ppa_code, deleted=deleted)


@router.get("/{ppa_id}", response_model=ScadaPpa)
@audit_action(
    AuditAction.ACCESS,
    "scada_ppa",
    lambda result, *args, **kwargs: str(kwargs.get("ppa_id", "unknown")),
    description="Viewed a SCADA PPA",
)
async def get_scada_ppa(
    ppa_id: int,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
) -> Any:
    row = await ScadaPpaService(db).get_ppa(ppa_id)
    if row is None:
        raise HTTPException(status_code=404, detail="SCADA PPA not found")
    return row


@router.post("", response_model=List[ScadaPpa], status_code=201)
@audit_action(AuditAction.CREATE, "scada_ppa", description="Created a SCADA PPA")
async def create_scada_ppa(
    payload: ScadaPpaCreate,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
) -> Any:
    """Create one PPA across one or more windfarms — N rows sharing the ``ppa_code``."""
    service = ScadaPpaService(db)

    missing = await service.missing_windfarm_ids(payload.windfarm_ids)
    if missing:
        raise HTTPException(
            status_code=400, detail=f"Unknown windfarm_id(s): {sorted(missing)}"
        )

    try:
        return await service.create_ppas(payload)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail=_DUPLICATE_DETAIL)


@router.put("/{ppa_id}", response_model=ScadaPpa)
@audit_action(
    AuditAction.UPDATE,
    "scada_ppa",
    lambda result, *args, **kwargs: str(kwargs.get("ppa_id", "unknown")),
    description="Updated a SCADA PPA",
)
async def update_scada_ppa(
    ppa_id: int,
    payload: ScadaPpaUpdate,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
) -> Any:
    """Edit ONE farm's leg. Terms may legitimately diverge per farm, so this never fans out."""
    service = ScadaPpaService(db)

    existing = await service.get_ppa(ppa_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="SCADA PPA not found")

    _validate_merged_row(existing, payload)

    if payload.windfarm_id is not None:
        missing = await service.missing_windfarm_ids([payload.windfarm_id])
        if missing:
            raise HTTPException(
                status_code=400, detail=f"Unknown windfarm_id(s): {sorted(missing)}"
            )

    try:
        updated = await service.update_ppa(ppa_id, payload)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail=_DUPLICATE_DETAIL)

    if updated is None:
        raise HTTPException(status_code=404, detail="SCADA PPA not found")
    return updated


@router.delete("/{ppa_id}", response_model=ScadaPpa)
@audit_action(
    AuditAction.DELETE,
    "scada_ppa",
    lambda result, *args, **kwargs: str(kwargs.get("ppa_id", "unknown")),
    description="Deleted a SCADA PPA row",
)
async def delete_scada_ppa(
    ppa_id: int,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_superuser),
) -> Any:
    """Remove one farm from a PPA, leaving the contract's other legs in place."""
    deleted = await ScadaPpaService(db).delete_ppa(ppa_id)
    if deleted is None:
        raise HTTPException(status_code=404, detail="SCADA PPA not found")
    return deleted
