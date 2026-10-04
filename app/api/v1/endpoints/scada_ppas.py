"""SCADA PPA endpoints (EPR-97) — the detailed, SCADA-only offtake contract structure.

Completely separate from ``/api/v1/ppas`` (the Perform PPA); the two share no models, schemas,
services or routes, and no Perform code path reads this table.

Two departures from the rest of ``/scada``, both deliberate:

* **Auth is ``get_current_internal_user``, not ``get_current_active_user``.** Every other SCADA
  endpoint relies on the frontend's ``canAccessScada`` gate; this one holds commercial contract
  terms, so the backend requires a superuser that is ALSO flagged ``is_internal`` (EnergyExe staff,
  EPR-143). A superuser without the flag gets a 403 on every route.
* **Reads and writes are audited.** ``@audit_action`` is not used on any other SCADA endpoint; contract
  data is worth the audit trail (see docs/features/audit-system.md). The decorator resolves ``db`` / ``request`` / ``current_user`` by kwarg name,
  so those names are load-bearing in the handler signatures below.
* **The register is SHARED per wind farm (EPR-143, Aje 2026-09-25: "one official set of terms per
  farm").** EPR-136's per-user privacy is gone: every internal user sees and edits the same rows.
  ``created_by_id`` is "entered by" provenance (stamped from the token on create, read-only on the
  response); the natural key is ``(ppa_code, windfarm_id)`` again.

* **Overlapping Active terms are refused at entry (EPR-143, Aje 2026-09-28, D-041).** Two Active
  rows on one farm whose date ranges share a day are "conflicting terms" -> 409. The service does
  the check inside the write transaction under row/farm locks (see its module docstring); this
  layer only maps :class:`ActiveTermsConflict` to 409 and :class:`ScadaPpaTermsError` to 400, and
  rolls the session back so the locks go.

``get_db`` comes from ``app.core.deps`` (the SCADA convention) — the copy in ``app.core.database`` is
a different function object, and test ``dependency_overrides`` only match the one actually imported.
"""

from typing import Any, List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.audit import audit_action
from app.core.deps import get_current_internal_user, get_db
from app.models.audit_log import AuditAction
from app.models.scada_ppa import ScadaPpa as ScadaPpaModel
from app.models.user import User
from app.schemas.scada_ppa import (
    PpaStatusLiteral,
    ScadaPpa,
    ScadaPpaCreate,
    ScadaPpaDeleteResult,
    ScadaPpaListResponse,
    ScadaPpaUpdate,
)
from app.services.scada_ppa_service import (
    ActiveTermsConflict,
    ScadaPpaService,
    ScadaPpaTermsError,
    validate_merged_terms,
)

logger = structlog.get_logger()

router = APIRouter()

_DUPLICATE_DETAIL = (
    "A PPA with this code already exists for one of those windfarms "
    "(the register is shared: ppa_code + windfarm must be unique across it)."
)


def _validate_merged_row(row: ScadaPpaModel, patch: ScadaPpaUpdate) -> None:
    """Re-check the cross-field rules against the row as it will be AFTER the patch.

    ``ScadaPpaUpdate`` can only validate pairs where the caller supplied both halves; a patch that
    sets just ``expiration_date`` has to be judged against the stored ``effective_date``. This is
    the cheap early answer on the row as read; the service applies the same rules again to the
    LOCKED row before writing, because two concurrent half-patches can each pass this check.
    """
    supplied = patch.model_dump(exclude_unset=True)
    merged = {
        field: supplied.get(field, getattr(row, field)) for field in ScadaPpaUpdate.model_fields
    }
    try:
        validate_merged_terms(merged)
    except ScadaPpaTermsError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("", response_model=ScadaPpaListResponse)
@audit_action(AuditAction.ACCESS, "scada_ppa", description="Listed SCADA PPAs")
async def list_scada_ppas(
    windfarm_id: Optional[int] = Query(None, description="Only this windfarm's contract legs"),
    ppa_code: Optional[str] = Query(None, description="Exact contract code"),
    ppa_status: Optional[PpaStatusLiteral] = Query(
        None, description="Draft/Active/Superseded/Terminated/Expired (anything else is a 422)"
    ),
    q: Optional[str] = Query(None, description="Substring match on buyer or code"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    current_user: User = Depends(get_current_internal_user),
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
    current_user: User = Depends(get_current_internal_user),
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
    current_user: User = Depends(get_current_internal_user),
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
    current_user: User = Depends(get_current_internal_user),
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
    current_user: User = Depends(get_current_internal_user),
) -> Any:
    """Create one PPA across one or more windfarms — N rows sharing the ``ppa_code``."""
    service = ScadaPpaService(db)

    missing = await service.missing_windfarm_ids(payload.windfarm_ids)
    if missing:
        raise HTTPException(status_code=400, detail=f"Unknown windfarm_id(s): {sorted(missing)}")
    taken = await service.existing_pairs(payload.ppa_code, payload.windfarm_ids)
    if taken:
        raise HTTPException(
            status_code=409,
            detail=f"{_DUPLICATE_DETAIL} Already present for windfarm_id(s): {taken}",
        )

    try:
        return await service.create_ppas(payload, user_id=current_user.id)
    except ActiveTermsConflict as exc:
        # Overlapping Active terms on one of the farms (D-041); nothing was written.
        await db.rollback()
        raise HTTPException(status_code=409, detail=str(exc))
    except ScadaPpaTermsError as exc:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(exc))
    except IntegrityError:
        # The pre-check above lost a race; the constraint is the authority.
        await db.rollback()
        raise HTTPException(status_code=409, detail=_DUPLICATE_DETAIL)


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
    current_user: User = Depends(get_current_internal_user),
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
    except ActiveTermsConflict as exc:
        # The merged row would be Active over another Active row on that farm (D-041).
        await db.rollback()
        raise HTTPException(status_code=409, detail=str(exc))
    except ScadaPpaTermsError as exc:
        # Re-validation on the LOCKED row failed (a concurrent patch changed the other half).
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(exc))
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail=_DUPLICATE_DETAIL)

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
    current_user: User = Depends(get_current_internal_user),
) -> Any:
    """Remove one farm from a PPA, leaving the contract's other legs in place."""
    deleted = await ScadaPpaService(db).delete_ppa(ppa_id)
    if deleted is None:
        raise HTTPException(status_code=404, detail="SCADA PPA not found")
    return deleted
