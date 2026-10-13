"""SCADA opportunities endpoints — the Revenue-at-Risk register over gold schema ``scada``.

Thin wrappers over ScadaOpportunityService. 503 gracefully until the findings tables exist (before
the pipeline's persist step lands on this env) and 404 on unknown farm slugs. Same auth posture as
the rest of /scada — any authenticated (active, approved) user may read (D-014) — except a farm the
pipeline marks internal (``scada.dim_farm.internal``, D-081), which only EnergyExe staff (superuser and
``is_internal``) may read: hidden from the farm list and 403 on a direct request. The client surfaces this
only inside the superadmin-gated SCADA portal (D-022).

D-081 (EPR-183): every route takes ``asset``; it may be omitted when the farm publishes one asset and is
required when it publishes several (Raggovidda RG1/RG2).
"""

from typing import Any, List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.agent_access import is_internal_staff
from app.core.deps import get_current_active_user, get_db
from app.models.scada_finding_action import SCADA_FINDING_WRITABLE_STATUSES
from app.models.user import User
from app.schemas.scada_opportunity import (
    ScadaAssetStatement,
    ScadaFindingActionResult,
    ScadaFindingActionUpdate,
    ScadaInterpretationRecord,
    ScadaOpportunity,
    ScadaOpportunityByYear,
    ScadaOpportunityListResponse,
    ScadaOpportunitySummary,
    ScadaOpportunityText,
    ScadaTrigger,
)
from app.services.scada_finding_service import ScadaFindingService
from app.services.scada_opportunity_service import (
    ScadaOpportunityService,
    scada_asset_keys_present,
    scada_by_year_present,
    scada_opportunities_present,
    scada_records_present,
)

logger = structlog.get_logger(__name__)
router = APIRouter()

DEFAULT_FARM = "hill_of_towie"


_ASSET_QUERY = Query(
    None, description="Asset code (e.g. RG1); required when the farm publishes several assets"
)


async def _service(db: AsyncSession, user: User) -> ScadaOpportunityService:
    if not await scada_opportunities_present(db):
        raise HTTPException(status_code=503, detail="SCADA opportunities not available")
    if not await scada_asset_keys_present(db):
        raise HTTPException(
            status_code=503,
            detail="SCADA opportunities schema predates per-asset keys (pipeline migration e5b8c2d4f6a1)",
        )
    return ScadaOpportunityService(db, include_internal=is_internal_staff(user))


async def _validated_farm(service: ScadaOpportunityService, farm: str) -> str:
    if farm in await service.farm_slugs():
        return farm
    if not service.include_internal and farm in await service.farm_slugs(include_internal=True):
        raise HTTPException(status_code=403, detail="Internal EnergyExe access required")
    raise HTTPException(status_code=404, detail=f"No SCADA opportunities for farm: {farm}")


async def _validated_asset(
    service: ScadaOpportunityService, farm: str, asset: Optional[str]
) -> str:
    """The asset to serve: the one named, or the farm's only asset. Several assets and none named is a 400."""
    codes = [a["asset_code"] for a in await service.assets(farm)]
    if asset is None:
        if len(codes) == 1:
            return codes[0]
        if not codes:
            raise HTTPException(status_code=404, detail=f"No register run for farm: {farm}")
        raise HTTPException(
            status_code=400,
            detail=f"farm {farm} publishes several assets ({', '.join(codes)}); pass asset",
        )
    if asset not in codes:
        raise HTTPException(status_code=404, detail=f"No asset {asset} for farm {farm}")
    return asset


async def _scoped(
    db: AsyncSession, user: User, farm: str, asset: Optional[str]
) -> tuple[ScadaOpportunityService, str]:
    service = await _service(db, user)
    await _validated_farm(service, farm)
    return service, await _validated_asset(service, farm, asset)


_NOT_PUBLISHED = (
    "Interpretation not published on this environment "
    "(scada.opportunity_record / opportunity_statement not materialised)"
)


async def _records_or_404(db: AsyncSession) -> None:
    """404 (not 503) until the interpretation tables exist: the client renders 'not yet published' and never
    reports it, as for the per-year view."""
    if not await scada_records_present(db):
        raise HTTPException(status_code=404, detail=_NOT_PUBLISHED)


def _display_name(user: User) -> str:
    """Human label for the actor — matches the lifecycle_actor the read side derives in SQL."""
    full = f"{user.first_name or ''} {user.last_name or ''}".strip()
    return full or user.username or user.email


@router.get("/", response_model=ScadaOpportunityListResponse)
async def list_opportunities(
    farm: str = Query(DEFAULT_FARM),
    cls: Optional[str] = Query(
        None, description="REALIZED/RECOVERABLE/CURTAILMENT/CONDITIONAL/CONTEXT/ALERT"
    ),
    domain: Optional[str] = Query(
        None, description="OPS/MKT/FIN/DIAG/DET/DQ/FND (from the trigger catalog)"
    ),
    trigger: Optional[str] = Query(None, description="A trigger code, e.g. OPS_01"),
    additive_only: bool = Query(
        False, description="Only the priced lines that sum into a class total"
    ),
    min_gbp: Optional[float] = Query(
        None, description="Minimum rank amount (|gbp/yr| or conditional exposure), asset currency"
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    asset: Optional[str] = _ASSET_QUERY,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """The ranked Revenue-at-Risk register for one asset (amount-desc). Summary is the asset-level snapshot."""
    service, code = await _scoped(db, current_user, farm, asset)
    return await service.list_opportunities(
        farm,
        asset=code,
        cls=cls,
        domain=domain,
        trigger=trigger,
        additive_only=additive_only,
        min_gbp=min_gbp,
        limit=limit,
        offset=offset,
    )


@router.get("/summary", response_model=ScadaOpportunitySummary)
async def get_summary(
    farm: str = Query(DEFAULT_FARM),
    asset: Optional[str] = _ASSET_QUERY,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """Headline amount, per-class rollup, currency and run provenance (as-of) for one asset."""
    service, code = await _scoped(db, current_user, farm, asset)
    summary = await service.summary(farm, code)
    if summary is None:
        raise HTTPException(status_code=404, detail=f"No register run for farm: {farm}")
    return summary


@router.get("/triggers", response_model=List[ScadaTrigger])
async def list_triggers(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """The schema-v7 trigger catalog (labels/domain/persona/action for filter chips)."""
    service = await _service(db, current_user)
    return await service.triggers()


@router.get("/by-year", response_model=ScadaOpportunityByYear)
async def get_by_year(
    farm: str = Query(DEFAULT_FARM),
    asset: Optional[str] = _ASSET_QUERY,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """Revenue at risk per calendar year (EPR-131): the real-price and constant-price series per
    (year, trigger), class totals per year, and the fitted trend over full years.

    Declared above ``/{id}`` so it is not parsed as an id. 404 (not 503) until the by-year tables
    are materialised on this environment — the client renders that as 'not yet loaded' and never
    reports it, whereas a 503 would page the error tracker.
    """
    service, code = await _scoped(db, current_user, farm, asset)
    if not await scada_by_year_present(db):
        raise HTTPException(
            status_code=404,
            detail=(
                "Per-year view not available on this environment "
                "(scada.opportunity_register_by_year not materialised)"
            ),
        )
    out = await service.by_year(farm, code)
    if out is None:
        raise HTTPException(status_code=404, detail=f"No per-year register for farm: {farm}")
    return out


@router.get("/statement", response_model=ScadaAssetStatement)
async def get_statement(
    farm: str = Query(DEFAULT_FARM),
    asset: Optional[str] = _ASSET_QUERY,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """The asset statement (EPR-181): each value class with its own sum, the overlap adjustment, the
    reporting floor, the ranked findings and the coverage section. Never a sum across classes or currencies.
    """
    service, code = await _scoped(db, current_user, farm, asset)
    await _records_or_404(db)
    out = await service.statement(farm, code)
    if out is None:
        raise HTTPException(status_code=404, detail=f"No asset statement for farm: {farm}")
    return out


@router.get("/conditions", response_model=List[ScadaInterpretationRecord])
async def list_conditions(
    farm: str = Query(DEFAULT_FARM),
    asset: Optional[str] = _ASSET_QUERY,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """The condition records (EPR-180): exposure per event, never a probability or a lead time, with the
    lifecycle state of each record's register key."""
    service, code = await _scoped(db, current_user, farm, asset)
    await _records_or_404(db)
    return await service.conditions(farm, code)


@router.put("/actions", response_model=ScadaFindingActionResult)
async def set_finding_action(
    payload: ScadaFindingActionUpdate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """Record the human lifecycle state of a finding (Phase 7b).

    Keyed by the finding's stable natural key ``(farm, asset_code, trigger, scope, cls)`` — never the
    volatile register ``id``; ``asset_code`` may be omitted when the farm publishes one asset (D-081).
    Upserts the ``scada_finding_action`` row and attributes it to the caller. The lifecycle then rides
    on the register rows via the list/detail read joins.
    """
    if payload.status not in SCADA_FINDING_WRITABLE_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"status must be one of {sorted(SCADA_FINDING_WRITABLE_STATUSES)}",
        )
    # 503 until the register exists on this env; 403/404 on a farm the caller may not read
    _, code = await _scoped(db, current_user, payload.farm, payload.asset_code)
    action = await ScadaFindingService(db).set_action(
        farm=payload.farm,
        asset_code=code,
        trigger=payload.trigger,
        scope=payload.scope,
        cls=payload.cls,
        status=payload.status,
        notes=payload.notes,
        user_id=current_user.id,
    )
    if action is None:
        raise HTTPException(status_code=404, detail="No SCADA finding matches that key")
    return ScadaFindingActionResult(
        farm=action.farm,
        asset_code=action.asset_code,
        trigger=action.trigger,
        scope=action.scope,
        cls=action.cls,
        status=action.status,
        notes=action.notes,
        acknowledged_at=action.acknowledged_at,
        resolved_at=action.resolved_at,
        updated_at=action.updated_at,
        actor=_display_name(current_user),
    )


@router.get("/{id}", response_model=ScadaOpportunity)
async def get_opportunity(
    id: int,
    farm: str = Query(DEFAULT_FARM),
    asset: Optional[str] = _ASSET_QUERY,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """One register line by (farm, asset, id) — evidence for the detail drawer."""
    service, code = await _scoped(db, current_user, farm, asset)
    row = await service.get(farm, id, code)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No opportunity {id} for farm {farm}")
    return row


@router.get("/{id}/text", response_model=ScadaOpportunityText)
async def get_opportunity_text(
    id: int,
    farm: str = Query(DEFAULT_FARM),
    limit: int = Query(
        50, ge=1, le=500, description="Findings returned (ranked); findings_total counts all"
    ),
    asset: Optional[str] = _ASSET_QUERY,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """ISSUE / EVIDENCE / ACTION for one register row: the line and its findings, or the condition records."""
    service, code = await _scoped(db, current_user, farm, asset)
    await _records_or_404(db)
    out = await service.text_for(farm, id, asset=code, limit=limit)
    if out is None:
        raise HTTPException(status_code=404, detail=f"No opportunity {id} for farm {farm}")
    return out
