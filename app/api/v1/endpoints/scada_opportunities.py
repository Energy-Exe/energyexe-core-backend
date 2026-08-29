"""SCADA opportunities endpoints — the Revenue-at-Risk register over gold schema ``scada``.

Thin wrappers over ScadaOpportunityService. 503 gracefully until the findings tables exist (before
the pipeline's persist step lands on this env) and 404 on unknown farm slugs. Same auth posture as
the rest of /scada — any authenticated (active, approved) user may read, no farm-level ACL yet
(D-014). The client surfaces this only inside the superadmin-gated SCADA portal (D-022).
"""

from typing import Any, List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.scada_finding_action import SCADA_FINDING_WRITABLE_STATUSES
from app.models.user import User
from app.schemas.scada_opportunity import (
    ScadaFindingActionResult,
    ScadaFindingActionUpdate,
    ScadaOpportunity,
    ScadaOpportunityListResponse,
    ScadaOpportunitySummary,
    ScadaTrigger,
)
from app.services.scada_finding_service import ScadaFindingService
from app.services.scada_opportunity_service import (
    ScadaOpportunityService,
    scada_opportunities_present,
)

logger = structlog.get_logger(__name__)
router = APIRouter()

DEFAULT_FARM = "hill_of_towie"


async def _service(db: AsyncSession) -> ScadaOpportunityService:
    if not await scada_opportunities_present(db):
        raise HTTPException(status_code=503, detail="SCADA opportunities not available")
    return ScadaOpportunityService(db)


async def _validated_farm(service: ScadaOpportunityService, farm: str) -> str:
    if farm not in await service.farm_slugs():
        raise HTTPException(status_code=404, detail=f"No SCADA opportunities for farm: {farm}")
    return farm


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
        None, description="Minimum rank £ (|gbp/yr| or conditional exposure)"
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """The ranked Revenue-at-Risk register for a farm (£-desc). Summary is the farm-level snapshot."""
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.list_opportunities(
        farm,
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
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """Headline £, per-class rollup, and run provenance (as-of) for a farm."""
    service = await _service(db)
    await _validated_farm(service, farm)
    summary = await service.summary(farm)
    if summary is None:
        raise HTTPException(status_code=404, detail=f"No register run for farm: {farm}")
    return summary


@router.get("/triggers", response_model=List[ScadaTrigger])
async def list_triggers(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """The schema-v7 trigger catalog (labels/domain/persona/action for filter chips)."""
    service = await _service(db)
    return await service.triggers()


@router.put("/actions", response_model=ScadaFindingActionResult)
async def set_finding_action(
    payload: ScadaFindingActionUpdate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """Record the human lifecycle state of a finding (Phase 7b).

    Keyed by the finding's stable natural key ``(farm, trigger, scope, cls)`` — never the volatile
    register ``id``. Upserts the ``scada_finding_action`` row and attributes it to the caller. The
    lifecycle then rides on the register rows via the list/detail read joins.
    """
    if payload.status not in SCADA_FINDING_WRITABLE_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"status must be one of {sorted(SCADA_FINDING_WRITABLE_STATUSES)}",
        )
    await _service(db)  # 503 until the register exists on this env
    action = await ScadaFindingService(db).set_action(
        farm=payload.farm,
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
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """One register line by (farm, id) — evidence for the detail drawer."""
    service = await _service(db)
    await _validated_farm(service, farm)
    row = await service.get(farm, id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"No opportunity {id} for farm {farm}")
    return row
