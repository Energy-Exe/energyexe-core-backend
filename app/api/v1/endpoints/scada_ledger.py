"""SCADA value ledger endpoints: the new method's results per asset (rollout Stage 3).

Read-only, internal staff only (superuser AND ``is_internal``, ``get_current_internal_user``). Off on production
unless ``SCADA_LEDGER_ENABLED`` says otherwise (503). A farm without ledger tables or runs lists no assets, and an
asset without a ledger run is a 404, which the portal shows as today's view. A stale ledger (built beside an
older register run) is a 200 with ``status: "stale"`` and no document.
"""

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.deps import get_current_internal_user, get_db
from app.models.user import User
from app.schemas.scada_ledger import LedgerAsset, LedgerResponse
from app.services.scada_ledger_service import ScadaLedgerService, scada_ledger_present

router = APIRouter()

FARM = Query(..., min_length=1, max_length=64, pattern=r"^[a-z0-9_]+$")
ASSET = Query(..., min_length=1, max_length=16, pattern=r"^[A-Z0-9]+$")


def _enabled() -> None:
    if not get_settings().scada_ledger_enabled:
        raise HTTPException(
            status_code=503, detail="Value ledger is not enabled on this environment"
        )


@router.get("/assets", response_model=List[LedgerAsset])
async def list_ledger_assets(
    farm: str = FARM,
    current_user: User = Depends(get_current_internal_user),
    db: AsyncSession = Depends(get_db),
) -> List[dict]:
    """The farm's assets with a ledger run, each ``ok`` or ``stale`` (empty when none)."""
    _enabled()
    if not await scada_ledger_present(db):
        return []
    return await ScadaLedgerService(db).assets(farm)


@router.get("/", response_model=LedgerResponse)
async def get_ledger(
    farm: str = FARM,
    asset: str = ASSET,
    current_user: User = Depends(get_current_internal_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """One asset's ledger document (format ``value-ledger/2``) and the finding statuses on its causes."""
    _enabled()
    ledger = (
        await ScadaLedgerService(db).get(farm, asset) if await scada_ledger_present(db) else None
    )
    if ledger is None:
        raise HTTPException(
            status_code=404, detail=f"No value ledger for farm {farm} asset {asset}"
        )
    return ledger
