"""Ledger preview endpoints: the value-ledger sandbox's results for the internal preview page.

Read-only, internal staff only (superuser AND ``is_internal``, ``get_current_internal_user``): these
are sandbox figures, not the register, and never a client surface. Off on production unless
``SCADA_LEDGER_PREVIEW_ENABLED`` says otherwise (503). An unknown farm or asset is a 404, which the
portal shows as "no preview for this asset".
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.config import get_settings
from app.core.deps import get_current_internal_user
from app.models.user import User
from app.services.scada_ledger_preview_service import (
    LedgerPreviewNotFound,
    ScadaLedgerPreviewService,
)

router = APIRouter()

FARM = Query("hill_of_towie", min_length=1, max_length=100, pattern=r"^[a-z0-9_]+$")
ASSET = Query(None, min_length=1, max_length=20, pattern=r"^[A-Z0-9]+$")


def _service() -> ScadaLedgerPreviewService:
    if not get_settings().scada_ledger_preview_enabled:
        raise HTTPException(
            status_code=503, detail="Ledger preview is not enabled on this environment"
        )
    return ScadaLedgerPreviewService()


@router.get("/assets")
async def list_ledger_preview_assets(
    farm: str = FARM,
    current_user: User = Depends(get_current_internal_user),
) -> List[Dict[str, Any]]:
    """The assets of a farm that have a preview (empty list when none)."""
    return _service().assets(farm)


@router.get("/")
async def get_ledger_preview(
    farm: str = FARM,
    asset: Optional[str] = ASSET,
    current_user: User = Depends(get_current_internal_user),
) -> Dict[str, Any]:
    """One asset's preview document (schema ``ledger-preview/v1``)."""
    try:
        return _service().get(farm, asset)
    except LedgerPreviewNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))
