"""SCADA Builder endpoints (EPR-221): prompt → chart spec, and one dataset's rows.

Read-only. The spec call costs money, so it is superuser-only (the SCADA portal is
superuser-only anyway); a 503 from it tells the client to use its keyword fallback.
"""

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoints.scada import _service, _validated_farm
from app.core.deps import get_current_active_user, get_current_superuser, get_db
from app.models.user import User
from app.services.scada_builder_service import (
    DATASETS,
    MONTHS,
    BuilderSpecUnavailable,
    ScadaBuilderService,
    spec_from_prompt,
)
from app.services.scada_performance_service import PerformanceColumnsMissing

router = APIRouter()


class SpecRequest(BaseModel):
    prompt: str = Field(..., min_length=1, max_length=500)


@router.post("/spec")
async def post_spec(
    body: SpecRequest,
    current_user: User = Depends(get_current_superuser),
) -> Dict[str, Any]:
    try:
        return await spec_from_prompt(body.prompt.strip())
    except BuilderSpecUnavailable:
        raise HTTPException(status_code=503, detail="Chart assistant not available")


@router.get("/data")
async def get_data(
    farm: str = Query(...),
    dataset: str = Query(...),
    months: int = Query(12),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    if dataset not in DATASETS:
        raise HTTPException(status_code=422, detail=f"Unknown dataset: {dataset}")
    if months not in MONTHS:
        raise HTTPException(status_code=422, detail="months must be 3, 6, 12 or 24")
    await _validated_farm(await _service(db), farm)
    try:
        return await ScadaBuilderService(db).data(farm, dataset, months)
    except PerformanceColumnsMissing:
        raise HTTPException(status_code=503, detail="Performance data not available yet")
