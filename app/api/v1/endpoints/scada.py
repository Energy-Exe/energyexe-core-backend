"""SCADA portal endpoints — the 14-chart dashboard over gold schema `scada`.

Thin wrappers over ScadaService (one endpoint per chart; SQL source of truth is
energyexe-scada-pipeline/docs/ui/queries.sql). All endpoints 503 gracefully when
the scada schema is absent (prod before the scada prod cut) and 404 on unknown
farm slugs. Any authenticated (active, approved) user may read — no farm-level
ACL yet by explicit product decision; revisit before onboarding a second client.
"""

from datetime import date
from typing import Any, Dict, List

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.schemas.scada import ScadaFarmsResponse
from app.services.scada_service import ScadaService, scada_schema_present

logger = structlog.get_logger(__name__)
router = APIRouter()

DEFAULT_FARM = "hill_of_towie"


async def _service(db: AsyncSession) -> ScadaService:
    if not await scada_schema_present(db):
        raise HTTPException(status_code=503, detail="SCADA data not available")
    return ScadaService(db)


async def _validated_farm(service: ScadaService, farm: str) -> str:
    if farm not in await service.farm_slugs():
        raise HTTPException(status_code=404, detail=f"Unknown SCADA farm: {farm}")
    return farm


@router.get("/farms", response_model=ScadaFarmsResponse)
async def get_farms(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """Farm metadata + data-through dates for the freshness header."""
    service = await _service(db)
    return await service.farms()


@router.get("/heartbeat")
async def get_heartbeat(
    days: int = Query(90, ge=7, le=365),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    return await service.heartbeat(days=days)


@router.get("/energy-waterfall")
async def get_energy_waterfall(
    farm: str = Query(DEFAULT_FARM),
    start: date = Query(...),
    end: date = Query(...),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.energy_waterfall(farm=farm, start=start, end=end)


@router.get("/revenue-waterfall")
async def get_revenue_waterfall(
    farm: str = Query(DEFAULT_FARM),
    start: date = Query(...),
    end: date = Query(...),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.revenue_waterfall(farm=farm, start=start, end=end)


@router.get("/availability")
async def get_availability(
    farm: str = Query(DEFAULT_FARM),
    days: int = Query(60, ge=7, le=366),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.availability(farm=farm, days=days)


@router.get("/completeness")
async def get_completeness(
    farm: str = Query("penmanshiel"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.completeness(farm=farm)


@router.get("/degradation")
async def get_degradation(
    farm: str = Query(DEFAULT_FARM),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.degradation(farm=farm)


@router.get("/downtime-fingerprint")
async def get_downtime_fingerprint(
    farm: str = Query(DEFAULT_FARM),
    year: int = Query(..., ge=2015, le=2100),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.downtime_fingerprint(farm=farm, year=year)


@router.get("/alarm-pareto")
async def get_alarm_pareto(
    farm: str = Query(DEFAULT_FARM),
    year: int = Query(..., ge=2015, le=2100),
    limit: int = Query(10, ge=1, le=50),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.alarm_pareto(farm=farm, year=year, limit=limit)


@router.get("/cumulative-losses")
async def get_cumulative_losses(
    farm: str = Query(DEFAULT_FARM),
    year: int = Query(..., ge=2015, le=2100),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> List[Dict[str, Any]]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.cumulative_losses(farm=farm, year=year)


@router.get("/settlement-recon")
async def get_settlement_recon(
    farm: str = Query(DEFAULT_FARM),
    year: int = Query(..., ge=2015, le=2100),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.settlement_recon(farm=farm, year=year)


@router.get("/scada-vs-boav")
async def get_scada_vs_boav(
    farm: str = Query(DEFAULT_FARM),
    year: int = Query(..., ge=2015, le=2100),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.scada_vs_boav(farm=farm, year=year)


@router.get("/aeroup")
async def get_aeroup(
    farm: str = Query(DEFAULT_FARM),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.aeroup(farm=farm)


@router.get("/annual-cost")
async def get_annual_cost(
    start_year: int = Query(2022, ge=2015, le=2100),
    end_year: int = Query(2024, ge=2015, le=2100),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    if end_year < start_year:
        raise HTTPException(status_code=422, detail="end_year must be >= start_year")
    service = await _service(db)
    return await service.annual_cost(start_year=start_year, end_year=end_year)


@router.get("/method-mix")
async def get_method_mix(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> List[Dict[str, Any]]:
    service = await _service(db)
    return await service.method_mix()


@router.get("/league")
async def get_league(
    farm: str = Query(DEFAULT_FARM),
    year: int = Query(..., ge=2015, le=2100),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> List[Dict[str, Any]]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.league(farm=farm, year=year)


# ---- silver-viz charts (2026-08-03 build) -----------------------------------
# Same guard rails as above; every endpoint is farm+year scoped unless the
# chart is inherently multi-year. Farm-limited signals return empty payloads.

_FARM_YEAR_CHARTS = {
    "curtailment-episodes": "curtailment_episodes",
    "storms": "storms",
    "icing": "icing",
    "outage-gantt": "outage_gantt",
    "state-hours": "state_hours",
    "alarm-heatmap": "alarm_heatmap",
    "temp-cohort": "temp_cohort",
    "yaw-misalignment": "yaw_misalignment",
    "watchdog": "watchdog",
    "layout-map": "layout_map",
    "wake": "wake",
    "wind-rose": "wind_rose",
    "turbulence": "turbulence",
    "power-curve-density": "power_curve_density",
    "grid-quality": "grid_quality",
    "pq-envelope": "pq_envelope",
    "energy-recon": "energy_recon",
    "qc-bits": "qc_bits",
    # round-4: reliability & losses page + folded additions
    "loss-league": "loss_league",
    "loss-monthly": "loss_monthly",
    "mtbf-mttr": "mtbf_mttr",
    "alarm-transitions": "alarm_transitions",
    "fleet-timers": "fleet_timers",
    "duration-curve": "duration_curve",
    "pitch-curve": "pitch_curve",
    "diurnal": "diurnal",
    "density-curve": "density_curve",
    "ramps": "ramps",
}


def _register_farm_year_chart(path: str, method_name: str) -> None:
    @router.get(f"/{path}", name=f"get_{method_name}")
    async def _endpoint(
        farm: str = Query(DEFAULT_FARM),
        year: int = Query(..., ge=2015, le=2100),
        current_user: User = Depends(get_current_active_user),
        db: AsyncSession = Depends(get_db),
    ) -> Dict[str, Any]:
        service = await _service(db)
        await _validated_farm(service, farm)
        return await getattr(service, method_name)(farm=farm, year=year)


for _path, _method in _FARM_YEAR_CHARTS.items():
    _register_farm_year_chart(_path, _method)


@router.get("/wind-index")
async def get_wind_index(
    farm: str = Query(DEFAULT_FARM),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.wind_index(farm=farm)


@router.get("/midwind-fade")
async def get_midwind_fade(
    farm: str = Query(DEFAULT_FARM),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Per-turbine mean power in the 7-8 m/s band across years."""
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.midwind_fade(farm=farm)


@router.get("/portfolio")
async def get_portfolio(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """All farms on one normalized monthly CF/availability strip."""
    service = await _service(db)
    return await service.portfolio()


@router.get("/self-consumption")
async def get_self_consumption(
    farm: str = Query(DEFAULT_FARM),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.self_consumption(farm=farm)


# --- turbine detail page: one dossier per turbine ---


@router.get("/turbines")
async def get_turbines(
    farm: str = Query(DEFAULT_FARM),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Dict[str, Any]:
    """Turbine picker payload for the detail page."""
    service = await _service(db)
    await _validated_farm(service, farm)
    return await service.turbines(farm=farm)


# turbine + farm + year charts
_TURBINE_YEAR_CHARTS = {
    "turbine-summary": "turbine_summary",
    "turbine-power-curve": "turbine_power_curve",
    "turbine-daily-rel": "turbine_daily_rel",
    "turbine-timers": "turbine_timers",
    "turbine-alarm-timeline": "turbine_alarm_timeline",
    "turbine-alarm-pareto": "turbine_alarm_pareto",
    "turbine-rose": "turbine_rose",
    "turbine-losses": "turbine_losses",
}

# turbine + farm whole-history charts (the life-view lanes)
_TURBINE_CHARTS = {
    "turbine-life": "turbine_life",
    "turbine-perf-index": "turbine_perf_index",
    "turbine-temp-trend": "turbine_temp_trend",
}


def _register_turbine_year_chart(path: str, method_name: str) -> None:
    @router.get(f"/{path}", name=f"get_{method_name}")
    async def _endpoint(
        turbine: str = Query(..., min_length=1, max_length=32),
        farm: str = Query(DEFAULT_FARM),
        year: int = Query(..., ge=2015, le=2100),
        current_user: User = Depends(get_current_active_user),
        db: AsyncSession = Depends(get_db),
    ) -> Dict[str, Any]:
        service = await _service(db)
        await _validated_farm(service, farm)
        return await getattr(service, method_name)(farm=farm, turbine=turbine, year=year)


def _register_turbine_chart(path: str, method_name: str) -> None:
    @router.get(f"/{path}", name=f"get_{method_name}")
    async def _endpoint(
        turbine: str = Query(..., min_length=1, max_length=32),
        farm: str = Query(DEFAULT_FARM),
        current_user: User = Depends(get_current_active_user),
        db: AsyncSession = Depends(get_db),
    ) -> Dict[str, Any]:
        service = await _service(db)
        await _validated_farm(service, farm)
        return await getattr(service, method_name)(farm=farm, turbine=turbine)


for _path, _method in _TURBINE_YEAR_CHARTS.items():
    _register_turbine_year_chart(_path, _method)

for _path, _method in _TURBINE_CHARTS.items():
    _register_turbine_chart(_path, _method)
