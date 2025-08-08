"""Main API router."""

from fastapi import APIRouter

from app.api.v1.endpoints import (
    audit_logs,
    auth,
    bidzones,
    cables,
    control_areas,
    countries,
    eia,
    elexon,
    entsoe,
    generation_units,
    market_balance_areas,
    owners,
    projects,
    regions,
    states,
    substations,
    taipower,
    turbine_models,
    turbine_units,
    users,
    windfarms,
)

api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(audit_logs.router, prefix="/audit-logs", tags=["audit-logs"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(countries.router, prefix="/countries", tags=["countries"])
api_router.include_router(states.router, prefix="/states", tags=["states"])
api_router.include_router(regions.router, prefix="/regions", tags=["regions"])
api_router.include_router(bidzones.router, prefix="/bidzones", tags=["bidzones"])
api_router.include_router(
    market_balance_areas.router, prefix="/market-balance-areas", tags=["market-balance-areas"]
)
api_router.include_router(control_areas.router, prefix="/control-areas", tags=["control-areas"])
api_router.include_router(
    generation_units.router, prefix="/generation-units", tags=["generation-units"]
)
api_router.include_router(owners.router, prefix="/owners", tags=["owners"])
api_router.include_router(projects.router, prefix="/projects", tags=["projects"])
api_router.include_router(turbine_models.router, prefix="/turbine-models", tags=["turbine-models"])
api_router.include_router(windfarms.router, prefix="/windfarms", tags=["windfarms"])
api_router.include_router(substations.router, prefix="/substations", tags=["substations"])
api_router.include_router(turbine_units.router, prefix="/turbine-units", tags=["turbine-units"])
api_router.include_router(cables.router, prefix="/cables", tags=["cables"])
api_router.include_router(entsoe.router, prefix="/entsoe", tags=["entsoe"])
api_router.include_router(elexon.router, prefix="/elexon", tags=["elexon"])
api_router.include_router(eia.router, prefix="/eia", tags=["eia"])
api_router.include_router(taipower.router, prefix="/taipower", tags=["taipower"])
