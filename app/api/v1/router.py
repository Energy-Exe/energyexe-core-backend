"""Main API router."""

from fastapi import APIRouter

from app.api.v1.endpoints import auth, countries, states, users, regions, bidzones, market_balance_areas, control_areas, owners, projects, turbine_models

api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(countries.router, prefix="/countries", tags=["countries"])
api_router.include_router(states.router, prefix="/states", tags=["states"])
api_router.include_router(regions.router, prefix="/regions", tags=["regions"])
api_router.include_router(bidzones.router, prefix="/bidzones", tags=["bidzones"])
api_router.include_router(market_balance_areas.router, prefix="/market-balance-areas", tags=["market-balance-areas"])
api_router.include_router(control_areas.router, prefix="/control-areas", tags=["control-areas"])
api_router.include_router(owners.router, prefix="/owners", tags=["owners"])
api_router.include_router(projects.router, prefix="/projects", tags=["projects"])
api_router.include_router(turbine_models.router, prefix="/turbine-models", tags=["turbine-models"])
