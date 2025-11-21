"""Main API router."""

from fastapi import APIRouter

from app.api.v1.endpoints import (
    audit_logs,
    auth,
    bidzones,
    cables,
    comparison,
    control_areas,
    countries,
    data_anomalies,
    external_data_sources,
    generation,
    generation_units,
    import_jobs,
    market_balance_areas,
    owners,
    projects,
    raw_data_fetch,
    regions,
    report_commentary,
    states,
    substations,
    turbine_models,
    turbine_units,
    users,
    weather_data,
    weather_imports,
    windfarms,
    windfarm_timeline,
    windfarm_reports,
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

# New unified generation data endpoints
api_router.include_router(generation.router, prefix="/generation", tags=["generation"])

# Comparison and analytics endpoints
api_router.include_router(comparison.router, prefix="/comparison", tags=["comparison"])

# Windfarm timeline and evolution endpoints
api_router.include_router(windfarm_timeline.router, prefix="/windfarms", tags=["windfarm-timeline"])

# External data sources endpoints
api_router.include_router(external_data_sources.router, prefix="/external-sources", tags=["external-sources"])

# Raw data fetching endpoints (fetch from APIs and store in generation_data_raw)
api_router.include_router(raw_data_fetch.router, prefix="/raw-data", tags=["raw-data"])

# Data quality and anomaly detection endpoints
api_router.include_router(data_anomalies.router, prefix="/data-anomalies", tags=["data-anomalies"])

# Scheduled import jobs management
api_router.include_router(import_jobs.router, prefix="/import-jobs", tags=["import-jobs"])

# Weather data endpoints
api_router.include_router(weather_data.router)

# Weather import jobs management
api_router.include_router(weather_imports.router, prefix="/weather-imports", tags=["weather-imports"])

# Windfarm performance reports endpoints
api_router.include_router(windfarm_reports.router, tags=["windfarm-reports"])

# Report commentary (LLM-generated) endpoints
api_router.include_router(report_commentary.router, prefix="/report-commentary", tags=["report-commentary"])

# Legacy endpoints - commented out as they're replaced by unified generation endpoints
# api_router.include_router(entsoe.router, prefix="/entsoe", tags=["entsoe"])
# api_router.include_router(elexon.router, prefix="/elexon", tags=["elexon"])
# api_router.include_router(eia.router, prefix="/eia", tags=["eia"])
# api_router.include_router(taipower.router, prefix="/taipower", tags=["taipower"])
