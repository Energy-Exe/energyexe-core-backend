"""Main API router."""

from fastapi import APIRouter

from app.api.v1.endpoints import (
    admin,
    agent_question_templates,
    alerts,
    audit_logs,
    auth,
    bidzones,
    brain_agent,
    cables,
    comparison,
    control_areas,
    countries,
    data_anomalies,
    exchange_rates,
    export,
    external_data_sources,
    financial_data,
    financial_entities,
    generation,
    generation_units,
    import_jobs,
    map as map_endpoints,
    market_balance_areas,
    opportunities,
    owners,
    performance_pipeline,
    portfolio,
    ppas,
    price_data,
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
    p50_targets,
)

api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(admin.router, prefix="/admin", tags=["admin"])
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
api_router.include_router(ppas.router, prefix="/ppas", tags=["ppas"])
api_router.include_router(
    financial_entities.router, prefix="/financial-entities", tags=["financial-entities"]
)
api_router.include_router(
    financial_data.router, prefix="/financial-data", tags=["financial-data"]
)
api_router.include_router(
    exchange_rates.router, prefix="/exchange-rates", tags=["exchange-rates"]
)

# New unified generation data endpoints
api_router.include_router(generation.router, prefix="/generation", tags=["generation"])

# Price data and analytics endpoints
api_router.include_router(price_data.router)

# Comparison and analytics endpoints
api_router.include_router(comparison.router, prefix="/comparison", tags=["comparison"])

# Portfolio management endpoints
api_router.include_router(portfolio.router, prefix="/portfolios", tags=["portfolios"])

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

# P50 target management and analysis endpoints
api_router.include_router(p50_targets.router, tags=["p50-targets"])

# Report commentary (LLM-generated) endpoints
api_router.include_router(report_commentary.router, prefix="/report-commentary", tags=["report-commentary"])

# Data export endpoints
api_router.include_router(export.router, prefix="/export", tags=["export"])

# Opportunity detection endpoints
api_router.include_router(opportunities.router, prefix="/opportunities", tags=["opportunities"])

# Performance analysis pipeline endpoints
api_router.include_router(
    performance_pipeline.router, prefix="/performance-pipeline", tags=["performance-pipeline"]
)

# Alerts and notifications endpoints
api_router.include_router(alerts.router, prefix="/alerts", tags=["alerts"])

# Brain Agent (Claude Agent SDK) endpoints
api_router.include_router(brain_agent.router, prefix="/brain-agent", tags=["brain-agent"])

# Map page endpoints (client-ui #44 — performance scores, financial metrics, AI interpretation)
api_router.include_router(map_endpoints.router, prefix="/map", tags=["map"])

# Agent question templates (per-route suggested questions for client portal)
api_router.include_router(
    agent_question_templates.router,
    prefix="/agent-question-templates",
    tags=["agent-question-templates"],
)

# Legacy endpoints - commented out as they're replaced by unified generation endpoints
# api_router.include_router(entsoe.router, prefix="/entsoe", tags=["entsoe"])
# api_router.include_router(elexon.router, prefix="/elexon", tags=["elexon"])
# api_router.include_router(eia.router, prefix="/eia", tags=["eia"])
# api_router.include_router(taipower.router, prefix="/taipower", tags=["taipower"])
