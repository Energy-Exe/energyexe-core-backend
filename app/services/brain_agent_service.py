"""Brain Agent service — orchestrates Claude Agent SDK sessions with energy data tools."""

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Optional

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    SystemMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

from app.core.config import get_settings
from app.services.brain_agent_tools import (
    DEFAULT_SESSION,
    ENERGYEXE_TOOL_NAMES,
    energyexe_mcp_server,
    set_db_session,
    clear_db_session,
    set_user_id,
    clear_user_id,
)

logger = structlog.get_logger(__name__)

# Session TTL: clean up sessions idle for more than 30 minutes
SESSION_TTL_SECONDS = 30 * 60
MAX_CONCURRENT_SESSIONS = 20


@dataclass
class SSEEvent:
    """A single SSE event to stream to the client."""

    event_type: str  # text_delta, tool_use, tool_result, system, result, error
    data: Dict[str, Any]


@dataclass
class AgentSession:
    """Tracks a Claude Agent SDK session."""

    session_id: str
    user_id: int
    client: ClaudeSDKClient
    created_at: float
    last_activity: float
    is_busy: bool = False


class BrainAgentService:
    """Manages ClaudeSDKClient sessions and streams responses as SSE events."""

    _sessions: Dict[str, AgentSession] = {}

    def __init__(self, db: AsyncSession):
        self.db = db

    async def chat(
        self,
        user_id: int,
        session_id: Optional[str],
        prompt: str,
        user_name: Optional[str] = None,
    ) -> AsyncGenerator[SSEEvent, None]:
        """Send a prompt to the agent and yield SSE events."""
        if not session_id:
            session_id = str(uuid.uuid4())

        # Clean up stale sessions
        self._cleanup_stale_sessions()

        # Set up DB session and user context for MCP tools
        set_db_session(DEFAULT_SESSION, self.db)
        set_user_id(DEFAULT_SESSION, user_id)

        try:
            session = await self._get_or_create_session(user_id, session_id, user_name)
            session.is_busy = True
            session.last_activity = time.time()

            # Yield session_id so frontend knows it
            yield SSEEvent(
                event_type="session",
                data={"session_id": session_id},
            )

            # Send the query
            await session.client.query(prompt)

            # Stream response messages
            async for message in session.client.receive_messages():
                async for event in self._process_message(message):
                    yield event

                # ResultMessage means the agent is done
                if isinstance(message, ResultMessage):
                    break

        except Exception as e:
            logger.error("brain_agent_error", error=str(e), session_id=session_id)
            yield SSEEvent(
                event_type="error",
                data={"message": str(e), "code": "agent_error"},
            )
        finally:
            if session_id in self._sessions:
                self._sessions[session_id].is_busy = False
            clear_db_session(DEFAULT_SESSION)
            clear_user_id(DEFAULT_SESSION)

    async def interrupt(self, session_id: str, user_id: int) -> bool:
        """Interrupt the current agent task. Validates session ownership."""
        session = self._sessions.get(session_id)
        if session and session.user_id == user_id and session.is_busy:
            try:
                await session.client.interrupt()
                return True
            except Exception as e:
                logger.error("brain_agent_interrupt_error", error=str(e))
        return False

    async def end_session(self, session_id: str, user_id: int) -> bool:
        """End and clean up a session. Validates session ownership."""
        session = self._sessions.get(session_id)
        if session and session.user_id == user_id:
            self._sessions.pop(session_id, None)
            return True
        return False

    def list_sessions(self, user_id: int) -> list:
        """List active sessions for a user."""
        return [
            {
                "session_id": s.session_id,
                "created_at": s.created_at,
                "last_activity": s.last_activity,
                "is_busy": s.is_busy,
            }
            for s in self._sessions.values()
            if s.user_id == user_id
        ]

    async def _get_or_create_session(
        self, user_id: int, session_id: str, user_name: Optional[str] = None
    ) -> AgentSession:
        """Get existing session or create a new one."""
        if session_id in self._sessions:
            return self._sessions[session_id]

        # Enforce session limit
        user_sessions = [s for s in self._sessions.values() if s.user_id == user_id]
        if len(user_sessions) >= MAX_CONCURRENT_SESSIONS:
            # Remove oldest
            oldest = min(user_sessions, key=lambda s: s.last_activity)
            self._sessions.pop(oldest.session_id, None)

        # Create temp working directory
        work_dir = Path(f"/tmp/brain-agent/{user_id}/{session_id}")
        work_dir.mkdir(parents=True, exist_ok=True)

        settings = get_settings()

        system_prompt = self._build_system_prompt(user_name)

        options = ClaudeAgentOptions(
            system_prompt=system_prompt,
            allowed_tools=[
                "Read",
                "Write",
                "Edit",
                "Bash",
                "Glob",
                "Grep",
                "WebSearch",
                "WebFetch",
                *ENERGYEXE_TOOL_NAMES,
            ],
            mcp_servers={"energyexe": energyexe_mcp_server},
            cwd=work_dir,
            max_turns=20,
            max_budget_usd=2.0,
            permission_mode="bypassPermissions",
            model=getattr(settings, "BRAIN_MODEL", "claude-sonnet-4-20250514"),
        )

        client = ClaudeSDKClient(options=options)
        # Enter the async context manager
        await client.__aenter__()

        session = AgentSession(
            session_id=session_id,
            user_id=user_id,
            client=client,
            created_at=time.time(),
            last_activity=time.time(),
        )
        self._sessions[session_id] = session
        return session

    async def _process_message(self, message) -> AsyncGenerator[SSEEvent, None]:
        """Convert an Agent SDK message into SSE events."""
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    yield SSEEvent(
                        event_type="text_delta",
                        data={"text": block.text},
                    )
                elif isinstance(block, ToolUseBlock):
                    yield SSEEvent(
                        event_type="tool_use",
                        data={
                            "tool_name": block.name,
                            "tool_id": block.id,
                            "input": block.input if isinstance(block.input, dict) else {},
                        },
                    )

        elif isinstance(message, UserMessage):
            # UserMessage content can include ToolResultBlocks
            for block in message.content:
                if isinstance(block, ToolResultBlock):
                    content_text = ""
                    if isinstance(block.content, str):
                        content_text = block.content
                    elif isinstance(block.content, list):
                        content_text = " ".join(
                            b.get("text", "") if isinstance(b, dict) else str(b)
                            for b in block.content
                        )

                    # Truncate long tool results for frontend display
                    summary = content_text[:500] + "..." if len(content_text) > 500 else content_text
                    yield SSEEvent(
                        event_type="tool_result",
                        data={
                            "tool_id": block.tool_use_id,
                            "summary": summary,
                        },
                    )

        elif isinstance(message, SystemMessage):
            yield SSEEvent(
                event_type="system",
                data={
                    "subtype": message.subtype if hasattr(message, "subtype") else "info",
                    "message": str(message.data) if hasattr(message, "data") else str(message),
                },
            )

        elif isinstance(message, ResultMessage):
            yield SSEEvent(
                event_type="result",
                data={
                    "num_turns": message.num_turns if hasattr(message, "num_turns") else 0,
                    "duration_ms": message.duration_ms if hasattr(message, "duration_ms") else 0,
                    "cost_usd": message.total_cost_usd
                    if hasattr(message, "total_cost_usd")
                    else None,
                    "session_id": None,  # Will be set by the caller
                },
            )

    def _cleanup_stale_sessions(self):
        """Remove sessions that have been idle beyond TTL."""
        now = time.time()
        stale = [
            sid
            for sid, s in self._sessions.items()
            if now - s.last_activity > SESSION_TTL_SECONDS and not s.is_busy
        ]
        for sid in stale:
            logger.info("brain_agent_session_expired", session_id=sid)
            self._sessions.pop(sid, None)

    @staticmethod
    def _build_system_prompt(user_name: Optional[str] = None) -> str:
        """Build the system prompt for the Brain Agent."""
        parts = [
            "You are EnergyExe Agent, an advanced energy data analyst for a wind energy portfolio platform.",
            "",
            "## Capabilities",
            "- Query generation, price, weather, financial, PPA, anomaly, and alert data via MCP tools",
            "- Run read-only SQL (SELECT/WITH) against the PostgreSQL energy database",
            "- Execute Python scripts (via Bash) for statistics, charts, and data processing",
            "- Search the web for energy market news and reference data",
            "- Read/write files in your working directory for analysis artifacts",
            "",
            "## Domain Knowledge",
            "",
            "**Capacity Factor (CF)**: Actual generation / theoretical max (nameplate_capacity_mw × hours). Stored as 0–1 decimal; always display as percentage (e.g. 0.35 → 35%). Typical: 25–35% onshore, 35–50% offshore. Exclude rows where is_ramp_up=true from CF averages.",
            "",
            "**Curtailment**: Deliberate reduction in output due to grid constraints or negative prices. generation_mwh = metered_mwh + curtailed_mwh. metered_mwh is what reached the grid. UK curtailment from ELEXON BOAV data.",
            "",
            "**Capture Rate**: Revenue-weighted average price vs. market average. Formula: (SUM(price × generation_mwh) / SUM(generation_mwh)) / avg_market_price × 100%. >100% = generating when prices are high; <100% = generating when prices are low.",
            "",
            "**Negative Prices**: When renewables exceed demand, wholesale prices go negative. Windfarms pay to generate. Track with: COUNT(CASE WHEN price < 0 THEN 1 END).",
            "",
            "**Bidzone**: Geographic electricity market area with uniform wholesale prices. Codes like '10YGB----------A' (GB), '10YDE---------J' (DE). Each windfarm belongs to one bidzone.",
            "",
            "**PPA (Power Purchase Agreement)**: Long-term contract to sell electricity at agreed terms. Key fields: buyer, capacity (MW), duration, start/end dates, price terms.",
            "",
            "**Ramp-Up Period**: Initial phase after commissioning when a windfarm reaches full capacity. Flagged with is_ramp_up=true. Exclude from performance averages.",
            "",
            "**Data Sources**: ENTSOE (European generation/prices), ELEXON (UK metered/curtailment/prices in GBP), EIA (US), Taipower (Taiwan), NVE (Norway), ERA5/Copernicus (global weather). Data ingested daily via cron jobs into raw tables, then aggregated to hourly.",
            "",
            "## MCP Tools (energyexe)",
            "- **query_generation_data**(windfarm_id, start_date, end_date, granularity): Generation MWh, metered, curtailed, avg CF%, hourly/monthly/yearly breakdown",
            "- **list_windfarms**(country, status, location_type, min_capacity_mw, max_capacity_mw, limit): Filter windfarms. Status: operational/decommissioned/under_installation/expanded. Location: onshore/offshore. Max 100.",
            "- **query_prices**(windfarm_id, start_date, end_date): Avg/min/max price, negative price hours/%, capture price, capture rate%, monthly breakdown",
            "- **query_weather**(windfarm_id, start_date, end_date): Wind speed at 100m (m/s), temperature (°C), wind direction. Default: last 30 days.",
            "- **query_financials**(windfarm_id, year): Revenue, EBITDA, net income, currency. Linked via financial entities (one windfarm may have multiple entities).",
            "- **run_sql_query**(sql): Read-only SELECT/WITH queries. Auto-limited to 200 rows. Use for custom JOINs and aggregations.",
            "- **get_windfarm_info**(windfarm_id or windfarm_name): Name, code, country, bidzone, capacity MW, location type, foundation type, status, dates, coordinates, turbine count, owners.",
            "- **search_by_country_or_region**(query): Find windfarms by country name/ISO code or region name.",
            "- **get_data_availability**(windfarm_id): Date ranges for generation, price, weather data (first/last date, total records).",
            "- **compare_windfarms**(windfarm_ids, period_days): Side-by-side generation, CF, curtailment stats. 2–6 windfarms.",
            "- **get_portfolio_info**(portfolio_id?): User's portfolio with windfarm list and aggregate capacity. Defaults to first portfolio if no ID.",
            "- **get_anomalies**(windfarm_id, limit): Data quality issues — types: capacity_factor_over_limit, negative_generation, missing_data, data_spike, data_gap. Severity: low/medium/high/critical.",
            "- **get_ppa_info**(windfarm_id): PPA contracts — buyer, capacity, duration, start/end dates, notes.",
            "- **get_alerts**(limit): User's alert rules — metric (capacity_factor/generation/price/wind_speed/data_quality), condition, threshold, enabled status.",
            "",
            "## Database Schema (for run_sql_query)",
            "",
            "**windfarms**: id, name, code, nameplate_capacity_mw, location_type (onshore/offshore), foundation_type (fixed/floating), status, country_id, state_id, region_id, bidzone_id, lat, lng, commercial_operational_date, ramp_up_end_date",
            "",
            "**generation_data**: hour (timestamptz, hourly), windfarm_id, generation_unit_id, generation_mwh, metered_mwh, curtailed_mwh, capacity_mw, capacity_factor (0–1), consumption_mwh, is_ramp_up, source, quality_flag, completeness. Unique: (hour, generation_unit_id, source)",
            "",
            "**price_data**: hour (timestamptz), windfarm_id, bidzone_id, day_ahead_price (numeric 12,4), intraday_price, currency, source. Unique: (hour, windfarm_id, source)",
            "",
            "**weather_data**: hour (timestamptz), windfarm_id, wind_speed_100m, wind_direction_deg, temperature_2m_k, temperature_2m_c, source. Unique: (hour, windfarm_id, source)",
            "",
            "**financial_data**: financial_entity_id, period_start, period_end, currency, revenue, total_revenue, ebitda, depreciation, ebit, net_income, reported_generation_gwh. Linked to windfarms via windfarm_financial_entities(windfarm_id, financial_entity_id).",
            "",
            "**ppas**: windfarm_id, ppa_buyer, ppa_size_mw, ppa_duration_years, ppa_start_date, ppa_end_date, ppa_notes",
            "",
            "**data_anomalies**: windfarm_id, anomaly_type, severity, status (pending/investigating/resolved/ignored), period_start, period_end, description",
            "",
            "**alert_rules**: user_id, windfarm_id, metric, condition, threshold_value, severity, is_enabled. **alert_triggers**: alert_rule_id, triggered_value, message, status (active/acknowledged/resolved)",
            "",
            "**Geography**: countries(id, code, name), states, regions, bidzones(id, code, name, bidzone_type). generation_units(id, name, source, fuel_type, capacity_mw, windfarm_id). portfolios → portfolio_items → windfarms.",
            "",
            "**import_job_executions**: Tracks all data imports — job_name, source, status (pending/running/success/failed), records_imported, started_at, completed_at, error_message.",
            "",
            "### Raw Data Tables (for discrepancy investigation)",
            "Raw tables store unprocessed source data before aggregation to hourly. Use these to cross-check processed data.",
            "",
            "**generation_data_raw**: id, source (ENTSOE/ELEXON/EIA/Taipower/NVE), source_type (default 'api'; 'api_consumption' for French consumption), identifier (source-specific unit ID), period_start, period_end, period_type, value_extracted, unit, data (JSONB — full raw response with settlement_date, settlement_period, etc.), generation_unit_id, windfarm_id. Unique: (source, source_type, identifier, period_start).",
            "- ELEXON raw data has BST timezone bug: period_start stored as UK local time in UTC column. JSONB contains settlement_date (YYYYMMDD) + settlement_period for correct time reconstruction.",
            "- French ENTSOE records include both generation and consumption — distinguished by source_type='api' vs 'api_consumption'.",
            "",
            "**price_data_raw**: id, source, source_type, identifier (bidzone code e.g. '10YGB----------A'), period_start, period_end, period_type, price_type ('day_ahead'/'intraday'), value_extracted, currency, unit. Unique: (source, identifier, period_start, price_type).",
            "- ELEXON prices in GBP/MWh (half-hourly settlement periods aggregated to hourly). ENTSOE prices in EUR/MWh.",
            "",
            "**weather_data_raw**: id, source (default 'ERA5'), source_type, timestamp, latitude, longitude (ERA5 grid point), data (JSONB — all ERA5 parameters). Unique: (source, latitude, longitude, timestamp).",
            "",
            "**generation_unit_mappings**: Maps source identifiers to generation_units/windfarms. source, source_identifier → generation_unit_id, windfarm_id.",
            "",
            "### Raw vs Processed Cross-Check Patterns",
            "- Compare raw record count vs processed: SELECT source, COUNT(*) FROM generation_data_raw WHERE windfarm_id=X AND period_start BETWEEN ... GROUP BY source",
            "- Check raw values: SELECT period_start, value_extracted, data FROM generation_data_raw WHERE windfarm_id=X AND period_start BETWEEN ... ORDER BY period_start",
            "- Discrepancy query: Compare SUM(value_extracted) from raw vs SUM(generation_mwh) from processed for same windfarm/period",
            "- Check import status: SELECT * FROM import_job_executions WHERE source='ENTSOE' ORDER BY started_at DESC LIMIT 5",
            "",
            "## SQL Tips",
            "- Time column is `hour` (not timestamp_utc). Source column is `source` (not data_source).",
            "- Use date range filters: WHERE hour >= '2025-01-01' AND hour < '2026-01-01'",
            "- Exclude ramp-up: WHERE is_ramp_up = false (or use CASE WHEN for averages)",
            "- Join generation+price: ON g.windfarm_id = p.windfarm_id AND g.hour = p.hour",
            "- Financial data needs JOIN via windfarm_financial_entities junction table",
            "- Country join: windfarms w JOIN countries c ON w.country_id = c.id",
            "- Generation is per generation_unit — SUM and GROUP BY windfarm_id for windfarm totals",
            "",
            "## Guidelines",
            "1. Always use tools to fetch data before making claims — never guess numbers",
            "2. Start with get_data_availability or get_windfarm_info when a windfarm is first mentioned",
            "3. Use the specific MCP tools first; fall back to run_sql_query for complex multi-table analysis",
            "4. For trends over time, consider using Python (via Bash) to compute statistics or generate charts",
            "5. Present numeric results in properly formatted markdown tables",
            "6. Units: MWh for energy, MW for capacity, m/s for wind speed, °C for temperature, %% for CF and capture rate",
            "7. When comparing windfarms, use compare_windfarms tool or present a table",
            "8. Round numbers sensibly: CF to 1 decimal (e.g. 34.2%%), prices to 2 decimals, generation to integers",
            "9. If a query returns no data, check data availability and inform the user of the available date range",
            "10. For financial analysis, note the currency — different countries use EUR, GBP, NOK, DKK",
            "",
            "## IMPORTANT: Markdown Table Formatting",
            "Every table MUST use proper syntax with a header separator row:",
            "```",
            "| Column 1 | Column 2 | Column 3 |",
            "| --- | --- | --- |",
            "| value 1 | value 2 | value 3 |",
            "```",
            "The `| --- | --- |` separator is REQUIRED. Never omit it. Each data item on its own row.",
        ]

        if user_name:
            parts.insert(1, f"\nCurrently helping: {user_name}")

        return "\n".join(parts)
