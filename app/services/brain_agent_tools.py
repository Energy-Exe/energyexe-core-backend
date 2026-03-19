"""Brain Agent custom MCP tools — energy database tools for the Claude Agent SDK."""

import functools
import json
import re
from contextvars import ContextVar
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import structlog
from sqlalchemy import func, select, and_, case, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from claude_agent_sdk import tool, create_sdk_mcp_server

from app.models.generation_data import GenerationData
from app.models.price_data import PriceData
from app.models.windfarm import Windfarm
from app.models.generation_unit import GenerationUnit
from app.models.country import Country

logger = structlog.get_logger(__name__)


def _parse_date(val: Optional[str], default: date) -> date:
    if not val:
        return default
    try:
        return date.fromisoformat(val)
    except (ValueError, TypeError):
        return default


# ─── Database session management ────────────────────────────────────────
# Each tool call creates a SHORT-LIVED session from the pool, uses it, and
# closes it immediately. This prevents the Brain Agent from holding a
# connection for the entire multi-turn conversation (which starves the
# pool for regular API requests).
#
# We keep set_db_session/clear_db_session as no-ops for backwards compat
# with BrainAgentService.chat().

_db_session_var: ContextVar[Optional[AsyncSession]] = ContextVar("_db_session_var", default=None)


def set_db_session(db: AsyncSession):
    """Legacy — kept for backwards compatibility with BrainAgentService.chat()."""
    _db_session_var.set(db)


def clear_db_session():
    """Legacy — kept for backwards compatibility."""
    _db_session_var.set(None)


def _get_session_factory():
    """Get the async session factory for creating short-lived sessions."""
    from app.core.database import get_session_factory
    return get_session_factory()


def _get_db() -> AsyncSession:
    """Get the current request's database session (legacy fallback)."""
    db = _db_session_var.get()
    if not db:
        raise RuntimeError("No database session available for brain agent tools")
    return db


def with_db_session(fn):
    """Decorator that creates a short-lived DB session for each tool call.

    The session is created from the pool, passed as `db` in args, and
    closed immediately after the tool returns — so the connection is only
    held for the duration of a single tool call, not the entire agent turn.
    """
    @functools.wraps(fn)
    async def wrapper(args: dict[str, Any]) -> dict[str, Any]:
        async with _get_session_factory()() as db:
            return await fn(args, db)
    return wrapper


# ─── User context holder ────────────────────────────────────────────────
# Some tools (alerts, portfolio) need the current user's ID.

_user_id_var: ContextVar[Optional[int]] = ContextVar("_user_id_var", default=None)


def set_user_id(user_id: int):
    """Set the user ID for the current async task."""
    _user_id_var.set(user_id)


def get_user_id() -> Optional[int]:
    """Get the current request's user ID."""
    return _user_id_var.get()


def clear_user_id():
    """Remove the user ID reference for the current async task."""
    _user_id_var.set(None)


# ─── LIKE wildcard escaping ─────────────────────────────────────────────

def _escape_like(value: str) -> str:
    """Escape SQL LIKE wildcards (%, _) in user input."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# ─── Tool definitions ───────────────────────────────────────────────────


@tool(
    "query_generation_data",
    "Query generation data for a windfarm. Returns total MWh, capacity factor, metered output, curtailment, and time breakdown.",
    {
        "windfarm_id": int,
        "start_date": str,
        "end_date": str,
        "granularity": str,
    },
)
@with_db_session
async def query_generation_data(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    windfarm_id = args["windfarm_id"]
    end = _parse_date(args.get("end_date"), date.today())
    start = _parse_date(args.get("start_date"), end - timedelta(days=365))
    granularity = args.get("granularity", "monthly")

    start_dt = datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = datetime.combine(end, datetime.max.time()).replace(tzinfo=timezone.utc)

    wf_result = await db.execute(
        select(Windfarm.name, Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
    )
    wf = wf_result.one_or_none()
    if not wf:
        return {"content": [{"type": "text", "text": json.dumps({"error": "Windfarm not found"})}]}

    summary_query = select(
        func.sum(GenerationData.generation_mwh).label("total_gen"),
        func.sum(func.coalesce(GenerationData.metered_mwh, GenerationData.generation_mwh)).label(
            "total_metered"
        ),
        func.sum(func.coalesce(GenerationData.curtailed_mwh, 0)).label("total_curtailed"),
        func.avg(
            case((GenerationData.is_ramp_up == True, None), else_=GenerationData.capacity_factor)
        ).label("avg_cf"),
        func.count(GenerationData.id).label("data_points"),
    ).where(
        and_(
            GenerationData.windfarm_id == windfarm_id,
            GenerationData.hour >= start_dt,
            GenerationData.hour <= end_dt,
        )
    )
    sum_result = await db.execute(summary_query)
    s = sum_result.one()

    if granularity == "yearly":
        trunc = func.date_trunc("year", GenerationData.hour)
    elif granularity == "quarterly":
        trunc = func.date_trunc("quarter", GenerationData.hour)
    else:
        trunc = func.date_trunc("month", GenerationData.hour)

    breakdown_query = (
        select(
            trunc.label("period"),
            func.sum(GenerationData.generation_mwh).label("gen_mwh"),
            func.avg(
                case(
                    (GenerationData.is_ramp_up == True, None),
                    else_=GenerationData.capacity_factor,
                )
            ).label("avg_cf"),
            func.count(GenerationData.id).label("hours"),
        )
        .where(
            and_(
                GenerationData.windfarm_id == windfarm_id,
                GenerationData.hour >= start_dt,
                GenerationData.hour <= end_dt,
            )
        )
        .group_by("period")
        .order_by("period")
    )

    bd_result = await db.execute(breakdown_query)
    breakdown = [
        {
            "period": str(row.period.date()) if row.period else None,
            "generation_mwh": round(float(row.gen_mwh), 1) if row.gen_mwh else 0,
            "avg_capacity_factor_pct": round(float(row.avg_cf) * 100, 1) if row.avg_cf else 0,
            "data_hours": row.hours,
        }
        for row in bd_result.all()
    ]

    result = {
        "windfarm": wf.name,
        "windfarm_id": windfarm_id,
        "capacity_mw": float(wf.nameplate_capacity_mw) if wf.nameplate_capacity_mw else None,
        "period": f"{start} to {end}",
        "total_generation_mwh": round(float(s.total_gen), 1) if s.total_gen else 0,
        "total_metered_mwh": round(float(s.total_metered), 1) if s.total_metered else 0,
        "total_curtailed_mwh": round(float(s.total_curtailed), 1) if s.total_curtailed else 0,
        "avg_capacity_factor_pct": round(float(s.avg_cf) * 100, 1) if s.avg_cf else 0,
        "data_hours": s.data_points,
        "breakdown": breakdown,
    }
    return {"content": [{"type": "text", "text": json.dumps(result, default=str)}]}


@tool(
    "list_windfarms",
    "List windfarms with optional filters by country, status, location type, or capacity range.",
    {
        "country": str,
        "status": str,
        "location_type": str,
        "min_capacity_mw": float,
        "max_capacity_mw": float,
        "limit": int,
    },
)
@with_db_session
async def list_windfarms(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    query = select(Windfarm).options(selectinload(Windfarm.country))
    conditions = []

    country = args.get("country")
    if country:
        query = query.join(Windfarm.country)
        escaped_country = _escape_like(country)
        conditions.append(
            (Country.name.ilike(f"%{escaped_country}%", escape="\\")) | (Country.iso_code == country.upper())
        )

    status = args.get("status")
    if status:
        conditions.append(Windfarm.status == status)

    location_type = args.get("location_type")
    if location_type:
        conditions.append(Windfarm.location_type == location_type)

    min_cap = args.get("min_capacity_mw")
    if min_cap is not None:
        conditions.append(Windfarm.nameplate_capacity_mw >= min_cap)

    max_cap = args.get("max_capacity_mw")
    if max_cap is not None:
        conditions.append(Windfarm.nameplate_capacity_mw <= max_cap)

    if conditions:
        query = query.where(and_(*conditions))

    limit = min(args.get("limit", 50) or 50, 100)
    query = query.order_by(Windfarm.name).limit(limit)

    result = await db.execute(query)
    windfarms = result.scalars().all()

    data = {
        "count": len(windfarms),
        "windfarms": [
            {
                "id": wf.id,
                "name": wf.name,
                "code": wf.code,
                "country": wf.country.name if wf.country else None,
                "capacity_mw": float(wf.nameplate_capacity_mw) if wf.nameplate_capacity_mw else None,
                "status": wf.status,
                "location_type": wf.location_type,
            }
            for wf in windfarms
        ],
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "query_prices",
    "Query electricity price data for a windfarm's bidzone. Returns avg/min/max prices, capture rate, negative price hours, and monthly breakdown.",
    {
        "windfarm_id": int,
        "start_date": str,
        "end_date": str,
    },
)
@with_db_session
async def query_prices(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    windfarm_id = args["windfarm_id"]
    end = _parse_date(args.get("end_date"), date.today())
    start = _parse_date(args.get("start_date"), end - timedelta(days=365))

    start_dt = datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = datetime.combine(end, datetime.max.time()).replace(tzinfo=timezone.utc)

    summary_query = select(
        func.avg(PriceData.day_ahead_price).label("avg_price"),
        func.max(PriceData.day_ahead_price).label("max_price"),
        func.min(PriceData.day_ahead_price).label("min_price"),
        func.count(PriceData.id).label("total_hours"),
        func.count(case((PriceData.day_ahead_price < 0, 1))).label("negative_hours"),
    ).where(
        and_(
            PriceData.windfarm_id == windfarm_id,
            PriceData.hour >= start_dt,
            PriceData.hour <= end_dt,
        )
    )
    result = await db.execute(summary_query)
    s = result.one()

    if not s.total_hours:
        return {
            "content": [
                {"type": "text", "text": json.dumps({"error": "No price data for this period"})}
            ]
        }

    capture_query = select(
        func.sum(PriceData.day_ahead_price * GenerationData.generation_mwh).label("weighted_sum"),
        func.sum(GenerationData.generation_mwh).label("total_gen"),
    ).join(
        GenerationData,
        and_(
            GenerationData.windfarm_id == PriceData.windfarm_id,
            GenerationData.hour == PriceData.hour,
        ),
    ).where(
        and_(
            PriceData.windfarm_id == windfarm_id,
            PriceData.hour >= start_dt,
            PriceData.hour <= end_dt,
        )
    )
    cap_result = await db.execute(capture_query)
    cap = cap_result.one()

    capture_price = None
    capture_rate = None
    if cap.total_gen and cap.weighted_sum and s.avg_price:
        capture_price = float(cap.weighted_sum) / float(cap.total_gen)
        capture_rate = capture_price / float(s.avg_price) * 100

    monthly_query = (
        select(
            func.date_trunc("month", PriceData.hour).label("month"),
            func.avg(PriceData.day_ahead_price).label("avg_price"),
            func.count(case((PriceData.day_ahead_price < 0, 1))).label("neg_hours"),
        )
        .where(
            and_(
                PriceData.windfarm_id == windfarm_id,
                PriceData.hour >= start_dt,
                PriceData.hour <= end_dt,
            )
        )
        .group_by("month")
        .order_by("month")
    )
    mo_result = await db.execute(monthly_query)
    monthly = [
        {
            "month": str(r.month.date()) if r.month else None,
            "avg_price": round(float(r.avg_price), 2) if r.avg_price else 0,
            "negative_hours": r.neg_hours,
        }
        for r in mo_result.all()
    ]

    data = {
        "windfarm_id": windfarm_id,
        "period": f"{start} to {end}",
        "avg_price": round(float(s.avg_price), 2) if s.avg_price else 0,
        "max_price": round(float(s.max_price), 2) if s.max_price else 0,
        "min_price": round(float(s.min_price), 2) if s.min_price else 0,
        "total_hours": s.total_hours,
        "negative_price_hours": s.negative_hours,
        "capture_price": round(capture_price, 2) if capture_price else None,
        "capture_rate_pct": round(capture_rate, 1) if capture_rate else None,
        "monthly_breakdown": monthly,
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "query_weather",
    "Query weather data for a windfarm (wind speed, direction, temperature).",
    {
        "windfarm_id": int,
        "start_date": str,
        "end_date": str,
    },
)
@with_db_session
async def query_weather(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    from app.models.weather_data import WeatherData

    windfarm_id = args["windfarm_id"]
    end = _parse_date(args.get("end_date"), date.today())
    start = _parse_date(args.get("start_date"), end - timedelta(days=30))

    start_dt = datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = datetime.combine(end, datetime.max.time()).replace(tzinfo=timezone.utc)

    query = select(
        func.avg(WeatherData.wind_speed_100m).label("avg_wind_speed"),
        func.max(WeatherData.wind_speed_100m).label("max_wind_speed"),
        func.min(WeatherData.wind_speed_100m).label("min_wind_speed"),
        func.avg(WeatherData.temperature_2m).label("avg_temp"),
        func.avg(WeatherData.wind_direction_100m).label("avg_direction"),
        func.count(WeatherData.id).label("data_points"),
    ).where(
        and_(
            WeatherData.windfarm_id == windfarm_id,
            WeatherData.hour >= start_dt,
            WeatherData.hour <= end_dt,
        )
    )
    result = await db.execute(query)
    row = result.one()

    if not row.data_points:
        return {
            "content": [
                {"type": "text", "text": json.dumps({"error": "No weather data for this period"})}
            ]
        }

    data = {
        "windfarm_id": windfarm_id,
        "period": f"{start} to {end}",
        "avg_wind_speed_ms": round(float(row.avg_wind_speed), 2) if row.avg_wind_speed else None,
        "max_wind_speed_ms": round(float(row.max_wind_speed), 2) if row.max_wind_speed else None,
        "min_wind_speed_ms": round(float(row.min_wind_speed), 2) if row.min_wind_speed else None,
        "avg_temperature_c": round(float(row.avg_temp), 1) if row.avg_temp else None,
        "avg_wind_direction_deg": round(float(row.avg_direction), 0) if row.avg_direction else None,
        "data_points": row.data_points,
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "query_financials",
    "Query financial data for a windfarm (revenue, EBITDA, net income).",
    {
        "windfarm_id": int,
        "year": int,
    },
)
@with_db_session
async def query_financials(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    from app.models.financial_data import FinancialData
    from app.models.financial_entity import FinancialEntity
    from app.models.windfarm_financial_entity import WindfarmFinancialEntity

    windfarm_id = args["windfarm_id"]
    year = args.get("year")

    entity_query = (
        select(FinancialEntity.id, FinancialEntity.name)
        .join(
            WindfarmFinancialEntity,
            WindfarmFinancialEntity.financial_entity_id == FinancialEntity.id,
        )
        .where(WindfarmFinancialEntity.windfarm_id == windfarm_id)
    )
    entities_result = await db.execute(entity_query)
    entities = entities_result.all()

    if not entities:
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({"error": "No financial entities linked to this windfarm"}),
                }
            ]
        }

    entity_ids = [e.id for e in entities]
    fin_query = select(FinancialData).where(FinancialData.financial_entity_id.in_(entity_ids))
    if year:
        fin_query = fin_query.where(func.extract("year", FinancialData.period_start) == year)
    fin_query = fin_query.order_by(FinancialData.period_start.desc()).limit(10)

    fin_result = await db.execute(fin_query)
    records = fin_result.scalars().all()

    if not records:
        return {
            "content": [
                {"type": "text", "text": json.dumps({"error": "No financial data found"})}
            ]
        }

    data_records = []
    for r in records:
        d = r.data if hasattr(r, "data") and r.data else {}
        data_records.append(
            {
                "entity": next((e.name for e in entities if e.id == r.financial_entity_id), None),
                "period": str(r.period_start) if r.period_start else None,
                "revenue": d.get("total_revenue") or d.get("revenue"),
                "ebitda": d.get("ebitda"),
                "net_income": d.get("net_income"),
                "currency": getattr(r, "currency", None),
            }
        )

    data = {
        "windfarm_id": windfarm_id,
        "financial_entities": [{"id": e.id, "name": e.name} for e in entities],
        "records": data_records,
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "run_sql_query",
    "Run a read-only SQL SELECT query against the energy database. Only SELECT/WITH queries are allowed — no mutations. Auto-limited to 200 rows. See the Database Schema section in your system prompt for full table definitions and SQL tips.",
    {
        "sql": str,
    },
)
@with_db_session
async def run_sql_query(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    sql_str = args.get("sql", "").strip()

    if not sql_str:
        return {"content": [{"type": "text", "text": json.dumps({"error": "Empty SQL query"})}]}

    # Block semicolons (prevents multi-statement attacks)
    if ";" in sql_str:
        return {
            "content": [
                {"type": "text", "text": json.dumps({"error": "Semicolons not allowed in queries"})}
            ]
        }

    # Strip SQL comments before keyword checking
    sql_cleaned = re.sub(r"--[^\n]*", " ", sql_str)  # single-line comments
    sql_cleaned = re.sub(r"/\*.*?\*/", " ", sql_cleaned, flags=re.DOTALL)  # block comments
    sql_upper = sql_cleaned.upper().strip()

    # Security: only allow SELECT queries
    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {"error": "Only SELECT/WITH queries are allowed. No mutations permitted."}
                    ),
                }
            ]
        }

    # Block dangerous keywords using word-boundary regex
    dangerous = [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
        "CREATE", "GRANT", "REVOKE", "EXECUTE", "COPY", "VACUUM",
    ]
    for keyword in dangerous:
        if re.search(rf"\b{keyword}\b", sql_upper):
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({"error": f"Mutation keyword '{keyword}' not allowed"}),
                    }
                ]
            }

    try:
        # Add LIMIT if not present
        if "LIMIT" not in sql_upper:
            sql_str = sql_str.rstrip(";") + " LIMIT 200"

        # Execute within a read-only transaction with statement timeout
        await db.execute(text("SET LOCAL statement_timeout = '30000'"))
        await db.execute(text("SET TRANSACTION READ ONLY"))
        result = await db.execute(text(sql_str))
        rows = result.fetchall()
        columns = list(result.keys()) if rows else []

        data = {
            "columns": columns,
            "row_count": len(rows),
            "rows": [dict(zip(columns, [str(v) if v is not None else None for v in row])) for row in rows[:200]],
        }

        result_text = json.dumps(data, default=str)
        if len(result_text) > 12000:
            result_text = result_text[:12000] + '... [truncated]'

        return {"content": [{"type": "text", "text": result_text}]}
    except Exception as e:
        logger.error("brain_agent_sql_error", error=str(e), sql=sql_str[:200])
        return {
            "content": [
                {"type": "text", "text": json.dumps({"error": f"SQL error: {str(e)}"})}
            ]
        }


@tool(
    "get_windfarm_info",
    "Get detailed information about a specific windfarm including capacity, location, owners, turbines, and status.",
    {
        "windfarm_id": int,
        "windfarm_name": str,
    },
)
@with_db_session
async def get_windfarm_info(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    windfarm_id = args.get("windfarm_id")
    windfarm_name = args.get("windfarm_name")

    query = select(Windfarm).options(
        selectinload(Windfarm.country),
        selectinload(Windfarm.state),
        selectinload(Windfarm.region),
        selectinload(Windfarm.bidzone),
        selectinload(Windfarm.windfarm_owners),
        selectinload(Windfarm.turbine_units),
        selectinload(Windfarm.project),
    )

    if windfarm_id:
        query = query.where(Windfarm.id == windfarm_id)
    elif windfarm_name:
        escaped_name = _escape_like(windfarm_name)
        query = query.where(Windfarm.name.ilike(f"%{escaped_name}%", escape="\\"))
    else:
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({"error": "Provide windfarm_id or windfarm_name"}),
                }
            ]
        }

    result = await db.execute(query)
    wf = result.scalar_one_or_none()
    if not wf:
        return {
            "content": [{"type": "text", "text": json.dumps({"error": "Windfarm not found"})}]
        }

    data = {
        "id": wf.id,
        "name": wf.name,
        "code": wf.code,
        "country": wf.country.name if wf.country else None,
        "state": wf.state.name if wf.state else None,
        "region": wf.region.name if wf.region else None,
        "bidzone": wf.bidzone.code if wf.bidzone else None,
        "nameplate_capacity_mw": float(wf.nameplate_capacity_mw)
        if wf.nameplate_capacity_mw
        else None,
        "location_type": wf.location_type,
        "foundation_type": wf.foundation_type,
        "status": wf.status,
        "commercial_operational_date": str(wf.commercial_operational_date)
        if wf.commercial_operational_date
        else None,
        "lat": wf.lat,
        "lng": wf.lng,
        "project": wf.project.name if wf.project else None,
        "turbine_count": len(wf.turbine_units),
        "owner_count": len(wf.windfarm_owners),
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "search_by_country_or_region",
    "Find windfarms by country name, ISO code, or region name.",
    {"query": str},
)
@with_db_session
async def search_by_country_or_region(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    from app.models.region import Region

    query_str = args["query"]

    country_query = (
        select(Windfarm)
        .options(selectinload(Windfarm.country))
        .join(Windfarm.country)
        .where((Country.name.ilike(f"%{_escape_like(query_str)}%", escape="\\")) | (Country.iso_code == query_str.upper()))
        .order_by(Windfarm.name)
        .limit(50)
    )
    result = await db.execute(country_query)
    windfarms = result.scalars().all()

    if not windfarms:
        region_query = (
            select(Windfarm)
            .options(selectinload(Windfarm.country), selectinload(Windfarm.region))
            .join(Windfarm.region)
            .where(Region.name.ilike(f"%{_escape_like(query_str)}%", escape="\\"))
            .order_by(Windfarm.name)
            .limit(50)
        )
        result = await db.execute(region_query)
        windfarms = result.scalars().all()

    data = {
        "query": query_str,
        "count": len(windfarms),
        "windfarms": [
            {
                "id": wf.id,
                "name": wf.name,
                "country": wf.country.name if wf.country else None,
                "capacity_mw": float(wf.nameplate_capacity_mw)
                if wf.nameplate_capacity_mw
                else None,
                "status": wf.status,
                "location_type": wf.location_type,
            }
            for wf in windfarms
        ],
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "get_data_availability",
    "Check what data is available for a windfarm — date ranges for generation, price, and weather data.",
    {"windfarm_id": int},
)
@with_db_session
async def get_data_availability(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    from app.models.weather_data import WeatherData

    windfarm_id = args["windfarm_id"]

    gen_query = select(
        func.min(GenerationData.hour).label("first"),
        func.max(GenerationData.hour).label("last"),
        func.count(GenerationData.id).label("count"),
    ).where(GenerationData.windfarm_id == windfarm_id)

    price_query = select(
        func.min(PriceData.hour).label("first"),
        func.max(PriceData.hour).label("last"),
        func.count(PriceData.id).label("count"),
    ).where(PriceData.windfarm_id == windfarm_id)

    weather_query = select(
        func.min(WeatherData.hour).label("first"),
        func.max(WeatherData.hour).label("last"),
        func.count(WeatherData.id).label("count"),
    ).where(WeatherData.windfarm_id == windfarm_id)

    g = (await db.execute(gen_query)).one()
    p = (await db.execute(price_query)).one()
    w = (await db.execute(weather_query)).one()

    data = {
        "windfarm_id": windfarm_id,
        "generation": {
            "first_date": str(g.first) if g.first else None,
            "last_date": str(g.last) if g.last else None,
            "total_records": g.count,
        },
        "price": {
            "first_date": str(p.first) if p.first else None,
            "last_date": str(p.last) if p.last else None,
            "total_records": p.count,
        },
        "weather": {
            "first_date": str(w.first) if w.first else None,
            "last_date": str(w.last) if w.last else None,
            "total_records": w.count,
        },
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "compare_windfarms",
    "Compare multiple windfarms side-by-side. Returns generation, capacity factor, curtailment, and availability for each.",
    {
        "windfarm_ids": list,
        "period_days": int,
    },
)
@with_db_session
async def compare_windfarms(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    from app.services.comparison_service import ComparisonService

    windfarm_ids = args["windfarm_ids"][:6]  # Max 6
    period_days = args.get("period_days", 365)

    service = ComparisonService(db)
    stats = await service.get_windfarm_statistics(windfarm_ids, period_days=period_days)

    data = {
        "period_days": period_days,
        "windfarms": stats,
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "get_portfolio_info",
    "Get information about the user's portfolio including aggregate stats across all windfarms in the portfolio.",
    {
        "portfolio_id": int,
    },
)
@with_db_session
async def get_portfolio_info(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    from app.models.portfolio import Portfolio, PortfolioItem

    user_id = get_user_id()
    portfolio_id = args.get("portfolio_id")

    if portfolio_id:
        query = select(Portfolio).where(Portfolio.id == portfolio_id)
    elif user_id:
        query = select(Portfolio).where(Portfolio.user_id == user_id).limit(1)
    else:
        return {
            "content": [
                {"type": "text", "text": json.dumps({"error": "No portfolio_id or user context available"})}
            ]
        }

    result = await db.execute(query)
    portfolio = result.scalar_one_or_none()
    if not portfolio:
        return {
            "content": [{"type": "text", "text": json.dumps({"error": "No portfolio found"})}]
        }

    # Get items
    items_query = (
        select(PortfolioItem, Windfarm)
        .join(Windfarm, PortfolioItem.windfarm_id == Windfarm.id)
        .where(PortfolioItem.portfolio_id == portfolio.id)
    )
    items_result = await db.execute(items_query)
    items = items_result.all()

    data = {
        "portfolio_id": portfolio.id,
        "name": portfolio.name,
        "windfarm_count": len(items),
        "windfarms": [
            {
                "windfarm_id": wf.id,
                "name": wf.name,
                "capacity_mw": float(wf.nameplate_capacity_mw) if wf.nameplate_capacity_mw else None,
            }
            for _, wf in items
        ],
        "total_capacity_mw": sum(
            float(wf.nameplate_capacity_mw) for _, wf in items if wf.nameplate_capacity_mw
        ),
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "get_anomalies",
    "Get data quality anomalies detected for a windfarm. Returns issues like missing data, spikes, capacity factor violations.",
    {
        "windfarm_id": int,
        "limit": int,
    },
)
@with_db_session
async def get_anomalies(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    from app.models.data_anomaly import DataAnomaly

    windfarm_id = args["windfarm_id"]
    limit = min(args.get("limit", 20) or 20, 50)

    query = (
        select(DataAnomaly)
        .where(DataAnomaly.windfarm_id == windfarm_id)
        .order_by(DataAnomaly.created_at.desc())
        .limit(limit)
    )

    result = await db.execute(query)
    anomalies = result.scalars().all()

    data = {
        "windfarm_id": windfarm_id,
        "count": len(anomalies),
        "anomalies": [
            {
                "id": a.id,
                "type": a.anomaly_type.value if hasattr(a.anomaly_type, "value") else str(a.anomaly_type),
                "severity": a.severity.value if hasattr(a.severity, "value") else str(a.severity),
                "status": a.status.value if hasattr(a.status, "value") else str(a.status),
                "description": a.description,
                "detected_at": str(a.created_at),
            }
            for a in anomalies
        ],
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "get_ppa_info",
    "Get Power Purchase Agreement (PPA) details for a windfarm including contract terms, pricing, and counterparty.",
    {
        "windfarm_id": int,
    },
)
@with_db_session
async def get_ppa_info(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    from app.models.ppa import PPA

    windfarm_id = args["windfarm_id"]

    query = select(PPA).where(PPA.windfarm_id == windfarm_id).order_by(PPA.start_date.desc())
    result = await db.execute(query)
    ppas = result.scalars().all()

    if not ppas:
        return {
            "content": [
                {"type": "text", "text": json.dumps({"error": "No PPA data found for this windfarm", "windfarm_id": windfarm_id})}
            ]
        }

    data = {
        "windfarm_id": windfarm_id,
        "count": len(ppas),
        "ppas": [
            {
                "id": p.id,
                "counterparty": p.counterparty if hasattr(p, "counterparty") else None,
                "start_date": str(p.start_date) if p.start_date else None,
                "end_date": str(p.end_date) if p.end_date else None,
                "price_type": p.price_type if hasattr(p, "price_type") else None,
                "price": float(p.price) if hasattr(p, "price") and p.price else None,
                "currency": p.currency if hasattr(p, "currency") else None,
                "volume_mwh": float(p.volume_mwh) if hasattr(p, "volume_mwh") and p.volume_mwh else None,
                "status": p.status if hasattr(p, "status") else None,
            }
            for p in ppas
        ],
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


@tool(
    "get_alerts",
    "Get active alert rules and recent alert triggers for the user.",
    {
        "limit": int,
    },
)
@with_db_session
async def get_alerts(args: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
    from app.models.alert import AlertRule

    user_id = get_user_id()
    limit = min(args.get("limit", 20) or 20, 50)

    if not user_id:
        return {
            "content": [
                {"type": "text", "text": json.dumps({"error": "User context required for alerts"})}
            ]
        }

    rules_query = (
        select(AlertRule)
        .where(AlertRule.user_id == user_id)
        .order_by(AlertRule.created_at.desc())
        .limit(limit)
    )

    result = await db.execute(rules_query)
    rules = result.scalars().all()

    data = {
        "count": len(rules),
        "alert_rules": [
            {
                "id": r.id,
                "name": r.name if hasattr(r, "name") else None,
                "metric": r.metric.value if hasattr(r.metric, "value") else str(r.metric) if r.metric else None,
                "condition": r.condition.value if hasattr(r.condition, "value") else str(r.condition) if r.condition else None,
                "is_enabled": r.is_enabled if hasattr(r, "is_enabled") else None,
            }
            for r in rules
        ],
    }
    return {"content": [{"type": "text", "text": json.dumps(data, default=str)}]}


# ─── MCP Server creation ────────────────────────────────────────────────

ALL_TOOLS = [
    query_generation_data,
    list_windfarms,
    query_prices,
    query_weather,
    query_financials,
    run_sql_query,
    get_windfarm_info,
    search_by_country_or_region,
    get_data_availability,
    compare_windfarms,
    get_portfolio_info,
    get_anomalies,
    get_ppa_info,
    get_alerts,
]

energyexe_mcp_server = create_sdk_mcp_server(
    name="energyexe",
    version="1.0.0",
    tools=ALL_TOOLS,
)

# Tool names prefixed with mcp__energyexe__
ENERGYEXE_TOOL_NAMES = [f"mcp__energyexe__{t.name}" for t in ALL_TOOLS]
