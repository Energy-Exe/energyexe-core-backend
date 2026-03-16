"""Brain tool registry — tools that Claude can call to answer energy data questions."""

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import structlog
from sqlalchemy import func, select, and_, case
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, joinedload

from app.models.generation_data import GenerationData
from app.models.price_data import PriceData
from app.models.windfarm import Windfarm
from app.models.generation_unit import GenerationUnit
from app.models.country import Country

logger = structlog.get_logger(__name__)


class BrainToolRegistry:
    """Registry of tools available to the Brain agent."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self._tools: Dict[str, dict] = {}
        self._register_all()

    def _register_all(self):
        """Register all available tools."""
        self._register(
            name="get_windfarm_info",
            description="Get detailed information about a specific windfarm including capacity, location, owners, turbines, dates, and status. Use when the user asks about a specific windfarm.",
            input_schema={
                "type": "object",
                "properties": {
                    "windfarm_id": {"type": "integer", "description": "Windfarm database ID"},
                    "windfarm_name": {"type": "string", "description": "Windfarm name (partial match). Use this if you don't know the ID."},
                },
            },
            executor=self._get_windfarm_info,
        )
        self._register(
            name="list_windfarms",
            description="List windfarms with optional filters. Use to find windfarms by country, status, location type, or capacity range.",
            input_schema={
                "type": "object",
                "properties": {
                    "country": {"type": "string", "description": "Country name or ISO code to filter by"},
                    "status": {"type": "string", "enum": ["operational", "decommissioned", "under_installation", "expanded"], "description": "Windfarm status filter"},
                    "location_type": {"type": "string", "enum": ["onshore", "offshore"], "description": "Onshore or offshore"},
                    "min_capacity_mw": {"type": "number", "description": "Minimum nameplate capacity in MW"},
                    "max_capacity_mw": {"type": "number", "description": "Maximum nameplate capacity in MW"},
                    "limit": {"type": "integer", "description": "Max results to return (default 50)", "default": 50},
                },
            },
            executor=self._list_windfarms,
        )
        self._register(
            name="get_generation_summary",
            description="Get generation data summary for a windfarm over a time period. Returns total MWh, average capacity factor, metered output, curtailment, and monthly breakdown.",
            input_schema={
                "type": "object",
                "properties": {
                    "windfarm_id": {"type": "integer", "description": "Windfarm ID"},
                    "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD). Defaults to 1 year ago."},
                    "end_date": {"type": "string", "description": "End date (YYYY-MM-DD). Defaults to today."},
                    "granularity": {"type": "string", "enum": ["monthly", "quarterly", "yearly"], "description": "Aggregation level (default: monthly)"},
                },
                "required": ["windfarm_id"],
            },
            executor=self._get_generation_summary,
        )
        self._register(
            name="get_weather_summary",
            description="Get weather data summary for a windfarm (wind speed, direction, temperature). Use for wind resource questions.",
            input_schema={
                "type": "object",
                "properties": {
                    "windfarm_id": {"type": "integer", "description": "Windfarm ID"},
                    "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD). Defaults to 30 days ago."},
                    "end_date": {"type": "string", "description": "End date (YYYY-MM-DD). Defaults to today."},
                },
                "required": ["windfarm_id"],
            },
            executor=self._get_weather_summary,
        )
        self._register(
            name="get_price_analytics",
            description="Get electricity price analytics for a windfarm's bidzone. Returns average prices, capture rate, negative price hours, and monthly breakdown.",
            input_schema={
                "type": "object",
                "properties": {
                    "windfarm_id": {"type": "integer", "description": "Windfarm ID"},
                    "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD). Defaults to 1 year ago."},
                    "end_date": {"type": "string", "description": "End date (YYYY-MM-DD). Defaults to today."},
                },
                "required": ["windfarm_id"],
            },
            executor=self._get_price_analytics,
        )
        self._register(
            name="get_financial_summary",
            description="Get financial data summary for a windfarm (revenue, EBITDA, net income, financial ratios). Returns yearly financial performance.",
            input_schema={
                "type": "object",
                "properties": {
                    "windfarm_id": {"type": "integer", "description": "Windfarm ID"},
                    "year": {"type": "integer", "description": "Specific year. If omitted, returns all available years."},
                },
                "required": ["windfarm_id"],
            },
            executor=self._get_financial_summary,
        )
        self._register(
            name="get_anomalies",
            description="Get data quality anomalies detected for a windfarm. Returns issues like missing data, spikes, capacity factor violations.",
            input_schema={
                "type": "object",
                "properties": {
                    "windfarm_id": {"type": "integer", "description": "Windfarm ID"},
                    "limit": {"type": "integer", "description": "Max anomalies to return (default 20)", "default": 20},
                },
                "required": ["windfarm_id"],
            },
            executor=self._get_anomalies,
        )
        self._register(
            name="compare_windfarms",
            description="Compare multiple windfarms side-by-side. Returns generation, capacity factor, curtailment, and availability for each.",
            input_schema={
                "type": "object",
                "properties": {
                    "windfarm_ids": {"type": "array", "items": {"type": "integer"}, "description": "List of windfarm IDs to compare (2-6)"},
                    "period_days": {"type": "integer", "description": "Number of days to look back (default 365)", "default": 365},
                },
                "required": ["windfarm_ids"],
            },
            executor=self._compare_windfarms,
        )
        self._register(
            name="search_by_country_or_region",
            description="Find windfarms by country name, ISO code, or region. Use when user asks about windfarms in a specific area.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Country name, ISO code, or region name to search"},
                },
                "required": ["query"],
            },
            executor=self._search_by_country_or_region,
        )
        self._register(
            name="get_data_availability",
            description="Check what data is available for a windfarm — date ranges for generation, price, and weather data.",
            input_schema={
                "type": "object",
                "properties": {
                    "windfarm_id": {"type": "integer", "description": "Windfarm ID"},
                },
                "required": ["windfarm_id"],
            },
            executor=self._get_data_availability,
        )
        self._register(
            name="get_portfolio_info",
            description="Get information about the user's portfolio including aggregate stats across all windfarms in the portfolio.",
            input_schema={
                "type": "object",
                "properties": {
                    "portfolio_id": {"type": "integer", "description": "Portfolio ID. If omitted, returns the user's first portfolio."},
                },
            },
            executor=self._get_portfolio_info,
        )
        self._register(
            name="get_ppa_info",
            description="Get Power Purchase Agreement (PPA) details for a windfarm including contract terms, pricing, and counterparty.",
            input_schema={
                "type": "object",
                "properties": {
                    "windfarm_id": {"type": "integer", "description": "Windfarm ID"},
                },
                "required": ["windfarm_id"],
            },
            executor=self._get_ppa_info,
        )
        self._register(
            name="get_windfarm_report",
            description="Get a comprehensive performance report for a windfarm including generation, capacity factor, peer comparison, and rankings. Use for detailed analysis requests.",
            input_schema={
                "type": "object",
                "properties": {
                    "windfarm_id": {"type": "integer", "description": "Windfarm ID"},
                    "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD). Defaults to 1 year ago."},
                    "end_date": {"type": "string", "description": "End date (YYYY-MM-DD). Defaults to today."},
                },
                "required": ["windfarm_id"],
            },
            executor=self._get_windfarm_report,
        )
        self._register(
            name="get_alerts",
            description="Get active alerts and recent alert triggers for the user.",
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max alerts to return (default 20)", "default": 20},
                },
            },
            executor=self._get_alerts,
        )

    def _register(self, name: str, description: str, input_schema: dict, executor: Callable):
        self._tools[name] = {
            "name": name,
            "description": description,
            "input_schema": input_schema,
            "executor": executor,
        }

    def get_definitions(self) -> List[dict]:
        """Return tool definitions in Anthropic API format."""
        return [
            {
                "name": t["name"],
                "description": t["description"],
                "input_schema": t["input_schema"],
            }
            for t in self._tools.values()
        ]

    async def execute(self, tool_name: str, params: dict, user_id: Optional[int] = None) -> str:
        """Execute a tool by name and return a summary string."""
        tool = self._tools.get(tool_name)
        if not tool:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})
        try:
            result = await tool["executor"](params, user_id=user_id)
            # Truncate large results
            result_str = json.dumps(result, default=str)
            if len(result_str) > 8000:
                result_str = result_str[:8000] + '... [truncated]'
            return result_str
        except Exception as e:
            logger.error("brain_tool_error", tool=tool_name, error=str(e))
            return json.dumps({"error": f"Tool execution failed: {str(e)}"})

    # ─── Tool executors ───────────────────────────────────────────────

    async def _get_windfarm_info(self, params: dict, **kwargs) -> dict:
        windfarm_id = params.get("windfarm_id")
        windfarm_name = params.get("windfarm_name")

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
            query = query.where(Windfarm.name.ilike(f"%{windfarm_name}%"))
        else:
            return {"error": "Provide windfarm_id or windfarm_name"}

        result = await self.db.execute(query)
        wf = result.scalar_one_or_none()
        if not wf:
            return {"error": "Windfarm not found"}

        return {
            "id": wf.id,
            "name": wf.name,
            "code": wf.code,
            "country": wf.country.name if wf.country else None,
            "state": wf.state.name if wf.state else None,
            "region": wf.region.name if wf.region else None,
            "bidzone": wf.bidzone.code if wf.bidzone else None,
            "nameplate_capacity_mw": float(wf.nameplate_capacity_mw) if wf.nameplate_capacity_mw else None,
            "location_type": wf.location_type,
            "foundation_type": wf.foundation_type,
            "status": wf.status,
            "commercial_operational_date": str(wf.commercial_operational_date) if wf.commercial_operational_date else None,
            "first_power_date": str(wf.first_power_date) if wf.first_power_date else None,
            "ramp_up_end_date": str(wf.ramp_up_end_date) if wf.ramp_up_end_date else None,
            "lat": wf.lat,
            "lng": wf.lng,
            "project": wf.project.name if wf.project else None,
            "turbine_count": len(wf.turbine_units),
            "owner_count": len(wf.windfarm_owners),
            "notes": wf.notes,
        }

    async def _list_windfarms(self, params: dict, **kwargs) -> dict:
        query = select(Windfarm).options(selectinload(Windfarm.country))
        conditions = []

        country = params.get("country")
        if country:
            query = query.join(Windfarm.country)
            conditions.append(
                (Country.name.ilike(f"%{country}%")) | (Country.iso_code == country.upper())
            )

        status = params.get("status")
        if status:
            conditions.append(Windfarm.status == status)

        location_type = params.get("location_type")
        if location_type:
            conditions.append(Windfarm.location_type == location_type)

        min_cap = params.get("min_capacity_mw")
        if min_cap is not None:
            conditions.append(Windfarm.nameplate_capacity_mw >= min_cap)

        max_cap = params.get("max_capacity_mw")
        if max_cap is not None:
            conditions.append(Windfarm.nameplate_capacity_mw <= max_cap)

        if conditions:
            query = query.where(and_(*conditions))

        limit = min(params.get("limit", 50), 100)
        query = query.order_by(Windfarm.name).limit(limit)

        result = await self.db.execute(query)
        windfarms = result.scalars().all()

        return {
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

    async def _get_generation_summary(self, params: dict, **kwargs) -> dict:
        windfarm_id = params["windfarm_id"]
        end = self._parse_date(params.get("end_date"), date.today())
        start = self._parse_date(params.get("start_date"), end - timedelta(days=365))
        granularity = params.get("granularity", "monthly")

        start_dt = datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_dt = datetime.combine(end, datetime.max.time()).replace(tzinfo=timezone.utc)

        # Get windfarm name
        wf_result = await self.db.execute(select(Windfarm.name, Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id))
        wf = wf_result.one_or_none()
        if not wf:
            return {"error": "Windfarm not found"}

        # Overall summary
        summary_query = select(
            func.sum(GenerationData.generation_mwh).label("total_gen"),
            func.sum(func.coalesce(GenerationData.metered_mwh, GenerationData.generation_mwh)).label("total_metered"),
            func.sum(func.coalesce(GenerationData.curtailed_mwh, 0)).label("total_curtailed"),
            func.avg(case((GenerationData.is_ramp_up == True, None), else_=GenerationData.capacity_factor)).label("avg_cf"),
            func.count(GenerationData.id).label("data_points"),
        ).where(
            and_(
                GenerationData.windfarm_id == windfarm_id,
                GenerationData.hour >= start_dt,
                GenerationData.hour <= end_dt,
            )
        )
        sum_result = await self.db.execute(summary_query)
        s = sum_result.one()

        # Monthly/quarterly breakdown
        if granularity == "yearly":
            trunc = func.date_trunc("year", GenerationData.hour)
        elif granularity == "quarterly":
            trunc = func.date_trunc("quarter", GenerationData.hour)
        else:
            trunc = func.date_trunc("month", GenerationData.hour)

        breakdown_query = select(
            trunc.label("period"),
            func.sum(GenerationData.generation_mwh).label("gen_mwh"),
            func.avg(case((GenerationData.is_ramp_up == True, None), else_=GenerationData.capacity_factor)).label("avg_cf"),
            func.count(GenerationData.id).label("hours"),
        ).where(
            and_(
                GenerationData.windfarm_id == windfarm_id,
                GenerationData.hour >= start_dt,
                GenerationData.hour <= end_dt,
            )
        ).group_by("period").order_by("period")

        bd_result = await self.db.execute(breakdown_query)
        breakdown = [
            {
                "period": str(row.period.date()) if row.period else None,
                "generation_mwh": round(float(row.gen_mwh), 1) if row.gen_mwh else 0,
                "avg_capacity_factor": round(float(row.avg_cf) * 100, 1) if row.avg_cf else 0,
                "data_hours": row.hours,
            }
            for row in bd_result.all()
        ]

        return {
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

    async def _get_weather_summary(self, params: dict, **kwargs) -> dict:
        from app.models.weather_data import WeatherData

        windfarm_id = params["windfarm_id"]
        end = self._parse_date(params.get("end_date"), date.today())
        start = self._parse_date(params.get("start_date"), end - timedelta(days=30))

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

        result = await self.db.execute(query)
        row = result.one()

        if not row.data_points:
            return {"error": "No weather data available for this period", "windfarm_id": windfarm_id}

        return {
            "windfarm_id": windfarm_id,
            "period": f"{start} to {end}",
            "avg_wind_speed_ms": round(float(row.avg_wind_speed), 2) if row.avg_wind_speed else None,
            "max_wind_speed_ms": round(float(row.max_wind_speed), 2) if row.max_wind_speed else None,
            "min_wind_speed_ms": round(float(row.min_wind_speed), 2) if row.min_wind_speed else None,
            "avg_temperature_c": round(float(row.avg_temp), 1) if row.avg_temp else None,
            "avg_wind_direction_deg": round(float(row.avg_direction), 0) if row.avg_direction else None,
            "data_points": row.data_points,
        }

    async def _get_price_analytics(self, params: dict, **kwargs) -> dict:
        windfarm_id = params["windfarm_id"]
        end = self._parse_date(params.get("end_date"), date.today())
        start = self._parse_date(params.get("start_date"), end - timedelta(days=365))

        start_dt = datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_dt = datetime.combine(end, datetime.max.time()).replace(tzinfo=timezone.utc)

        # Overall price summary
        summary_query = select(
            func.avg(PriceData.price).label("avg_price"),
            func.max(PriceData.price).label("max_price"),
            func.min(PriceData.price).label("min_price"),
            func.count(PriceData.id).label("total_hours"),
            func.count(case((PriceData.price < 0, 1))).label("negative_hours"),
            func.sum(PriceData.price).label("sum_price"),
        ).where(
            and_(
                PriceData.windfarm_id == windfarm_id,
                PriceData.hour >= start_dt,
                PriceData.hour <= end_dt,
            )
        )
        result = await self.db.execute(summary_query)
        s = result.one()

        if not s.total_hours:
            return {"error": "No price data available for this windfarm/period", "windfarm_id": windfarm_id}

        # Capture rate: avg price weighted by generation / avg price
        capture_query = select(
            func.sum(PriceData.price * GenerationData.generation_mwh).label("weighted_sum"),
            func.sum(GenerationData.generation_mwh).label("total_gen"),
        ).join(
            GenerationData,
            and_(
                GenerationData.windfarm_id == PriceData.windfarm_id,
                GenerationData.hour == PriceData.hour,
            )
        ).where(
            and_(
                PriceData.windfarm_id == windfarm_id,
                PriceData.hour >= start_dt,
                PriceData.hour <= end_dt,
            )
        )
        cap_result = await self.db.execute(capture_query)
        cap = cap_result.one()

        capture_price = None
        capture_rate = None
        if cap.total_gen and cap.weighted_sum and s.avg_price:
            capture_price = float(cap.weighted_sum) / float(cap.total_gen)
            capture_rate = capture_price / float(s.avg_price) * 100

        # Monthly breakdown
        monthly_query = select(
            func.date_trunc("month", PriceData.hour).label("month"),
            func.avg(PriceData.price).label("avg_price"),
            func.count(case((PriceData.price < 0, 1))).label("neg_hours"),
        ).where(
            and_(
                PriceData.windfarm_id == windfarm_id,
                PriceData.hour >= start_dt,
                PriceData.hour <= end_dt,
            )
        ).group_by("month").order_by("month")

        mo_result = await self.db.execute(monthly_query)
        monthly = [
            {
                "month": str(r.month.date()) if r.month else None,
                "avg_price": round(float(r.avg_price), 2) if r.avg_price else 0,
                "negative_hours": r.neg_hours,
            }
            for r in mo_result.all()
        ]

        return {
            "windfarm_id": windfarm_id,
            "period": f"{start} to {end}",
            "avg_price": round(float(s.avg_price), 2) if s.avg_price else 0,
            "max_price": round(float(s.max_price), 2) if s.max_price else 0,
            "min_price": round(float(s.min_price), 2) if s.min_price else 0,
            "total_hours": s.total_hours,
            "negative_price_hours": s.negative_hours,
            "negative_price_pct": round(s.negative_hours / s.total_hours * 100, 1) if s.total_hours else 0,
            "capture_price": round(capture_price, 2) if capture_price else None,
            "capture_rate_pct": round(capture_rate, 1) if capture_rate else None,
            "monthly_breakdown": monthly,
        }

    async def _get_financial_summary(self, params: dict, **kwargs) -> dict:
        from app.models.financial_data import FinancialData
        from app.models.financial_entity import FinancialEntity
        from app.models.windfarm_financial_entity import WindfarmFinancialEntity

        windfarm_id = params["windfarm_id"]
        year = params.get("year")

        # Find financial entities linked to this windfarm
        entity_query = select(FinancialEntity.id, FinancialEntity.name).join(
            WindfarmFinancialEntity,
            WindfarmFinancialEntity.financial_entity_id == FinancialEntity.id,
        ).where(WindfarmFinancialEntity.windfarm_id == windfarm_id)

        entities_result = await self.db.execute(entity_query)
        entities = entities_result.all()

        if not entities:
            return {"error": "No financial entities linked to this windfarm", "windfarm_id": windfarm_id}

        entity_ids = [e.id for e in entities]

        fin_query = select(FinancialData).where(
            FinancialData.financial_entity_id.in_(entity_ids)
        )
        if year:
            fin_query = fin_query.where(
                func.extract("year", FinancialData.period_start) == year
            )
        fin_query = fin_query.order_by(FinancialData.period_start.desc()).limit(10)

        fin_result = await self.db.execute(fin_query)
        records = fin_result.scalars().all()

        if not records:
            return {"error": "No financial data found", "windfarm_id": windfarm_id}

        data = []
        for r in records:
            d = r.data if hasattr(r, "data") and r.data else {}
            data.append({
                "entity": next((e.name for e in entities if e.id == r.financial_entity_id), None),
                "period": str(r.period_start) if r.period_start else None,
                "revenue": d.get("total_revenue") or d.get("revenue"),
                "ebitda": d.get("ebitda"),
                "net_income": d.get("net_income"),
                "total_operating_expenses": d.get("total_operating_expenses"),
                "currency": getattr(r, "currency", None),
            })

        return {
            "windfarm_id": windfarm_id,
            "financial_entities": [{"id": e.id, "name": e.name} for e in entities],
            "records": data,
        }

    async def _get_anomalies(self, params: dict, **kwargs) -> dict:
        from app.models.data_anomaly import DataAnomaly

        windfarm_id = params["windfarm_id"]
        limit = min(params.get("limit", 20), 50)

        query = select(DataAnomaly).where(
            DataAnomaly.windfarm_id == windfarm_id
        ).order_by(DataAnomaly.created_at.desc()).limit(limit)

        result = await self.db.execute(query)
        anomalies = result.scalars().all()

        return {
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

    async def _compare_windfarms(self, params: dict, **kwargs) -> dict:
        from app.services.comparison_service import ComparisonService

        windfarm_ids = params["windfarm_ids"][:6]  # Max 6
        period_days = params.get("period_days", 365)

        service = ComparisonService(self.db)
        stats = await service.get_windfarm_statistics(windfarm_ids, period_days=period_days)

        return {
            "period_days": period_days,
            "windfarms": stats,
        }

    async def _search_by_country_or_region(self, params: dict, **kwargs) -> dict:
        from app.models.region import Region

        query_str = params["query"]

        # Try country match first
        country_query = select(Windfarm).options(
            selectinload(Windfarm.country)
        ).join(Windfarm.country).where(
            (Country.name.ilike(f"%{query_str}%")) | (Country.iso_code == query_str.upper())
        ).order_by(Windfarm.name).limit(50)

        result = await self.db.execute(country_query)
        windfarms = result.scalars().all()

        if not windfarms:
            # Try region match
            region_query = select(Windfarm).options(
                selectinload(Windfarm.country),
                selectinload(Windfarm.region),
            ).join(Windfarm.region).where(
                Region.name.ilike(f"%{query_str}%")
            ).order_by(Windfarm.name).limit(50)

            result = await self.db.execute(region_query)
            windfarms = result.scalars().all()

        return {
            "query": query_str,
            "count": len(windfarms),
            "windfarms": [
                {
                    "id": wf.id,
                    "name": wf.name,
                    "country": wf.country.name if wf.country else None,
                    "capacity_mw": float(wf.nameplate_capacity_mw) if wf.nameplate_capacity_mw else None,
                    "status": wf.status,
                    "location_type": wf.location_type,
                }
                for wf in windfarms
            ],
        }

    async def _get_data_availability(self, params: dict, **kwargs) -> dict:
        from app.models.weather_data import WeatherData

        windfarm_id = params["windfarm_id"]

        # Generation data range
        gen_query = select(
            func.min(GenerationData.hour).label("first"),
            func.max(GenerationData.hour).label("last"),
            func.count(GenerationData.id).label("count"),
        ).where(GenerationData.windfarm_id == windfarm_id)

        # Price data range
        price_query = select(
            func.min(PriceData.hour).label("first"),
            func.max(PriceData.hour).label("last"),
            func.count(PriceData.id).label("count"),
        ).where(PriceData.windfarm_id == windfarm_id)

        # Weather data range
        weather_query = select(
            func.min(WeatherData.hour).label("first"),
            func.max(WeatherData.hour).label("last"),
            func.count(WeatherData.id).label("count"),
        ).where(WeatherData.windfarm_id == windfarm_id)

        gen_res = await self.db.execute(gen_query)
        price_res = await self.db.execute(price_query)
        weather_res = await self.db.execute(weather_query)

        g = gen_res.one()
        p = price_res.one()
        w = weather_res.one()

        return {
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

    async def _get_portfolio_info(self, params: dict, **kwargs) -> dict:
        from app.models.portfolio import Portfolio
        from app.models.portfolio_item import PortfolioItem

        user_id = kwargs.get("user_id")
        portfolio_id = params.get("portfolio_id")

        if portfolio_id:
            query = select(Portfolio).where(Portfolio.id == portfolio_id)
        elif user_id:
            query = select(Portfolio).where(Portfolio.user_id == user_id).limit(1)
        else:
            return {"error": "No portfolio_id or user context available"}

        result = await self.db.execute(query)
        portfolio = result.scalar_one_or_none()
        if not portfolio:
            return {"error": "No portfolio found"}

        # Get items
        items_query = select(PortfolioItem, Windfarm).join(
            Windfarm, PortfolioItem.windfarm_id == Windfarm.id
        ).where(PortfolioItem.portfolio_id == portfolio.id)

        items_result = await self.db.execute(items_query)
        items = items_result.all()

        return {
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

    async def _get_ppa_info(self, params: dict, **kwargs) -> dict:
        from app.models.ppa import PPA

        windfarm_id = params["windfarm_id"]

        query = select(PPA).where(PPA.windfarm_id == windfarm_id).order_by(PPA.start_date.desc())
        result = await self.db.execute(query)
        ppas = result.scalars().all()

        if not ppas:
            return {"error": "No PPA data found for this windfarm", "windfarm_id": windfarm_id}

        return {
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

    async def _get_windfarm_report(self, params: dict, **kwargs) -> dict:
        """Get a summarized version of the full windfarm report."""
        from app.services.windfarm_report_service import WindfarmReportService

        windfarm_id = params["windfarm_id"]
        end = self._parse_date(params.get("end_date"), date.today())
        start = self._parse_date(params.get("start_date"), end - timedelta(days=365))

        start_dt = datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_dt = datetime.combine(end, datetime.max.time()).replace(tzinfo=timezone.utc)

        try:
            service = WindfarmReportService(self.db)
            report = await service.generate_report_data(
                windfarm_id=windfarm_id,
                start_date=start_dt,
                end_date=end_dt,
                generate_commentary=False,
            )

            # Extract key summary data
            summary = {}
            if hasattr(report, "performance_summary") and report.performance_summary:
                ps = report.performance_summary
                summary["performance"] = {
                    k: v for k, v in (ps.dict() if hasattr(ps, "dict") else ps.__dict__).items()
                    if v is not None
                }

            if hasattr(report, "rankings") and report.rankings:
                rk = report.rankings
                summary["rankings"] = {
                    k: v for k, v in (rk.dict() if hasattr(rk, "dict") else rk.__dict__).items()
                    if v is not None
                }

            summary["windfarm_id"] = windfarm_id
            summary["period"] = f"{start} to {end}"
            return summary
        except Exception as e:
            logger.warning("windfarm_report_tool_error", error=str(e))
            return {"error": f"Could not generate report: {str(e)}", "windfarm_id": windfarm_id}

    async def _get_alerts(self, params: dict, **kwargs) -> dict:
        from app.models.alert_rule import AlertRule
        from app.models.alert_trigger import AlertTrigger

        user_id = kwargs.get("user_id")
        limit = min(params.get("limit", 20), 50)

        if not user_id:
            return {"error": "User context required for alerts"}

        rules_query = select(AlertRule).where(
            AlertRule.user_id == user_id
        ).order_by(AlertRule.created_at.desc()).limit(limit)

        result = await self.db.execute(rules_query)
        rules = result.scalars().all()

        return {
            "count": len(rules),
            "alert_rules": [
                {
                    "id": r.id,
                    "name": r.name if hasattr(r, "name") else None,
                    "metric": r.metric if hasattr(r, "metric") else None,
                    "condition": r.condition if hasattr(r, "condition") else None,
                    "is_active": r.is_active if hasattr(r, "is_active") else None,
                }
                for r in rules
            ],
        }

    # ─── Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _parse_date(val: Optional[str], default: date) -> date:
        if not val:
            return default
        try:
            return date.fromisoformat(val)
        except (ValueError, TypeError):
            return default
