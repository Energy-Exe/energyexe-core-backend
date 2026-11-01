"""SCADA Performance page (EPR-203): wind-normalised index, P50 and the lost-energy league.

Everything comes from the gold tables the SCADA pipeline publishes:

* ``scada.losses_daily`` holds, per turbine-day, the performance bucket on its own
  (``perf_actual_kwh``, ``perf_potential_kwh``, ``perf_intervals``: intervals that were
  neither downtime nor binding curtailment) and the mean measured nacelle wind
  (``ws_mean_ms`` over ``ws_intervals``). Potential is the turbine's own empirical
  power curve for its configuration epoch at the measured wind.
* ``scada.energy_daily`` holds every interval's energy (meter first) for the P50 chart.

Definitions (one place, mirrored in the client copy):

    b         = Σperf_actual ÷ Σperf_potential over the farm's whole post-COD history
    expected  = perf_potential × b                  (the same farm b for one turbine)
    index     = actual ÷ expected × 100              (100 = the farm's long-run average)
    gain      = actual − expected                    (> 0: energy gained above expected)

A bar is a gap (null) when performance intervals are less than ``COVERAGE_MIN`` of the
intervals that carry data (attributed or without a curve bin; missing turbines are not
counted, so a farm with a few turbines out of the data still shows an index from the
rest). A day lost mostly to downtime or curtailment therefore has no bar. Gaps are left
out of the mean, the counts and the rolling average.

Each interval is judged against its own configuration epoch's curve, so a
retrofit such as Hill of Towie's AeroUp re-centres the index rather than showing as
uplift, and per-turbine all-history gain is near zero by construction.

P50 is the operator target from Perform's ``p50_targets`` when the farm is linked to a
core wind farm and has one; otherwise an estimate = the mean generation of the complete
calendar years in SCADA (``ESTIMATE_COVERAGE`` of the year's intervals with known energy).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.generation_analytics_service import utc_midnight
from app.services.performance_analytics_service import (
    EMPTY_P50_ANALYSIS,
    Month,
    PerformanceAnalyticsService,
    add_months,
    month_key,
    month_range,
)
from app.services.price_analytics_service import price_source_for

PERIOD_MONTHS: Dict[str, Optional[int]] = {"12m": 12, "24m": 24, "3y": 36, "5y": 60, "all": None}
GRANULARITIES = ("day", "month", "year")
INTERVALS_PER_DAY = 144
DAILY_VIEW_MONTHS = 3
# Share of a bar's reporting intervals that must be performance intervals for an index.
COVERAGE_MIN: Dict[str, float] = {"day": 0.5, "month": 0.3, "year": 0.3}
# A complete calendar year for the P50 estimate (mirrors Perform's FULL_YEAR_HOURS_SHARE).
ESTIMATE_COVERAGE = 0.99
ESTIMATE_MIN_YEARS = 2


class PerformanceColumnsMissing(Exception):
    """The gold publish carrying the EPR-203 columns has not reached this database."""


@dataclass
class Sums:
    """Additive sums over a set of turbine-days."""

    actual_kwh: float = 0.0
    potential_kwh: float = 0.0
    perf_intervals: int = 0
    reporting_intervals: int = 0
    ws_weighted: float = 0.0
    ws_intervals: int = 0

    def __add__(self, other: "Sums") -> "Sums":
        return Sums(
            self.actual_kwh + other.actual_kwh,
            self.potential_kwh + other.potential_kwh,
            self.perf_intervals + other.perf_intervals,
            self.reporting_intervals + other.reporting_intervals,
            self.ws_weighted + other.ws_weighted,
            self.ws_intervals + other.ws_intervals,
        )


def _f(value: Any) -> float:
    return float(value) if value is not None else 0.0


def _i(value: Any) -> int:
    return int(value) if value is not None else 0


def _sums(row: Any) -> Sums:
    return Sums(
        _f(row.actual),
        _f(row.potential),
        _i(row.perf_n),
        _i(row.reporting_n),
        _f(row.ws_weighted),
        _i(row.ws_n),
    )


# ─── Pure computation (unit-tested without a DB) ─────────────────────────────


def baseline(actual_kwh: float, potential_kwh: float) -> Optional[float]:
    """The farm's long-run performance ratio b; None without potential."""
    return actual_kwh / potential_kwh if potential_kwh > 0 and actual_kwh > 0 else None


def resolve_window(period: str, first_day: date, last_day: date) -> Tuple[date, date]:
    """Whole calendar months ending with the month of the last data day.

    The start is clamped to the first data day ("All", or a farm with a short history).
    """
    months = PERIOD_MONTHS[period]
    end_month: Month = (last_day.year, last_day.month)
    if months is None:
        return first_day, last_day
    start_month = add_months(end_month, -(months - 1))
    return max(first_day, date(start_month[0], start_month[1], 1)), last_day


def daily_start(start: date, end: date) -> date:
    """The daily view shows the latest three calendar months of the window."""
    first: Month = add_months((end.year, end.month), -(DAILY_VIEW_MONTHS - 1))
    return max(start, date(first[0], first[1], 1))


def months_in_window(start: date, end: date) -> List[Month]:
    return month_range((start.year, start.month), (end.year, end.month))


def index_point(s: Sums, b: Optional[float], granularity: str) -> Dict[str, Any]:
    """One bar: index, energy against expected and mean wind. Index null below coverage."""
    wind = round(s.ws_weighted / s.ws_intervals, 2) if s.ws_intervals else None
    coverage = s.perf_intervals / s.reporting_intervals if s.reporting_intervals else 0.0
    point: Dict[str, Any] = {
        "index": None,
        "actual_mwh": None,
        "expected_mwh": None,
        "gain_mwh": None,
        "coverage": round(coverage, 3),
        "wind_ms": wind,
    }
    if b is None or s.potential_kwh <= 0 or coverage < COVERAGE_MIN[granularity]:
        return point
    expected = s.potential_kwh * b
    point.update(
        index=round(s.actual_kwh / expected * 100.0, 2),
        actual_mwh=round(s.actual_kwh / 1000.0, 3),
        expected_mwh=round(expected / 1000.0, 3),
        gain_mwh=round((s.actual_kwh - expected) / 1000.0, 3),
    )
    return point


def roll_up_years(
    monthly: Dict[Month, Sums], window_months: List[Month]
) -> List[Tuple[int, Sums, int]]:
    """(year, sums, months of that year inside the window) — ratio of sums per year."""
    by_year: Dict[int, Tuple[Sums, int]] = {}
    for m in window_months:
        acc, n = by_year.get(m[0], (Sums(), 0))
        by_year[m[0]] = (acc + monthly.get(m, Sums()), n + 1)
    return [(y, s, n) for y, (s, n) in sorted(by_year.items())]


def league(per_turbine: Dict[str, Sums], b: Optional[float]) -> Dict[str, Any]:
    """Per-turbine energy gained (+) or lost (−) against expected, worst first."""
    rows: List[Dict[str, Any]] = []
    if b is not None:
        for t, s in per_turbine.items():
            if s.potential_kwh <= 0:
                continue
            expected = s.potential_kwh * b
            rows.append(
                {
                    "t": t,
                    "gain_mwh": round((s.actual_kwh - expected) / 1000.0, 3),
                    "actual_mwh": round(s.actual_kwh / 1000.0, 3),
                    "expected_mwh": round(expected / 1000.0, 3),
                }
            )
    rows.sort(key=lambda r: (r["gain_mwh"], r["t"]))
    return {
        "turbines": rows,
        "fleet_net_mwh": round(sum(r["gain_mwh"] for r in rows), 3),
        "n_above": sum(1 for r in rows if r["gain_mwh"] > 0),
        "n_turbines": len(rows),
    }


def estimate_annual_gwh(
    yearly: Dict[int, Tuple[float, int]], n_turbines: int
) -> Tuple[Optional[float], List[int]]:
    """Mean generation of the complete calendar years: (GWh/yr, years used).

    ``yearly`` maps year -> (energy kWh, intervals with known energy). A year is complete
    when known intervals reach ESTIMATE_COVERAGE of every turbine's every interval.
    """
    if n_turbines <= 0:
        return None, []
    full = []
    for year, (kwh, known) in sorted(yearly.items()):
        days = (date(year + 1, 1, 1) - date(year, 1, 1)).days
        if known >= ESTIMATE_COVERAGE * days * INTERVALS_PER_DAY * n_turbines:
            full.append((year, kwh))
    if len(full) < ESTIMATE_MIN_YEARS:
        return None, []
    return sum(kwh for _, kwh in full) / len(full) / 1e6, [y for y, _ in full]


def p50_months(window_months: List[Month], start: date, end: date) -> List[Month]:
    """Window months with a whole month of data: a partial first or last month is dropped."""
    out = list(window_months)
    if out and start.day != 1:
        out = out[1:]
    if out and (end + timedelta(days=1)).day != 1:
        out = out[:-1]
    return out


# ─── Service ─────────────────────────────────────────────────────────────────


class ScadaPerformanceService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _require_columns(self) -> None:
        result = await self.db.execute(
            text(
                "SELECT 1 FROM information_schema.columns WHERE table_schema = 'scada' "
                "AND table_name = 'losses_daily' AND column_name = 'perf_actual_kwh'"
            )
        )
        if result.scalar() is None:
            raise PerformanceColumnsMissing()

    async def _farm(self, farm: str) -> Any:
        result = await self.db.execute(
            text(
                """
                SELECT f.farm, f.name, f.windfarm_id, f.bidzone,
                       COALESCE(f.rated_kw_total,
                                (SELECT SUM(rated_kw) FROM scada.dim_turbine t
                                 WHERE t.farm = f.farm)) AS rated_kw,
                       (SELECT COUNT(*) FROM scada.dim_turbine t WHERE t.farm = f.farm)
                           AS n_turbines,
                       EXISTS (SELECT 1 FROM scada.dim_signal_capability c
                               WHERE c.farm = f.farm AND c.signal = 'power_setpoint_kw'
                                 AND c.status = 'reported') AS has_setpoint,
                       (SELECT name FROM bidzones b WHERE b.code = f.bidzone LIMIT 1)
                           AS zone_name
                FROM scada.dim_farm f WHERE f.farm = :farm
                """
            ),
            {"farm": farm},
        )
        return result.fetchone()

    async def turbine_rated_kw(self, farm: str, turbine: str) -> Optional[float]:
        """None when the turbine is not in the farm; 0.0 when its rating is unknown."""
        result = await self.db.execute(
            text("SELECT rated_kw FROM scada.dim_turbine WHERE farm = :farm AND turbine = :t"),
            {"farm": farm, "t": turbine},
        )
        row = result.fetchone()
        if row is None:
            return None
        return float(row.rated_kw) if row.rated_kw is not None else 0.0

    async def _data_span(self, farm: str) -> Optional[Tuple[date, date]]:
        result = await self.db.execute(
            text(
                """
                SELECT MIN(date_utc) AS first, MAX(date_utc) AS last
                FROM scada.losses_daily
                WHERE farm = :farm AND NOT pre_cod AND perf_intervals > 0
                """
            ),
            {"farm": farm},
        )
        row = result.fetchone()
        if row is None or row.first is None:
            return None
        return row.first, row.last

    async def _baseline(self, farm: str) -> Optional[float]:
        result = await self.db.execute(
            text(
                """
                SELECT SUM(perf_actual_kwh) AS actual, SUM(perf_potential_kwh) AS potential
                FROM scada.losses_daily WHERE farm = :farm AND NOT pre_cod
                """
            ),
            {"farm": farm},
        )
        row = result.fetchone()
        return baseline(_f(row.actual), _f(row.potential)) if row else None

    _SUMS_SQL = """
        SUM(perf_actual_kwh) AS actual,
        SUM(perf_potential_kwh) AS potential,
        SUM(perf_intervals) AS perf_n,
        SUM(intervals_attributed + intervals_no_curve) AS reporting_n,
        SUM(ws_mean_ms * ws_intervals) AS ws_weighted,
        SUM(ws_intervals) AS ws_n
    """

    async def _series(
        self,
        farm: str,
        turbine: Optional[str],
        unit: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> Dict[date, Sums]:
        """Sums per day or month (unit is a fixed literal, never user input)."""
        bucket = {"day": "date_utc", "month": "date_trunc('month', date_utc)::date"}[unit]
        where = ["farm = :farm", "NOT pre_cod"]
        params: Dict[str, Any] = {"farm": farm}
        if turbine:
            where.append("turbine = :turbine")
            params["turbine"] = turbine
        if start is not None:
            where.append("date_utc >= :start")
            params["start"] = start
        if end is not None:
            where.append("date_utc <= :end")
            params["end"] = end
        result = await self.db.execute(
            text(
                f"SELECT {bucket} AS p, {self._SUMS_SQL} FROM scada.losses_daily "
                f"WHERE {' AND '.join(where)} GROUP BY 1"
            ),
            params,
        )
        return {r.p: _sums(r) for r in result.all()}

    async def _prices(
        self, windfarm_id: int, bidzone: Optional[str], unit: str, start: date, end: date
    ) -> Tuple[Dict[date, float], Optional[str]]:
        bucket = {"day": "day", "month": "month", "year": "year"}[unit]
        result = await self.db.execute(
            text(
                f"""
                SELECT (date_trunc('{bucket}', hour AT TIME ZONE 'UTC'))::date AS p,
                       AVG(day_ahead_price) AS price, MAX(currency) AS currency
                FROM price_data
                WHERE windfarm_id = :id AND source = :source
                  AND day_ahead_price IS NOT NULL
                  AND hour >= :start AND hour < :end
                GROUP BY 1
                """
            ),
            {
                "id": windfarm_id,
                "source": price_source_for(bidzone),
                "start": utc_midnight(start),
                "end": utc_midnight(end + timedelta(days=1)),
            },
        )
        rows = result.all()
        currency = next((r.currency for r in rows if r.currency), None)
        return {r.p: round(float(r.price), 2) for r in rows}, currency

    async def _energy_monthly(
        self, farm: str, turbine: Optional[str]
    ) -> Tuple[Dict[Month, float], Dict[int, Tuple[float, int]]]:
        """Full history: (GWh by month, (kWh, known intervals) by calendar year)."""
        where = "farm = :farm AND NOT pre_cod"
        params: Dict[str, Any] = {"farm": farm}
        if turbine:
            where += " AND turbine = :turbine"
            params["turbine"] = turbine
        result = await self.db.execute(
            text(
                f"""
                SELECT date_trunc('month', date_utc)::date AS m,
                       SUM(energy_kwh) AS kwh,
                       SUM(intervals_meter + intervals_integral) AS known
                FROM scada.energy_daily WHERE {where} GROUP BY 1
                """
            ),
            params,
        )
        monthly: Dict[Month, float] = {}
        yearly: Dict[int, Tuple[float, int]] = {}
        for r in result.all():
            kwh = _f(r.kwh)
            monthly[(r.m.year, r.m.month)] = kwh / 1e6
            y_kwh, y_known = yearly.get(r.m.year, (0.0, 0))
            yearly[r.m.year] = (y_kwh + kwh, y_known + _i(r.known))
        return monthly, yearly

    async def _p50(
        self,
        f: Any,
        turbine: Optional[str],
        turbine_kw: Optional[float],
        start: date,
        end: date,
    ) -> Optional[Dict[str, Any]]:
        months = p50_months(months_in_window(start, end), start, end)
        actual, yearly = await self._energy_monthly(f.farm, turbine)
        share = 1.0
        if turbine:
            farm_kw = _f(f.rated_kw)
            share = (turbine_kw or 0.0) / farm_kw if farm_kw > 0 else 0.0

        perform = PerformanceAnalyticsService(self.db)
        targets = await perform._p50_targets(f.windfarm_id) if f.windfarm_id else []
        if targets and share > 0:
            scaled = [{**t, "annual_gwh": t["annual_gwh"] * share} for t in targets]
            p50_by_month, used = perform.monthly_p50(scaled, months)
            latest = (used or scaled)[-1]
            return {
                "kind": "operator",
                "annual_target_gwh": round(latest["annual_gwh"], 3),
                "sources": [t["source"] for t in used if t["source"]],
                "basis_years": [],
                **(perform.p50_analysis(months, actual, p50_by_month) or EMPTY_P50_ANALYSIS),
                "annual_waterfalls": [],
            }

        n = 1 if turbine else _i(f.n_turbines)
        annual, years = estimate_annual_gwh(yearly, n)
        if annual is None:
            return None
        p50_by_month = {m: annual / 12.0 for m in months}
        return {
            "kind": "estimate",
            "annual_target_gwh": round(annual, 3),
            "sources": [],
            "basis_years": years,
            **(perform.p50_analysis(months, actual, p50_by_month) or EMPTY_P50_ANALYSIS),
            "annual_waterfalls": [],
        }

    # ─── Public ──────────────────────────────────────────────────────────

    async def performance(
        self, farm: str, period: str, granularity: str, turbine: Optional[str] = None
    ) -> Dict[str, Any]:
        await self.db.execute(text("SET LOCAL statement_timeout = '30s'"))
        await self._require_columns()
        f = await self._farm(farm)
        turbine_kw = await self.turbine_rated_kw(farm, turbine) if turbine else None
        farm_block = {
            "farm": farm,
            "name": f.name,
            "rated_mw": round(_f(f.rated_kw) / 1000.0, 1) if f.rated_kw else None,
            "has_curtailment_signal": bool(f.has_setpoint),
            "linked": f.windfarm_id is not None,
        }
        price_block = {
            "currency": None,
            "source": price_source_for(f.bidzone) if f.windfarm_id else None,
            "zone": f.zone_name,
        }
        span = await self._data_span(farm)
        if span is None:
            return {
                "farm": farm_block,
                "turbine": turbine,
                "granularity": granularity,
                "window": None,
                "price": price_block,
                "periods": [],
                "p50": None,
            }

        start, end = resolve_window(period, *span)
        b = await self._baseline(farm)
        history = await self._series(farm, turbine, "month")
        monthly = {(d.year, d.month): s for d, s in history.items()}
        window_months = months_in_window(start, end)
        day_from = daily_start(start, end)

        prices: Dict[date, float] = {}
        if f.windfarm_id:
            price_from = day_from if granularity == "day" else start
            prices, price_block["currency"] = await self._prices(
                f.windfarm_id, f.bidzone, granularity, price_from, end
            )

        periods: List[Dict[str, Any]] = []
        if granularity == "day":
            daily = await self._series(farm, turbine, "day", day_from, end)
            d = day_from
            while d <= end:
                periods.append(
                    {
                        "period": d.isoformat(),
                        "partial": False,
                        **index_point(daily.get(d, Sums()), b, "day"),
                        "spot_price": prices.get(d),
                        "rolling_12m": None,
                    }
                )
                d += timedelta(days=1)
        elif granularity == "month":
            history_index = {
                m: p["index"]
                for m, p in ((m, index_point(s, b, "month")) for m, s in monthly.items())
                if p["index"] is not None
            }
            rolling = PerformanceAnalyticsService.rolling_mean(history_index, window_months)
            for m in window_months:
                periods.append(
                    {
                        "period": month_key(*m),
                        "partial": False,
                        **index_point(monthly.get(m, Sums()), b, "month"),
                        "spot_price": prices.get(date(m[0], m[1], 1)),
                        "rolling_12m": rolling.get(m),
                    }
                )
        else:
            for year, s, n in roll_up_years(monthly, window_months):
                periods.append(
                    {
                        "period": str(year),
                        "partial": n < 12,
                        "months": n,
                        **index_point(s, b, "year"),
                        "spot_price": prices.get(date(year, 1, 1)),
                        "rolling_12m": None,
                    }
                )

        return {
            "farm": farm_block,
            "turbine": turbine,
            "granularity": granularity,
            "window": {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "day_start": day_from.isoformat(),
            },
            "price": price_block,
            "periods": periods,
            "p50": await self._p50(f, turbine, turbine_kw, start, end),
        }

    async def league(self, farm: str, period: str) -> Dict[str, Any]:
        await self.db.execute(text("SET LOCAL statement_timeout = '30s'"))
        await self._require_columns()
        span = await self._data_span(farm)
        if span is None:
            return {"window": None, **league({}, None)}
        start, end = resolve_window(period, *span)
        b = await self._baseline(farm)
        result = await self.db.execute(
            text(
                f"""
                SELECT turbine, {self._SUMS_SQL}
                FROM scada.losses_daily
                WHERE farm = :farm AND NOT pre_cod AND date_utc BETWEEN :start AND :end
                GROUP BY turbine
                """
            ),
            {"farm": farm, "start": start, "end": end},
        )
        per_turbine = {r.turbine: _sums(r) for r in result.all()}
        return {
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            **league(per_turbine, b),
        }
