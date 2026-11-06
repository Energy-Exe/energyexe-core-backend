"""
Generation page of the Performance Analytics section (EPR-187).

One payload for up to six wind farms over a day, month or year window:
net generation per period, capacity factor, and the baseload day-ahead price
of every price zone the farms sit in.

- Month and year views read `mv_generation_monthly_by_windfarm` (refreshed
  nightly; ~ms per farm). Years are rolled up from months.
- The day view reads hourly `generation_data` directly, so its window is
  capped (MAX_DAY_WINDOW_DAYS). Monthly-only sources (EIA, Energistyrelsen)
  store one row per month and are left out of the day view.
- Each farm-month uses its single best-covered source (sources are never
  added together, so a farm reported by two sources is not double counted);
  a monthly-only source covers its whole month.
- Capacity factor = MWh / (capacity in force x hours covered). Capacity comes
  from the unit registry per month (`app/utils/capacity.py`), so a farm that
  expanded or was partly decommissioned inside the window is measured against
  what was installed at the time. Years weight the monthly capacity by the
  hours each month covers.
- Prices are `AVG(day_ahead_price)` per period, ELEXON for GB and ENTSOE
  elsewhere, in the zone's native currency (as on the Performance page).
"""

import calendar
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional, Tuple, Union

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.generation_data import MONTHLY_SOURCES
from app.services.price_analytics_service import price_source_for
from app.utils.capacity import UnitWindow, load_unit_windows, monthly_capacity, weighted_capacity

Granularity = Literal["day", "month", "year"]

MAX_FARMS = 6
MAX_DAY_WINDOW_DAYS = 400

# A capacity is either one number for the whole window (legacy / tests) or a
# per-period mapping keyed like the points ('YYYY', 'YYYY-MM' or 'YYYY-MM-DD').
Capacity = Union[float, Mapping[str, float], None]


def capacity_for(capacity: Capacity, period: str) -> Optional[float]:
    if isinstance(capacity, Mapping):
        return capacity.get(period)
    return capacity


def period_key(d: date, granularity: Granularity) -> str:
    if granularity == "day":
        return d.isoformat()
    if granularity == "month":
        return f"{d.year:04d}-{d.month:02d}"
    return f"{d.year:04d}"


def period_keys(start: date, end: date, granularity: Granularity) -> List[str]:
    """Every period touching [start, end], oldest first."""
    keys: List[str] = []
    if granularity == "day":
        d = start
        while d <= end:
            keys.append(d.isoformat())
            d += timedelta(days=1)
    elif granularity == "month":
        y, m = start.year, start.month
        while (y, m) <= (end.year, end.month):
            keys.append(f"{y:04d}-{m:02d}")
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    else:
        keys = [f"{y:04d}" for y in range(start.year, end.year + 1)]
    return keys


def hours_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1] * 24


def utc_midnight(d: date) -> datetime:
    """`hour` is timestamptz; a bare `date` would bind as process-local midnight."""
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def best_source_month(rows: Iterable[Any]) -> Dict[int, Dict[str, Tuple[float, int]]]:
    """{windfarm_id: {'YYYY-MM': (MWh, hours)}} keeping one source per farm-month.

    `rows` carry windfarm_id, y, m, source, mwh, hours (one per source). The
    source covering the most hours wins (ties: more MWh); a monthly-only source
    covers its whole month. Sources are never summed, as in
    financial_opex_metrics.pick_best_generation.
    """
    out: Dict[int, Dict[str, Tuple[float, int]]] = {}
    for r in rows:
        hours = hours_in_month(r.y, r.m) if r.source in MONTHLY_SOURCES else int(r.hours or 0)
        cell = (float(r.mwh or 0), hours)
        farm = out.setdefault(r.windfarm_id, {})
        key = f"{r.y:04d}-{r.m:02d}"
        if key not in farm or (cell[1], cell[0]) > (farm[key][1], farm[key][0]):
            farm[key] = cell
    return out


class GenerationAnalyticsService:
    def __init__(self, db: AsyncSession):
        self.db = db

    # ------------------------------------------------------------------ pure

    @staticmethod
    def roll_up_years(
        monthly: Dict[str, Tuple[float, int]],
    ) -> Dict[str, Tuple[float, int]]:
        """{'YYYY-MM': (mwh, hours)} -> {'YYYY': (mwh, hours)}."""
        out: Dict[str, Tuple[float, int]] = {}
        for key, (mwh, hours) in monthly.items():
            y = key[:4]
            prev = out.get(y, (0.0, 0))
            out[y] = (prev[0] + mwh, prev[1] + hours)
        return out

    @staticmethod
    def capacity_factor(
        mwh: Optional[float], capacity_mw: Optional[float], hours: Optional[int]
    ) -> Optional[float]:
        if mwh is None or not capacity_mw or not hours:
            return None
        return mwh / (capacity_mw * hours)

    @staticmethod
    def capacity_by_period(
        windows: List[UnitWindow],
        nameplate: Optional[float],
        keys: List[str],
        granularity: Granularity,
        monthly_gen: Dict[str, Tuple[float, int]],
        windfarm_id: Optional[int] = None,
    ) -> Capacity:
        """Capacity in force per point key.

        Months use the registry directly; days take their month's value; years
        weight the months by the hours each one covers (months without data do
        not count), falling back to the plain mean when a year has no data.
        """
        if not nameplate:
            return None
        np_ = float(nameplate)
        if granularity == "month":
            return monthly_capacity(windows, keys, np_, windfarm_id=windfarm_id)
        if granularity == "day":
            months = sorted({k[:7] for k in keys})
            mc = monthly_capacity(windows, months, np_, windfarm_id=windfarm_id)
            return {k: mc[k[:7]] for k in keys}
        out: Dict[str, float] = {}
        for y in keys:
            months = [f"{y}-{m:02d}" for m in range(1, 13)]
            mc = monthly_capacity(windows, months, np_, windfarm_id=windfarm_id)
            hours = {m: monthly_gen[m][1] for m in months if m in monthly_gen}
            wc = weighted_capacity(mc, hours)
            out[y] = wc if wc is not None else sum(mc.values()) / len(mc)
        return out

    @staticmethod
    def summarise(points: List[Dict[str, Any]], capacity_mw: Capacity) -> Dict[str, Any]:
        """Window totals over the periods that have data."""
        filled = [p for p in points if p["mwh"] is not None]
        if not filled:
            return {
                "total_mwh": None,
                "avg_mwh": None,
                "cf": None,
                "periods_with_data": 0,
                "best": None,
                "worst": None,
            }
        total = sum(p["mwh"] for p in filled)
        best = max(filled, key=lambda p: p["mwh"])
        worst = min(filled, key=lambda p: p["mwh"])
        # Σ MWh / Σ (hours × capacity in force that period)
        cap_hours = 0.0
        for p in filled:
            cap = capacity_for(capacity_mw, p["period"])
            if not cap:
                cap_hours = 0.0
                break
            cap_hours += (p["hours"] or 0) * cap
        cf = total / cap_hours if cap_hours > 0 else None
        return {
            "total_mwh": round(total, 3),
            "avg_mwh": round(total / len(filled), 3),
            "cf": round(cf, 4) if cf is not None else None,
            "periods_with_data": len(filled),
            "best": {"period": best["period"], "mwh": best["mwh"]},
            "worst": {"period": worst["period"], "mwh": worst["mwh"]},
        }

    @classmethod
    def build_points(
        cls,
        keys: Iterable[str],
        by_period: Dict[str, Tuple[float, int]],
        capacity_mw: Capacity,
    ) -> List[Dict[str, Any]]:
        points = []
        for k in keys:
            if k in by_period:
                mwh, hours = by_period[k]
                cf = cls.capacity_factor(mwh, capacity_for(capacity_mw, k), hours)
                points.append(
                    {
                        "period": k,
                        "mwh": round(mwh, 3),
                        "hours": hours,
                        "cf": round(cf, 4) if cf is not None else None,
                    }
                )
            else:
                points.append({"period": k, "mwh": None, "hours": None, "cf": None})
        return points

    # --------------------------------------------------------------- queries

    async def _windfarms(self, ids: List[int]) -> List[Any]:
        result = await self.db.execute(
            text(
                """
                SELECT w.id, w.name, w.nameplate_capacity_mw, w.bidzone_id,
                       b.code AS bidzone_code, b.name AS bidzone_name,
                       c.code AS country_code
                FROM windfarms w
                LEFT JOIN bidzones b ON b.id = w.bidzone_id
                LEFT JOIN countries c ON c.id = w.country_id
                WHERE w.id IN :ids AND w.is_deleted = false
                """
            ).bindparams(bindparam("ids", expanding=True)),
            {"ids": ids},
        )
        by_id = {r.id: r for r in result.fetchall()}
        return [by_id[i] for i in ids if i in by_id]

    async def _monthly_generation(
        self, ids: List[int], start: Optional[date] = None, end: Optional[date] = None
    ) -> Dict[int, Dict[str, Tuple[float, int]]]:
        """{windfarm_id: {'YYYY-MM': (net MWh, hours covered)}}, best source per month.

        Shared by the Performance, Generation and Comparison pages; no bounds
        means the farm's whole history.
        """
        result = await self.db.execute(
            text(
                """
                SELECT windfarm_id, source, EXTRACT(YEAR FROM month)::int AS y,
                       EXTRACT(MONTH FROM month)::int AS m,
                       net_mwh AS mwh, hours_with_data AS hours
                FROM mv_generation_monthly_by_windfarm
                WHERE windfarm_id IN :ids AND hours_with_data > 0
                  AND month >= :start AND month <= :end
                """
            ).bindparams(bindparam("ids", expanding=True)),
            {
                "ids": ids,
                "start": date(start.year, start.month, 1) if start else date.min,
                "end": end or date.max,
            },
        )
        return best_source_month(result.all())

    async def _daily_generation(
        self, ids: List[int], start: date, end: date
    ) -> Dict[int, Dict[str, Tuple[float, int]]]:
        """{windfarm_id: {'YYYY-MM-DD': (net MWh, hours covered)}} from hourly rows."""
        result = await self.db.execute(
            text(
                """
                SELECT windfarm_id, day, SUM(mwh) AS mwh, MAX(hours) AS hours
                FROM (
                    SELECT windfarm_id, source,
                           (date_trunc('day', hour AT TIME ZONE 'UTC'))::date AS day,
                           SUM(generation_mwh - COALESCE(consumption_mwh, 0)) AS mwh,
                           COUNT(DISTINCT hour) AS hours
                    FROM generation_data
                    WHERE windfarm_id IN :ids AND source NOT IN :monthly
                      AND hour >= :start AND hour < :end
                    GROUP BY 1, 2, 3
                ) per_source
                GROUP BY 1, 2
                """
            ).bindparams(bindparam("ids", expanding=True), bindparam("monthly", expanding=True)),
            {
                "ids": ids,
                "monthly": list(MONTHLY_SOURCES),
                "start": utc_midnight(start),
                "end": utc_midnight(end + timedelta(days=1)),
            },
        )
        out: Dict[int, Dict[str, Tuple[float, int]]] = {}
        for r in result.all():
            out.setdefault(r.windfarm_id, {})[r.day.isoformat()] = (
                float(r.mwh or 0),
                int(r.hours or 0),
            )
        return out

    async def _zone_prices(
        self,
        windfarm_id: int,
        source: str,
        granularity: Granularity,
        start: date,
        end: date,
    ) -> Tuple[Dict[str, float], Optional[str]]:
        """Mean day-ahead price per period for one zone.

        `price_data` is keyed by wind farm and every farm in a zone carries the
        zone price, so one farm's series is the zone's.
        """
        result = await self.db.execute(
            text(
                f"""
                SELECT (date_trunc('{granularity}', hour AT TIME ZONE 'UTC'))::date AS p,
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
                "source": source,
                "start": utc_midnight(start),
                "end": utc_midnight(end + timedelta(days=1)),
            },
        )
        rows = result.all()
        currency = next((r.currency for r in rows if r.currency), None)
        return {period_key(r.p, granularity): round(float(r.price), 2) for r in rows}, currency

    # ------------------------------------------------------------------ main

    async def get_generation(
        self,
        windfarm_ids: List[int],
        granularity: Granularity,
        start: date,
        end: date,
    ) -> Dict[str, Any]:
        await self.db.execute(text("SET LOCAL statement_timeout = '30s'"))
        farms = await self._windfarms(windfarm_ids)
        ids = [f.id for f in farms]
        keys = period_keys(start, end, granularity)

        monthly: Dict[int, Dict[str, Tuple[float, int]]] = {}
        if not ids:
            gen: Dict[int, Dict[str, Tuple[float, int]]] = {}
        elif granularity == "day":
            gen = await self._daily_generation(ids, start, end)
        else:
            monthly = await self._monthly_generation(ids, start, end)
            gen = monthly
            if granularity == "year":
                gen = {wf: self.roll_up_years(m) for wf, m in monthly.items()}
        windows = await load_unit_windows(self.db, ids) if ids else {}

        farm_rows = []
        for f in farms:
            caps = self.capacity_by_period(
                windows.get(f.id, []),
                f.nameplate_capacity_mw,
                keys,
                granularity,
                monthly.get(f.id, {}),
                windfarm_id=f.id,
            )
            points = self.build_points(keys, gen.get(f.id, {}), caps)
            farm_rows.append(
                {
                    "id": f.id,
                    "name": f.name,
                    "capacity_mw": f.nameplate_capacity_mw,
                    "price_zone": f.bidzone_name,
                    "country": f.country_code,
                    "points": points,
                    "summary": self.summarise(points, caps),
                }
            )

        # One price series per zone, in the order the farms were picked.
        zones: Dict[int, List[Any]] = {}
        for f in farms:
            if f.bidzone_id is not None:
                zones.setdefault(f.bidzone_id, []).append(f)
        prices = []
        for zone_farms in zones.values():
            head = zone_farms[0]
            source = price_source_for(head.bidzone_code)
            by_period, currency = await self._zone_prices(head.id, source, granularity, start, end)
            if not by_period:
                continue
            prices.append(
                {
                    "zone": head.bidzone_name,
                    "currency": currency,
                    "source": source,
                    "windfarm_ids": [f.id for f in zone_farms],
                    "points": [{"period": k, "price": by_period.get(k)} for k in keys],
                }
            )

        return {
            "granularity": granularity,
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "farms": farm_rows,
            "prices": prices,
        }
