"""
Comparison page of the Performance Analytics section (EPR-188).

Monthly capacity factor for every wind farm in the compared markets (Norway,
the UK and Denmark), with the metadata the page filters on: country, price
zone, onshore/offshore and owners. Filtering, averages and highlighting happen
in the browser, so one cached payload serves every filter combination.

- Generation comes from `mv_generation_monthly_by_windfarm` (net MWh summed
  over sources, hours covered = the best single source's hour count; a
  monthly-only source covers its whole month), as on the Generation page.
- Capacity factor = MWh / (current nameplate MW x hours covered).
- A month is left out (null) when its sources cover under half of it, when
  it reports no net generation, or when the capacity factor exceeds 100 %:
  partial months, gaps stored as zeros and data glitches would otherwise pull
  the market average around.
"""

from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.generation_data import MONTHLY_SOURCES
from app.services.generation_analytics_service import (
    GenerationAnalyticsService,
    hours_in_month,
    period_keys,
)

# ISO alpha-3 codes of the compared markets (user decision, 2026-10-20).
COUNTRIES = ("NOR", "GBR", "DNK")
MIN_MONTH_COVERAGE = 0.5


def month_cf(
    mwh: float, capacity_mw: Optional[float], hours: int, month_key: str
) -> Optional[float]:
    """Capacity factor in % (1 decimal), or None when the month is unusable."""
    y, m = int(month_key[:4]), int(month_key[5:7])
    if hours < MIN_MONTH_COVERAGE * hours_in_month(y, m):
        return None
    cf = GenerationAnalyticsService.capacity_factor(mwh, capacity_mw, hours)
    if cf is None or cf <= 0 or cf > 1:
        return None
    return round(cf * 100, 1)


def cf_series(
    months: Sequence[str],
    by_month: Dict[str, Tuple[float, int]],
    capacity_mw: Optional[float],
) -> List[Optional[float]]:
    """One value per month in `months`; None where there is no usable data."""
    out: List[Optional[float]] = []
    for k in months:
        cell = by_month.get(k)
        out.append(month_cf(cell[0], capacity_mw, cell[1], k) if cell else None)
    return out


class ComparisonAnalyticsService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _windfarms(self) -> List[Any]:
        result = await self.db.execute(
            text(
                """
                SELECT w.id, w.name, w.nameplate_capacity_mw, w.location_type,
                       c.code AS country_code, b.name AS bidzone_name
                FROM windfarms w
                JOIN countries c ON c.id = w.country_id
                LEFT JOIN bidzones b ON b.id = w.bidzone_id
                WHERE w.is_deleted = false AND c.code IN :countries
                  AND w.nameplate_capacity_mw > 0
                ORDER BY w.name
                """
            ).bindparams(bindparam("countries", expanding=True)),
            {"countries": list(COUNTRIES)},
        )
        return list(result.fetchall())

    async def _owners(self) -> Dict[int, List[Dict[str, Any]]]:
        result = await self.db.execute(
            text(
                """
                SELECT wo.windfarm_id, o.name, wo.ownership_percentage AS pct
                FROM windfarm_owners wo
                JOIN owners o ON o.id = wo.owner_id
                JOIN windfarms w ON w.id = wo.windfarm_id
                JOIN countries c ON c.id = w.country_id
                WHERE w.is_deleted = false AND c.code IN :countries
                ORDER BY wo.windfarm_id, wo.ownership_percentage DESC, o.name
                """
            ).bindparams(bindparam("countries", expanding=True)),
            {"countries": list(COUNTRIES)},
        )
        out: Dict[int, List[Dict[str, Any]]] = {}
        for r in result.all():
            out.setdefault(r.windfarm_id, []).append(
                {"name": r.name, "pct": float(r.pct) if r.pct is not None else None}
            )
        return out

    async def _monthly_generation(
        self, start: date, end: date
    ) -> Dict[int, Dict[str, Tuple[float, int]]]:
        """{windfarm_id: {'YYYY-MM': (net MWh over sources, hours covered)}}."""
        result = await self.db.execute(
            text(
                """
                SELECT mv.windfarm_id, EXTRACT(YEAR FROM mv.month)::int AS y,
                       EXTRACT(MONTH FROM mv.month)::int AS m,
                       SUM(mv.net_mwh) AS mwh,
                       MAX(CASE WHEN mv.source IN :monthly THEN -1
                                ELSE mv.hours_with_data END) AS hours
                FROM mv_generation_monthly_by_windfarm mv
                JOIN windfarms w ON w.id = mv.windfarm_id
                JOIN countries c ON c.id = w.country_id
                WHERE c.code IN :countries AND w.is_deleted = false
                  AND mv.hours_with_data > 0
                  AND mv.month >= :start AND mv.month <= :end
                GROUP BY 1, 2, 3
                """
            ).bindparams(
                bindparam("monthly", expanding=True),
                bindparam("countries", expanding=True),
            ),
            {
                "monthly": list(MONTHLY_SOURCES),
                "countries": list(COUNTRIES),
                "start": start,
                "end": end,
            },
        )
        out: Dict[int, Dict[str, Tuple[float, int]]] = {}
        for r in result.all():
            # -1 marks a monthly-only source: it covers the whole month.
            hours = int(r.hours or 0)
            if hours < 0:
                hours = hours_in_month(r.y, r.m)
            out.setdefault(r.windfarm_id, {})[f"{r.y:04d}-{r.m:02d}"] = (
                float(r.mwh or 0),
                hours,
            )
        return out

    async def get_comparison(self, start: date, end: date) -> Dict[str, Any]:
        """`start` is the first day of the first month, `end` the last month's last day."""
        await self.db.execute(text("SET LOCAL statement_timeout = '30s'"))
        months = period_keys(start, end, "month")
        farms = await self._windfarms()
        owners = await self._owners()
        gen = await self._monthly_generation(start, end)

        rows = []
        for f in farms:
            cf = cf_series(months, gen.get(f.id, {}), f.nameplate_capacity_mw)
            if all(v is None for v in cf):
                continue
            rows.append(
                {
                    "id": f.id,
                    "name": f.name,
                    "capacity_mw": f.nameplate_capacity_mw,
                    "country": f.country_code,
                    "zone": f.bidzone_name,
                    "type": f.location_type,
                    "owners": owners.get(f.id, []),
                    "cf": cf,
                }
            )

        return {
            "window": {"start": months[0], "end": months[-1]},
            "months": months,
            "farms": rows,
        }
