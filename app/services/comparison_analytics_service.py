"""
Comparison page of the Performance Analytics section (EPR-188).

Monthly capacity factor for every wind farm in the compared markets (Norway,
the UK and Denmark), with the metadata the page filters on: country, price
zone, onshore/offshore and owners. Filtering, averages and highlighting happen
in the browser, so one cached payload serves every filter combination.

- Generation comes from `mv_generation_monthly_by_windfarm` through the
  Generation page's reader (best single source per farm-month; a monthly-only
  source covers its whole month).
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

    async def owners(self, ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        """Owners per farm id, largest stake first."""
        result = await self.db.execute(
            text(
                """
                SELECT wo.windfarm_id, o.name, wo.ownership_percentage AS pct
                FROM windfarm_owners wo
                JOIN owners o ON o.id = wo.owner_id
                WHERE wo.windfarm_id IN :ids
                ORDER BY wo.windfarm_id, wo.ownership_percentage DESC, o.name
                """
            ).bindparams(bindparam("ids", expanding=True)),
            {"ids": ids},
        )
        out: Dict[int, List[Dict[str, Any]]] = {}
        for r in result.all():
            out.setdefault(r.windfarm_id, []).append(
                {"name": r.name, "pct": float(r.pct) if r.pct is not None else None}
            )
        return out

    async def get_comparison(self, start: date, end: date) -> Dict[str, Any]:
        """`start` and `end` are days inside the first and last months."""
        await self.db.execute(text("SET LOCAL statement_timeout = '30s'"))
        months = period_keys(start, end, "month")
        farms = await self._windfarms()
        ids = [f.id for f in farms]
        owners = await self.owners(ids)
        gen = await GenerationAnalyticsService(self.db)._monthly_generation(ids, start, end)

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
