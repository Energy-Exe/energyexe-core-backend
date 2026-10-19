"""
Performance Analytics — one aggregate payload per wind farm (EPR-186).

The client "Performance" page needs four things for one farm over a month
window, and each is one indexed, month-grouped query:

1. The monthly wind-normalised performance index (`performance_summaries`,
   written nightly by the pipeline; 100 = the farm's own long-run mean).
2. The monthly baseload day-ahead price in the farm's price zone
   (`price_data`, ELEXON for GB, ENTSOE elsewhere — native currency).
3. Optionally, the mean index of the other farms in the same price zone or
   country, as a benchmark.
4. Monthly net generation (`mv_generation_monthly_by_windfarm`, refreshed
   nightly) against the operator-published P50 target, with a waterfall that
   splits the gap into wind resource and the farm's own performance.

Generation comes from the monthly materialised view rather than the hourly
table: one farm's full history is ~7 ms there versus tens of seconds cold on
`generation_data`. The view does not drop ramp-up hours, so totals can differ
slightly from the wind-farm page's P50 card in a farm's first months.

Waterfall (per month with an index):
    expected = actual / (index / 100)   # output at the farm's normal performance
    wind     = expected - p50_month
    perf     = actual - expected
A month without an index puts its whole gap on wind (perf = 0). The totals
therefore always reconcile: p50 + wind + perf = actual.
"""

from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Seeded fallback targets (3-yr historical mean) are not operator P50s; the
# page treats them as "no P50" (scripts/seeds/p50/import_p50_targets.py).
FALLBACK_P50_MARKER = "auto-computed"

ROLLING_WINDOW_MONTHS = 12
ROLLING_MIN_VALUES = 9

GB_BIDZONE_CODE = "10YGB----------A"

Month = Tuple[int, int]


def month_key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def parse_month(value: str) -> Month:
    """'YYYY-MM' -> (year, month); raises ValueError on anything else."""
    year_s, month_s = value.split("-")
    year, month = int(year_s), int(month_s)
    if not 1 <= month <= 12 or len(year_s) != 4:
        raise ValueError(value)
    return year, month


def add_months(m: Month, n: int) -> Month:
    idx = m[0] * 12 + (m[1] - 1) + n
    return idx // 12, idx % 12 + 1


def month_range(start: Month, end: Month) -> List[Month]:
    out: List[Month] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur = add_months(cur, 1)
    return out


def is_fallback_target(comment: Optional[str], source: Optional[str]) -> bool:
    for value in (comment, source):
        if value and (FALLBACK_P50_MARKER in value.lower() or "fallback computed" in value.lower()):
            return True
    return False


class PerformanceAnalyticsService:
    def __init__(self, db: AsyncSession):
        self.db = db

    # ─── Pure computation (unit-tested without a DB) ─────────────────────

    @staticmethod
    def rolling_mean(
        index_by_month: Dict[Month, float], months: List[Month]
    ) -> Dict[Month, Optional[float]]:
        """Trailing 12-calendar-month mean of the index for each month.

        Uses the full history, so the first month of the window already has a
        full trailing year. Null when fewer than 9 of the 12 months have data.
        """
        out: Dict[Month, Optional[float]] = {}
        for m in months:
            values = [
                index_by_month[w]
                for w in (add_months(m, -i) for i in range(ROLLING_WINDOW_MONTHS))
                if w in index_by_month
            ]
            out[m] = (
                round(sum(values) / len(values), 2) if len(values) >= ROLLING_MIN_VALUES else None
            )
        return out

    @staticmethod
    def p50_analysis(
        months: List[Month],
        actual_gwh: Dict[Month, float],
        p50_gwh: Dict[Month, float],
        index_by_month: Dict[Month, float],
    ) -> Optional[Dict[str, Any]]:
        """Cumulative P50 vs actual and the wind/performance waterfall.

        Only months that have both a P50 target and generation data count.
        """
        used = [m for m in months if m in p50_gwh and m in actual_gwh]
        if not used:
            return None

        cumulative: List[Dict[str, Any]] = []
        cum_p50 = cum_actual = 0.0
        wind = perf = 0.0
        without_index = 0
        for m in used:
            actual = actual_gwh[m]
            target = p50_gwh[m]
            cum_p50 += target
            cum_actual += actual
            cumulative.append(
                {
                    "month": month_key(*m),
                    "cumulative_p50_gwh": round(cum_p50, 3),
                    "cumulative_actual_gwh": round(cum_actual, 3),
                    "cumulative_gap_gwh": round(cum_actual - cum_p50, 3),
                }
            )
            index = index_by_month.get(m)
            if index is not None and index > 0:
                expected = actual / (index / 100.0)
                wind += expected - target
                perf += actual - expected
            else:
                without_index += 1
                wind += actual - target

        n = len(used)
        return {
            "months_used": n,
            "first_month": month_key(*used[0]),
            "last_month": month_key(*used[-1]),
            "avg_annual_generation_gwh": round(cum_actual / n * 12, 3),
            "gap_gwh": round(cum_actual - cum_p50, 3),
            "cumulative": cumulative,
            "waterfall": {
                "p50_gwh": round(cum_p50, 3),
                "wind_resource_gwh": round(wind, 3),
                "performance_gwh": round(perf, 3),
                "actual_gwh": round(cum_actual, 3),
                "months_without_index": without_index,
            },
        }

    @staticmethod
    def monthly_p50(
        targets: List[Dict[str, Any]], months: List[Month]
    ) -> Tuple[Dict[Month, float], List[Dict[str, Any]]]:
        """Annual / 12 for each month a (non-fallback) target covers.

        Returns the per-month P50 and the targets actually used, in order.
        """
        by_month: Dict[Month, float] = {}
        used: List[Dict[str, Any]] = []
        for m in months:
            first_day = date(m[0], m[1], 1)
            covering = [
                t
                for t in targets
                if t["start"] <= first_day and (t["end"] is None or t["end"] >= first_day)
            ]
            if not covering:
                continue
            t = max(covering, key=lambda x: x["start"])
            by_month[m] = t["annual_gwh"] / 12.0
            if t not in used:
                used.append(t)
        return by_month, used

    # ─── Queries ─────────────────────────────────────────────────────────

    async def _windfarm(self, windfarm_id: int) -> Optional[Any]:
        result = await self.db.execute(
            text(
                """
                SELECT w.id, w.name, w.nameplate_capacity_mw, w.bidzone_id, w.country_id,
                       b.code AS bidzone_code, b.name AS bidzone_name,
                       c.name AS country_name
                FROM windfarms w
                LEFT JOIN bidzones b ON b.id = w.bidzone_id
                LEFT JOIN countries c ON c.id = w.country_id
                WHERE w.id = :id AND w.is_deleted = false
                """
            ),
            {"id": windfarm_id},
        )
        return result.fetchone()

    async def _index_history(self, windfarm_id: int) -> Dict[Month, float]:
        result = await self.db.execute(
            text(
                """
                SELECT year, month, norm_index_p50
                FROM performance_summaries
                WHERE windfarm_id = :id AND period_type = 'month'
                  AND month IS NOT NULL AND norm_index_p50 IS NOT NULL
                """
            ),
            {"id": windfarm_id},
        )
        return {(r.year, r.month): float(r.norm_index_p50) for r in result.all()}

    async def _monthly_prices(
        self, windfarm_id: int, source: str, start: Month, end: Month
    ) -> Tuple[Dict[Month, float], Optional[str]]:
        result = await self.db.execute(
            text(
                """
                SELECT EXTRACT(YEAR FROM hour)::int AS y, EXTRACT(MONTH FROM hour)::int AS m,
                       AVG(day_ahead_price) AS price, MAX(currency) AS currency
                FROM price_data
                WHERE windfarm_id = :id AND source = :source
                  AND day_ahead_price IS NOT NULL
                  AND hour >= :start AND hour < :end
                GROUP BY 1, 2
                """
            ),
            {
                "id": windfarm_id,
                "source": source,
                "start": date(start[0], start[1], 1),
                "end": date(*add_months(end, 1), 1),
            },
        )
        rows = result.all()
        currency = next((r.currency for r in rows if r.currency), None)
        return {(r.y, r.m): round(float(r.price), 2) for r in rows}, currency

    async def _benchmark(
        self, wf: Any, kind: str, start: Month, end: Month
    ) -> Tuple[Dict[Month, float], int, Optional[str]]:
        if kind == "zone":
            if wf.bidzone_id is None:
                return {}, 0, None
            column, value, name = "bidzone_id", wf.bidzone_id, f"Price zone {wf.bidzone_name}"
        else:
            column, value, name = "country_id", wf.country_id, wf.country_name
        result = await self.db.execute(
            text(
                f"""
                WITH peer AS (
                    SELECT ps.year, ps.month, ps.windfarm_id, ps.norm_index_p50
                    FROM performance_summaries ps
                    JOIN windfarms w ON w.id = ps.windfarm_id
                    WHERE ps.period_type = 'month' AND ps.norm_index_p50 IS NOT NULL
                      AND w.{column} = :value AND w.id <> :id AND w.is_deleted = false
                      AND (ps.year * 100 + ps.month) BETWEEN :lo AND :hi
                )
                SELECT year, month, AVG(norm_index_p50) AS idx,
                       (SELECT COUNT(DISTINCT windfarm_id) FROM peer) AS peers
                FROM peer
                GROUP BY year, month
                """
            ),
            {
                "value": value,
                "id": wf.id,
                "lo": start[0] * 100 + start[1],
                "hi": end[0] * 100 + end[1],
            },
        )
        rows = result.all()
        peer_count = int(rows[0].peers) if rows else 0
        return {(r.year, r.month): round(float(r.idx), 2) for r in rows}, peer_count, name

    async def _monthly_generation(
        self, windfarm_id: int, start: Month, end: Month
    ) -> Dict[Month, float]:
        result = await self.db.execute(
            text(
                """
                SELECT EXTRACT(YEAR FROM month)::int AS y, EXTRACT(MONTH FROM month)::int AS m,
                       SUM(net_mwh) / 1000.0 AS gwh
                FROM mv_generation_monthly_by_windfarm
                WHERE windfarm_id = :id AND month >= :start AND month <= :end
                  AND hours_with_data > 0
                GROUP BY 1, 2
                """
            ),
            {
                "id": windfarm_id,
                "start": date(start[0], start[1], 1),
                "end": date(end[0], end[1], 1),
            },
        )
        return {(r.y, r.m): float(r.gwh or 0) for r in result.all()}

    async def _p50_targets(self, windfarm_id: int) -> List[Dict[str, Any]]:
        result = await self.db.execute(
            text(
                """
                SELECT p50_target_start_date, p50_target_end_date, p50_target_volume_gwh,
                       source, comment
                FROM p50_targets
                WHERE windfarm_id = :id
                ORDER BY p50_target_start_date
                """
            ),
            {"id": windfarm_id},
        )
        return [
            {
                "start": r.p50_target_start_date,
                "end": r.p50_target_end_date,
                "annual_gwh": float(r.p50_target_volume_gwh),
                "source": r.source,
            }
            for r in result.all()
            if r.p50_target_volume_gwh is not None and not is_fallback_target(r.comment, r.source)
        ]

    # ─── Public ──────────────────────────────────────────────────────────

    async def get_performance(
        self,
        windfarm_id: int,
        start: Month,
        end: Month,
        benchmark: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        wf = await self._windfarm(windfarm_id)
        if wf is None:
            return None

        await self.db.execute(text("SET LOCAL statement_timeout = '30s'"))

        months = month_range(start, end)
        index_by_month = await self._index_history(windfarm_id)
        rolling = self.rolling_mean(index_by_month, months)

        price_source = "ELEXON" if wf.bidzone_code == GB_BIDZONE_CODE else "ENTSOE"
        prices, currency = await self._monthly_prices(windfarm_id, price_source, start, end)

        bench: Dict[Month, float] = {}
        bench_meta: Optional[Dict[str, Any]] = None
        if benchmark in ("zone", "country"):
            bench, peer_count, bench_name = await self._benchmark(wf, benchmark, start, end)
            if bench_name:
                bench_meta = {"kind": benchmark, "name": bench_name, "peer_count": peer_count}

        generation = await self._monthly_generation(windfarm_id, start, end)
        targets = await self._p50_targets(windfarm_id)
        p50_by_month, used_targets = self.monthly_p50(targets, months)

        p50_block: Optional[Dict[str, Any]] = None
        analysis = self.p50_analysis(months, generation, p50_by_month, index_by_month)
        if analysis is not None:
            latest = used_targets[-1]
            p50_block = {
                "annual_target_gwh": round(latest["annual_gwh"], 3),
                "sources": [t["source"] for t in used_targets if t["source"]],
                **analysis,
            }

        available = sorted(index_by_month)
        return {
            "windfarm": {
                "id": wf.id,
                "name": wf.name,
                "capacity_mw": wf.nameplate_capacity_mw,
                "price_zone": wf.bidzone_name,
                "country": wf.country_name,
            },
            "window": {"start": month_key(*start), "end": month_key(*end)},
            "available_range": (
                {"first": month_key(*available[0]), "last": month_key(*available[-1])}
                if available
                else None
            ),
            "price": {"currency": currency, "source": price_source, "zone": wf.bidzone_name},
            "benchmark": bench_meta,
            "months": [
                {
                    "month": month_key(*m),
                    "index": (round(index_by_month[m], 2) if m in index_by_month else None),
                    "rolling_12m": rolling[m],
                    "spot_price": prices.get(m),
                    "benchmark_index": bench.get(m),
                    "actual_gwh": (round(generation[m], 3) if m in generation else None),
                    "p50_gwh": (round(p50_by_month[m], 3) if m in p50_by_month else None),
                }
                for m in months
            ],
            "p50": p50_block,
        }
