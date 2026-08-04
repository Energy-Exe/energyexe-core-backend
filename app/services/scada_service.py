"""SCADA portal service — read-only analytics over the gold schema `scada`.

Serves the 14 dashboard charts prototyped in
energyexe-scada-pipeline/docs/ui/ (queries.sql is the SQL source of truth;
dashdata.js is the shape/fixture reference). Payloads mirror the prototype's
embedded snapshot exactly so the frontend renderers port 1:1.

Semantics rules baked in (docs/ui/README.md):
- completeness uses rows_valid_core, never rows_present (greenbyte NaN padding)
- per-turbine reliability = forced-bucket events_started, never alarm hours
- settlement recon signal = monthly ratio-of-sums, never averaged daily pct
- degradation interval-weights epoch-straddling turbine-years
- all tables are UTC-day keyed (date_utc)

The scada schema is NOT on the search_path — every query is schema-qualified.
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

_ONE_HOUR = timedelta(hours=1)

# Once the schema is seen it never disappears mid-process; absence is rechecked
# per call so the prod cut lights the feature up without a restart.
_schema_seen = False


async def scada_schema_present(db: AsyncSession) -> bool:
    """True when the gold schema exists in the connected database."""
    global _schema_seen
    if _schema_seen:
        return True
    try:
        result = await db.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'scada' AND table_name = 'dim_farm' LIMIT 1"
            )
        )
        present = result.scalar() is not None
    except Exception:
        logger.warning("scada_schema_check_failed", exc_info=True)
        return False
    if present:
        _schema_seen = True
    return present


def _median(values: Sequence[float]) -> Optional[float]:
    vals = sorted(values)
    if not vals:
        return None
    mid = len(vals) // 2
    if len(vals) % 2:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2


def _percentile(values: Sequence[float], pct: float) -> Optional[float]:
    """Exclusive-method quantile ((n+1)·p), matching statistics.quantiles."""
    vals = sorted(values)
    if not vals:
        return None
    k = (len(vals) + 1) * pct - 1
    k = min(max(k, 0.0), len(vals) - 1)
    lo, hi = int(k), min(int(k) + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def _slope_pct_per_year(points: Sequence[Tuple[int, float]]) -> Optional[float]:
    """Least-squares slope of performance index vs year, in %/yr."""
    if len(points) < 3:
        return None
    n = len(points)
    mx = sum(p[0] for p in points) / n
    my = sum(p[1] for p in points) / n
    var = sum((p[0] - mx) ** 2 for p in points)
    if var == 0:
        return None
    cov = sum((p[0] - mx) * (p[1] - my) for p in points)
    return cov / var * 100


def _spearman_rho(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    """Spearman rank correlation with average ranks for ties."""
    if len(xs) < 3 or len(xs) != len(ys):
        return None

    def ranks(vals: Sequence[float]) -> List[float]:
        order = sorted(range(len(vals)), key=lambda i: vals[i])
        rank = [0.0] * len(vals)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and vals[order[j + 1]] == vals[order[i]]:
                j += 1
            avg = (i + j) / 2 + 1
            for k in range(i, j + 1):
                rank[order[k]] = avg
            i = j + 1
        return rank

    rx, ry = ranks(xs), ranks(ys)
    n = len(rx)
    mx, my = sum(rx) / n, sum(ry) / n
    sx = sum((r - mx) ** 2 for r in rx) ** 0.5
    sy = sum((r - my) ** 2 for r in ry) ** 0.5
    if sx == 0 or sy == 0:
        return None
    return sum((rx[i] - mx) * (ry[i] - my) for i in range(n)) / (sx * sy)


class ScadaService:
    """One method per dashboard chart; all read-only raw SQL over scada.*."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def farm_slugs(self) -> List[str]:
        result = await self.db.execute(text("SELECT farm FROM scada.dim_farm ORDER BY farm"))
        return [row.farm for row in result.fetchall()]

    async def farms(self) -> Dict[str, Any]:
        """Farm metadata + per-farm data-through for the freshness header."""
        dims = await self.db.execute(text("SELECT * FROM scada.dim_farm ORDER BY farm"))
        through = await self.db.execute(
            text(
                "SELECT farm, MAX(date_utc) AS data_through, COUNT(*) AS days "
                "FROM scada.farm_kpis_daily GROUP BY farm"
            )
        )
        through_by_farm = {
            row.farm: {"data_through": row.data_through.isoformat(), "days": row.days}
            for row in through.fetchall()
        }
        turbines = await self.db.execute(
            text("SELECT farm, COUNT(*) AS n FROM scada.dim_turbine GROUP BY farm")
        )
        n_by_farm = {row.farm: row.n for row in turbines.fetchall()}
        farms = []
        for row in dims.fetchall():
            entry = {
                k: (v.isoformat() if isinstance(v, date) else v) for k, v in row._mapping.items()
            }
            entry["n_turbines"] = n_by_farm.get(entry["farm"], 0)
            entry.update(through_by_farm.get(entry["farm"], {}))
            farms.append(entry)
        return {"farms": farms}

    async def energy_waterfall(self, farm: str, start: date, end: date) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT SUM(potential_kwh)        AS pot,
                       SUM(loss_downtime_kwh)    AS dt,
                       SUM(loss_curtailment_kwh) AS ct,
                       SUM(loss_performance_kwh) AS pf,
                       SUM(loss_total_kwh)       AS lt,
                       SUM(energy_kwh)           AS energy
                FROM scada.farm_kpis_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                """
            ),
            {"farm": farm, "start": start, "end": end},
        )
        row = result.fetchone()
        return {
            k: (float(getattr(row, k)) if getattr(row, k) is not None else None)
            for k in ("pot", "dt", "ct", "pf", "lt", "energy")
        }

    async def revenue_waterfall(self, farm: str, start: date, end: date) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT SUM(revenue_gross_gbp)       AS g,
                       SUM(revenue_downtime_gbp)    AS d,
                       SUM(revenue_curtailment_gbp) AS c,
                       SUM(revenue_performance_gbp) AS p,
                       SUM(revenue_loss_total_gbp)  AS lt,
                       SUM(hours_priced)            AS hp,
                       SUM(hours_unpriced)          AS hu,
                       COUNT(*)                     AS days
                FROM scada.revenue_impact_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                """
            ),
            {"farm": farm, "start": start, "end": end},
        )
        row = result.fetchone()
        out: Dict[str, Any] = {
            k: (float(getattr(row, k)) if getattr(row, k) is not None else None)
            for k in ("g", "d", "c", "p", "lt")
        }
        out["hp"] = int(row.hp or 0)
        out["hu"] = int(row.hu or 0)
        out["days"] = int(row.days or 0)
        out["cur"] = "GBP"
        return out

    async def availability(self, farm: str, days: int) -> Dict[str, Any]:
        """Turbine × day grid over the farm's last `days` data days."""
        result = await self.db.execute(
            text(
                """
                WITH mx AS (
                    SELECT MAX(date_utc) AS m FROM scada.availability_daily WHERE farm = :farm
                )
                SELECT turbine, date_utc,
                       ROUND(unavailable_h::numeric, 2)   AS un,
                       ROUND(unaccounted_h::numeric, 2)   AS ua,
                       ROUND(availability_pct::numeric, 1) AS av
                FROM scada.availability_daily, mx
                WHERE farm = :farm AND date_utc > mx.m - CAST(:days AS integer)
                ORDER BY turbine, date_utc
                """
            ),
            {"farm": farm, "days": days},
        )
        rows = result.fetchall()
        turbines = sorted({r.turbine for r in rows})
        dates = sorted({r.date_utc for r in rows})
        d_idx = {d: i for i, d in enumerate(dates)}
        t_idx = {t: i for i, t in enumerate(turbines)}
        grid: List[List[Optional[List[float]]]] = [[None] * len(dates) for _ in turbines]
        for r in rows:
            grid[t_idx[r.turbine]][d_idx[r.date_utc]] = [
                float(r.un or 0),
                float(r.ua or 0),
                float(r.av) if r.av is not None else None,
            ]
        return {
            "turbines": turbines,
            "dates": [d.isoformat() for d in dates],
            "grid": grid,
        }

    async def completeness(self, farm: str) -> Dict[str, Any]:
        """Turbine × month grid. Valid-core is the truth; present shown for contrast."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, TO_CHAR(DATE_TRUNC('month', date_utc), 'YYYY-MM') AS mo,
                       SUM(rows_present)       AS rp,
                       SUM(rows_valid_core)    AS rv,
                       SUM(expected_intervals) AS ex,
                       BOOL_OR(pre_cod)        AS pre
                FROM scada.completeness_daily
                WHERE farm = :farm
                GROUP BY 1, 2
                ORDER BY 1, 2
                """
            ),
            {"farm": farm},
        )
        rows = result.fetchall()
        turbines = sorted({r.turbine for r in rows})
        present = sorted({r.mo for r in rows})
        months: List[str] = []
        if present:
            # Full-year span (Jan of first data year → Dec of last) so farm-wide
            # holes render as visible gaps instead of silently vanishing.
            for y in range(int(present[0][:4]), int(present[-1][:4]) + 1):
                months.extend(f"{y:04d}-{m:02d}" for m in range(1, 13))
        m_idx = {m: i for i, m in enumerate(months)}
        t_idx = {t: i for i, t in enumerate(turbines)}
        grid: List[List[Optional[List[float]]]] = [[None] * len(months) for _ in turbines]
        for r in rows:
            ex = float(r.ex or 0)
            if ex <= 0:
                continue
            grid[t_idx[r.turbine]][m_idx[r.mo]] = [
                round(float(r.rv or 0) / ex * 100, 1),
                1 if r.pre else 0,
                round(float(r.rp or 0) / ex * 100, 1),
            ]
        return {"turbines": turbines, "months": months, "grid": grid}

    async def _weighted_pi_by_turbine_year(self, farm: str) -> Dict[str, Dict[int, float]]:
        """performance_index interval-weighted across epoch-straddling configs."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, year, performance_index AS pi, intervals_used AS iu
                FROM scada.turbine_performance_yearly
                WHERE farm = :farm
                ORDER BY turbine, year
                """
            ),
            {"farm": farm},
        )
        acc: Dict[str, Dict[int, List[float]]] = {}
        for r in result.fetchall():
            if r.pi is None or not r.iu:
                continue
            cell = acc.setdefault(r.turbine, {}).setdefault(int(r.year), [0.0, 0.0])
            cell[0] += float(r.pi) * float(r.iu)
            cell[1] += float(r.iu)
        return {
            t: {y: v[0] / v[1] for y, v in years.items() if v[1] > 0} for t, years in acc.items()
        }

    async def degradation(self, farm: str) -> Dict[str, Any]:
        weighted = await self._weighted_pi_by_turbine_year(farm)
        years = sorted({y for series in weighted.values() for y in series})
        series = {
            t: [round(vals[y], 4) if y in vals else None for y in years]
            for t, vals in sorted(weighted.items())
        }
        median = [
            round(m, 4)
            if (m := _median([vals[y] for vals in weighted.values() if y in vals])) is not None
            else None
            for y in years
        ]
        # Slope fits exclude the trailing partial year — a few months of data
        # would dominate the fit (matches the validated prototype snapshot).
        through = await self.db.execute(
            text("SELECT MAX(date_utc) AS m FROM scada.farm_kpis_daily WHERE farm = :farm"),
            {"farm": farm},
        )
        max_date = through.scalar()
        last_complete = (
            max_date.year
            if max_date and (max_date.month, max_date.day) == (12, 31)
            else (max_date.year - 1 if max_date else None)
        )
        slopes = {
            t: s
            for t, vals in weighted.items()
            if (
                s := _slope_pct_per_year(
                    sorted(
                        (y, v)
                        for y, v in vals.items()
                        if last_complete is None or y <= last_complete
                    )
                )
            )
            is not None
        }
        worst = min(slopes, key=slopes.get) if slopes else None
        return {
            "years": years,
            "series": series,
            "median": median,
            "worst": worst,
            "worstSlope": round(slopes[worst], 2) if worst else None,
            "medSlope": round(m, 2) if (m := _median(list(slopes.values()))) is not None else None,
        }

    async def heartbeat(self, days: int) -> Dict[str, Any]:
        """Per-farm KPI strips over each farm's own last `days` data days."""
        kpis = await self.db.execute(
            text(
                """
                WITH mx AS (SELECT farm, MAX(date_utc) AS m FROM scada.farm_kpis_daily GROUP BY farm)
                SELECT f.farm, f.date_utc,
                       ROUND((f.energy_kwh / 1e3)::numeric, 1)  AS e_mwh,
                       ROUND(f.availability_pct::numeric, 1)     AS av,
                       ROUND((100 * f.loss_total_kwh / NULLIF(f.potential_kwh, 0))::numeric, 1) AS loss_pct
                FROM scada.farm_kpis_daily f JOIN mx ON mx.farm = f.farm
                WHERE f.date_utc > mx.m - CAST(:days AS integer)
                ORDER BY 1, 2
                """
            ),
            {"days": days},
        )
        alarms = await self.db.execute(
            text(
                """
                WITH mx AS (SELECT farm, MAX(date_utc) AS m FROM scada.farm_kpis_daily GROUP BY farm)
                SELECT a.farm, a.date_utc, ROUND(SUM(a.alarm_hours)::numeric, 1) AS ah
                FROM scada.alarm_code_daily a JOIN mx ON mx.farm = a.farm
                WHERE a.date_utc > mx.m - CAST(:days AS integer)
                GROUP BY 1, 2
                ORDER BY 1, 2
                """
            ),
            {"days": days},
        )
        ah_by_key = {(r.farm, r.date_utc): float(r.ah or 0) for r in alarms.fetchall()}
        out: Dict[str, Any] = {}
        for r in kpis.fetchall():
            entry = out.setdefault(r.farm, {"dates": [], "e": [], "av": [], "lp": [], "ah": []})
            entry["dates"].append(r.date_utc.isoformat())
            entry["e"].append(float(r.e_mwh) if r.e_mwh is not None else None)
            entry["av"].append(float(r.av) if r.av is not None else None)
            entry["lp"].append(float(r.loss_pct) if r.loss_pct is not None else None)
            entry["ah"].append(ah_by_key.get((r.farm, r.date_utc), 0.0))
        return out

    async def alarm_pareto(self, farm: str, year: int, limit: int) -> Dict[str, Any]:
        """Top codes by alarm hours — a burden metric, NOT downtime (co-firing sums)."""
        start, end = date(year, 1, 1), date(year, 12, 31)
        rows_res = await self.db.execute(
            text(
                """
                SELECT a.source_code AS code,
                       COALESCE(d.bucket, '?')  AS bucket,
                       COALESCE(d.status, '')   AS st,
                       LEFT(COALESCE(d.message, ''), 40) AS msg,
                       ROUND(SUM(a.alarm_hours)::numeric, 0) AS h
                FROM scada.alarm_code_daily a
                LEFT JOIN scada.dim_alarm_code d
                  ON d.source_code = a.source_code
                 AND d.source_format = (
                        SELECT MIN(source_format) FROM scada.dim_alarm_code
                        WHERE source_code = a.source_code)
                WHERE a.farm = :farm AND a.date_utc BETWEEN :start AND :end
                GROUP BY 1, 2, 3, 4
                ORDER BY h DESC
                LIMIT :limit
                """
            ),
            {"farm": farm, "start": start, "end": end, "limit": limit},
        )
        total_res = await self.db.execute(
            text(
                """
                SELECT SUM(alarm_hours) AS total FROM scada.alarm_code_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                """
            ),
            {"farm": farm, "start": start, "end": end},
        )
        total = total_res.scalar()
        return {
            "rows": [
                {"code": r.code, "bucket": r.bucket, "st": r.st, "msg": r.msg, "h": float(r.h)}
                for r in rows_res.fetchall()
            ],
            "total": round(float(total), 0) if total is not None else 0,
        }

    async def downtime_fingerprint(self, farm: str, year: int) -> Dict[str, Any]:
        """Hour-of-day × month grid of lost energy (MWh)."""
        result = await self.db.execute(
            text(
                """
                SELECT EXTRACT(MONTH FROM date_utc)::int AS mo,
                       EXTRACT(HOUR FROM hour_utc AT TIME ZONE 'UTC')::int AS hr,
                       ROUND(SUM(loss_total_kwh)::numeric) AS lk
                FROM scada.losses_hourly
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                GROUP BY 1, 2
                ORDER BY 1, 2
                """
            ),
            {"farm": farm, "start": date(year, 1, 1), "end": date(year, 12, 31)},
        )
        grid = [[0.0] * 12 for _ in range(24)]
        for r in result.fetchall():
            grid[int(r.hr)][int(r.mo) - 1] = round(float(r.lk or 0) / 1e3, 1)
        return {"grid": grid, "max": max((v for row in grid for v in row), default=0)}

    async def cumulative_losses(self, farm: str, year: int) -> List[Dict[str, Any]]:
        """Daily £ loss rows; missing dates are real gaps — render as gaps, never interpolate."""
        result = await self.db.execute(
            text(
                """
                SELECT date_utc,
                       ROUND(revenue_downtime_gbp::numeric)    AS d,
                       ROUND(revenue_curtailment_gbp::numeric) AS c,
                       ROUND(revenue_performance_gbp::numeric) AS p
                FROM scada.revenue_impact_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                ORDER BY 1
                """
            ),
            {"farm": farm, "start": date(year, 1, 1), "end": date(year, 12, 31)},
        )
        return [
            {
                "date_utc": r.date_utc.isoformat(),
                "d": float(r.d or 0),
                "c": float(r.c or 0),
                "p": float(r.p or 0),
            }
            for r in result.fetchall()
        ]

    async def settlement_recon(self, farm: str, year: int) -> Dict[str, Any]:
        """Daily deltas are noisy; the signal is the monthly ratio-of-sums."""
        start, end = date(year, 1, 1), date(year, 12, 31)
        daily = await self.db.execute(
            text(
                """
                SELECT date_utc, ROUND(energy_delta_pct::numeric, 2) AS dp, hours_both AS hb
                FROM scada.settlement_recon_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                ORDER BY 1
                """
            ),
            {"farm": farm, "start": start, "end": end},
        )
        monthly = await self.db.execute(
            text(
                """
                SELECT TO_CHAR(DATE_TRUNC('month', date_utc), 'YYYY-MM') AS mo,
                       ROUND((100 * (SUM(scada_energy_mwh) - SUM(settlement_metered_mwh))
                             / NULLIF(SUM(scada_energy_mwh), 0))::numeric, 2) AS dp
                FROM scada.settlement_recon_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                  AND hours_both >= 20
                GROUP BY 1
                ORDER BY 1
                """
            ),
            {"farm": farm, "start": start, "end": end},
        )
        rows = [
            [r.date_utc.isoformat(), float(r.dp) if r.dp is not None else None, int(r.hb or 0)]
            for r in daily.fetchall()
        ]
        comparable = [r[1] for r in rows if r[1] is not None]
        return {
            "rows": rows,
            "med": round(m, 1) if (m := _median(comparable)) is not None else None,
            "p5": round(p, 1) if (p := _percentile(comparable, 0.05)) is not None else None,
            "p95": round(p, 1) if (p := _percentile(comparable, 0.95)) is not None else None,
            "monthly": [
                [r.mo, float(r.dp) if r.dp is not None else None] for r in monthly.fetchall()
            ],
        }

    async def scada_vs_boav(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT date_utc,
                       ROUND(scada_curtailment_mwh::numeric, 1)    AS s,
                       ROUND(settlement_curtailed_mwh::numeric, 1) AS b
                FROM scada.settlement_recon_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                  AND (scada_curtailment_mwh > 0.5 OR settlement_curtailed_mwh > 0.5)
                ORDER BY 1
                """
            ),
            {"farm": farm, "start": date(year, 1, 1), "end": date(year, 12, 31)},
        )
        pts = [
            [float(r.s or 0), float(r.b or 0), r.date_utc.isoformat()] for r in result.fetchall()
        ]
        rho = _spearman_rho([p[0] for p in pts], [p[1] for p in pts])
        return {"pts": pts, "rho": round(rho, 2) if rho is not None else None}

    async def aeroup(self, farm: str) -> Dict[str, Any]:
        """Epoch power-curve comparison — a measurement CAVEAT, not an uplift figure.

        Nacelle-wind epoch curves cannot measure retrofit uplift (anemometer flow
        distortion + year confounds); the fleet's apparent deltas prove it.
        Display turbine = the one closest to the fleet-median apparent delta.
        """
        result = await self.db.execute(
            text(
                """
                SELECT turbine, config, ws_bin, power_p50_kw AS p50, n
                FROM scada.power_curve_bins
                WHERE farm = :farm
                ORDER BY turbine, config, ws_bin
                """
            ),
            {"farm": farm},
        )
        curves: Dict[str, Dict[str, Dict[float, float]]] = {}
        counts: Dict[str, Dict[str, Dict[float, int]]] = {}
        for r in result.fetchall():
            if r.p50 is None:
                continue
            curves.setdefault(r.turbine, {}).setdefault(r.config, {})[float(r.ws_bin)] = float(
                r.p50
            )
            counts.setdefault(r.turbine, {}).setdefault(r.config, {})[float(r.ws_bin)] = int(
                r.n or 0
            )
        deltas: Dict[str, float] = {}
        for turbine, configs in curves.items():
            if len(configs) < 2:
                continue
            names = sorted(configs, key=lambda c: (c != "baseline", c))
            base, up = configs[names[0]], configs[names[-1]]
            bins = [b for b in base if b in up and 5 <= b <= 12 and base[b] > 0]
            if not bins:
                continue
            # Sample-count-weighted mean of per-bin % deltas (matches the
            # validated prototype snapshot; unweighted mean/median do not).
            num = den = 0.0
            for b in bins:
                w = counts[turbine][names[0]].get(b, 0) + counts[turbine][names[-1]].get(b, 0)
                num += (up[b] - base[b]) / base[b] * 100 * w
                den += w
            if den > 0:
                deltas[turbine] = num / den
        if not deltas:
            return {
                "turbine": None,
                "base": [],
                "up": [],
                "uplift": None,
                "fleetMin": None,
                "fleetMax": None,
                "fleetMed": None,
            }
        fleet_med = _median(list(deltas.values()))
        display = min(deltas, key=lambda t: (abs(deltas[t] - fleet_med), t))
        configs = curves[display]
        names = sorted(configs, key=lambda c: (c != "baseline", c))
        return {
            "turbine": display,
            "base": sorted([[b, round(p, 1)] for b, p in configs[names[0]].items()]),
            "up": sorted([[b, round(p, 1)] for b, p in configs[names[-1]].items()]),
            "uplift": round(deltas[display], 1),
            "fleetMin": round(min(deltas.values()), 1),
            "fleetMax": round(max(deltas.values()), 1),
            "fleetMed": round(fleet_med, 1),
        }

    async def annual_cost(self, start_year: int, end_year: int) -> Dict[str, Any]:
        """All farms' GWh losses by cause + HoT £ labels.

        Kelmarsh/Penmanshiel curtailment 0.00 is a known signal gap (no setpoint
        reported), not zero curtailment — the frontend labels it.
        """
        gwh = await self.db.execute(
            text(
                """
                SELECT farm, EXTRACT(YEAR FROM date_utc)::int AS y,
                       ROUND((SUM(loss_downtime_kwh) / 1e6)::numeric, 2)    AS dt,
                       ROUND((SUM(loss_curtailment_kwh) / 1e6)::numeric, 2) AS ct,
                       ROUND((SUM(loss_performance_kwh) / 1e6)::numeric, 2) AS pf
                FROM scada.farm_kpis_daily
                WHERE date_utc BETWEEN :start AND :end
                GROUP BY 1, 2
                ORDER BY 1, 2
                """
            ),
            {"start": date(start_year, 1, 1), "end": date(end_year, 12, 31)},
        )
        gbp = await self.db.execute(
            text(
                """
                SELECT EXTRACT(YEAR FROM date_utc)::int AS y,
                       ROUND(SUM(revenue_loss_total_gbp)::numeric / 1000) AS k
                FROM scada.revenue_impact_daily
                WHERE farm = 'hill_of_towie' AND date_utc BETWEEN :start AND :end
                GROUP BY 1
                ORDER BY 1
                """
            ),
            {"start": date(start_year, 1, 1), "end": date(end_year, 12, 31)},
        )
        return {
            "rows": [
                {
                    "farm": r.farm,
                    "y": int(r.y),
                    "dt": float(r.dt or 0),
                    "ct": float(r.ct or 0),
                    "pf": float(r.pf or 0),
                }
                for r in gwh.fetchall()
            ],
            "gbp": {str(int(r.y)): int(r.k or 0) for r in gbp.fetchall()},
        }

    async def method_mix(self) -> List[Dict[str, Any]]:
        result = await self.db.execute(
            text(
                """
                SELECT TO_CHAR(DATE_TRUNC('month', date_utc), 'YYYY-MM') AS mo,
                       SUM(n_meter) AS m, SUM(n_integral) AS i, SUM(n_mixed) AS x
                FROM scada.farm_kpis_daily
                GROUP BY 1
                ORDER BY 1
                """
            )
        )
        return [
            {"mo": r.mo, "m": int(r.m or 0), "i": int(r.i or 0), "x": int(r.x or 0)}
            for r in result.fetchall()
        ]

    async def league(self, farm: str, year: int) -> List[Dict[str, Any]]:
        """Turbines ranked by interval-weighted perf index for `year`.

        Reliability column = forced-bucket events_started — alarm HOURS sum
        across co-firing codes and read absurd per-turbine.
        """
        weighted = await self._weighted_pi_by_turbine_year(farm)
        start, end = date(year, 1, 1), date(year, 12, 31)
        avail = await self.db.execute(
            text(
                """
                SELECT turbine,
                       ROUND((100 * SUM(available_h) / NULLIF(SUM(expected_h), 0))::numeric, 1) AS av
                FROM scada.availability_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                GROUP BY 1
                """
            ),
            {"farm": farm, "start": start, "end": end},
        )
        av_by_t = {r.turbine: float(r.av) if r.av is not None else None for r in avail.fetchall()}
        forced = await self.db.execute(
            text(
                """
                SELECT a.turbine, SUM(a.events_started) AS ev
                FROM scada.alarm_code_daily a
                JOIN scada.dim_alarm_code d
                  ON d.source_code = a.source_code AND d.bucket = 'forced'
                WHERE a.farm = :farm AND a.date_utc BETWEEN :start AND :end
                GROUP BY 1
                """
            ),
            {"farm": farm, "start": start, "end": end},
        )
        ev_by_t = {r.turbine: int(r.ev or 0) for r in forced.fetchall()}
        rows = []
        for turbine, vals in weighted.items():
            if year not in vals:
                continue
            # Derive from the 4dp series precision (what the degradation chart
            # shows) so the two charts never disagree in their last digit.
            pi4 = round(vals[year], 4)
            pi = round(pi4, 3)
            prev = round(round(vals[year - 1], 4), 3) if year - 1 in vals else None
            rows.append(
                {
                    "t": turbine,
                    "pi": pi,
                    "d": round(pi - prev, 3) if prev is not None else None,
                    "av": av_by_t.get(turbine),
                    "ev": ev_by_t.get(turbine, 0),
                }
            )
        # Rank on the displayed precision; ties stay in turbine order (stable sort).
        rows.sort(key=lambda r: r["pi"], reverse=True)
        return rows

    # ---- silver-viz charts (2026-08-03 build) --------------------------------
    # Rollup tables are computed by the pipeline's `scada gold-viz` lane; these
    # methods stay thin. Farm-limited signals (setpoint/freq/PQ = HoT, IEC
    # categories = greenbyte) return empty payloads the frontend maps to
    # honest empty states.

    _IEC_UNAVAILABLE = (
        "Forced outage",
        "Scheduled Maintenance",
        "Requested Shutdown",
        "Out of Electrical Specification",
    )
    _ICE_CODES = ("102", "8210", "8230", "8234", "8235", "8236")

    async def curtailment_episodes(self, farm: str, year: int) -> Dict[str, Any]:
        """Episodes from the viz lane; kWh prorated from losses_hourly so the
        portal never shows a second curtailment total."""
        eps_res = await self.db.execute(
            text(
                """
                SELECT episode_start AS s, episode_end AS e, duration_min AS dur,
                       n_turbines_max AS nmax, depth_pct_mean AS depth,
                       turbine_intervals AS ti
                FROM scada.viz_curtailment_episodes
                WHERE farm = :farm AND EXTRACT(YEAR FROM episode_start) = :year
                ORDER BY episode_start
                """
            ),
            {"farm": farm, "year": year},
        )
        episodes = eps_res.fetchall()
        hours_res = await self.db.execute(
            text(
                """
                SELECT hour_utc, loss_curtailment_kwh AS kwh
                FROM scada.losses_hourly
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                  AND loss_curtailment_kwh > 0
                """
            ),
            {"farm": farm, "start": date(year, 1, 1), "end": date(year, 12, 31)},
        )
        kwh_by_hour = {r.hour_utc.replace(tzinfo=None): float(r.kwh) for r in hours_res.fetchall()}
        year_total = round(sum(kwh_by_hour.values()), 0)
        out = []
        for r in episodes:
            kwh = 0.0
            h = r.s.replace(minute=0, second=0, microsecond=0)
            while h < r.e:
                hour_kwh = kwh_by_hour.get(h)
                if hour_kwh:
                    overlap_s = (min(r.e, h + _ONE_HOUR) - max(r.s, h)).total_seconds()
                    kwh += hour_kwh * max(0.0, min(1.0, overlap_s / 3600))
                h += _ONE_HOUR
            out.append(
                {
                    "s": r.s.isoformat(),
                    "e": r.e.isoformat(),
                    "dur": float(r.dur),
                    "nmax": int(r.nmax),
                    "depth": round(float(r.depth), 1) if r.depth is not None else None,
                    "kwh": round(kwh, 0),
                }
            )
        return {"episodes": out, "year_curtailment_kwh": year_total}

    async def storms(self, farm: str, year: int) -> Dict[str, Any]:
        days_res = await self.db.execute(
            text(
                """
                SELECT date_utc AS d, gust_max_ms AS gust, ws_mean_ms AS ws,
                       energy_mwh AS mwh, pct_of_rated_energy AS pct,
                       stopped_turbine_h AS sth
                FROM scada.viz_storm_days
                WHERE farm = :farm AND EXTRACT(YEAR FROM date_utc) = :year
                ORDER BY date_utc
                """
            ),
            {"farm": farm, "year": year},
        )
        days = [
            {
                "d": r.d.isoformat(),
                "gust": round(float(r.gust), 1),
                "ws": round(float(r.ws), 1) if r.ws is not None else None,
                "mwh": round(float(r.mwh), 1) if r.mwh is not None else None,
                "pct": round(float(r.pct), 1) if r.pct is not None else None,
                "sth": round(float(r.sth), 1) if r.sth is not None else None,
            }
            for r in days_res.fetchall()
        ]
        worst = None
        if days:
            worst_day = max(days, key=lambda d: d["gust"])["d"]
            pts_res = await self.db.execute(
                text(
                    """
                    SELECT ts_start_utc AS ts, fleet_mw AS mw, gust_max_ms AS gust,
                           ws_mean_ms AS ws, n_stopped AS ns
                    FROM scada.viz_storm_10min
                    WHERE farm = :farm AND ts_start_utc::date = :d
                    ORDER BY ts_start_utc
                    """
                ),
                {"farm": farm, "d": date.fromisoformat(worst_day)},
            )
            pts = [
                {
                    "t": r.ts.isoformat(),
                    "mw": round(float(r.mw), 2) if r.mw is not None else None,
                    "gust": round(float(r.gust), 1) if r.gust is not None else None,
                    "ws": round(float(r.ws), 1) if r.ws is not None else None,
                    "ns": int(r.ns or 0),
                }
                for r in pts_res.fetchall()
            ]
            worst = {"day": worst_day, "pts": pts}
        return {"days": days, "worst": worst}

    async def icing(self, farm: str, year: int) -> Dict[str, Any]:
        """Detector days + per-turbine hours; ice-alarm hours overlaid where the
        OEM reports ice codes. No lost-energy figure by design."""
        cal_res = await self.db.execute(
            text(
                """
                SELECT date_utc AS d, COUNT(*) AS nt,
                       ROUND((SUM(intervals_detector) / 6.0)::numeric, 1) AS h
                FROM scada.viz_icing_daily
                WHERE farm = :farm AND EXTRACT(YEAR FROM date_utc) = :year
                  AND intervals_detector > 0
                GROUP BY 1 ORDER BY 1
                """
            ),
            {"farm": farm, "year": year},
        )
        turb_res = await self.db.execute(
            text(
                """
                SELECT turbine, ROUND((SUM(intervals_detector) / 6.0)::numeric, 1) AS h
                FROM scada.viz_icing_daily
                WHERE farm = :farm AND EXTRACT(YEAR FROM date_utc) = :year
                GROUP BY 1 ORDER BY 1
                """
            ),
            {"farm": farm, "year": year},
        )
        alarm_res = await self.db.execute(
            text(
                """
                SELECT turbine, ROUND(SUM(alarm_hours)::numeric, 1) AS h
                FROM scada.alarm_code_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                  AND source_code = ANY(:codes)
                GROUP BY 1
                """
            ),
            {
                "farm": farm,
                "start": date(year, 1, 1),
                "end": date(year, 12, 31),
                "codes": list(self._ICE_CODES),
            },
        )
        alarm_by_t = {r.turbine: float(r.h) for r in alarm_res.fetchall()}
        return {
            "days": [
                {"d": r.d.isoformat(), "nt": int(r.nt), "h": float(r.h)} for r in cal_res.fetchall()
            ],
            "turbines": [
                {
                    "t": r.turbine,
                    "det_h": float(r.h),
                    "alarm_h": alarm_by_t.get(r.turbine),
                }
                for r in turb_res.fetchall()
            ],
        }

    async def outage_gantt(self, farm: str, year: int, min_hours: float = 1.0) -> Dict[str, Any]:
        """Significant stoppage intervals, clipped to the year.

        Greenbyte farms: IEC unavailable categories. Siemens: stop-class
        alarms, categorized by the taxonomy bucket (proposed|confirmed)."""
        fmt_res = await self.db.execute(
            text("SELECT source_format FROM scada.dim_farm WHERE farm = :farm"),
            {"farm": farm},
        )
        source_format = fmt_res.scalar()
        ystart, yend = datetime(year, 1, 1), datetime(year + 1, 1, 1)
        if source_format == "greenbyte":
            where = "iec_category = ANY(:cats)"
            cat_expr = "iec_category"
            params: Dict[str, Any] = {"cats": list(self._IEC_UNAVAILABLE)}
        else:
            where = "severity_class = 'stop'"
            cat_expr = """
                COALESCE((SELECT d.bucket FROM scada.dim_alarm_code d
                          WHERE d.source_code = a.source_code
                            AND d.source_format = :fmt LIMIT 1), 'other')
            """
            params = {"fmt": source_format}
        result = await self.db.execute(
            text(
                f"""
                SELECT a.turbine,
                       GREATEST(a.time_on, :ystart) AS s,
                       LEAST(a.time_off, :yend)     AS e,
                       {cat_expr}                   AS cat,
                       a.source_code                AS code,
                       LEFT(COALESCE(a.message, ''), 60) AS msg
                FROM scada.alarm_events a
                WHERE a.farm = :farm AND {where}
                  AND a.time_off IS NOT NULL
                  AND a.duration_h >= :min_h
                  AND a.time_on < :yend AND a.time_off > :ystart
                ORDER BY a.turbine, a.time_on
                """
            ),
            {
                "farm": farm,
                "ystart": ystart,
                "yend": yend,
                "min_h": min_hours,
                **params,
            },
        )
        intervals = [
            {
                "t": r.turbine,
                "s": r.s.isoformat(),
                "e": r.e.isoformat(),
                "cat": r.cat,
                "code": r.code,
                "msg": r.msg,
            }
            for r in result.fetchall()
        ]
        turbines = sorted({i["t"] for i in intervals})
        return {"turbines": turbines, "intervals": intervals, "min_hours": min_hours}

    async def state_hours(self, farm: str, year: int) -> Dict[str, Any]:
        """Availability-state hours per turbine from availability_daily — the
        pipeline's precedence/overlap-merged accounting. Raw alarm_events are
        NOT summed here: greenbyte status events are mostly unbracketed
        (KWF1 2023: 102 of 4,855 Full Performance events carry time_off), so
        interval sums undercount wildly and unevenly."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, MIN(method) AS method,
                       ROUND(SUM(available_h)::numeric, 1)          AS avail,
                       ROUND(SUM(unavail_forced_h)::numeric, 1)     AS forced,
                       ROUND(SUM(unavail_scheduled_h)::numeric, 1)  AS sched,
                       ROUND(SUM(unavail_external_h)::numeric, 1)   AS ext,
                       ROUND(SUM(unavail_requested_h)::numeric, 1)  AS req,
                       ROUND(SUM(unavail_unclassified_h)::numeric, 1) AS uncls,
                       ROUND(SUM(unaccounted_h)::numeric, 1)        AS unacc
                FROM scada.availability_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                  AND NOT pre_cod
                GROUP BY 1 ORDER BY 1
                """
            ),
            {"farm": farm, "start": date(year, 1, 1), "end": date(year, 12, 31)},
        )
        turbines = []
        for r in result.fetchall():
            cats = {
                "available": float(r.avail or 0),
                "forced": float(r.forced or 0),
                "scheduled": float(r.sched or 0),
                "external": float(r.ext or 0),
                "requested": float(r.req or 0),
                "unclassified": float(r.uncls or 0),
                "unaccounted": float(r.unacc or 0),
            }
            turbines.append({"t": r.turbine, "method": r.method, "cats": cats})
        return {"turbines": turbines}

    async def alarm_heatmap(self, farm: str, year: int, limit: int = 12) -> Dict[str, Any]:
        """Alarm onsets per turbine x ISO week + the year's chattiest codes."""
        grid_res = await self.db.execute(
            text(
                """
                SELECT turbine, DATE_TRUNC('week', date_utc)::date AS wk,
                       SUM(events_started) AS ev
                FROM scada.alarm_code_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                GROUP BY 1, 2 ORDER BY 1, 2
                """
            ),
            {"farm": farm, "start": date(year, 1, 1), "end": date(year, 12, 31)},
        )
        rows = grid_res.fetchall()
        turbines = sorted({r.turbine for r in rows})
        weeks = sorted({r.wk for r in rows})
        w_idx = {w: i for i, w in enumerate(weeks)}
        t_idx = {t: i for i, t in enumerate(turbines)}
        grid: List[List[int]] = [[0] * len(weeks) for _ in turbines]
        for r in rows:
            grid[t_idx[r.turbine]][w_idx[r.wk]] = int(r.ev or 0)
        chatter_res = await self.db.execute(
            text(
                """
                SELECT a.source_code AS code,
                       LEFT(COALESCE(d.message, ''), 40) AS msg,
                       SUM(a.events_started) AS ev,
                       ROUND(SUM(a.alarm_hours)::numeric, 0) AS h
                FROM scada.alarm_code_daily a
                LEFT JOIN scada.dim_alarm_code d
                  ON d.source_code = a.source_code
                 AND d.source_format = (
                        SELECT MIN(source_format) FROM scada.dim_alarm_code
                        WHERE source_code = a.source_code)
                WHERE a.farm = :farm AND a.date_utc BETWEEN :start AND :end
                GROUP BY 1, 2 ORDER BY ev DESC
                LIMIT :limit
                """
            ),
            {
                "farm": farm,
                "start": date(year, 1, 1),
                "end": date(year, 12, 31),
                "limit": limit,
            },
        )
        return {
            "turbines": turbines,
            "weeks": [w.isoformat() for w in weeks],
            "grid": grid,
            "chatter": [
                {"code": r.code, "msg": r.msg, "ev": int(r.ev or 0), "h": float(r.h or 0)}
                for r in chatter_res.fetchall()
            ],
        }

    async def temp_cohort(self, farm: str, year: int) -> Dict[str, Any]:
        """High-load temperature cohort strip + per-channel load curves."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, channel, p_pct_bin AS p, n, temp_med_c AS tv
                FROM scada.viz_temp_power_bins_yearly
                WHERE farm = :farm AND year = :year
                ORDER BY channel, turbine, p_pct_bin
                """
            ),
            {"farm": farm, "year": year},
        )
        rows = result.fetchall()
        curves: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        strip_acc: Dict[Tuple[str, str], List[Tuple[float, int]]] = {}
        for r in rows:
            if r.tv is None:
                continue
            curves.setdefault(r.channel, {}).setdefault(r.turbine, []).append(
                {"p": int(r.p), "t": round(float(r.tv), 1), "n": int(r.n)}
            )
            if int(r.p) >= 80:
                strip_acc.setdefault((r.channel, r.turbine), []).append((float(r.tv), int(r.n)))
        strip: List[Dict[str, Any]] = []
        for (channel, turbine), pairs in sorted(strip_acc.items()):
            wsum = sum(n for _, n in pairs)
            if wsum < 50:
                continue
            strip.append(
                {
                    "ch": channel,
                    "t": turbine,
                    "temp": round(sum(t * n for t, n in pairs) / wsum, 1),
                    "n": wsum,
                }
            )
        return {
            "strip": strip,
            "curves": [
                {
                    "ch": channel,
                    "turbines": [
                        {"t": turbine, "bins": bins} for turbine, bins in sorted(by_t.items())
                    ],
                }
                for channel, by_t in sorted(curves.items())
            ],
        }

    async def yaw_misalignment(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT turbine, mis_bin AS b, n
                FROM scada.viz_yaw_misalignment_yearly
                WHERE farm = :farm AND year = :year
                ORDER BY turbine, mis_bin
                """
            ),
            {"farm": farm, "year": year},
        )
        by_turbine: Dict[str, List[Tuple[int, int]]] = {}
        for r in result.fetchall():
            by_turbine.setdefault(r.turbine, []).append((int(r.b), int(r.n)))
        out = []
        for turbine, bins in sorted(by_turbine.items()):
            total = sum(n for _, n in bins)
            if total < 500:
                continue
            # median / p90(|mis|) from bin midpoints — 2-deg resolution
            mids = []
            for b, n in bins:
                mids.append((b + 1, n))
            cum, med = 0, None
            for m, n in sorted(mids):
                cum += n
                if med is None and cum >= total / 2:
                    med = m
            abs_sorted = sorted(((abs(m), n) for m, n in mids))
            cum, p90 = 0, None
            for m, n in abs_sorted:
                cum += n
                if p90 is None and cum >= total * 0.9:
                    p90 = m
            out.append(
                {
                    "t": turbine,
                    "bins": [{"b": b, "n": n} for b, n in bins],
                    "med": med,
                    "p90": p90,
                    "total": total,
                }
            )
        return {"turbines": out}

    async def watchdog(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT turbine, month_utc AS m,
                       pitch_spread_med_deg AS psm, pitch_spread_p99_deg AS psp,
                       anemo_diff_med_ms AS anm, anemo_diff_p99_ms AS anp,
                       anemo_pct_gt2 AS pct, n
                FROM scada.viz_watchdog_monthly
                WHERE farm = :farm AND EXTRACT(YEAR FROM month_utc) = :year
                ORDER BY turbine, month_utc
                """
            ),
            {"farm": farm, "year": year},
        )
        rows = [
            {
                "t": r.turbine,
                "m": r.m.isoformat()[:7],
                "psm": round(float(r.psm), 3) if r.psm is not None else None,
                "psp": round(float(r.psp), 2) if r.psp is not None else None,
                "anm": round(float(r.anm), 2) if r.anm is not None else None,
                "anp": round(float(r.anp), 2) if r.anp is not None else None,
                "pct": round(float(r.pct), 1) if r.pct is not None else None,
            }
            for r in result.fetchall()
            if r.n >= 100
        ]
        return {"rows": rows}

    async def layout_map(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT dt.turbine, dt.lat, dt.lon, dt.rated_kw,
                       e.mwh, a.av, al.h AS alarm_h
                FROM scada.dim_turbine dt
                LEFT JOIN (
                    SELECT turbine, ROUND((SUM(energy_kwh) / 1000.0)::numeric, 0) AS mwh
                    FROM scada.energy_daily
                    WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                    GROUP BY 1
                ) e ON e.turbine = dt.turbine
                LEFT JOIN (
                    SELECT turbine,
                           ROUND((100 * SUM(available_h) / NULLIF(SUM(expected_h), 0))::numeric, 1) AS av
                    FROM scada.availability_daily
                    WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                    GROUP BY 1
                ) a ON a.turbine = dt.turbine
                LEFT JOIN (
                    SELECT turbine, ROUND(SUM(alarm_hours)::numeric, 0) AS h
                    FROM scada.alarm_code_daily
                    WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                    GROUP BY 1
                ) al ON al.turbine = dt.turbine
                WHERE dt.farm = :farm AND dt.lat IS NOT NULL
                ORDER BY dt.turbine
                """
            ),
            {"farm": farm, "start": date(year, 1, 1), "end": date(year, 12, 31)},
        )
        return {
            "turbines": [
                {
                    "t": r.turbine,
                    "lat": float(r.lat),
                    "lon": float(r.lon),
                    "rated": float(r.rated_kw) if r.rated_kw is not None else None,
                    "mwh": float(r.mwh) if r.mwh is not None else None,
                    "av": float(r.av) if r.av is not None else None,
                    "alarm_h": float(r.alarm_h) if r.alarm_h is not None else None,
                }
                for r in result.fetchall()
            ]
        }

    async def wake(self, farm: str, year: int, min_n: int = 50) -> Dict[str, Any]:
        """Per-turbine deficit vs the fleet mean in each nacelle sector,
        fixed 6-10 m/s band."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, sector, n, power_mean_kw AS p
                FROM scada.viz_wake_sector_yearly
                WHERE farm = :farm AND year = :year
                ORDER BY turbine, sector
                """
            ),
            {"farm": farm, "year": year},
        )
        rows = result.fetchall()
        sectors = list(range(0, 360, 30))
        fleet: Dict[int, Tuple[float, int]] = {}
        for r in rows:
            p, n = fleet.get(r.sector, (0.0, 0))
            fleet[r.sector] = (p + float(r.p) * int(r.n), n + int(r.n))
        fleet_mean = {s: (p / n if n else None) for s, (p, n) in fleet.items()}
        turbines = sorted({r.turbine for r in rows})
        t_idx = {t: i for i, t in enumerate(turbines)}
        s_idx = {s: i for i, s in enumerate(sectors)}
        grid: List[List[Optional[float]]] = [[None] * len(sectors) for _ in turbines]
        for r in rows:
            fm = fleet_mean.get(r.sector)
            if fm and int(r.n) >= min_n:
                grid[t_idx[r.turbine]][s_idx[r.sector]] = round((float(r.p) - fm) / fm * 100, 1)
        return {
            "sectors": sectors,
            "turbines": turbines,
            "grid": grid,
            "fleet": [
                {
                    "sector": s,
                    "mean_kw": round(fleet_mean[s], 0) if fleet_mean.get(s) else None,
                    "hours": round(fleet[s][1] / 6, 0) if s in fleet else 0,
                }
                for s in sectors
                if s in fleet
            ],
        }

    async def wind_rose(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT sector, ws_class AS c, hours, energy_mwh AS mwh
                FROM scada.viz_wind_rose_yearly
                WHERE farm = :farm AND year = :year
                ORDER BY sector
                """
            ),
            {"farm": farm, "year": year},
        )
        rows = result.fetchall()
        by_sector: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            entry = by_sector.setdefault(
                int(r.sector), {"sector": int(r.sector), "classes": {}, "mwh": 0.0}
            )
            entry["classes"][r.c] = round(float(r.hours), 1)
            entry["mwh"] = round(entry["mwh"] + float(r.mwh or 0), 1)
        total_hours = round(sum(float(r.hours) for r in rows), 0)
        return {
            "sectors": sorted(by_sector.values(), key=lambda s: s["sector"]),
            "total_hours": total_hours,
        }

    async def turbulence(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT ws_bin AS ws, ti_med, ti_p90, n
                FROM scada.viz_turbulence_yearly
                WHERE farm = :farm AND year = :year
                ORDER BY ws_bin
                """
            ),
            {"farm": farm, "year": year},
        )
        return {
            "bins": [
                {
                    "ws": int(r.ws),
                    "med": round(float(r.ti_med), 3) if r.ti_med is not None else None,
                    "p90": round(float(r.ti_p90), 3) if r.ti_p90 is not None else None,
                    "n": int(r.n),
                }
                for r in result.fetchall()
                if r.n >= 100
            ]
        }

    async def power_curve_density(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT ws_bin AS ws, p_bin_kw AS p, n, n_derated AS nd
                FROM scada.viz_power_curve_density_yearly
                WHERE farm = :farm AND year = :year
                """
            ),
            {"farm": farm, "year": year},
        )
        rated_res = await self.db.execute(
            text("SELECT MAX(rated_kw) AS r FROM scada.dim_turbine WHERE farm = :farm"),
            {"farm": farm},
        )
        rated = rated_res.scalar()
        cells = [
            {"ws": float(r.ws), "p": int(r.p), "n": int(r.n), "nd": int(r.nd)}
            for r in result.fetchall()
        ]
        return {
            "cells": cells,
            "rated_kw": float(rated) if rated is not None else None,
            "n_derated": sum(c["nd"] for c in cells),
        }

    async def wind_index(self, farm: str) -> Dict[str, Any]:
        ws_res = await self.db.execute(
            text(
                """
                SELECT year, ws_mean_ms AS ws, hours
                FROM scada.viz_wind_resource_yearly
                WHERE farm = :farm ORDER BY year
                """
            ),
            {"farm": farm},
        )
        en_res = await self.db.execute(
            text(
                """
                SELECT EXTRACT(YEAR FROM date_utc)::int AS year,
                       SUM(energy_kwh) / 1e6 AS gwh
                FROM scada.farm_kpis_daily
                WHERE farm = :farm GROUP BY 1
                """
            ),
            {"farm": farm},
        )
        gwh_by_year = {int(r.year): float(r.gwh or 0) for r in en_res.fetchall()}
        years = [
            {
                "year": int(r.year),
                "ws": round(float(r.ws), 2) if r.ws is not None else None,
                "hours": round(float(r.hours), 0) if r.hours is not None else None,
                "gwh": round(gwh_by_year.get(int(r.year), 0), 1),
            }
            for r in ws_res.fetchall()
        ]
        # index base = mean over full years (>= 8000 farm-hours of data)
        full = [y for y in years if (y["hours"] or 0) >= 8000 and y["ws"] and y["gwh"]]
        ws_base = sum(y["ws"] for y in full) / len(full) if full else None
        gwh_base = sum(y["gwh"] for y in full) / len(full) if full else None
        for y in years:
            y["ws_idx"] = round(y["ws"] / ws_base * 100, 1) if ws_base and y["ws"] else None
            y["prod_idx"] = round(y["gwh"] / gwh_base * 100, 1) if gwh_base and y["gwh"] else None
            y["full"] = (y["hours"] or 0) >= 8000
        return {"years": years}

    async def grid_quality(self, farm: str, year: int) -> Dict[str, Any]:
        hist_res = await self.db.execute(
            text(
                """
                SELECT freq_mhz_bin AS b, n FROM scada.viz_grid_freq_yearly
                WHERE farm = :farm AND year = :year ORDER BY freq_mhz_bin
                """
            ),
            {"farm": farm, "year": year},
        )
        ev_res = await self.db.execute(
            text(
                """
                SELECT ts_start_utc AS ts, freq_min_hz AS fmin, freq_max_hz AS fmax,
                       fleet_mw AS mw
                FROM scada.viz_grid_freq_events
                WHERE farm = :farm AND EXTRACT(YEAR FROM ts_start_utc) = :year
                ORDER BY ts_start_utc
                """
            ),
            {"farm": farm, "year": year},
        )
        events = [
            {
                "t": r.ts.isoformat(),
                "fmin": round(float(r.fmin), 3) if r.fmin is not None else None,
                "fmax": round(float(r.fmax), 3) if r.fmax is not None else None,
                "mw": round(float(r.mw), 1) if r.mw is not None else None,
            }
            for r in ev_res.fetchall()
        ]
        return {
            "hist": [{"b": int(r.b), "n": int(r.n)} for r in hist_res.fetchall()],
            "events": events,
        }

    async def pq_envelope(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT p_bin_kw AS p, q_min_kvar AS qmin, q_p05_kvar AS q05,
                       q_med_kvar AS qmed, q_p95_kvar AS q95, q_max_kvar AS qmax, n
                FROM scada.viz_pq_yearly
                WHERE farm = :farm AND year = :year ORDER BY p_bin_kw
                """
            ),
            {"farm": farm, "year": year},
        )
        return {
            "bins": [
                {
                    "p": int(r.p),
                    "qmin": round(float(r.qmin), 0) if r.qmin is not None else None,
                    "q05": round(float(r.q05), 0) if r.q05 is not None else None,
                    "qmed": round(float(r.qmed), 0) if r.qmed is not None else None,
                    "q95": round(float(r.q95), 0) if r.q95 is not None else None,
                    "qmax": round(float(r.qmax), 0) if r.qmax is not None else None,
                    "n": int(r.n),
                }
                for r in result.fetchall()
                if r.n >= 50
            ]
        }

    async def self_consumption(self, farm: str) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT year, n_rows, n_neg_rows AS neg,
                       import_mwh_neg_power AS imp, meter_import_mwh AS meter
                FROM scada.viz_self_consumption_yearly
                WHERE farm = :farm ORDER BY year
                """
            ),
            {"farm": farm},
        )
        return {
            "years": [
                {
                    "year": int(r.year),
                    "imp_mwh": round(float(r.imp), 1) if r.imp is not None else None,
                    "meter_mwh": round(float(r.meter), 1) if r.meter is not None else None,
                    "pct_neg": round(100 * int(r.neg) / int(r.n_rows), 1) if r.n_rows else None,
                }
                for r in result.fetchall()
            ]
        }

    async def energy_recon(self, farm: str, year: int) -> Dict[str, Any]:
        """Export counter vs integrated power over 10-min rows where BOTH are
        reported (viz lane — energy_daily stores one OR the other per method,
        so the cross-check must come from silver)."""
        result = await self.db.execute(
            text(
                """
                SELECT counter_gwh, integral_gwh, n_rows
                FROM scada.viz_energy_recon_yearly
                WHERE farm = :farm AND year = :year
                """
            ),
            {"farm": farm, "year": year},
        )
        r = result.fetchone()
        if r is None:
            return {"counter_gwh": None, "integral_gwh": None, "delta_pct": None, "n_rows": 0}
        counter = float(r.counter_gwh) if r.counter_gwh is not None else None
        integral = float(r.integral_gwh) if r.integral_gwh is not None else None
        delta_pct = round((integral - counter) / counter * 100, 2) if counter and integral else None
        return {
            "counter_gwh": round(counter, 2) if counter is not None else None,
            "integral_gwh": round(integral, 2) if integral is not None else None,
            "delta_pct": delta_pct,
            "n_rows": int(r.n_rows),
        }

    async def qc_bits(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT bit_name AS b, n_rows AS n FROM scada.viz_qc_bits_yearly
                WHERE farm = :farm AND year = :year
                """
            ),
            {"farm": farm, "year": year},
        )
        rows = {r.b: int(r.n) for r in result.fetchall()}
        total = rows.pop("total", 0)
        clean = rows.pop("clean", 0)
        bits = [
            {"b": name, "n": n} for name, n in sorted(rows.items(), key=lambda kv: -kv[1]) if n > 0
        ]
        return {"total": total, "clean": clean, "bits": bits}

    # --- turbine detail page (single-turbine dossier) ---

    async def turbines(self, farm: str) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT turbine, model, rated_kw
                FROM scada.dim_turbine WHERE farm = :farm ORDER BY turbine
                """
            ),
            {"farm": farm},
        )
        return {
            "turbines": [
                {
                    "t": r.turbine,
                    "model": r.model,
                    "rated_kw": float(r.rated_kw) if r.rated_kw is not None else None,
                }
                for r in result.fetchall()
            ]
        }

    async def turbine_summary(self, farm: str, turbine: str, year: int) -> Dict[str, Any]:
        """Identity + year KPIs, each with its rank across the farm."""
        ident_res = await self.db.execute(
            text(
                """
                SELECT t.turbine, t.model, t.rated_kw, t.lat, t.lon,
                       (SELECT MIN(date_utc) FROM scada.energy_daily e
                        WHERE e.farm = t.farm AND e.turbine = t.turbine
                          AND NOT e.pre_cod) AS first_date,
                       (SELECT MAX(date_utc) FROM scada.energy_daily e
                        WHERE e.farm = t.farm AND e.turbine = t.turbine) AS last_date
                FROM scada.dim_turbine t
                WHERE t.farm = :farm AND t.turbine = :turbine
                """
            ),
            {"farm": farm, "turbine": turbine},
        )
        ident = ident_res.fetchone()
        if ident is None:
            return {"turbine": None}
        ystart, yend = date(year, 1, 1), date(year, 12, 31)
        kpi_res = await self.db.execute(
            text(
                """
                WITH e AS (
                  SELECT turbine, SUM(energy_kwh) / 1000.0 AS mwh
                  FROM scada.energy_daily
                  WHERE farm = :farm AND date_utc BETWEEN :ystart AND :yend
                    AND NOT pre_cod
                  GROUP BY 1
                ), a AS (
                  SELECT turbine,
                         100.0 * SUM(available_h) / NULLIF(SUM(expected_h), 0) AS avail,
                         SUM(unavailable_h) AS downtime_h
                  FROM scada.availability_daily
                  WHERE farm = :farm AND date_utc BETWEEN :ystart AND :yend
                    AND NOT pre_cod
                  GROUP BY 1
                ), c AS (
                  SELECT turbine,
                         100.0 * SUM(rows_valid_core) / NULLIF(SUM(expected_intervals), 0)
                           AS compl
                  FROM scada.completeness_daily
                  WHERE farm = :farm AND date_utc BETWEEN :ystart AND :yend
                    AND NOT pre_cod
                  GROUP BY 1
                ), p AS (
                  SELECT turbine,
                         SUM(performance_index * intervals_used)
                           / NULLIF(SUM(intervals_used), 0) AS pi,
                         MAX(ws_coverage_pct) AS ws_cov
                  FROM scada.turbine_performance_yearly
                  WHERE farm = :farm AND year = :year
                  GROUP BY 1
                )
                SELECT e.turbine, e.mwh, a.avail, a.downtime_h, c.compl, p.pi, p.ws_cov,
                       RANK() OVER (ORDER BY e.mwh DESC NULLS LAST)   AS mwh_rank,
                       RANK() OVER (ORDER BY a.avail DESC NULLS LAST) AS avail_rank,
                       RANK() OVER (ORDER BY p.pi DESC NULLS LAST)    AS pi_rank,
                       COUNT(*) OVER ()                               AS n_turbines
                FROM e
                LEFT JOIN a USING (turbine)
                LEFT JOIN c USING (turbine)
                LEFT JOIN p USING (turbine)
                """
            ),
            {"farm": farm, "ystart": ystart, "yend": yend, "year": year},
        )
        kpis: Dict[str, Any] = {}
        for r in kpi_res.fetchall():
            if r.turbine == turbine:
                kpis = {
                    "energy_mwh": round(float(r.mwh), 1) if r.mwh is not None else None,
                    "energy_rank": int(r.mwh_rank),
                    "availability_pct": (round(float(r.avail), 1) if r.avail is not None else None),
                    "availability_rank": int(r.avail_rank),
                    "perf_index": round(float(r.pi), 3) if r.pi is not None else None,
                    "perf_rank": int(r.pi_rank) if r.pi is not None else None,
                    "ws_coverage_pct": (
                        round(float(r.ws_cov), 1) if r.ws_cov is not None else None
                    ),
                    "downtime_h": (
                        round(float(r.downtime_h), 1) if r.downtime_h is not None else None
                    ),
                    "completeness_pct": (round(float(r.compl), 1) if r.compl is not None else None),
                    "n_turbines": int(r.n_turbines),
                }
        return {
            "turbine": ident.turbine,
            "model": ident.model,
            "rated_kw": float(ident.rated_kw) if ident.rated_kw is not None else None,
            "lat": float(ident.lat) if ident.lat is not None else None,
            "lon": float(ident.lon) if ident.lon is not None else None,
            "first_date": ident.first_date.isoformat() if ident.first_date else None,
            "last_date": ident.last_date.isoformat() if ident.last_date else None,
            "kpis": kpis,
        }

    async def turbine_life(self, farm: str, turbine: str) -> Dict[str, Any]:
        """Whole-history monthly energy + availability — the life strip."""
        result = await self.db.execute(
            text(
                """
                SELECT m.month_utc AS m, m.energy_kwh / 1000.0 AS mwh, a.avail
                FROM scada.energy_monthly_utc m
                LEFT JOIN (
                  SELECT DATE_TRUNC('month', date_utc)::date AS mo,
                         100.0 * SUM(available_h) / NULLIF(SUM(expected_h), 0) AS avail
                  FROM scada.availability_daily
                  WHERE farm = :farm AND turbine = :turbine AND NOT pre_cod
                  GROUP BY 1
                ) a ON a.mo = m.month_utc
                WHERE m.farm = :farm AND m.turbine = :turbine
                ORDER BY m.month_utc
                """
            ),
            {"farm": farm, "turbine": turbine},
        )
        epochs_res = await self.db.execute(
            text(
                """
                SELECT config, valid_from FROM scada.dim_turbine_config
                WHERE farm = :farm AND turbine = :turbine
                ORDER BY valid_from
                """
            ),
            {"farm": farm, "turbine": turbine},
        )
        return {
            "months": [
                {
                    "m": r.m.isoformat(),
                    "mwh": round(float(r.mwh), 1) if r.mwh is not None else None,
                    "avail": round(float(r.avail), 1) if r.avail is not None else None,
                }
                for r in result.fetchall()
            ],
            "epochs": [
                {"config": r.config, "from": r.valid_from.isoformat() if r.valid_from else None}
                for r in epochs_res.fetchall()
            ],
        }

    async def turbine_perf_index(self, farm: str, turbine: str) -> Dict[str, Any]:
        """Yearly epoch-normalized performance index vs the farm distribution.
        ws_coverage_pct < ~90 means the index is unreliable that year."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, year,
                       SUM(performance_index * intervals_used)
                         / NULLIF(SUM(intervals_used), 0) AS pi,
                       MIN(ws_coverage_pct) AS ws_cov
                FROM scada.turbine_performance_yearly
                WHERE farm = :farm
                GROUP BY 1, 2 ORDER BY 2
                """
            ),
            {"farm": farm},
        )
        by_year: Dict[int, List[float]] = {}
        own: Dict[int, Dict[str, Any]] = {}
        for r in result.fetchall():
            if r.pi is None:
                continue
            by_year.setdefault(int(r.year), []).append(float(r.pi))
            if r.turbine == turbine:
                own[int(r.year)] = {
                    "pi": round(float(r.pi), 3),
                    "ws_cov": round(float(r.ws_cov), 1) if r.ws_cov is not None else None,
                }
        years = []
        for yr in sorted(by_year):
            vals = sorted(by_year[yr])
            years.append(
                {
                    "year": yr,
                    "pi": own.get(yr, {}).get("pi"),
                    "ws_cov": own.get(yr, {}).get("ws_cov"),
                    "farm_min": round(vals[0], 3),
                    "farm_med": round(vals[len(vals) // 2], 3),
                    "farm_max": round(vals[-1], 3),
                }
            )
        return {"years": years}

    async def turbine_temp_trend(self, farm: str, turbine: str) -> Dict[str, Any]:
        """Yearly high-load (>=80% rated) median per channel: turbine vs fleet
        median — component drift across years, weather held constant by load."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, year, channel AS ch,
                       SUM(temp_med_c * n) / NULLIF(SUM(n), 0) AS tv,
                       SUM(n) AS n
                FROM scada.viz_temp_power_bins_yearly
                WHERE farm = :farm AND p_pct_bin >= 80 AND channel != 'ambient_temp_c'
                GROUP BY 1, 2, 3 HAVING SUM(n) >= 200
                ORDER BY 2
                """
            ),
            {"farm": farm},
        )
        fleet: Dict[str, Dict[int, List[float]]] = {}
        own: Dict[str, Dict[int, float]] = {}
        for r in result.fetchall():
            if r.tv is None:
                continue
            fleet.setdefault(r.ch, {}).setdefault(int(r.year), []).append(float(r.tv))
            if r.turbine == turbine:
                own.setdefault(r.ch, {})[int(r.year)] = float(r.tv)
        channels = []
        for ch, own_years in own.items():
            pts = []
            for yr in sorted(fleet[ch]):
                vals = sorted(fleet[ch][yr])
                pts.append(
                    {
                        "year": yr,
                        "t": round(own_years[yr], 1) if yr in own_years else None,
                        "fleet_med": round(vals[len(vals) // 2], 1),
                    }
                )
            channels.append({"ch": ch, "pts": pts})
        return {"channels": channels}

    async def turbine_power_curve(self, farm: str, turbine: str, year: int) -> Dict[str, Any]:
        """This turbine's yearly curves (selected + up to 4 prior years) plus
        the farm-mean reference for the selected year."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, year, ws_bin AS ws,
                       SUM(power_mean_kw * n) / NULLIF(SUM(n), 0) AS p,
                       SUM(n) AS n
                FROM scada.power_curve_bins_yearly
                WHERE farm = :farm AND year BETWEEN :y0 AND :year
                GROUP BY 1, 2, 3 HAVING SUM(n) >= 30
                ORDER BY 2, 3
                """
            ),
            {"farm": farm, "y0": year - 4, "year": year},
        )
        own: Dict[int, List[Dict[str, Any]]] = {}
        ref: Dict[float, List[float]] = {}
        for r in result.fetchall():
            if r.p is None:
                continue
            ws = float(r.ws)
            if r.turbine == turbine:
                own.setdefault(int(r.year), []).append({"ws": ws, "p": round(float(r.p), 1)})
            if int(r.year) == year:
                ref.setdefault(ws, []).append(float(r.p))
        rated_res = await self.db.execute(
            text(
                "SELECT rated_kw FROM scada.dim_turbine"
                " WHERE farm = :farm AND turbine = :turbine"
            ),
            {"farm": farm, "turbine": turbine},
        )
        rated = rated_res.scalar()
        return {
            "years": [{"year": yr, "bins": bins} for yr, bins in sorted(own.items())],
            "farm_ref": [
                {"ws": ws, "p": round(sum(v) / len(v), 1)} for ws, v in sorted(ref.items())
            ],
            "rated_kw": float(rated) if rated is not None else None,
        }

    async def turbine_daily_rel(self, farm: str, turbine: str, year: int) -> Dict[str, Any]:
        """Calendar of daily energy relative to the fleet median that day."""
        result = await self.db.execute(
            text(
                """
                WITH d AS (
                  SELECT turbine, date_utc, energy_kwh
                  FROM scada.energy_daily
                  WHERE farm = :farm AND date_utc BETWEEN :ystart AND :yend
                    AND NOT pre_cod AND energy_kwh IS NOT NULL
                ), f AS (
                  SELECT date_utc,
                         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY energy_kwh)
                           AS fleet_med
                  FROM d GROUP BY 1
                )
                SELECT d.date_utc AS dt, d.energy_kwh / 1000.0 AS mwh,
                       d.energy_kwh / NULLIF(f.fleet_med, 0) AS rel,
                       f.fleet_med / 1000.0 AS fleet_mwh
                FROM d JOIN f USING (date_utc)
                WHERE d.turbine = :turbine
                ORDER BY 1
                """
            ),
            {
                "farm": farm,
                "turbine": turbine,
                "ystart": date(year, 1, 1),
                "yend": date(year, 12, 31),
            },
        )
        days = []
        for r in result.fetchall():
            days.append(
                {
                    "d": r.dt.isoformat(),
                    "mwh": round(float(r.mwh), 2),
                    "rel": round(float(r.rel), 3) if r.rel is not None else None,
                    "fleet_mwh": (
                        round(float(r.fleet_mwh), 2) if r.fleet_mwh is not None else None
                    ),
                }
            )
        return {"days": days}

    async def turbine_timers(self, farm: str, turbine: str, year: int) -> Dict[str, Any]:
        """OEM integrated run/ready/error counters by month — independent of
        the alarm-event lane (HoT stop events are mostly unbracketed)."""
        result = await self.db.execute(
            text(
                """
                SELECT month_utc AS m, run_h, ready_h, error_h, n
                FROM scada.viz_timers_monthly
                WHERE farm = :farm AND turbine = :turbine
                  AND EXTRACT(YEAR FROM month_utc) = :year
                ORDER BY month_utc
                """
            ),
            {"farm": farm, "turbine": turbine, "year": year},
        )
        return {
            "months": [
                {
                    "m": r.m.isoformat(),
                    "run_h": round(float(r.run_h or 0), 1),
                    "ready_h": round(float(r.ready_h or 0), 1),
                    "error_h": round(float(r.error_h or 0), 1),
                }
                for r in result.fetchall()
            ]
        }

    async def turbine_alarm_timeline(
        self, farm: str, turbine: str, year: int, min_hours: float = 0.25
    ) -> Dict[str, Any]:
        """Bracketed stoppages for one turbine-year — same taxonomy as the
        fleet gantt but a finer duration floor (one turbine fits the width)."""
        fmt_res = await self.db.execute(
            text("SELECT source_format FROM scada.dim_farm WHERE farm = :farm"),
            {"farm": farm},
        )
        source_format = fmt_res.scalar()
        ystart, yend = datetime(year, 1, 1), datetime(year + 1, 1, 1)
        if source_format == "greenbyte":
            where = "iec_category = ANY(:cats)"
            cat_expr = "iec_category"
            params: Dict[str, Any] = {"cats": list(self._IEC_UNAVAILABLE)}
        else:
            where = "severity_class = 'stop'"
            cat_expr = """
                COALESCE((SELECT d.bucket FROM scada.dim_alarm_code d
                          WHERE d.source_code = a.source_code
                            AND d.source_format = :fmt LIMIT 1), 'other')
            """
            params = {"fmt": source_format}
        result = await self.db.execute(
            text(
                f"""
                SELECT GREATEST(a.time_on, :ystart) AS s,
                       LEAST(a.time_off, :yend)     AS e,
                       {cat_expr}                   AS cat,
                       a.source_code                AS code,
                       LEFT(COALESCE(a.message, ''), 60) AS msg
                FROM scada.alarm_events a
                WHERE a.farm = :farm AND a.turbine = :turbine AND {where}
                  AND a.time_off IS NOT NULL
                  AND a.duration_h >= :min_h
                  AND a.time_on < :yend AND a.time_off > :ystart
                ORDER BY a.time_on
                """
            ),
            {
                "farm": farm,
                "turbine": turbine,
                "ystart": ystart,
                "yend": yend,
                "min_h": min_hours,
                **params,
            },
        )
        intervals = [
            {
                "s": r.s.isoformat(),
                "e": r.e.isoformat(),
                "cat": r.cat,
                "code": r.code,
                "msg": r.msg,
            }
            for r in result.fetchall()
        ]
        return {"intervals": intervals, "min_hours": min_hours}

    async def turbine_alarm_pareto(
        self, farm: str, turbine: str, year: int, limit: int = 10
    ) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT c.source_code AS code,
                       SUM(c.alarm_hours)    AS hrs,
                       SUM(c.events_started) AS onsets,
                       COALESCE(MIN(d.message), '') AS msg,
                       COALESCE(MIN(d.bucket), 'other') AS bucket
                FROM scada.alarm_code_daily c
                LEFT JOIN scada.dim_alarm_code d
                  ON d.source_code = c.source_code
                 AND d.source_format = (
                       SELECT MIN(source_format) FROM scada.dim_alarm_code
                       WHERE source_code = c.source_code
                     )
                WHERE c.farm = :farm AND c.turbine = :turbine
                  AND c.date_utc BETWEEN :ystart AND :yend
                GROUP BY 1
                ORDER BY hrs DESC
                LIMIT :limit
                """
            ),
            {
                "farm": farm,
                "turbine": turbine,
                "ystart": date(year, 1, 1),
                "yend": date(year, 12, 31),
                "limit": limit,
            },
        )
        return {
            "codes": [
                {
                    "code": r.code,
                    "hrs": round(float(r.hrs), 1),
                    "onsets": int(r.onsets),
                    "msg": (r.msg or "")[:60],
                    "bucket": r.bucket,
                }
                for r in result.fetchall()
            ]
        }

    async def turbine_rose(self, farm: str, turbine: str, year: int) -> Dict[str, Any]:
        """This turbine's wind/energy rose vs the farm-total outline."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, sector, hours, energy_mwh
                FROM scada.viz_turbine_rose_yearly
                WHERE farm = :farm AND year = :year
                ORDER BY sector
                """
            ),
            {"farm": farm, "year": year},
        )
        own: Dict[int, Dict[str, float]] = {}
        fleet: Dict[int, Dict[str, float]] = {}
        for r in result.fetchall():
            sec = int(r.sector)
            f = fleet.setdefault(sec, {"hours": 0.0, "mwh": 0.0})
            f["hours"] += float(r.hours)
            f["mwh"] += float(r.energy_mwh or 0)
            if r.turbine == turbine:
                own[sec] = {
                    "hours": round(float(r.hours), 1),
                    "mwh": round(float(r.energy_mwh or 0), 1),
                }
        n_turb_res = await self.db.execute(
            text(
                """
                SELECT COUNT(DISTINCT turbine) FROM scada.viz_turbine_rose_yearly
                WHERE farm = :farm AND year = :year
                """
            ),
            {"farm": farm, "year": year},
        )
        n_turb = int(n_turb_res.scalar() or 0)
        return {
            "sectors": [
                {
                    "sector": sec,
                    "hours": own.get(sec, {}).get("hours", 0.0),
                    "mwh": own.get(sec, {}).get("mwh", 0.0),
                    # per-turbine fleet average for a comparable outline
                    "fleet_hours": round(fleet[sec]["hours"] / n_turb, 1) if n_turb else 0.0,
                    "fleet_mwh": round(fleet[sec]["mwh"] / n_turb, 1) if n_turb else 0.0,
                }
                for sec in sorted(fleet)
            ]
        }

    # --- round-4 charts: reliability & losses page + folded additions ---

    async def loss_league(self, farm: str, year: int) -> Dict[str, Any]:
        """Per-turbine lost energy by cause. losses_daily is one row per
        turbine-day (method is an attribute, not a key), so plain SUM holds."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, MIN(method) AS method,
                       SUM(loss_downtime_kwh) / 1000.0    AS dt,
                       SUM(loss_curtailment_kwh) / 1000.0 AS ct,
                       SUM(loss_performance_kwh) / 1000.0 AS pf,
                       SUM(potential_kwh) / 1000.0        AS pot,
                       SUM(actual_kwh) / 1000.0           AS act
                FROM scada.losses_daily
                WHERE farm = :farm AND NOT pre_cod
                  AND date_utc BETWEEN :start AND :end
                GROUP BY 1 ORDER BY 1
                """
            ),
            {"farm": farm, "start": date(year, 1, 1), "end": date(year, 12, 31)},
        )
        turbines = []
        for r in result.fetchall():
            pot = float(r.pot or 0)
            loss = float(r.dt or 0) + float(r.ct or 0) + float(r.pf or 0)
            turbines.append(
                {
                    "t": r.turbine,
                    "method": r.method,
                    "dt": round(float(r.dt or 0), 1),
                    "ct": round(float(r.ct or 0), 1),
                    "pf": round(float(r.pf or 0), 1),
                    "pot": round(pot, 1),
                    "pct": round(100.0 * loss / pot, 2) if pot > 0 else None,
                }
            )
        return {"turbines": turbines}

    async def loss_monthly(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT DATE_TRUNC('month', date_utc)::date AS mo,
                       SUM(loss_downtime_kwh) / 1000.0    AS dt,
                       SUM(loss_curtailment_kwh) / 1000.0 AS ct,
                       SUM(loss_performance_kwh) / 1000.0 AS pf,
                       SUM(potential_kwh) / 1000.0        AS pot
                FROM scada.losses_daily
                WHERE farm = :farm AND NOT pre_cod
                  AND date_utc BETWEEN :start AND :end
                GROUP BY 1 ORDER BY 1
                """
            ),
            {"farm": farm, "start": date(year, 1, 1), "end": date(year, 12, 31)},
        )
        return {
            "months": [
                {
                    "m": r.mo.isoformat(),
                    "dt": round(float(r.dt or 0), 1),
                    "ct": round(float(r.ct or 0), 1),
                    "pf": round(float(r.pf or 0), 1),
                    "pot": round(float(r.pot or 0), 1),
                }
                for r in result.fetchall()
            ]
        }

    async def mtbf_mttr(self, farm: str, year: int, min_hours: float = 1.0) -> Dict[str, Any]:
        """Reliability quadrant: stop episodes >= min_hours (overlapping stop
        intervals merged per turbine — HoT stop alarms co-fire) vs uptime.
        MTBF = available hours / episodes; MTTR = mean merged episode length."""
        fmt_res = await self.db.execute(
            text("SELECT source_format FROM scada.dim_farm WHERE farm = :farm"),
            {"farm": farm},
        )
        source_format = fmt_res.scalar()
        ystart, yend = datetime(year, 1, 1), datetime(year + 1, 1, 1)
        if source_format == "greenbyte":
            where = "iec_category = ANY(:cats)"
            params: Dict[str, Any] = {"cats": list(self._IEC_UNAVAILABLE)}
        else:
            where = "severity_class = 'stop'"
            params = {}
        result = await self.db.execute(
            text(
                f"""
                SELECT turbine,
                       GREATEST(time_on, :ystart) AS s,
                       LEAST(time_off, :yend)     AS e
                FROM scada.alarm_events
                WHERE farm = :farm AND {where}
                  AND time_off IS NOT NULL AND duration_h >= :min_h
                  AND time_on < :yend AND time_off > :ystart
                ORDER BY turbine, time_on
                """
            ),
            {"farm": farm, "ystart": ystart, "yend": yend, "min_h": min_hours, **params},
        )
        episodes: Dict[str, List[List[datetime]]] = {}
        for r in result.fetchall():
            spans = episodes.setdefault(r.turbine, [])
            if spans and r.s <= spans[-1][1]:
                spans[-1][1] = max(spans[-1][1], r.e)
            else:
                spans.append([r.s, r.e])
        avail_res = await self.db.execute(
            text(
                """
                SELECT turbine, SUM(available_h) AS avail_h
                FROM scada.availability_daily
                WHERE farm = :farm AND NOT pre_cod
                  AND date_utc BETWEEN :start AND :end
                GROUP BY 1 ORDER BY 1
                """
            ),
            {"farm": farm, "start": date(year, 1, 1), "end": date(year, 12, 31)},
        )
        turbines = []
        for r in avail_res.fetchall():
            spans = episodes.get(r.turbine, [])
            n_stops = len(spans)
            durs = [(e - s).total_seconds() / 3600.0 for s, e in spans]
            avail_h = float(r.avail_h or 0)
            turbines.append(
                {
                    "t": r.turbine,
                    "n_stops": n_stops,
                    "mtbf_h": round(avail_h / n_stops, 1) if n_stops else None,
                    "mttr_h": round(sum(durs) / n_stops, 1) if n_stops else None,
                    "avail_h": round(avail_h, 1),
                }
            )
        return {"turbines": turbines, "min_hours": min_hours}

    async def alarm_transitions(self, farm: str, year: int, limit: int = 12) -> Dict[str, Any]:
        """Code x code co-occurrence at day grain from alarm_code_daily:
        among the year's top codes, on how many turbine-days both were active.
        Day grain is deliberate — alarm_events self-joins (chatter codes fire
        100k+ times) take 15s+ on the staging instance; the daily rollup
        answers the 'which alarms travel together' question in milliseconds."""
        start, end = date(year, 1, 1), date(year, 12, 31)
        top_res = await self.db.execute(
            text(
                """
                SELECT source_code FROM scada.alarm_code_daily
                WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                GROUP BY 1 ORDER BY SUM(events_started) DESC LIMIT :limit
                """
            ),
            {"farm": farm, "start": start, "end": end, "limit": limit},
        )
        codes = [r.source_code for r in top_res.fetchall()]
        if not codes:
            return {"codes": [], "pairs": []}
        pair_res = await self.db.execute(
            text(
                """
                WITH ev AS (
                  SELECT turbine, date_utc, source_code
                  FROM scada.alarm_code_daily
                  WHERE farm = :farm AND date_utc BETWEEN :start AND :end
                    AND source_code = ANY(:codes)
                )
                SELECT a.source_code AS src, b.source_code AS dst,
                       COUNT(*) AS n
                FROM ev a
                JOIN ev b ON b.turbine = a.turbine AND b.date_utc = a.date_utc
                         AND b.source_code <> a.source_code
                GROUP BY 1, 2
                """
            ),
            {"farm": farm, "start": start, "end": end, "codes": codes},
        )
        pairs = [{"src": r.src, "dst": r.dst, "n": int(r.n)} for r in pair_res.fetchall()]
        msg_res = await self.db.execute(
            text(
                """
                SELECT source_code, MIN(bucket) AS bucket,
                       MIN(message) AS msg
                FROM scada.dim_alarm_code
                WHERE source_code = ANY(:codes)
                GROUP BY 1
                """
            ),
            {"codes": codes},
        )
        meta = {r.source_code: {"bucket": r.bucket, "msg": r.msg} for r in msg_res.fetchall()}
        return {
            "codes": [
                {
                    "code": c,
                    "bucket": meta.get(c, {}).get("bucket"),
                    "msg": meta.get(c, {}).get("msg"),
                }
                for c in codes
            ],
            "pairs": pairs,
        }

    async def fleet_timers(self, farm: str, year: int) -> Dict[str, Any]:
        """Farm-summed OEM utilization timers by month (siemens_wps only)."""
        result = await self.db.execute(
            text(
                """
                SELECT month_utc, SUM(run_h) AS run_h, SUM(ready_h) AS ready_h,
                       SUM(error_h) AS error_h
                FROM scada.viz_timers_monthly
                WHERE farm = :farm AND EXTRACT(YEAR FROM month_utc) = :year
                GROUP BY 1 ORDER BY 1
                """
            ),
            {"farm": farm, "year": year},
        )
        return {
            "months": [
                {
                    "m": r.month_utc.isoformat(),
                    "run": round(float(r.run_h or 0), 1),
                    "ready": round(float(r.ready_h or 0), 1),
                    "err": round(float(r.error_h or 0), 1),
                }
                for r in result.fetchall()
            ]
        }

    async def midwind_fade(self, farm: str) -> Dict[str, Any]:
        """Mean power in the 7-8 m/s band per turbine-year — the mid-wind
        fade. n-weighted across ws bins AND config epochs in one GROUP BY."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, year,
                       SUM(power_mean_kw * n) / NULLIF(SUM(n), 0) AS p,
                       SUM(n) AS n
                FROM scada.power_curve_bins_yearly
                WHERE farm = :farm AND ws_bin >= 7.0 AND ws_bin < 8.0
                GROUP BY 1, 2 HAVING SUM(n) >= 100
                ORDER BY 1, 2
                """
            ),
            {"farm": farm},
        )
        by_turbine: Dict[str, List[Dict[str, Any]]] = {}
        by_year: Dict[int, List[float]] = {}
        for r in result.fetchall():
            if r.p is None:
                continue
            p = float(r.p)
            by_turbine.setdefault(r.turbine, []).append({"year": int(r.year), "p": round(p)})
            by_year.setdefault(int(r.year), []).append(p)
        fleet = [
            {"year": yr, "med": round(sorted(vals)[len(vals) // 2])}
            for yr, vals in sorted(by_year.items())
        ]
        return {
            "turbines": [{"t": tb, "points": pts} for tb, pts in sorted(by_turbine.items())],
            "fleet": fleet,
        }

    async def duration_curve(self, farm: str, year: int) -> Dict[str, Any]:
        """Power duration curve for the average turbine: cumulative hours at
        or above each 100 kW level, selected year vs the prior one."""
        result = await self.db.execute(
            text(
                """
                SELECT year, p_bin_kw, SUM(n) / 6.0 AS h
                FROM scada.viz_power_curve_density_yearly
                WHERE farm = :farm AND year IN (:year, :prior)
                GROUP BY 1, 2 ORDER BY 1, 2 DESC
                """
            ),
            {"farm": farm, "year": year, "prior": year - 1},
        )
        dims = await self.db.execute(
            text(
                """
                SELECT COUNT(*) AS n, SUM(rated_kw) AS rated
                FROM scada.dim_turbine WHERE farm = :farm
                """
            ),
            {"farm": farm},
        )
        d = dims.fetchone()
        n_turb = int(d.n or 0)
        curves: Dict[int, List[Dict[str, float]]] = {}
        cum: Dict[int, float] = {}
        for r in result.fetchall():
            yr = int(r.year)
            cum[yr] = cum.get(yr, 0.0) + float(r.h) / max(n_turb, 1)
            curves.setdefault(yr, []).append({"p": int(r.p_bin_kw), "h": round(cum[yr], 1)})
        return {
            "years": [
                {"year": yr, "bins": sorted(pts, key=lambda x: x["p"])}
                for yr, pts in sorted(curves.items())
            ],
            "rated_kw": float(d.rated or 0) / max(n_turb, 1),
            "n_turbines": n_turb,
        }

    async def portfolio(self) -> Dict[str, Any]:
        """All farms on one normalized strip: monthly capacity factor and
        availability. CF divides by capacity x hours actually covered by
        kpi days, so partial first/last months don't fake a dip."""
        rated_res = await self.db.execute(
            text("SELECT farm, SUM(rated_kw) AS rated FROM scada.dim_turbine GROUP BY 1")
        )
        rated = {r.farm: float(r.rated) for r in rated_res.fetchall()}
        cf_res = await self.db.execute(
            text(
                """
                SELECT farm, DATE_TRUNC('month', date_utc)::date AS mo,
                       SUM(energy_kwh) AS kwh, COUNT(DISTINCT date_utc) AS days
                FROM scada.farm_kpis_daily
                GROUP BY 1, 2 ORDER BY 1, 2
                """
            )
        )
        avail_res = await self.db.execute(
            text(
                """
                SELECT farm, DATE_TRUNC('month', date_utc)::date AS mo,
                       100.0 * SUM(available_h) / NULLIF(SUM(expected_h), 0) AS avail
                FROM scada.availability_daily
                WHERE NOT pre_cod
                GROUP BY 1, 2
                """
            )
        )
        avail = {
            (r.farm, r.mo): float(r.avail) for r in avail_res.fetchall() if r.avail is not None
        }
        farms: Dict[str, List[Dict[str, Any]]] = {}
        for r in cf_res.fetchall():
            cap = rated.get(r.farm)
            if not cap or not r.days:
                continue
            cf = 100.0 * float(r.kwh or 0) / (cap * 24.0 * int(r.days))
            farms.setdefault(r.farm, []).append(
                {
                    "m": r.mo.isoformat(),
                    "cf": round(cf, 1),
                    "avail": round(avail[(r.farm, r.mo)], 1) if (r.farm, r.mo) in avail else None,
                }
            )
        return {
            "farms": [
                {"farm": f, "rated_mw": round(rated[f] / 1000.0, 1), "months": m}
                for f, m in sorted(farms.items())
            ]
        }

    async def pitch_curve(self, farm: str, year: int) -> Dict[str, Any]:
        """Pitch operating curve per turbine + the fine-pitch cohort value
        (mean of 6-8 m/s bin medians)."""
        result = await self.db.execute(
            text(
                """
                SELECT turbine, ws_bin, pitch_med_deg, n
                FROM scada.viz_pitch_curve_yearly
                WHERE farm = :farm AND year = :year AND n >= 30
                ORDER BY turbine, ws_bin
                """
            ),
            {"farm": farm, "year": year},
        )
        by_turbine: Dict[str, List[Dict[str, Any]]] = {}
        for r in result.fetchall():
            by_turbine.setdefault(r.turbine, []).append(
                {"ws": float(r.ws_bin), "deg": round(float(r.pitch_med_deg), 2), "n": int(r.n)}
            )
        turbines = []
        for tb, pts in sorted(by_turbine.items()):
            fine_pts = [p["deg"] for p in pts if 6.0 <= p["ws"] < 8.0]
            turbines.append(
                {
                    "t": tb,
                    "fine": round(sum(fine_pts) / len(fine_pts), 2) if fine_pts else None,
                    "curve": pts,
                }
            )
        return {"turbines": turbines}

    async def diurnal(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT month_utc, hour, power_mean_kw, ws_mean_ms, n
                FROM scada.viz_diurnal_monthly
                WHERE farm = :farm AND EXTRACT(YEAR FROM month_utc) = :year
                ORDER BY 1, 2
                """
            ),
            {"farm": farm, "year": year},
        )
        return {
            "cells": [
                {
                    "m": r.month_utc.isoformat(),
                    "h": int(r.hour),
                    "p": round(float(r.power_mean_kw), 1) if r.power_mean_kw is not None else None,
                    "ws": round(float(r.ws_mean_ms), 2) if r.ws_mean_ms is not None else None,
                    "n": int(r.n),
                }
                for r in result.fetchall()
            ]
        }

    async def density_curve(self, farm: str, year: int) -> Dict[str, Any]:
        result = await self.db.execute(
            text(
                """
                SELECT temp_bin_c, power_med_kw, n
                FROM scada.viz_density_power_yearly
                WHERE farm = :farm AND year = :year AND n >= 100
                ORDER BY 1
                """
            ),
            {"farm": farm, "year": year},
        )
        return {
            "bins": [
                {"tc": int(r.temp_bin_c), "p": round(float(r.power_med_kw)), "n": int(r.n)}
                for r in result.fetchall()
                if r.power_med_kw is not None
            ]
        }

    async def ramps(self, farm: str, year: int) -> Dict[str, Any]:
        hist_res = await self.db.execute(
            text(
                """
                SELECT bin_mw, n FROM scada.viz_ramp_yearly
                WHERE farm = :farm AND year = :year ORDER BY 1
                """
            ),
            {"farm": farm, "year": year},
        )
        ev_res = await self.db.execute(
            text(
                """
                SELECT ts_utc, ramp_mw, mw_before, mw_after
                FROM scada.viz_ramp_events
                WHERE farm = :farm AND EXTRACT(YEAR FROM ts_utc) = :year
                ORDER BY ABS(ramp_mw) DESC LIMIT 12
                """
            ),
            {"farm": farm, "year": year},
        )
        rated_res = await self.db.execute(
            text("SELECT SUM(rated_kw) / 1000.0 FROM scada.dim_turbine WHERE farm = :farm"),
            {"farm": farm},
        )
        return {
            "bins": [{"bin": int(r.bin_mw), "n": int(r.n)} for r in hist_res.fetchall()],
            "events": [
                {
                    "ts": r.ts_utc.isoformat(),
                    "ramp": round(float(r.ramp_mw), 1),
                    "before": round(float(r.mw_before), 1) if r.mw_before is not None else None,
                    "after": round(float(r.mw_after), 1) if r.mw_after is not None else None,
                }
                for r in ev_res.fetchall()
            ],
            "rated_mw": round(float(rated_res.scalar() or 0), 1),
        }

    async def turbine_losses(self, farm: str, turbine: str, year: int) -> Dict[str, Any]:
        """Monthly loss mix for one turbine — the dossier's cost strip."""
        result = await self.db.execute(
            text(
                """
                SELECT DATE_TRUNC('month', date_utc)::date AS mo,
                       SUM(loss_downtime_kwh) / 1000.0    AS dt,
                       SUM(loss_curtailment_kwh) / 1000.0 AS ct,
                       SUM(loss_performance_kwh) / 1000.0 AS pf,
                       SUM(potential_kwh) / 1000.0        AS pot
                FROM scada.losses_daily
                WHERE farm = :farm AND turbine = :turbine AND NOT pre_cod
                  AND date_utc BETWEEN :start AND :end
                GROUP BY 1 ORDER BY 1
                """
            ),
            {
                "farm": farm,
                "turbine": turbine,
                "start": date(year, 1, 1),
                "end": date(year, 12, 31),
            },
        )
        return {
            "months": [
                {
                    "m": r.mo.isoformat(),
                    "dt": round(float(r.dt or 0), 1),
                    "ct": round(float(r.ct or 0), 1),
                    "pf": round(float(r.pf or 0), 1),
                    "pot": round(float(r.pot or 0), 1),
                }
                for r in result.fetchall()
            ]
        }
