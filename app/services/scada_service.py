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

from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

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
