"""SCADA Degradation page (EPR-202): ageing per turbine, projected to a horizon, and its cost.

Everything comes from the gold tables the SCADA pipeline publishes:

* ``scada.turbine_performance_yearly``: a yearly performance index per turbine and
  configuration period (this year's power curve against the period's pooled curve, with the
  wind held at one farm distribution). The "ladder".
* ``scada.power_curve_bins_yearly``: mean power per 0.5 m/s bin; the 7.0 and 7.5 bins give
  the "mid-wind fade" (plain mean of the two, so the wind mix inside 7–8 m/s cannot move it;
  both bins need ``FADE_MIN_N`` samples).
* ``scada.energy_daily``: energy and known intervals per turbine-year (coverage, baseline).
* ``scada.dim_turbine_config``: configuration periods (Hill of Towie: baseline, then AeroUp
  after a works gap; other farms have one ``default`` period).

Rules (one place; the client only slices and formats):

    full year      known intervals >= FULL_YEAR_COVERAGE of the year's, ws coverage >= 90 %,
                   the whole calendar year inside one period (by the config dates), and before
                   the current year. Other years are shown but never fitted.
    slope s        within-period least squares over full years: each period has its own mean,
                   periods with < 2 full years add nothing, >= MIN_FIT_YEARS points in total.
    chaining       every period is shifted onto the latest period's fitted line,
                   v' = v + (ȳ_latest − ȳ_p) + s·(x̄_p − x̄_latest). An upgrade step therefore
                   counts as neither ageing nor gain; without a slope, other periods are
                   returned un-chained and flagged.
    ladder         re-centred so the turbine's full years average exactly 1.00.
    fleet          median series across turbines; slope = median of turbine slopes, drawn
                   through the centroid of the median series over the years in which at least
                   half the turbines have a full year; band = the middle half of turbine rates
                   (q25..q75) fanning out from the last data year.
    loss           lost(y) = clamp((T(y0) − T(y)) ÷ T(y0), 0, 1) × baseline output, where T is
                   the fleet trend (or the selected turbine's own trend) and y0 its first full
                   year; baseline = mean of complete years (``estimate_annual_gwh``, 0.99 rule).
                   A rising trend loses nothing.
    measured       for the Total view (EPR-213): years up to ``measured_to`` (the last year
                   before the current one with farm energy) carry the measured generation
                   (already net of degradation) and the share of the year's intervals known;
                   later years have none, and the client draws them from the baseline.
                   A year with nothing recorded has no measured value (it is estimated too).
    price         a year with >= 50 % of its hours priced uses its own average; every other
                   year (forecast, thin years) uses the flat price = the mean of the yearly
                   averages of years with >= 90 % of hours. The farm's own platform wind farm
                   first; otherwise any platform wind farm in the same bidzone (prices are
                   zonal), lowest id first.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.price_analytics_service import price_source_for
from app.services.scada_performance_service import (
    INTERVALS_PER_DAY,
    ScadaPerformanceService,
    estimate_annual_gwh,
)
from app.services.scada_service import _median, _percentile

HORIZON_MIN = 2027
HORIZON_MAX = 2050
FULL_YEAR_COVERAGE = 0.90
WS_COVERAGE_MIN = 90.0
MIN_FIT_YEARS = 3
FADE_MIN_N = 50
OWN_PRICE_HOURS = 0.50
FLAT_PRICE_HOURS = 0.90
FARM_SCOPE = "_farm"
UPGRADE_LABELS = {"aeroup": "AeroUp"}


@dataclass(frozen=True)
class Period:
    config: str
    valid_from: Optional[date]
    valid_to: Optional[date]  # exclusive; None = open


@dataclass(frozen=True)
class Obs:
    """One metric value for (turbine, config, year), with its weight for straddling years."""

    config: str
    year: int
    value: float
    weight: float


Line = Tuple[float, float, float]  # (anchor year, anchor value, slope per year)


# ---- pure rules ---------------------------------------------------------------------------


def days_in_year(year: int) -> int:
    return (date(year + 1, 1, 1) - date(year, 1, 1)).days


def hours_in_year(year: int) -> int:
    return days_in_year(year) * 24


def whole_year_period(periods: Sequence[Period], year: int) -> Optional[str]:
    """The period that covers the whole calendar year, if one does."""
    start, end = date(year, 1, 1), date(year + 1, 1, 1)
    for p in periods:
        if (p.valid_from is None or p.valid_from <= start) and (
            p.valid_to is None or p.valid_to >= end
        ):
            return p.config
    return None


def full_year(
    year: int,
    known_intervals: int,
    current_year: int,
    whole_period: Optional[str],
    config: str,
    ws_coverage: Optional[float] = None,
) -> bool:
    if year >= current_year or whole_period != config:
        return False
    if ws_coverage is not None and ws_coverage < WS_COVERAGE_MIN:
        return False
    return known_intervals >= FULL_YEAR_COVERAGE * days_in_year(year) * INTERVALS_PER_DAY


def _mean(values: Iterable[float]) -> float:
    vals = list(values)
    return sum(vals) / len(vals)


def within_period_slope(points: Sequence[Tuple[str, int, float]]) -> Optional[float]:
    """Least-squares slope with one mean per period, over (config, year, value) full years."""
    groups: Dict[str, List[Tuple[int, float]]] = defaultdict(list)
    for config, year, value in points:
        groups[config].append((year, value))
    sxy = sxx = 0.0
    n = 0
    for pts in groups.values():
        if len(pts) < 2:
            continue
        mx = _mean(x for x, _ in pts)
        my = _mean(y for _, y in pts)
        sxy += sum((x - mx) * (y - my) for x, y in pts)
        sxx += sum((x - mx) ** 2 for x, _ in pts)
        n += len(pts)
    if n < MIN_FIT_YEARS or sxx == 0:
        return None
    return sxy / sxx


def chain(
    obs: Sequence[Obs], full: Set[Tuple[str, int]], slope: Optional[float], order: Sequence[str]
) -> Tuple[Dict[Tuple[str, int], float], Set[str]]:
    """Shift every period onto the latest period's line: (chained values, un-chained configs).

    A period's means come from its full years, or from all its points when it has none.
    """
    by_cfg: Dict[str, List[Obs]] = defaultdict(list)
    for o in obs:
        by_cfg[o.config].append(o)
    raw = {(o.config, o.year): o.value for o in obs}
    if len(by_cfg) <= 1:
        return raw, set()
    present = [c for c in order if c in by_cfg] + sorted(c for c in by_cfg if c not in order)
    ref = present[-1]
    if slope is None:
        return raw, {c for c in by_cfg if c != ref}

    def means(cfg: str) -> Tuple[float, float]:
        pts = [o for o in by_cfg[cfg] if (cfg, o.year) in full] or by_cfg[cfg]
        return _mean(o.year for o in pts), _mean(o.value for o in pts)

    xr, yr = means(ref)
    out: Dict[Tuple[str, int], float] = {}
    for cfg, items in by_cfg.items():
        xp, yp = means(cfg)
        shift = (yr - yp) + slope * (xp - xr)
        for o in items:
            out[(cfg, o.year)] = o.value + shift
    return out, set()


def combine_years(obs: Sequence[Obs], values: Dict[Tuple[str, int], float]) -> Dict[int, float]:
    """One value per year: the weight-weighted mean of its periods (plain mean if unweighted)."""
    acc: Dict[int, List[Tuple[float, float]]] = defaultdict(list)
    for o in obs:
        acc[o.year].append((values[(o.config, o.year)], o.weight))
    out = {}
    for year, parts in acc.items():
        total = sum(w for _, w in parts)
        out[year] = (
            sum(v * w for v, w in parts) / total if total > 0 else _mean(v for v, _ in parts)
        )
    return out


def recentre(values: Dict[int, float], full_years: Set[int]) -> Dict[int, float]:
    """Shift so the full years average exactly 1.00 (all years when none is full)."""
    base = [values[y] for y in full_years if y in values] or list(values.values())
    if not base:
        return {}
    shift = 1.0 - _mean(base)
    return {y: v + shift for y, v in values.items()}


def own_line(
    values: Dict[int, float], full_years: Set[int], slope: Optional[float]
) -> Optional[Line]:
    pts = [(y, values[y]) for y in sorted(full_years) if y in values]
    if slope is None or not pts:
        return None
    return _mean(y for y, _ in pts), _mean(v for _, v in pts), slope


def at(line: Line, year: int) -> float:
    ax, ay, s = line
    return ay + s * (year - ax)


def qualifying_years(full_by_turbine: Dict[str, Set[int]], n_turbines: int) -> List[int]:
    """Years in which at least half the turbines have a full year."""
    counts: Dict[int, int] = defaultdict(int)
    for years in full_by_turbine.values():
        for y in years:
            counts[y] += 1
    return sorted(y for y, c in counts.items() if n_turbines > 0 and c * 2 >= n_turbines)


def fleet_line(
    median: Dict[int, float], qualifying: Sequence[int], slopes: Sequence[float]
) -> Optional[Line]:
    """Median turbine slope through the centroid of the median series over qualifying years.

    The centroid is already the least-squares anchor for a line of fixed slope over those
    years, so no other anchor can sit closer to the median; ``fit_gap`` reports how close.
    """
    years = [y for y in qualifying if y in median]
    if len(years) < 2 or not slopes:
        return None
    s = _median(list(slopes))
    return _mean(years), _mean(median[y] for y in years), s


def fit_gap(line: Line, median: Dict[int, float], qualifying: Sequence[int]) -> Optional[float]:
    gaps = [abs(at(line, y) - median[y]) for y in qualifying if y in median]
    return max(gaps) if gaps else None


def band(
    line: Line, slopes: Sequence[float], last_year: int, years: Iterable[int]
) -> List[Dict[str, Any]]:
    """Middle half of turbine rates, fanning out from the last data year around the trend."""
    lo_s, hi_s = _percentile(slopes, 0.25), _percentile(slopes, 0.75)
    if lo_s is None or hi_s is None:
        return []
    s = line[2]
    out = []
    for y in years:
        if y < last_year:
            continue
        f = at(line, y)
        out.append(
            {
                "year": y,
                "lo": f + (lo_s - s) * (y - last_year),
                "hi": f + (hi_s - s) * (y - last_year),
            }
        )
    return out


def flat_price(
    by_year: Dict[int, Tuple[float, int]], current_year: int
) -> Tuple[Optional[float], List[int]]:
    years = sorted(
        y
        for y, (_, hours) in by_year.items()
        if y < current_year and hours >= FLAT_PRICE_HOURS * hours_in_year(y)
    )
    if not years:
        return None, []
    return _mean(by_year[y][0] for y in years), years


def year_prices(
    by_year: Dict[int, Tuple[float, int]],
    flat: Optional[float],
    years: Iterable[int],
    current_year: int,
) -> Dict[int, Optional[float]]:
    out: Dict[int, Optional[float]] = {}
    for y in years:
        own = by_year.get(y)
        if y <= current_year and own and own[1] >= OWN_PRICE_HOURS * hours_in_year(y):
            out[y] = own[0]
        else:
            out[y] = flat
    return out


def measured_to_year(yearly: Dict[int, Tuple[float, int]], current_year: int) -> Optional[int]:
    """The last year before the current one with any measured energy (the Total view's split)."""
    years = [y for y, (_, known) in yearly.items() if y < current_year and known > 0]
    return max(years) if years else None


def measured(
    yearly: Dict[int, Tuple[float, int]], year: int, n_turbines: int
) -> Tuple[Optional[float], float]:
    """(measured MWh or None when nothing was recorded, share of the year's intervals known)."""
    kwh, known = yearly.get(year, (0.0, 0))
    possible = days_in_year(year) * INTERVALS_PER_DAY * max(1, n_turbines)
    coverage = min(1.0, known / possible)
    return (kwh / 1000.0 if known > 0 else None), coverage


def loss_rows(
    trend: Optional[Callable[[int], float]],
    start: Optional[int],
    baseline_mwh: Optional[float],
    years: Iterable[int],
    prices: Dict[int, Optional[float]],
    current_year: int,
    *,
    yearly: Optional[Dict[int, Tuple[float, int]]] = None,
    n_turbines: int = 1,
    measured_to: Optional[int] = None,
) -> Optional[List[Dict[str, Any]]]:
    """Generation and revenue lost each year; None when there is no usable trend.

    Years up to ``measured_to`` also carry the measured generation (already net of
    degradation) and the share of the year's intervals it covers, for the Total view.
    """
    if trend is None or start is None:
        return None
    t0 = trend(start)
    if t0 <= 0:
        return None
    rows = []
    for y in years:
        frac = 0.0 if y < start else min(1.0, max(0.0, (t0 - trend(y)) / t0))
        mwh = baseline_mwh * frac if baseline_mwh is not None else None
        price = prices.get(y)
        got, coverage = (
            measured(yearly, y, n_turbines)
            if yearly is not None and measured_to is not None and y <= measured_to
            else (None, None)
        )
        rows.append(
            {
                "year": y,
                "lost_frac": round(frac, 6),
                "mwh": round(mwh, 3) if mwh is not None else None,
                "revenue": round(mwh * price, 2) if mwh is not None and price is not None else None,
                "price": round(price, 2) if price is not None else None,
                "projected": y > current_year,
                "measured_mwh": round(got, 3) if got is not None else None,
                "coverage": round(coverage, 4) if coverage is not None else None,
            }
        )
    return rows


def natural_key(name: str) -> Tuple[Any, ...]:
    return tuple(int(p) if p.isdigit() else p for p in re.split(r"(\d+)", name))


# ---- assembly -----------------------------------------------------------------------------


@dataclass
class TurbineSeries:
    values: Dict[int, float]
    full_years: Set[int]
    unchained_years: Set[int]
    slope: Optional[float]
    line: Optional[Line]


def build_series(
    obs: Sequence[Obs],
    full: Set[Tuple[str, int]],
    order: Sequence[str],
    recentred: bool,
) -> TurbineSeries:
    slope = within_period_slope(
        [(o.config, o.year, o.value) for o in obs if (o.config, o.year) in full]
    )
    chained, unchained_cfgs = chain(obs, full, slope, order)
    values = combine_years(obs, chained)
    full_years = {o.year for o in obs if (o.config, o.year) in full}
    if recentred:
        values = recentre(values, full_years)
    return TurbineSeries(
        values=values,
        full_years=full_years,
        unchained_years={o.year for o in obs if o.config in unchained_cfgs},
        slope=slope,
        line=own_line(values, full_years, slope),
    )


def metric_block(
    series: Dict[str, TurbineSeries],
    n_turbines: int,
    years: Sequence[int],
    last_data: int,
    digits: int,
    pct_of_level: bool,
) -> Dict[str, Any]:
    """Turbines, fleet median, fleet trend and band for one metric (ladder or fade)."""

    def slope_pct(ts: TurbineSeries) -> Optional[float]:
        if ts.slope is None:
            return None
        if not pct_of_level:
            return ts.slope * 100
        level = ts.line[1] if ts.line else None
        return ts.slope / level * 100 if level else None

    turbines = []
    for t in sorted(series, key=natural_key):
        ts = series[t]
        sp = slope_pct(ts)
        turbines.append(
            {
                "t": t,
                "points": [
                    {
                        "year": y,
                        "v": round(ts.values[y], digits),
                        "full": y in ts.full_years,
                        "unchained": y in ts.unchained_years,
                    }
                    for y in sorted(ts.values)
                ],
                "slope_pct": round(sp, 3) if sp is not None else None,
                "trend": [{"year": y, "v": round(at(ts.line, y), digits)} for y in years]
                if ts.line
                else [],
            }
        )

    all_years = sorted({y for ts in series.values() for y in ts.values})
    median = {
        y: m
        for y in all_years
        if (m := _median([ts.values[y] for ts in series.values() if y in ts.values])) is not None
    }
    sloped = {t: ts for t, ts in series.items() if ts.slope is not None}
    worst = min(sloped, key=lambda t: (sloped[t].slope, natural_key(t))) if sloped else None
    qualifying = qualifying_years({t: ts.full_years for t, ts in series.items()}, n_turbines)
    line = fleet_line(median, qualifying, [ts.slope for ts in sloped.values()])
    fleet = None
    if line is not None:
        pcts = [p for ts in sloped.values() if (p := slope_pct(ts)) is not None]
        fleet = {
            "slope_pct": round(_median(pcts), 3) if pcts else None,
            "start_year": qualifying[0],
            "trend": [{"year": y, "v": round(at(line, y), digits)} for y in years],
            "band": [
                {"year": b["year"], "lo": round(b["lo"], digits), "hi": round(b["hi"], digits)}
                for b in band(line, [ts.slope for ts in sloped.values()], last_data, years)
            ],
            "fit_gap": round(gap, digits)
            if (gap := fit_gap(line, median, qualifying)) is not None
            else None,
        }
    first_m, last_m = (median[all_years[0]], median[all_years[-1]]) if median else (None, None)
    return {
        "turbines": turbines,
        "median": [{"year": y, "v": round(v, digits)} for y, v in sorted(median.items())],
        "median_change_pct": round((last_m / first_m - 1) * 100, 2) if first_m else None,
        "fleet": fleet,
        "worst": worst,
        "_line": line,
        "_qualifying": qualifying,
    }


class ScadaDegradationService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _periods(self, farm: str) -> Dict[str, List[Period]]:
        result = await self.db.execute(
            text(
                "SELECT turbine, config, valid_from, valid_to FROM scada.dim_turbine_config "
                "WHERE farm = :farm"
            ),
            {"farm": farm},
        )
        out: Dict[str, List[Period]] = defaultdict(list)
        for r in result.all():
            out[r.turbine].append(Period(r.config, r.valid_from, r.valid_to))
        for periods in out.values():
            periods.sort(key=lambda p: p.valid_from or date.min)
        return out

    async def _turbines(self, farm: str) -> List[str]:
        result = await self.db.execute(
            text("SELECT turbine FROM scada.dim_turbine WHERE farm = :farm"), {"farm": farm}
        )
        return sorted((r.turbine for r in result.all()), key=natural_key)

    async def _ladder_rows(self, farm: str) -> List[Any]:
        result = await self.db.execute(
            text(
                """
                SELECT turbine, config, year, performance_index AS v,
                       intervals_used AS w, ws_coverage_pct AS cov
                FROM scada.turbine_performance_yearly
                WHERE farm = :farm AND performance_index IS NOT NULL
                """
            ),
            {"farm": farm},
        )
        return result.all()

    async def _fade_rows(self, farm: str) -> List[Any]:
        result = await self.db.execute(
            text(
                """
                SELECT turbine, config, year, AVG(power_mean_kw) AS v, SUM(n) AS w
                FROM scada.power_curve_bins_yearly
                WHERE farm = :farm AND ws_bin IN (7.00, 7.50) AND n >= :n_min
                  AND power_mean_kw IS NOT NULL
                GROUP BY 1, 2, 3
                HAVING COUNT(*) = 2
                """
            ),
            {"farm": farm, "n_min": FADE_MIN_N},
        )
        return result.all()

    async def _energy(self, farm: str) -> Dict[str, Dict[int, Tuple[float, int]]]:
        result = await self.db.execute(
            text(
                """
                SELECT turbine, EXTRACT(YEAR FROM date_utc)::int AS year,
                       SUM(energy_kwh) AS kwh,
                       SUM(intervals_meter + intervals_integral) AS known
                FROM scada.energy_daily
                WHERE farm = :farm AND NOT pre_cod
                GROUP BY 1, 2
                """
            ),
            {"farm": farm},
        )
        out: Dict[str, Dict[int, Tuple[float, int]]] = defaultdict(dict)
        for r in result.all():
            out[r.turbine][int(r.year)] = (float(r.kwh or 0), int(r.known or 0))
        return out

    async def _price_windfarm(
        self, windfarm_id: Optional[int], bidzone: Optional[str]
    ) -> Optional[int]:
        """The farm's own platform wind farm when it has prices, else one in the same bidzone."""
        source = price_source_for(bidzone)
        if windfarm_id is not None:
            own = await self.db.execute(
                text(
                    "SELECT 1 FROM price_data WHERE windfarm_id = :id AND source = :source LIMIT 1"
                ),
                {"id": windfarm_id, "source": source},
            )
            if own.scalar() is not None:
                return windfarm_id
        if not bidzone:
            return None
        result = await self.db.execute(
            text(
                """
                -- OFFSET 0 keeps the ordered list un-flattened, so the price check stops at
                -- the first farm that has prices instead of probing all of them (2.5 s → 75 ms
                -- for GB's 158 farms on staging).
                SELECT c.id FROM (
                    SELECT w.id FROM windfarms w JOIN bidzones b ON b.id = w.bidzone_id
                    WHERE b.code = :code ORDER BY w.id OFFSET 0
                ) c
                WHERE EXISTS (SELECT 1 FROM price_data p
                              WHERE p.windfarm_id = c.id AND p.source = :source)
                LIMIT 1
                """
            ),
            {"code": bidzone, "source": source},
        )
        return result.scalar()

    async def _prices_by_year(
        self, windfarm_id: int, source: str
    ) -> Tuple[Dict[int, Tuple[float, int]], Optional[str]]:
        result = await self.db.execute(
            text(
                """
                SELECT EXTRACT(YEAR FROM hour AT TIME ZONE 'UTC')::int AS year,
                       AVG(day_ahead_price) AS price, COUNT(*) AS hours,
                       MAX(currency) AS currency
                FROM price_data
                WHERE windfarm_id = :id AND source = :source AND day_ahead_price IS NOT NULL
                GROUP BY 1
                """
            ),
            {"id": windfarm_id, "source": source},
        )
        rows = result.all()
        currency = next((r.currency for r in rows if r.currency), None)
        return {int(r.year): (float(r.price), int(r.hours)) for r in rows}, currency

    async def outlook(self, farm: str, today: Optional[date] = None) -> Dict[str, Any]:
        await self.db.execute(text("SET LOCAL statement_timeout = '30s'"))
        today = today or datetime.now(timezone.utc).date()
        current_year = today.year

        facts = await ScadaPerformanceService(self.db)._farm(farm)
        turbines = await self._turbines(farm)
        periods = await self._periods(farm)
        energy = await self._energy(farm)
        ladder_rows = await self._ladder_rows(farm)
        fade_rows = await self._fade_rows(farm)
        n_turbines = len(turbines)

        order_by_turbine = {
            t: [p.config for p in periods.get(t, [])] or ["default"] for t in turbines
        }

        def full_set(rows: Sequence[Any], with_cov: bool) -> Dict[str, Set[Tuple[str, int]]]:
            out: Dict[str, Set[Tuple[str, int]]] = defaultdict(set)
            for r in rows:
                year = int(r.year)
                p = periods.get(r.turbine)
                whole = whole_year_period(p, year) if p else "default"
                known = energy.get(r.turbine, {}).get(year, (0.0, 0))[1]
                cov = (
                    float(r.cov) if with_cov and r.cov is not None else (0.0 if with_cov else None)
                )
                if full_year(year, known, current_year, whole, r.config, cov):
                    out[r.turbine].add((r.config, year))
            return out

        def obs_by_turbine(rows: Sequence[Any]) -> Dict[str, List[Obs]]:
            out: Dict[str, List[Obs]] = defaultdict(list)
            for r in rows:
                out[r.turbine].append(Obs(r.config, int(r.year), float(r.v), float(r.w or 0)))
            return out

        ladder_full = full_set(ladder_rows, with_cov=True)
        fade_full = full_set(fade_rows, with_cov=False)
        ladder_obs = obs_by_turbine(ladder_rows)
        fade_obs = obs_by_turbine(fade_rows)
        ladder_series = {
            t: build_series(o, ladder_full.get(t, set()), order_by_turbine.get(t, []), True)
            for t, o in ladder_obs.items()
        }
        fade_series = {
            t: build_series(o, fade_full.get(t, set()), order_by_turbine.get(t, []), False)
            for t, o in fade_obs.items()
        }

        data_years = {y for s in (*ladder_series.values(), *fade_series.values()) for y in s.values}
        if not data_years:
            return {
                "farm": self._farm_block(farm, facts, n_turbines, None),
                "years": None,
                "upgrade": None,
                "ladder": None,
                "fade": None,
                "loss": None,
            }
        first, last_data = min(data_years), max(data_years)
        years = list(range(first, HORIZON_MAX + 1))

        ladder = metric_block(ladder_series, n_turbines, years, last_data, 4, pct_of_level=False)
        fade = metric_block(fade_series, n_turbines, years, last_data, 1, pct_of_level=True)

        # ---- price ----
        price_block = None
        prices: Dict[int, Optional[float]] = {}
        wf = await self._price_windfarm(
            facts.windfarm_id if facts else None, facts.bidzone if facts else None
        )
        if wf is not None:
            by_year, currency = await self._prices_by_year(wf, price_source_for(facts.bidzone))
            flat, flat_years = flat_price(by_year, current_year)
            if flat is not None:
                prices = year_prices(by_year, flat, years, current_year)
                price_block = {
                    "zone": facts.zone_name or facts.bidzone,
                    "currency": currency,
                    "flat": round(flat, 2),
                    "flat_years": flat_years,
                    "by_year": {str(y): round(p, 2) for y, (p, _) in sorted(by_year.items())},
                }

        # ---- loss ----
        scopes: Dict[str, Any] = {}
        farm_yearly: Dict[int, Tuple[float, int]] = defaultdict(lambda: (0.0, 0))
        for per_year in energy.values():
            for y, (kwh, known) in per_year.items():
                k0, n0 = farm_yearly[y]
                farm_yearly[y] = (k0 + kwh, n0 + known)

        measured_to = measured_to_year(farm_yearly, current_year)

        def scope(
            line: Optional[Line],
            start: Optional[int],
            slope_pct: Optional[float],
            yearly: Dict[int, Tuple[float, int]],
            n: int,
        ) -> Dict[str, Any]:
            gwh, base_years = estimate_annual_gwh(dict(yearly), n)
            baseline = gwh * 1000 if gwh is not None else None
            trend = (lambda y, ln=line: at(ln, y)) if line is not None else None
            rows = loss_rows(
                trend,
                start,
                baseline,
                years,
                prices,
                current_year,
                yearly=yearly,
                n_turbines=n,
                measured_to=measured_to,
            )
            return {
                "baseline_mwh": round(baseline, 3) if baseline is not None else None,
                "baseline_years": base_years,
                "start_year": start,
                "slope_pct": round(slope_pct, 3) if slope_pct is not None else None,
                "rising": slope_pct is not None and slope_pct >= 0,
                "rows": rows,
            }

        fleet = ladder["fleet"]
        scopes[FARM_SCOPE] = scope(
            ladder["_line"],
            fleet["start_year"] if fleet else None,
            fleet["slope_pct"] if fleet else None,
            farm_yearly,
            n_turbines,
        )
        for t, ts in ladder_series.items():
            scopes[t] = scope(
                ts.line,
                min(ts.full_years) if ts.full_years else None,
                ts.slope * 100 if ts.slope is not None else None,
                energy.get(t, {}),
                1,
            )

        for block in (ladder, fade):
            block.pop("_line")
            block.pop("_qualifying")

        return {
            "farm": self._farm_block(farm, facts, n_turbines, price_block),
            "years": {
                "first": first,
                "last_data": last_data,
                "current": current_year,
                "measured_to": measured_to,
                "horizon_min": HORIZON_MIN,
                "horizon_max": HORIZON_MAX,
            },
            "upgrade": self._upgrade(periods),
            "ladder": ladder,
            "fade": fade,
            "loss": {"price": price_block, "scopes": scopes},
        }

    @staticmethod
    def _farm_block(
        farm: str, facts: Any, n_turbines: int, price: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        return {
            "slug": farm,
            "name": facts.name if facts else farm,
            "rated_mw": round(float(facts.rated_kw) / 1000, 1)
            if facts and facts.rated_kw
            else None,
            "n_turbines": n_turbines,
            "zone": (facts.zone_name or facts.bidzone) if facts else None,
            "currency": price["currency"] if price else None,
        }

    @staticmethod
    def _upgrade(periods: Dict[str, List[Period]]) -> Optional[Dict[str, Any]]:
        """The farm's configuration change window: earliest end of a period to latest new start."""
        ends: List[date] = []
        starts: List[date] = []
        label = None
        for ps in periods.values():
            for prev, nxt in zip(ps, ps[1:]):
                if prev.valid_to is not None:
                    ends.append(prev.valid_to)
                if nxt.valid_from is not None:
                    starts.append(nxt.valid_from)
                label = label or UPGRADE_LABELS.get(nxt.config, nxt.config.title())
        if not ends or not starts:
            return None
        return {"label": label, "from_year": min(ends).year, "to_year": max(starts).year}
