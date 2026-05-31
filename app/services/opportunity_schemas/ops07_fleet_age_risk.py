"""OPS-07 · Fleet-age / end-of-life risk — M3 new detector (issue #102).

Flags windfarms whose turbine fleet is approaching (or has passed) the end of its
**25-year design life**. The signal is each turbine's age relative to that design
life, computed from ``turbine_units.start_date`` (per-turbine commissioning date,
surfaced via ``ctx.load_turbine_start_dates()``).

Dynamic 20-year boundary (CRITICAL — no hardcoded year)
=======================================================
"Final 5 years of life" = the last 5 years of a 25-year design life, i.e. an age
of **20 or more years**. Age is computed against a dynamic ``as_of_date``::

    age = as_of_date.year - start_date.year

so the 20-year boundary is ALWAYS ``start_year <= as_of_year - 20`` and slides
forward every year — it is never a literal calendar year baked into the code. The
detector derives ``as_of_date`` from the detection period end (or today's date if
no period end is available), so re-running the pipeline in a later year correctly
re-classifies an ageing fleet.

Metrics (``compute_fleet_age_metrics``)
=======================================
    pct_in_final_5yr   = fraction of turbines with age >= 20 (final-5-years window)
    any_past_design_life = any turbine with age >= 25 (past the design life)

Severity tiers (``classify_fleet_age_severity``)
================================================
    any turbine past 25-yr design life     →  CONFIRMED   (end-of-life reached)
    >= 75% of turbines in their final 5 yrs →  CONFIRMED
    >= 50%                                  →  INDICATIVE
    >= 30%                                  →  WATCH
    otherwise                               →  None (no finding)

Comparisons are inclusive ``>=``. Boundaries (locked by tests):
    0.75 → CONFIRMED   0.50 → INDICATIVE   0.30 → WATCH   0.29 → None
    age 26 (start 2000, as_of 2026) → CONFIRMED (past design life)
    age 20 (start == as_of - 20) → counted in-window; age 19 (as_of - 19) → not.

The detector is pure: it reads only ``ctx.load_turbine_start_dates()`` and the
detection period, and returns a ``DetectorResult`` (or ``None`` when there is no
turbine data). Persistence is the orchestrator's job.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional, Sequence, Tuple

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# 25-year design life. "Final 5 years" = age within the last 5 years of that life,
# i.e. age >= 20. "Past design life" = age >= 25.
DESIGN_LIFE_YEARS = 25
FINAL_5YR_AGE = 20  # DESIGN_LIFE_YEARS - 5

# Fraction-of-fleet-in-final-5yr tier floors (inclusive ``>=``).
OPS07_PCT_CONFIRMED = 0.75
OPS07_PCT_INDICATIVE = 0.50
OPS07_PCT_WATCH = 0.30


# ─── Pure, DB-free helpers ────────────────────────────────────────────────────


def compute_fleet_age_metrics(
    turbine_start_dates: Sequence[date],
    as_of_date: date,
) -> Tuple[float, bool]:
    """Fleet-age metrics relative to the 25-year design life.

    Age is ``as_of_date.year - start_date.year`` — driven by the dynamic
    ``as_of_date`` so the 20-year boundary slides forward each year (never a
    hardcoded calendar year).

    Returns ``(pct_in_final_5yr, any_past_design_life)``:
        * ``pct_in_final_5yr`` — fraction of turbines with age ``>= 20`` (the final
          5 years of a 25-year life). ``0.0`` when the fleet is empty.
        * ``any_past_design_life`` — ``True`` if any turbine has age ``>= 25``.

    Examples:
        start 2000, as_of 2026 → age 26 → past design life (and in final 5yr).
        start == as_of_year - 20 → age 20 → counted in the final-5yr window.
        start == as_of_year - 19 → age 19 → NOT counted.
    """
    total = len(turbine_start_dates)
    if total == 0:
        return 0.0, False

    ages = [as_of_date.year - start.year for start in turbine_start_dates]
    in_final_5yr = sum(1 for age in ages if age >= FINAL_5YR_AGE)
    any_past_design_life = any(age >= DESIGN_LIFE_YEARS for age in ages)
    pct_in_final_5yr = in_final_5yr / total
    return pct_in_final_5yr, any_past_design_life


def classify_fleet_age_severity(
    pct_in_final_5yr: float,
    any_past_design_life: bool,
) -> Optional[Severity]:
    """Classify OPS-07 severity from the fleet-age metrics.

    Tiers (inclusive ``>=``):
        * any turbine past the 25-yr design life → CONFIRMED
        * ``pct_in_final_5yr >= 0.75``           → CONFIRMED
        * ``pct_in_final_5yr >= 0.50``           → INDICATIVE
        * ``pct_in_final_5yr >= 0.30``           → WATCH
        * otherwise                              → ``None`` (no finding)

    Boundaries (locked by tests): 0.75 → CONFIRMED, 0.50 → INDICATIVE,
    0.30 → WATCH, 0.29 → None; ``any_past_design_life`` always → CONFIRMED.
    """
    if any_past_design_life:
        return Severity.CONFIRMED
    if pct_in_final_5yr >= OPS07_PCT_CONFIRMED:
        return Severity.CONFIRMED
    if pct_in_final_5yr >= OPS07_PCT_INDICATIVE:
        return Severity.INDICATIVE
    if pct_in_final_5yr >= OPS07_PCT_WATCH:
        return Severity.WATCH
    return None


# ─── Detector entrypoint ──────────────────────────────────────────────────────


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-07: fleet-age / end-of-life risk.

    Loads the windfarm's turbine commissioning dates, computes the fleet-age
    metrics against a dynamic ``as_of_date`` (the detection period end, or today's
    date when no period end is available — NEVER a hardcoded year), and classifies
    severity.

    Returns ``None`` (no finding) when:
        * there is no turbine start-date data for the windfarm
          (``ctx.load_turbine_start_dates()`` is ``None`` / empty), or
        * the fleet-age metrics are sub-threshold (no turbine past design life and
          fewer than 30% of turbines in their final 5 years).

    Otherwise emits a ``DetectorResult`` classified per
    ``classify_fleet_age_severity`` (CONFIRMED / INDICATIVE / WATCH).
    """
    start_dates = await ctx.load_turbine_start_dates()
    if not start_dates:
        return None

    as_of_date = _as_of_date(ctx)

    pct_in_final_5yr, any_past_design_life = compute_fleet_age_metrics(start_dates, as_of_date)

    severity = classify_fleet_age_severity(pct_in_final_5yr, any_past_design_life)
    if severity is None:
        return None

    data_slots = {
        "turbine_count": len(start_dates),
        "pct_in_final_5yr": round(pct_in_final_5yr, 4),
        "any_past_design_life": any_past_design_life,
        "design_life_years": DESIGN_LIFE_YEARS,
        "as_of_year": as_of_date.year,
    }

    # Graceful-degradation: slots OPS-07 cannot yet populate. A confirmed
    # major-component replacement (full re-power) would downgrade the finding per
    # the spec, but that data is not yet tracked.
    missing_slots = [
        "major_components_replaced",
        "remaining_useful_life_years",
        "repowering_plan",
    ]

    return DetectorResult(
        schema_code=SchemaCode.OPS_07,
        severity=severity,
        branch=None,
        data_slots=data_slots,
        missing_slots=missing_slots,
    )


def _as_of_date(ctx: DetectionContext) -> date:
    """Derive the dynamic ``as_of_date`` for age computation.

    Uses the detection period end (``ctx.period_end``) when present — the most
    recent point the analysis covers — falling back to today's date. NEVER a
    hardcoded year: the 20-year boundary derives entirely from this value's year.
    """
    period_end = getattr(ctx, "period_end", None)
    if isinstance(period_end, datetime):
        return period_end.date()
    if isinstance(period_end, date):
        return period_end
    return date.today()


__all__ = [
    "DESIGN_LIFE_YEARS",
    "FINAL_5YR_AGE",
    "compute_fleet_age_metrics",
    "classify_fleet_age_severity",
    "detect",
]
