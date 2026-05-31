"""OPS-04 · Turbine degradation — M3 new detector (issue #99).

Reads the Module 5 degradation pipeline (``degradation_results``, an OLS
regression of normalized output against time) and flags windfarms whose
``slope_pct_per_year`` is significantly negative — i.e. the fleet is degrading
faster than expected.

CAPPED AT INDICATIVE (caveat cap)
=================================
``degradation_results.baseline_cap_pu`` is currently a hardcoded ``0.35``
placeholder (see issue #99 / tracking #116), so the absolute slope magnitude is
**indicative only** — the baseline the regression is measured against is not yet
trustworthy. Until the real ``baseline_cap_pu`` lands (#30/#116), OPS-04's
severity is **capped at INDICATIVE**: a result that would otherwise classify as
CONFIRMED is emitted as INDICATIVE, and ``data_slots["baseline_caveat"] = True``
records why. WATCH and "no finding" are unaffected by the cap.

Severity tiers (spec thresholds, slope in %/yr, ``p`` is the OLS p-value):

    slope < -3.5  AND p < 0.05  →  (CONFIRMED-eligible) → INDICATIVE (caveat cap)
    slope < -2.0  AND p < 0.05  →  INDICATIVE
    slope < -1.0  AND p < 0.10  →  WATCH
    otherwise                    →  None (no finding)

Comparisons are strict ``<`` on the slope (so ``-2.0`` is NOT below ``-2.0``) and
strict ``<`` on the p-value gate. Examples (locked by tests):
    (-4.0, 0.04)  → INDICATIVE  (CONFIRMED-eligible, capped)
    (-2.01, 0.04) → INDICATIVE
    (-1.01, 0.06) → WATCH
    (-1.01, 0.11) → None        (p above the 0.10 WATCH gate)

Guards (any → ``detect`` returns ``None``, no row):
    * **Floating foundation** — ``windfarm.foundation_type == "floating"``. The
      degradation methodology assumes a fixed-bottom power-curve baseline;
      floating assets are excluded until a floating-specific baseline exists.
    * **Modelling artefact** — ``abs(slope_pct_per_year) > 20`` %/yr is physically
      implausible for turbine degradation (e.g. Kincardine's -172 %/yr) and points
      at a regression/baseline artefact, not real degradation.
    * **Insufficient history** — fewer than ``MIN_YEARS_OF_DATA`` (3) years span
      the regression window; a slope fitted over a short window is unreliable.

The detector is pure: it reads only ``ctx.load_degradation_result()`` and
``ctx.windfarm``, and returns a ``DetectorResult`` (or ``None``). Persistence is
the orchestrator's job.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Slope thresholds (%/yr). Strict ``<`` — a slope must be MORE negative than the
# floor to qualify for the tier.
OPS04_SLOPE_CONFIRMED_PCT = -3.5
OPS04_SLOPE_INDICATIVE_PCT = -2.0
OPS04_SLOPE_WATCH_PCT = -1.0

# p-value gates (strict ``<``). CONFIRMED/INDICATIVE require significance at 0.05;
# WATCH relaxes to 0.10.
OPS04_P_SIGNIFICANT = 0.05
OPS04_P_WATCH = 0.10

# Slopes beyond this magnitude (%/yr) are treated as modelling artefacts, not real
# degradation, and excluded (e.g. Kincardine's -172 %/yr).
OPS04_ARTIFACT_SLOPE_PCT = 20.0

# Minimum years of regression history required before OPS-04 can fire.
MIN_YEARS_OF_DATA = 3.0


# ─── Pure, DB-free helpers ────────────────────────────────────────────────────


def classify_degradation_severity(
    slope_pct: Optional[float],
    p_value: Optional[float],
) -> Optional[Severity]:
    """Classify OPS-04 severity from the OLS slope (%/yr) and p-value.

    Tiers (strict ``<`` on both slope and p-value):
        * ``slope < -3.5 AND p < 0.05`` → CONFIRMED-eligible → **INDICATIVE**
          (the baseline caveat caps a would-be CONFIRMED result at INDICATIVE; see
          the module docstring).
        * ``slope < -2.0 AND p < 0.05`` → INDICATIVE
        * ``slope < -1.0 AND p < 0.10`` → WATCH
        * otherwise                      → ``None`` (no finding)

    Returns ``None`` when either input is missing. The CONFIRMED tier and the
    INDICATIVE tier therefore both surface as ``Severity.INDICATIVE`` while the
    caveat cap is in force; the distinction is recorded by the caller via
    ``data_slots`` (``baseline_caveat`` / ``confirmed_eligible``).
    """
    if slope_pct is None or p_value is None:
        return None

    # CONFIRMED-eligible → capped to INDICATIVE (baseline caveat).
    if slope_pct < OPS04_SLOPE_CONFIRMED_PCT and p_value < OPS04_P_SIGNIFICANT:
        return Severity.INDICATIVE
    if slope_pct < OPS04_SLOPE_INDICATIVE_PCT and p_value < OPS04_P_SIGNIFICANT:
        return Severity.INDICATIVE
    if slope_pct < OPS04_SLOPE_WATCH_PCT and p_value < OPS04_P_WATCH:
        return Severity.WATCH
    return None


def is_confirmed_eligible(slope_pct: Optional[float], p_value: Optional[float]) -> bool:
    """True when the result would be CONFIRMED but for the baseline caveat cap.

    ``slope < -3.5 AND p < 0.05``. Used to annotate the emitted INDICATIVE row so
    the caveat-removal work (#30/#116) can find rows that should become CONFIRMED.
    """
    if slope_pct is None or p_value is None:
        return False
    return slope_pct < OPS04_SLOPE_CONFIRMED_PCT and p_value < OPS04_P_SIGNIFICANT


def is_degradation_modelling_artifact(
    slope_pct: Optional[float],
    years_of_data: Optional[float],
) -> bool:
    """True when the slope is a modelling artefact rather than real degradation.

    A slope whose magnitude exceeds ``OPS04_ARTIFACT_SLOPE_PCT`` (20 %/yr) is
    physically implausible for turbine degradation (e.g. Kincardine's -172 %/yr)
    and indicates a regression/baseline artefact. ``years_of_data`` is accepted for
    a forward-compatible signature (the insufficient-history guard is applied
    separately in ``detect``); only the slope magnitude drives the artefact call.
    Returns ``False`` when the slope is missing.
    """
    if slope_pct is None:
        return False
    return abs(slope_pct) > OPS04_ARTIFACT_SLOPE_PCT


def is_floating_foundation(windfarm: Any) -> bool:
    """True when the windfarm has a floating foundation (excluded from OPS-04).

    Reads ``windfarm.foundation_type``; the comparison is case-insensitive
    (``"floating"``). A bare-int windfarm or a missing attribute → ``False`` (not
    floating).
    """
    foundation = getattr(windfarm, "foundation_type", None)
    if not foundation:
        return False
    return str(foundation).strip().lower() == "floating"


def _years_of_data(degradation: dict) -> Optional[float]:
    """Span (in years) of the degradation regression window, or ``None``.

    Derived from the result's ``analysis_start`` / ``analysis_end`` dates (the
    actual window the slope was fitted over). Returns ``None`` when either date is
    absent so the caller can decide how to treat unknown history.
    """
    start = degradation.get("analysis_start")
    end = degradation.get("analysis_end")
    if not isinstance(start, date) or not isinstance(end, date):
        return None
    return (end - start).days / 365.25


# ─── Detector entrypoint ──────────────────────────────────────────────────────


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-04: turbine degradation (capped at INDICATIVE pending baseline fix).

    Returns ``None`` (no finding) when:
        * there is no degradation result for the windfarm,
        * the windfarm has a floating foundation,
        * the regression window spans < ``MIN_YEARS_OF_DATA`` years,
        * the slope is a modelling artefact (``abs(slope) > 20`` %/yr), or
        * the slope/p-value are sub-threshold.

    Otherwise emits a ``DetectorResult`` whose severity is classified per
    ``classify_degradation_severity`` (capped at INDICATIVE) with
    ``data_slots["baseline_caveat"] = True``.
    """
    # Floating foundations are excluded regardless of the (possibly absent)
    # degradation row.
    if is_floating_foundation(ctx.windfarm):
        return None

    degradation = await ctx.load_degradation_result()
    if not degradation:
        return None

    slope_pct = degradation.get("slope_pct_per_year")
    p_value = degradation.get("p_value")

    years = _years_of_data(degradation)
    # Insufficient history: a known span below the minimum is excluded. (Unknown
    # span — no analysis dates — does not block; the slope/p-value gates still
    # apply.)
    if years is not None and years < MIN_YEARS_OF_DATA:
        return None

    # Modelling artefact (e.g. -172 %/yr) → exclude.
    if is_degradation_modelling_artifact(slope_pct, years):
        return None

    severity = classify_degradation_severity(slope_pct, p_value)
    if severity is None:
        return None

    confirmed_eligible = is_confirmed_eligible(slope_pct, p_value)

    data_slots = {
        "slope_pct_per_year": slope_pct,
        "p_value": p_value,
        "r_squared": degradation.get("r_squared"),
        "ci_lower_95_pct": degradation.get("ci_lower_95_pct"),
        "ci_upper_95_pct": degradation.get("ci_upper_95_pct"),
        "n_constraint_hours_excluded": degradation.get("n_constraint_hours_excluded"),
        "reference_curve": degradation.get("reference_curve"),
        "years_of_data": round(years, 2) if years is not None else None,
        # Caveat cap (issue #99): baseline_cap_pu is a placeholder, so the slope
        # magnitude is indicative-only and severity is capped at INDICATIVE.
        "baseline_caveat": True,
        "confirmed_eligible": confirmed_eligible,
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }

    # Graceful-degradation: slots OPS-04 cannot populate until the real baseline
    # and corroborating data land (tracked under #30/#116).
    missing_slots = [
        "real_baseline_cap_pu",
        "per_turbine_degradation",
        "warranty_status",
        "revenue_impact_eur",
    ]

    return DetectorResult(
        schema_code=SchemaCode.OPS_04,
        severity=severity,
        branch=None,
        data_slots=data_slots,
        missing_slots=missing_slots,
    )
