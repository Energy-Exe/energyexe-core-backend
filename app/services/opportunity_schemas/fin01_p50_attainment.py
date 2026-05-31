"""FIN-01 · P50 generation attainment — M5 new detector (issue #107).

Flags windfarms whose **actual annual generation** falls short of a *sourced* P50
target (the externally-provided annual energy-production estimate from a wind
resource assessment, stored in ``p50_targets.p50_target_volume_gwh``). This is a
purely **financial / contractual** schema: it compares against a sourced target
only and NEVER substitutes an internal estimate. Farms that have actual
generation but **no sourced P50 target** still surface a *blank finding* so the
data gap itself is visible (see "Blank finding" below).

Attainment metric (``compute_attainment_pct``)
==============================================
    attainment_pct = actual_gwh / p50_target_gwh * 100

so ``95 GWh`` against a ``100 GWh`` target → ``95.0%``. ``None`` when the target
is missing or 0 (no meaningful ratio — handled as the blank finding).

Severity tiers (``classify_attainment_severity``)
=================================================
Severity depends on the latest year's ``attainment`` AND the immediately prior
year's ``prior_attainment`` (``None`` when there is only one year of data). A
single below-target year always **caps at WATCH** — escalation to INDICATIVE /
CONFIRMED requires two consecutive years.

    prior_attainment is None (single year of data):
        attainment >= 95  →  None   (at / above P50 — healthy)
        attainment <  95  →  WATCH  (single below-target year, capped)

    prior_attainment present (two years of data):
        attainment < 85 AND prior < 85   →  CONFIRMED   (two consecutive <85)
        attainment < 85 AND prior >= 85  →  INDICATIVE  (severe single-year drop)
        attainment < 90 AND prior < 90   →  INDICATIVE  (two consecutive <90)
        attainment < 95                  →  WATCH       (below target, not escalated)
        otherwise (attainment >= 95)     →  None        (healthy)

Comparisons are strict ``<`` against the 95 / 90 / 85 thresholds, matching the
spec's "<95% / <90% / <85%" wording. Boundaries (locked by tests):
    (80, None) → WATCH          (single year caps at WATCH)
    (82, 84)   → CONFIRMED      (two consecutive below 85)
    (82, 91)   → INDICATIVE     (severe single-year drop below 85, prior healthy)
    (95, None) / (95, 95) → None (exactly at target is healthy)

COD partial-year exclusion (``is_cod_year_excluded``)
=====================================================
A windfarm's first (commissioning) calendar year is only a *partial* operating
year when the commercial operational date (COD) lands late in the year, so its
generation would be misleadingly low against a full-year P50 target. The first
year is therefore **excluded** when COD month is after May (month > 5):

    COD 2023-07-15, year 2023 → excluded (commissioned mid-year, partial year)
    COD 2023-03-01, year 2023 → kept     (operational for most of the year)
    COD 2023-07-15, year 2024 → kept     (a later, full year)

The detector is pure: ``detect`` reads only ``ctx.load_annual_generation_gwh()``,
``ctx.load_p50_target()`` and ``ctx.windfarm.commercial_operational_date``, and
returns a ``DetectorResult`` (or ``None``). Persistence is the orchestrator's job.

Snapshot safety (CRITICAL — read before editing ``detect``)
===========================================================
The M1 characterization scenarios inject NO generation data, so
``ctx.load_annual_generation_gwh()`` resolves to ``None`` for them. ``detect``
returns ``None`` whenever there is no actual annual generation, so FIN-01 fires on
NONE of the legacy scenarios and ``EXPECTED_SNAPSHOT`` / ``M1_LEGACY_BASELINE``
stay byte-identical. The blank finding (target missing) is reached ONLY when
actual generation exists.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Attainment-percentage tier thresholds (strict ``<``).
FIN01_ATTAINMENT_WATCH = 95.0  # below this (any single year) → WATCH
FIN01_ATTAINMENT_INDICATIVE = 90.0  # two consecutive years below this → INDICATIVE
FIN01_ATTAINMENT_CONFIRMED = 85.0  # two consecutive years below this → CONFIRMED

# COD month after which the commissioning calendar year is treated as a partial
# (excluded) year. month > 5 (i.e. June onward) → first year excluded.
FIN01_COD_PARTIAL_YEAR_MONTH = 5


# ─── Pure, DB-free helpers ────────────────────────────────────────────────────


def compute_attainment_pct(
    actual_gwh: Optional[float],
    p50_target_gwh: Optional[float],
) -> Optional[float]:
    """Attainment percentage = ``actual_gwh / p50_target_gwh * 100``.

    Returns ``None`` when ``p50_target_gwh`` is missing or 0 (no sourced target →
    no meaningful ratio; the detector turns this into a blank finding) or when
    ``actual_gwh`` is missing.

    Example: ``compute_attainment_pct(95, 100) == 95.0``.
    """
    if actual_gwh is None:
        return None
    if p50_target_gwh is None or p50_target_gwh == 0:
        return None
    return actual_gwh / p50_target_gwh * 100


def classify_attainment_severity(
    attainment: Optional[float],
    prior_attainment: Optional[float],
) -> Optional[Severity]:
    """Classify FIN-01 severity from the latest + prior years' attainment.

    A single below-target year caps at WATCH; escalation to INDICATIVE / CONFIRMED
    needs two consecutive years. Tiers (strict ``<`` against 95 / 90 / 85):

        prior_attainment is None (single year):
            attainment >= 95 → None;  attainment < 95 → WATCH

        prior_attainment present (two years):
            attainment < 85 AND prior < 85   → CONFIRMED
            attainment < 85 AND prior >= 85  → INDICATIVE
            attainment < 90 AND prior < 90   → INDICATIVE
            attainment < 95                  → WATCH
            otherwise                        → None

    Returns ``None`` when ``attainment`` is missing. Boundaries (locked by tests):
        (80, None) → WATCH, (82, 84) → CONFIRMED, (82, 91) → INDICATIVE,
        (95, None) → None.
    """
    if attainment is None:
        return None

    # Single year of data: a below-target year always caps at WATCH.
    if prior_attainment is None:
        if attainment < FIN01_ATTAINMENT_WATCH:
            return Severity.WATCH
        return None

    # Two years of data — escalation possible.
    if attainment < FIN01_ATTAINMENT_CONFIRMED and prior_attainment < FIN01_ATTAINMENT_CONFIRMED:
        return Severity.CONFIRMED
    if attainment < FIN01_ATTAINMENT_CONFIRMED:
        # Severe single-year drop (this year <85) with a healthy prior year.
        return Severity.INDICATIVE
    if attainment < FIN01_ATTAINMENT_INDICATIVE and prior_attainment < FIN01_ATTAINMENT_INDICATIVE:
        return Severity.INDICATIVE
    if attainment < FIN01_ATTAINMENT_WATCH:
        return Severity.WATCH
    return None


def is_cod_year_excluded(
    commercial_operational_date: Optional[date],
    year: int,
) -> bool:
    """True when ``year`` is the windfarm's first (partial) operating year.

    Excludes only the COMMISSIONING calendar year (``COD.year == year``) and only
    when COD landed after May (``COD.month > 5``) — late-in-year commissioning
    means a partial year whose generation would be misleadingly low against a
    full-year P50 target. Any later year, or an early-in-year COD, is kept.

    Returns ``False`` when COD is unknown (cannot prove the year is partial).
    Examples: (2023-07-15, 2023) → True; (2023-03-01, 2023) → False;
    (2023-07-15, 2024) → False.
    """
    if commercial_operational_date is None:
        return False
    if commercial_operational_date.year != year:
        return False
    return commercial_operational_date.month > FIN01_COD_PARTIAL_YEAR_MONTH


# ─── Detector entrypoint ──────────────────────────────────────────────────────


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """FIN-01: P50 generation attainment (sourced target only).

    Returns ``None`` (no finding) when there is **no actual annual generation
    data** (``ctx.load_annual_generation_gwh()`` is ``None`` / empty). This is the
    snapshot-safety path: the M1 legacy scenarios inject no generation, so FIN-01
    fires on none of them.

    When generation EXISTS but there is **no sourced P50 target**, returns
    ``None`` (no finding): attainment can't be computed without a target, so it
    isn't an actionable opportunity. (This previously emitted a blank ``WATCH``
    placeholder, which flooded the board on low-P50-coverage fleets; the
    missing-target gap is a data-coverage concern tracked elsewhere.)

    Otherwise computes the latest year's attainment (and the prior year's, when
    available), excluding the COD partial year, and classifies severity per
    ``classify_attainment_severity``.
    """
    annual = await ctx.load_annual_generation_gwh()
    if not annual:
        # No actual generation → nothing to assess. Snapshot-safety path.
        return None

    cod = _commercial_operational_date(ctx)

    # Keep only full operating years (drop the COD partial year), newest last.
    years = sorted(y for y in annual.keys() if not is_cod_year_excluded(cod, _as_year(y)))
    if not years:
        return None

    target = await ctx.load_p50_target()
    if target is None or target == 0:
        # No sourced P50 target → attainment cannot be computed, so there is no
        # actionable finding. (Previously emitted a blank WATCH placeholder with
        # missing_slots=["p50_target_gwh"]; on a fleet with low P50 coverage that
        # flooded the board with ~1.2k no-target rows that buried the real
        # findings. The missing-target coverage gap is a data-coverage concern,
        # not an opportunity, so it is no longer surfaced here.)
        return None

    latest_year = years[-1]
    actual_gwh = float(annual[latest_year])
    attainment = compute_attainment_pct(actual_gwh, target)

    prior_attainment: Optional[float] = None
    if len(years) >= 2:
        prior_year = years[-2]
        prior_actual = annual.get(prior_year)
        if prior_actual is not None:
            prior_attainment = compute_attainment_pct(float(prior_actual), target)

    severity = classify_attainment_severity(attainment, prior_attainment)
    if severity is None:
        return None

    data_slots = {
        "attainment_pct": round(attainment, 2) if attainment is not None else None,
        "actual_gwh": round(actual_gwh, 3),
        "p50_target_gwh": round(float(target), 3),
        "prior_attainment_pct": (
            round(prior_attainment, 2) if prior_attainment is not None else None
        ),
        "attainment_year": _as_year(latest_year),
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }

    return DetectorResult(
        schema_code=SchemaCode.FIN_01,
        severity=severity,
        branch=None,
        data_slots=data_slots,
    )


def _commercial_operational_date(ctx: DetectionContext) -> Optional[date]:
    """Read the windfarm's COD, None-safe (bare-int windfarm / detached ORM)."""
    try:
        cod = getattr(ctx.windfarm, "commercial_operational_date", None)
    except Exception:
        return None
    if isinstance(cod, datetime):
        return cod.date()
    if isinstance(cod, date):
        return cod
    return None


def _as_year(key: object) -> int:
    """Normalise a generation-year key (``int`` or ``"YYYY"`` string) to an int."""
    if isinstance(key, int):
        return key
    return int(str(key)[:4])


__all__ = [
    "FIN01_ATTAINMENT_WATCH",
    "FIN01_ATTAINMENT_INDICATIVE",
    "FIN01_ATTAINMENT_CONFIRMED",
    "compute_attainment_pct",
    "classify_attainment_severity",
    "is_cod_year_excluded",
    "detect",
]
