"""MKT-03 · High cannibalisation rates — recalibrated detector (issue #98, M2).

M1 (#93) migrated this detector verbatim by re-using the legacy
``OpportunityDetectionService`` severity/branch/suppression helpers (Approach A).
M2 (#98) applies the **11-May recalibration** with *local* helpers defined in
this module — the legacy service and its constants are left untouched:

* **WATCH entry raised to CI ≥ 1.08** (legacy ``MKT03_CI_WATCH`` was 1.05).
* **Trend downgrade**: a CONFIRMED finding is downgraded one tier (→ INDICATIVE)
  when the YoY CI trend is sustained-improving, i.e. ``ci_trend_yoy <= -0.08``.
* **Outlier exclusion**: any *prior* year whose CI > 2.0 is dropped before the
  trend is computed (a single anomalous low-capture year must not dominate the
  YoY delta).

``classify_cannibalisation_severity`` now takes a third argument,
``penetration_rising`` — CONFIRMED requires CI ≥ 1.20 **and** ≥ 2 sustained
years **and** rising zone penetration. The data layer exposes no
``zone_renewable_penetration_pct`` (it stays in ``missing_slots``), so the
detector uses the worsening CI trend as the proxy: ``penetration_rising`` is
``True`` when the YoY CI trend is positive (rising CI ⇒ falling capture ⇒
penetration eating into the price profile).

NOTE on ``years_sustained``: ``ctx.load_cannibalisation_index()`` computes its
``years_above_threshold`` against the *legacy* ``MKT03_CI_WATCH`` (1.05). Since
this module raises the WATCH entry to 1.08, the count is **recomputed locally**
from ``ci_by_year`` against the new local ``MKT03_CI_WATCH`` (1.08) so the
sustained-years gate is self-consistent with the recalibrated thresholds. The
legacy accessor/constant are intentionally not modified (the proper cross-schema
hook for penetration is #111).

MKT-03 is **independent of MKT-01** (registered between MKT-01 and MKT-02 in
``SCHEMA_REGISTRY`` with no entry in ``SCHEMA_DEPENDENCIES``). The existing
MKT-01 reclassification short-circuit hook is left as-is (proper hook: #111).

Data is obtained through ``ctx.load_cannibalisation_index()`` (CI = 1/capture
rate per year) and ``ctx.load_ppa_info()``. Returns a ``DetectorResult`` or
``None``; persistence is the orchestrator's job.
"""

from __future__ import annotations

from typing import Dict, Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_detection_service import OpportunityDetectionService
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# ─── Recalibrated thresholds (local — do NOT touch the legacy service) ────────
MKT03_CI_CONFIRMED = 1.20  # CONFIRMED also needs ≥2 sustained years + rising
MKT03_CI_INDICATIVE = 1.10
MKT03_CI_WATCH = 1.08  # raised from legacy 1.05
CI_TREND_DOWNGRADE = -0.08  # CONFIRMED → INDICATIVE when YoY trend ≤ this
CI_OUTLIER_EXCLUSION = 2.0  # prior years with CI > this are dropped from trend

# Branch / suppression are unchanged by the recalibration — re-use the legacy
# pure helpers (Approach A) so behaviour is identical to M1.
select_cannibalisation_branch = OpportunityDetectionService.select_mkt03_branch
check_cannibalisation_suppression = OpportunityDetectionService.check_mkt03_suppression


# ─── Recalibrated pure helpers (#98) ─────────────────────────────────────────
def classify_cannibalisation_severity(
    ci: float, years_sustained: int, penetration_rising: bool
) -> Optional[Severity]:
    """MKT-03 base severity from the cannibalisation index.

    * CI ≥ 1.20 **and** ``years_sustained`` ≥ 2 **and** ``penetration_rising``
      → ``CONFIRMED``.
    * CI ≥ 1.10 → ``INDICATIVE``.
    * CI ≥ 1.08 → ``WATCH``.
    * otherwise → ``None``.

    Boundaries: 1.08 → WATCH, 1.079 → None, 1.10 → INDICATIVE; CONFIRMED demands
    all three of (≥1.20, ≥2 sustained years, rising penetration).
    """
    if ci >= MKT03_CI_CONFIRMED and years_sustained >= 2 and penetration_rising:
        return Severity.CONFIRMED
    if ci >= MKT03_CI_INDICATIVE:
        return Severity.INDICATIVE
    if ci >= MKT03_CI_WATCH:
        return Severity.WATCH
    return None


def apply_ci_trend_downgrade(severity: Severity, ci_trend_yoy: Optional[float]) -> Severity:
    """Downgrade a CONFIRMED finding one tier when the CI trend is improving.

    A CONFIRMED severity becomes INDICATIVE when ``ci_trend_yoy <= -0.08``
    (sustained YoY improvement). ``-0.079`` (i.e. ``> -0.08``) leaves the
    severity unchanged, as does a ``None`` trend or any non-CONFIRMED severity.
    """
    if severity == Severity.CONFIRMED and ci_trend_yoy is not None:
        if ci_trend_yoy <= CI_TREND_DOWNGRADE:
            return Severity.INDICATIVE
    return severity


def compute_ci_trend(ci_by_year: Optional[Dict]) -> Optional[float]:
    """Mean YoY CI delta, excluding any *prior* year whose CI > 2.0.

    The latest year is always retained; only earlier (prior) years are eligible
    for the > 2.0 outlier exclusion. The trend is the mean of the year-over-year
    deltas across the remaining, chronologically-ordered series. ``None`` when
    fewer than two years survive.

    Verifiable: ``{2023: 2.5, 2024: 1.1, 2025: 1.2}`` → 2023 (prior, CI > 2.0) is
    excluded → trend over ``{2024: 1.1, 2025: 1.2}`` = ``+0.1``.
    """
    if not ci_by_year:
        return None

    # Sort chronologically (keys may be ints or year-strings like "2024").
    sorted_years = sorted(ci_by_year.keys(), key=lambda y: str(y))
    if len(sorted_years) < 2:
        return None

    latest_year = sorted_years[-1]
    kept = [y for y in sorted_years if y == latest_year or ci_by_year[y] <= CI_OUTLIER_EXCLUSION]
    if len(kept) < 2:
        return None

    deltas = [ci_by_year[kept[i]] - ci_by_year[kept[i - 1]] for i in range(1, len(kept))]
    return round(sum(deltas) / len(deltas), 4)


def _count_years_sustained(ci_by_year: Optional[Dict]) -> int:
    """Years with CI ≥ the recalibrated WATCH entry (1.08), computed locally.

    ``ctx.load_cannibalisation_index()`` reports ``years_above_threshold`` against
    the *legacy* WATCH (1.05); recompute here so the sustained-years gate matches
    this module's raised WATCH entry.
    """
    if not ci_by_year:
        return 0
    return sum(1 for v in ci_by_year.values() if v >= MKT03_CI_WATCH)


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """MKT-03: High cannibalisation rates (recalibrated, #98).

    Returns ``None`` when no row should be produced.
    """
    ci_data = await ctx.load_cannibalisation_index()
    if not ci_data or ci_data.get("ci_latest") is None:
        return None

    ci = ci_data["ci_latest"]
    ci_by_year = ci_data.get("ci_by_year")

    # Sustained-years recomputed against the recalibrated WATCH (1.08).
    years_sustained = _count_years_sustained(ci_by_year)

    # Penetration proxy: rising CI (worsening capture) stands in for the missing
    # zone_renewable_penetration_pct slot. Use the recomputed (outlier-excluded)
    # YoY trend; positive ⇒ rising.
    ci_trend_yoy = compute_ci_trend(ci_by_year)
    penetration_rising = ci_trend_yoy is not None and ci_trend_yoy > 0

    severity = classify_cannibalisation_severity(ci, years_sustained, penetration_rising)
    if severity is None:
        return None

    ppa_info = await ctx.load_ppa_info()

    # Suppression: long-dated fixed PPA (unchanged).
    if check_cannibalisation_suppression(ppa_info):
        return None

    # Trend downgrade: sustained YoY improvement (≤ -0.08) drops CONFIRMED a tier.
    severity = apply_ci_trend_downgrade(severity, ci_trend_yoy)

    data_slots = {
        "cannibalisation_index": round(ci, 4),
        "price_zone": ci_data.get("bidzone_code"),
        "ci_values_by_year": ci_by_year,
        "ci_trend_yoy": ci_trend_yoy,
        "ppa_status": ppa_info.get("ppa_status"),
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }
    missing = [
        "zone_renewable_penetration_pct",
        "peer_zone_ci",
        "portfolio_zone_correlation",
        "revenue_impact_eur",
        "alternative_zone_assets",
    ]

    # Branch selection still reads the raw worsening-trend signal from ci_data
    # (legacy ``ci_trend`` first-to-last delta), unchanged.
    branch = select_cannibalisation_branch(ci_data)

    return DetectorResult(
        schema_code=SchemaCode.MKT_03,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
