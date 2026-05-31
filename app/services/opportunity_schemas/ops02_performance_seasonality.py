"""OPS-02 · Performance seasonality — M2 HODI+SSR rewrite (issue #96).

Replaces the M1 verbatim detector entirely. The legacy detector had two latent
defects the M1 migration deliberately preserved:

* **Structurally-impossible firing condition.** It only fired when the low-wind
  season capacity factor *exceeded* the high-wind season CF
  (``low_wind_cf > high_wind_cf``). In Northern-hemisphere markets (NO/UK) the
  high-wind season (Oct–Mar) always out-produces summer, so the inversion is
  effectively impossible and the detector almost never fired.
* **WATCH force-cap.** Even when it did fire, a CONFIRMED/INDICATIVE severity was
  forced down to WATCH because ``wind_resource_index_monthly`` is always missing.

Both are removed here. The detector now measures **seasonal skew of
underperformance** directly from monthly ODI (``performance_summaries`` Module 3,
power-curve based, surfaced through ``ctx.load_monthly_performance()``):

    HODI (High-wind ODI)  = mean monthly ODI-underperformance over the high-wind
                            season months.
    SSR  (Seasonal Skew Ratio) = HODI(high-wind months) / HODI(all months).

A high HODI with SSR > 1 means underperformance is *concentrated* in the
high-wind season — when the asset earns most of its energy/revenue — which is the
actionable signal. A CONFIRMED result is no longer downgraded.

High-wind season gating (plan "High-wind season" default): gate on the
windfarm's country / bidzone. NO (``NOR`` / ``NO*``) and UK (``GBR`` /
``10YGB*``) use Oct–Mar (``{10,11,12,1,2,3}``). When the country/bidzone is
unknown (e.g. a bare-int windfarm in a DB-free test, or a missing relationship)
we **default to Oct–Mar** — the entire current fleet is Northern-hemisphere, so
Oct–Mar is the safe default; this is documented and revisited if a
Southern-hemisphere asset is ever onboarded.

Approach for pure helpers: the legacy static functions
(``OpportunityDetectionService.determine_ops02_severity`` etc.) are **no longer
reused** — they back the frozen ``M1_LEGACY_BASELINE`` characterization test and
must stay byte-for-byte unchanged. This module defines its own corrected, pure,
importable, DB-free helpers (``compute_hodi_ssr`` / ``classify_seasonality_severity``
/ ``select_seasonality_branch``) and ``detect`` calls those.

Data is obtained exclusively through ``DetectionContext.load_monthly_performance``
(monthly ODI). The detector returns a ``DetectorResult`` (or ``None``);
persistence + ``triggered_by_id`` wiring is the orchestrator's job.
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Default high-wind season for Northern-hemisphere markets (NO/UK): Oct–Mar.
NORTHERN_HIGH_WIND_MONTHS: Tuple[int, ...] = (10, 11, 12, 1, 2, 3)

# Minimum monthly observations required before OPS-02 can fire. The spec requires
# >=12 months of data (incl. a full high-wind season) so the seasonal skew is
# measured over a complete annual cycle rather than a partial window.
MIN_MONTHS_REQUIRED = 12

# Severity tiers (#96 spec): each tier requires BOTH a HODI floor (the magnitude
# of high-wind-season underperformance) AND an SSR floor (how concentrated that
# underperformance is in the high-wind season). Comparisons are inclusive (>=).
OPS02_HODI_CONFIRMED_PCT = 9.0
OPS02_SSR_CONFIRMED = 1.30
OPS02_HODI_INDICATIVE_PCT = 6.0
OPS02_SSR_INDICATIVE = 1.20
OPS02_HODI_WATCH_PCT = 4.0
OPS02_SSR_WATCH = 1.10


# ─── Pure, DB-free helpers (#96; NOT the legacy staticmethods) ────────────────


def compute_hodi_ssr(
    monthly_rows: List[dict],
    high_wind_months: Iterable[int],
) -> Tuple[Optional[float], Optional[float]]:
    """Compute HODI and SSR from monthly ODI-underperformance rows.

    Args:
        monthly_rows: monthly performance rows as returned by
            ``ctx.load_monthly_performance()``. Each row has a ``"month"`` key
            (``"YYYY-MM"``) and an ``"availability_pct"`` key, where the monthly
            ODI-underperformance is ``100 - availability_pct``.
        high_wind_months: the set of calendar month numbers (1–12) that make up
            the high-wind season.

    Returns:
        ``(hodi_pct, ssr)`` where

            * ``hodi_pct`` = mean ODI-underperformance over the high-wind-season
              months, and
            * ``ssr`` = ``hodi_pct / mean ODI-underperformance over ALL months``.

        Returns ``(None, None)`` when there are fewer than ``MIN_MONTHS_REQUIRED``
        monthly observations, when no high-wind-season month is present, or when
        the all-month mean underperformance is zero (SSR undefined).

    Examples:
        High-wind underperf ``[10,8,12,6,9,7]`` → HODI = 8.667.
        HODI_high = 9, HODI_all = 6 → SSR = 1.5.
    """
    if not monthly_rows or len(monthly_rows) < MIN_MONTHS_REQUIRED:
        return None, None

    high_set = set(high_wind_months)

    all_underperf: List[float] = []
    high_underperf: List[float] = []
    for row in monthly_rows:
        underperf = 100.0 - float(row["availability_pct"])
        all_underperf.append(underperf)
        if _month_number(row["month"]) in high_set:
            high_underperf.append(underperf)

    if not high_underperf or not all_underperf:
        return None, None

    hodi = sum(high_underperf) / len(high_underperf)
    hodi_all = sum(all_underperf) / len(all_underperf)
    if hodi_all == 0:
        return round(hodi, 3), None

    ssr = hodi / hodi_all
    return round(hodi, 3), round(ssr, 3)


def classify_seasonality_severity(
    hodi_pct: Optional[float],
    ssr: Optional[float],
) -> Optional[Severity]:
    """Classify OPS-02 severity from HODI + SSR (both conditions required, ``>=``).

    Spec tiers (#96):
        * ``hodi >= 9.0 AND ssr >= 1.30`` → CONFIRMED
        * ``hodi >= 6.0 AND ssr >= 1.20`` → INDICATIVE
        * ``hodi >= 4.0 AND ssr >= 1.10`` → WATCH
        * otherwise                        → ``None`` (no finding)

    Both the HODI floor and the SSR floor must be met for a tier; if a HODI floor
    is met but the matching SSR floor is not, the next lower tier is considered.

    Boundary behaviour (inclusive):
        * (9.0, 1.30)  → CONFIRMED
        * (8.99, 1.30) → INDICATIVE (HODI below the CONFIRMED floor)
        * (9.0, 1.29)  → INDICATIVE (SSR below the CONFIRMED floor)
        * (6.0, 1.20)  → INDICATIVE
        * (4.0, 1.10)  → WATCH
        * (3.99, 1.10) → None
    """
    if hodi_pct is None or ssr is None:
        return None

    if hodi_pct >= OPS02_HODI_CONFIRMED_PCT and ssr >= OPS02_SSR_CONFIRMED:
        return Severity.CONFIRMED
    if hodi_pct >= OPS02_HODI_INDICATIVE_PCT and ssr >= OPS02_SSR_INDICATIVE:
        return Severity.INDICATIVE
    if hodi_pct >= OPS02_HODI_WATCH_PCT and ssr >= OPS02_SSR_WATCH:
        return Severity.WATCH
    return None


def select_seasonality_branch(missing_slots: List[str]) -> str:
    """Select the OPS-02 root-cause branch.

    The branch identifies which corroborating data is available to explain the
    seasonal skew:
        * Branch A — turbine-scatter available (per-turbine spread points at a
          specific underperforming asset).
        * Branch B — maintenance calendar available (planned-outage timing
          explains the seasonal concentration).
        * Branch C — data-limited (neither corroborating slot present).
    """
    missing_set = set(missing_slots)
    if "turbine_scatter_spread" not in missing_set:
        return "A"
    if "maintenance_calendar" not in missing_set:
        return "B"
    return "C"


def _month_number(month_key: str) -> int:
    """Extract the calendar month (1–12) from a ``"YYYY-MM"`` month key."""
    return int(month_key[5:7])


def _resolve_high_wind_months(windfarm) -> Tuple[int, ...]:
    """Determine the high-wind season months for a windfarm.

    NO/UK → Oct–Mar. Default Oct–Mar when the country/bidzone is unknown (e.g. a
    bare-int windfarm in a DB-free test, or a missing relationship): the current
    fleet is entirely Northern-hemisphere so Oct–Mar is the safe default.

    The country ISO-3 code (``NOR`` / ``GBR``) and the bidzone code (``NO*`` /
    ``10YGB*``) are both checked so the gate works whether the windfarm is
    geo-tagged by country or only by bidzone. Any access failure (bare int,
    detached relationship) falls through to the Oct–Mar default.
    """
    try:
        country = getattr(windfarm, "country", None)
        country_code = (getattr(country, "code", None) or "").upper()
        bidzone = getattr(windfarm, "bidzone", None)
        bidzone_code = (getattr(bidzone, "code", None) or "").upper()
    except Exception:
        return NORTHERN_HIGH_WIND_MONTHS

    if country_code in ("NOR", "GBR"):
        return NORTHERN_HIGH_WIND_MONTHS
    if bidzone_code.startswith("NO") or bidzone_code.startswith("10YGB"):
        return NORTHERN_HIGH_WIND_MONTHS

    # Unknown geography: default to Northern-hemisphere Oct–Mar (documented).
    return NORTHERN_HIGH_WIND_MONTHS


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-02: Performance seasonality (#96 HODI+SSR rewrite).

    Returns ``None`` when there is no finding: fewer than 12 months of monthly
    ODI data, no high-wind-season month present, or a sub-threshold HODI/SSR.
    """
    monthly = await ctx.load_monthly_performance()
    if not monthly or len(monthly) < MIN_MONTHS_REQUIRED:
        return None

    high_wind_months = _resolve_high_wind_months(ctx.windfarm)
    hodi_pct, ssr = compute_hodi_ssr(monthly, high_wind_months)

    severity = classify_seasonality_severity(hodi_pct, ssr)
    if severity is None:
        return None

    high_set = set(high_wind_months)
    high_wind_month_keys = sorted(
        m["month"] for m in monthly if _month_number(m["month"]) in high_set
    )

    data_slots = {
        "hodi_pct": hodi_pct,
        "ssr": ssr,
        "high_wind_months": sorted(high_set),
        "months_observed": len(monthly),
        "high_wind_months_observed": high_wind_month_keys,
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }
    # Graceful-degradation data gaps: FLAGGED but, per #96, they no longer cap
    # severity (the old WATCH force-cap on a missing wind_resource_index_monthly
    # is removed).
    missing = [
        "wind_resource_index_monthly",
        "turbine_scatter_spread",
        "cannibalisation_index_seasonal",
        "maintenance_calendar",
        "revenue_uplift_potential_eur",
    ]

    branch = select_seasonality_branch(missing)

    return DetectorResult(
        schema_code=SchemaCode.OPS_02,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
