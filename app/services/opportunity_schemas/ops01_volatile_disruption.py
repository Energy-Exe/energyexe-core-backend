"""OPS-01 · Volatile disruption periods — M2 bug-fix (issue #95).

Implements the corrected OPS-01 behaviour per the 11-May-2026 spec, fixing the
two latent bugs the M1 verbatim migration deliberately preserved:

* **Force-downgrade removed.** Previously a CONFIRMED severity was force-downgraded
  to INDICATIVE whenever ``wind_resource_index`` was missing (which is always),
  so OPS-01 could never reach CONFIRMED. Graceful degradation should FLAG the gap,
  not cap the severity. ``"wind_resource_index"`` therefore stays in
  ``missing_slots`` but no longer touches severity.
* **Spec thresholds applied.** ODI below threshold in 8+ / 4+ / 2+ months →
  CONFIRMED / INDICATIVE / WATCH. Two consecutive low months escalate a WATCH to
  INDICATIVE. An average ODI > 97% soft-caps the result to WATCH (the shortfall is
  marginal).

Approach for pure helpers: the legacy static functions
(``OpportunityDetectionService.determine_ops01_severity`` etc.) are **no longer
reused** — they back the frozen ``M1_LEGACY_BASELINE`` characterization test and
must stay byte-for-byte unchanged. Instead this module defines its own corrected,
pure, importable, DB-free helpers (``classify_disruption_severity`` /
``select_disruption_branch`` / ``check_disruption_suppression``) and ``detect``
calls those.

Data is obtained exclusively through ``DetectionContext`` accessors
(``load_monthly_performance`` / ``load_ppa_info``). The detector returns a
``DetectorResult`` (or ``None``); persistence + ``triggered_by_id`` wiring is the
orchestrator's job (``run_for_windfarm``).
"""

from __future__ import annotations

from typing import List, Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_detection_service import LONG_PPA_YEARS, ODI_THRESHOLD_PCT
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Average ODI above which a finding is soft-capped to WATCH: the months-below
# count may be high but the shortfall is marginal, so it is not actionable beyond
# a watch flag. Spec (11-May-2026): "ODI ≥ 97% soft-caps to WATCH".
ODI_SOFT_CAP_PCT = 97.0


# ─── Pure, DB-free helpers (corrected for #95; NOT the legacy staticmethods) ──


def classify_disruption_severity(
    months_below: int,
    max_consecutive: int = 0,
    avg_odi_pct: Optional[float] = None,
) -> Optional[Severity]:
    """Classify OPS-01 severity from the disruption-month statistics.

    Spec thresholds (11-May-2026):
        * ``months_below >= 8``  → CONFIRMED
        * ``months_below >= 4``  → INDICATIVE
        * ``months_below >= 2``  → WATCH
        * otherwise              → ``None`` (no finding; 0 or 1 low month)

    Consecutive escalation:
        Two or more *consecutive* low months (``max_consecutive >= 2``) escalate a
        WATCH up to INDICATIVE — sustained back-to-back disruption is more
        actionable than the same count spread across the period. (Only the WATCH
        tier escalates; INDICATIVE / CONFIRMED are already at or above that.)

    Soft cap:
        ``avg_odi_pct > 97.0`` caps the result at WATCH — a high month-count whose
        average ODI is barely below the threshold is a marginal, watch-only signal.

    Args:
        months_below: count of months with ODI below the threshold.
        max_consecutive: longest run of consecutive low months.
        avg_odi_pct: mean ODI (availability) % across all observed months.

    Returns:
        The ``Severity`` tier, or ``None`` when there is no finding.
    """
    if months_below >= 8:
        severity: Optional[Severity] = Severity.CONFIRMED
    elif months_below >= 4:
        severity = Severity.INDICATIVE
    elif months_below >= 2:
        severity = Severity.WATCH
    else:
        return None

    # Consecutive escalation: 2+ back-to-back low months lift a WATCH to INDICATIVE.
    if severity == Severity.WATCH and max_consecutive >= 2:
        severity = Severity.INDICATIVE

    # Soft cap: a marginal average shortfall caps the finding at WATCH.
    if avg_odi_pct is not None and avg_odi_pct > ODI_SOFT_CAP_PCT:
        if severity in (Severity.CONFIRMED, Severity.INDICATIVE):
            severity = Severity.WATCH

    return severity


def select_disruption_branch(
    low_months: List[dict], years_affected: int, has_spot_exposure: bool
) -> str:
    """Select the OPS-01 root-cause branch.

    Identical branch policy to the legacy detector (re-implemented locally so this
    module owns its behaviour):
        * Branch C — exposure-amplified: spot exposure AND 2+ low months.
        * Branch B — structural / recurring: disruption spans 2+ years.
        * Branch A — event-driven: otherwise.
    """
    if has_spot_exposure and len(low_months) >= 2:
        return "C"
    if years_affected >= 2:
        return "B"
    return "A"


def check_disruption_suppression(ppa_info: dict) -> Optional[str]:
    """Check OPS-01 suppression conditions.

    Spec: a fixed-price, long-dated active PPA insulates the asset from the
    revenue impact of volatile disruption, so the finding is suppressed. Returns a
    human-readable reason when suppression applies, else ``None``.
    """
    if (
        ppa_info.get("contract_type") == "fixed_price"
        and (ppa_info.get("ppa_duration_years") or 0) >= LONG_PPA_YEARS
        and ppa_info.get("ppa_status") == "active"
    ):
        return "Fixed-price PPA with >=5yr duration — revenue insulated from disruption"
    return None


def _max_consecutive_low_months(low_month_keys: List[str]) -> int:
    """Longest run of consecutive calendar months among the low-ODI months.

    ``low_month_keys`` are ``"YYYY-MM"`` strings (in any order). Months are
    consecutive when their absolute month index (``year*12 + month``) differs by
    exactly 1.
    """
    if not low_month_keys:
        return 0

    indices = sorted(int(k[:4]) * 12 + int(k[5:7]) for k in low_month_keys)
    longest = 1
    run = 1
    for prev, cur in zip(indices, indices[1:]):
        if cur - prev == 1:
            run += 1
            longest = max(longest, run)
        else:
            run = 1
    return longest


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-01: Volatile disruption periods (#95 corrected behaviour).

    Returns ``None`` when there is no finding (no monthly data, fewer than 2 low
    months, or suppressed by a fixed-price long-dated PPA).
    """
    monthly = await ctx.load_monthly_performance()
    if not monthly:
        return None

    ppa_info = await ctx.load_ppa_info()

    low_months = [m for m in monthly if m["availability_pct"] < ODI_THRESHOLD_PCT]
    avg_odi_pct = round(sum(m["availability_pct"] for m in monthly) / len(monthly), 2)
    max_consecutive = _max_consecutive_low_months([m["month"] for m in low_months])

    severity = classify_disruption_severity(
        len(low_months),
        max_consecutive=max_consecutive,
        avg_odi_pct=avg_odi_pct,
    )
    if severity is None:
        return None

    # Gather data slots
    data_slots = {
        "odi_pct": avg_odi_pct,
        "odi_months_below_threshold": len(low_months),
        "odi_threshold": ODI_THRESHOLD_PCT,
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
        "disruption_month_list": [m["month"] for m in low_months],
        "ppa_status": ppa_info.get("ppa_status"),
    }
    missing = []
    if not ppa_info.get("ppa_status"):
        missing.append("ppa_status")
    # wind_resource_index is FLAGGED as a data gap (graceful degradation) but, per
    # #95, no longer caps severity — the force-downgrade has been removed.
    missing.extend(["peer_odi_p50", "maintenance_schedule", "wind_resource_index"])

    # Branch selection
    years_affected = len(set(m["month"][:4] for m in low_months))
    has_spot = ppa_info.get("contract_type") in (None, "merchant", "indexed")
    branch = select_disruption_branch(low_months, years_affected, has_spot)

    # Suppression: fixed-price long-dated active PPA insulates revenue.
    if check_disruption_suppression(ppa_info):
        return None

    return DetectorResult(
        schema_code=SchemaCode.OPS_01,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
