"""MKT-01 · Low capture rates (contracting) — corrected detector (issue #94).

History
=======
* #93 migrated the legacy ``_detect_mkt01`` assembly **verbatim** (Approach A:
  the pure helpers were re-exported from ``OpportunityDetectionService`` and
  reproduced the current behaviour, INCLUDING the two known bugs).
* #94 (this module) lands the M2 fixes and now **owns** the corrected pure
  helpers locally:

  - the never-fires zone-average bug is fixed at the data layer
    (``price_analytics_service.compare_capture_rates_by_bidzone`` now returns
    ``zone_average_capture_rate``), so ``ctx.load_capture_rate()`` resolves and
    this detector can fire; and
  - the gap-severity thresholds are recalibrated to the spec
    (``>10 / >6 / >3 pp`` → CONFIRMED / INDICATIVE / WATCH, up from the legacy
    ``>10 / >5 / >2``); and
  - curtailment suppression is wired in: a farm whose capture loss is driven by
    grid curtailment (>15%) is suppressed because the loss is grid-driven, not a
    contracting problem.

The legacy ``OpportunityDetectionService.determine_mkt01_severity`` /
``select_mkt01_branch`` / ``check_mkt01_suppression`` staticmethods are left
UNTOUCHED — the 57 legacy unit tests in ``tests/test_opportunity_detection.py``
still import and assert against them. The corrected logic lives here.

The MKT-03 reclassification short-circuit (``ci > MKT03_CI_CONFIRMED`` → ``None``)
that #94 carried forward inline has been REMOVED by #111: the cross-schema
redirect now lives in the ``reclassify_capture_to_cannibalisation`` post-pass in
``registry.py``, so MKT-01's ``detect`` always fires on its own capture-gap
signal and the registry's Phase-2 pass mutes it (→ ``SUPPRESSED`` + redirect
reason) when MKT-03 is the dominant (CI-driven) explanation.

Data is obtained exclusively through ``DetectionContext`` accessors
(``load_capture_rate`` / ``load_cannibalisation_index`` / ``load_ppa_info`` /
``load_curtailment_pct``). The detector returns a ``DetectorResult`` (or
``None``); persistence + ``triggered_by_id`` wiring is the orchestrator's job
(``run_for_windfarm``).
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_detection_service import LONG_PPA_YEARS, OpportunityDetectionService
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# ─── Recalibrated thresholds (issue #94 — spec 15 May 2026) ──────────────────
# Strictly-greater-than gap-in-percentage-points → severity tier.
MKT01_GAP_CONFIRMED_PP = 10.0
MKT01_GAP_INDICATIVE_PP = 6.0  # was 5.0 (legacy)
MKT01_GAP_WATCH_PP = 3.0  # was 2.0 (legacy)

# Curtailment above this % means the capture loss is grid-driven, not a
# contracting problem → suppress MKT-01 (revives the dead legacy
# ``CURTAILMENT_SUPPRESSION_PCT=15.0`` constant as a live suppression rule).
CURTAILMENT_SUPPRESSION_PCT = 15.0

_CURTAILMENT_SUPPRESSION_REASON = (
    "MKT-01 suppressed: curtailment >15% — capture loss is grid-driven"
)


def classify_capture_gap_severity(gap_pp: float) -> Optional[Severity]:
    """Map a capture-rate gap (percentage points) to a severity tier.

    Recalibrated thresholds (issue #94), strictly-greater-than::

        gap > 10  → CONFIRMED
        gap > 6   → INDICATIVE
        gap > 3   → WATCH
        else      → None  (no finding)

    ``gap_pp`` is positive when the windfarm under-captures vs the zone average.
    """
    if gap_pp > MKT01_GAP_CONFIRMED_PP:
        return Severity.CONFIRMED
    if gap_pp > MKT01_GAP_INDICATIVE_PP:
        return Severity.INDICATIVE
    if gap_pp > MKT01_GAP_WATCH_PP:
        return Severity.WATCH
    return None


# Branch selection is unchanged from the legacy logic; re-export for a
# self-contained module surface.
select_capture_branch = OpportunityDetectionService.select_mkt01_branch


def check_capture_suppression(ppa_info: dict, curtailment_pct: Optional[float]) -> Optional[str]:
    """Return a suppression reason for MKT-01, or ``None`` to not suppress.

    Two independent suppression conditions (issue #94):

    1. **Grid-driven capture loss** — ``curtailment_pct > 15.0``: the capture
       shortfall is caused by grid curtailment, not by contracting, so MKT-01 is
       not actionable. (``curtailment_pct=None`` — data unavailable — never
       triggers this.)
    2. **Locked market exposure** — a fixed-price PPA with ≥5yr duration that is
       currently active locks the farm out of market exposure, so the gap is not
       actionable. This preserves the legacy
       ``check_mkt01_suppression`` behaviour.
    """
    if curtailment_pct is not None and curtailment_pct > CURTAILMENT_SUPPRESSION_PCT:
        return _CURTAILMENT_SUPPRESSION_REASON

    if (
        ppa_info.get("contract_type") == "fixed_price"
        and ppa_info.get("ppa_duration_years", 0) >= LONG_PPA_YEARS
        and ppa_info.get("ppa_status") == "active"
    ):
        return "Fixed-price PPA with >5yr duration — market exposure locked"

    return None


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """MKT-01: Low capture rates — contracting.

    Returns ``None`` when there is no finding: no zone benchmark
    (``load_capture_rate`` is ``None``), gap below the WATCH threshold, or
    suppressed (curtailment >15% or locked fixed-price PPA). The MKT-03
    reclassification short-circuit was REMOVED by #111 — when cannibalisation is
    the dominant driver, MKT-01 still fires here and is muted to ``SUPPRESSED`` by
    the ``reclassify_capture_to_cannibalisation`` post-pass in ``registry.py``.
    """
    gap_data = await ctx.load_capture_rate()
    if gap_data is None:
        return None

    gap_pp = gap_data["gap_pp"]
    severity = classify_capture_gap_severity(gap_pp)
    if severity is None:
        return None

    ppa_info = await ctx.load_ppa_info()
    curtailment_pct = await ctx.load_curtailment_pct()

    # Suppression (curtailment >15% OR locked long fixed-price PPA)
    suppression = check_capture_suppression(ppa_info, curtailment_pct)
    if suppression:
        return None

    # CI for branch selection
    ci_data = await ctx.load_cannibalisation_index()
    ci = ci_data.get("ci_latest") if ci_data else None

    data_slots = {
        "capture_rate": gap_data.get("capture_rate"),
        "zone_avg_capture": gap_data.get("zone_avg"),
        "gap_pp": round(gap_pp, 2),
        "price_zone": gap_data.get("bidzone_code"),
        "ppa_status": ppa_info.get("ppa_status"),
        "cannibalisation_index": round(ci, 4) if ci else None,
        "ppa_expiry_date": str(ppa_info.get("ppa_end_date"))
        if ppa_info.get("ppa_end_date")
        else None,
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }
    missing = []
    if ci is None:
        missing.append("cannibalisation_index")
    missing.extend(
        ["pcc_slope", "peer_capture_p50", "revenue_impact_eur", "high_wind_capture_delta"]
    )
    if not ppa_info.get("ppa_end_date"):
        missing.append("ppa_expiry_date")

    # NOTE (#111): the legacy inline reclassification short-circuit
    # (``if ci and ci > MKT03_CI_CONFIRMED: return None``) has been REMOVED. MKT-01
    # now always fires on its own capture-gap signal; the cross-schema redirect
    # to MKT-03 when cannibalisation is the dominant driver is handled by the
    # ``reclassify_capture_to_cannibalisation`` post-pass in ``registry.py`` (Phase
    # 2 of ``run_for_windfarm``), so ALL cross-schema logic lives in one auditable
    # place over the full result set rather than buried inside this detector.

    # Branch selection
    branch = select_capture_branch(ci, ppa_info)

    return DetectorResult(
        schema_code=SchemaCode.MKT_01,
        severity=severity,
        branch=branch,
        data_slots=data_slots,
        missing_slots=missing,
    )
