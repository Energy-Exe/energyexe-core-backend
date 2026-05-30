"""OPS-08 · Structural export constraint — M3 new detector (issue #103).

Surfaces windfarms with a Module 1b *structural-constraint* flag: a sustained
period (>= 2 weeks) where output is systematically truncated relative to wind
conditions — the signature of a partial export-infrastructure failure (a single
cable down on a multi-cable farm, half-BMU offline, a sustained curtailment
campaign). These flags live in ``structural_constraint_flags`` and are surfaced
via ``ctx.load_structural_constraint_flags()``.

Analyst-review-driven severity (NOT raw magnitude)
==================================================
Unlike the other operational detectors, OPS-08 severity is driven by the
**analyst ``review_status``** of the flag plus the constraint's depth/duration —
not by a continuously-recomputed metric. A constraint only reaches CONFIRMED once
an analyst has confirmed it AND it is both deep and long-lived:

    review_status == "dismissed"                          → None   (suppressed)
    mean_q90_ratio >= 0.85  (constraint too shallow)      → None   (suppressed)
    "confirmed" AND duration >= 672h AND q90 < 0.65       → CONFIRMED
    "confirmed" AND duration >= 336h (below the dual bar) → INDICATIVE
    "confirmed" but duration < 336h                       → WATCH
    "pending_review" (auto-detected, not yet reviewed)    → WATCH

Threshold rationale (see ``docs/pipeline/module-1b-structural-constraint-detection.md``):
    * 672 h = 4 weeks — the CONFIRMED "long-lived" bar.
    * 336 h = 2 weeks — Module 1b's *minimum* detectable run, and the INDICATIVE
      duration floor. Every persisted flag is >= 336 h by construction, so the
      ``duration < 336`` → WATCH branch only fires on degenerate / injected input.
    * q90 < 0.65 = a deep truncation (output well below the P10 capability curve);
      q90 >= 0.85 = too shallow to be a material constraint → suppressed.

Comparisons are inclusive ``>=`` on the duration bars and strict ``<`` on the
q90 depth bar (so q90 == 0.65 misses CONFIRMED → INDICATIVE), matching the spec
and the locked test cases. The detector is pure: it reads only
``ctx.load_structural_constraint_flags()`` and returns a ``DetectorResult`` (or
``None`` when there is no flag row, or the flag is suppressed). Persistence is the
orchestrator's job.
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Duration bars (hours). 672 = 4 weeks (CONFIRMED); 336 = 2 weeks (INDICATIVE
# floor / Module 1b minimum detectable run).
OPS08_DURATION_CONFIRMED = 672
OPS08_DURATION_INDICATIVE = 336

# q90-ratio depth bars. Below 0.65 = deep truncation (CONFIRMED-eligible);
# at/above 0.85 = too shallow to be a material constraint → suppressed.
OPS08_Q90_CONFIRMED = 0.65
OPS08_Q90_SUPPRESS = 0.85


# ─── Pure, DB-free classifier ─────────────────────────────────────────────────


def classify_constraint_severity(
    review_status: str,
    duration_hours: Optional[int],
    mean_q90_ratio: Optional[float],
) -> Optional[Severity]:
    """Classify OPS-08 severity from the flag's review status + depth/duration.

    Args:
        review_status: analyst workflow state — ``"confirmed"`` /
            ``"pending_review"`` / ``"dismissed"``.
        duration_hours: length of the constrained run (hours). ``None`` is treated
            as 0 (cannot clear any duration bar).
        mean_q90_ratio: mean output / P10-capability ratio over the run; lower =
            deeper truncation. ``None`` is treated as "depth unknown" (does NOT
            trigger the shallow-suppression bar, and cannot clear the CONFIRMED
            depth bar → at most INDICATIVE).

    Returns:
        The :class:`Severity` tier, or ``None`` when the flag should not fire
        (``dismissed`` or a too-shallow ``mean_q90_ratio >= 0.85``).

    Tiers (see module docstring):
        dismissed                                   → None
        mean_q90_ratio >= 0.85                      → None (too shallow)
        confirmed AND dur >= 672 AND q90 < 0.65     → CONFIRMED
        confirmed AND dur >= 336                    → INDICATIVE
        confirmed (dur < 336)                       → WATCH
        pending_review                              → WATCH
    """
    if review_status == "dismissed":
        return None

    # Constraint too shallow to be material — suppress regardless of status.
    if mean_q90_ratio is not None and mean_q90_ratio >= OPS08_Q90_SUPPRESS:
        return None

    duration = duration_hours or 0

    if review_status == "confirmed":
        if (
            duration >= OPS08_DURATION_CONFIRMED
            and mean_q90_ratio is not None
            and mean_q90_ratio < OPS08_Q90_CONFIRMED
        ):
            return Severity.CONFIRMED
        if duration >= OPS08_DURATION_INDICATIVE:
            return Severity.INDICATIVE
        # Confirmed but below the 2-week INDICATIVE floor (degenerate / injected).
        return Severity.WATCH

    if review_status == "pending_review":
        return Severity.WATCH

    # Unknown / unexpected status → no finding.
    return None


# ─── Detector entrypoint ──────────────────────────────────────────────────────


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-08: structural export constraint (analyst-review driven).

    Loads the windfarm's most-relevant structural-constraint flag and classifies
    severity from its ``review_status`` + ``duration_hours`` + ``mean_q90_ratio``.

    Returns ``None`` (no finding) when:
        * the windfarm has no structural-constraint flag
          (``ctx.load_structural_constraint_flags()`` is ``None``), or
        * the flag is suppressed (``dismissed`` / ``mean_q90_ratio >= 0.85``).

    Otherwise emits a ``DetectorResult`` classified per
    ``classify_constraint_severity`` (CONFIRMED / INDICATIVE / WATCH).
    """
    flag = await ctx.load_structural_constraint_flags()
    if not flag:
        return None

    review_status = str(flag.get("review_status") or "")
    duration_hours = flag.get("duration_hours")
    mean_q90_ratio = flag.get("mean_q90_ratio")

    severity = classify_constraint_severity(review_status, duration_hours, mean_q90_ratio)
    if severity is None:
        return None

    data_slots = {
        "review_status": review_status,
        "duration_hours": duration_hours,
        "mean_q90_ratio": mean_q90_ratio,
        "mean_q50_ratio": flag.get("mean_q50_ratio"),
        "flag_trigger": flag.get("flag_trigger"),
        "period_start": flag.get("period_start"),
        "period_end": flag.get("period_end"),
    }

    # Graceful-degradation: a confirmed root cause (which cable / which BMU) and a
    # remediation plan would enrich the finding, but Module 1b does not yet track
    # them on the flag row.
    missing_slots = [
        "constraint_root_cause",
        "remediation_plan",
    ]

    return DetectorResult(
        schema_code=SchemaCode.OPS_08,
        severity=severity,
        branch=None,
        data_slots=data_slots,
        missing_slots=missing_slots,
    )


__all__ = [
    "OPS08_DURATION_CONFIRMED",
    "OPS08_DURATION_INDICATIVE",
    "OPS08_Q90_CONFIRMED",
    "OPS08_Q90_SUPPRESS",
    "classify_constraint_severity",
    "detect",
]
