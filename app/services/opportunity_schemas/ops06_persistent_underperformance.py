"""OPS-06 · Persistent power-curve underperformance — M3 new detector (issue #101).

Reads the Module 4 empirical-P50 normalised index
(``performance_summaries.norm_index_p50``, surfaced monthly via
``ctx.load_norm_index_series()``) and flags windfarms that sit **below their own
empirical P50 reference for a sustained run of months**. ``norm_index_p50 == 100``
means "performing exactly at the P50 power-curve reference"; a value below 100 is
underperformance against that empirical baseline.

Signal
======
The detector finds the **longest run of consecutive months strictly below the
tier floor (80)** in the chronological series, then classifies severity from the
representative norm_index over that run together with the run length.

Severity tiers (spec thresholds; require ``consecutive_months >= 6``):

    norm_index < 80  →  CONFIRMED
    norm_index < 85  →  INDICATIVE
    norm_index < 90  →  WATCH
    norm_index >= 90 →  None (no finding)

A run shorter than ``MIN_CONSECUTIVE_MONTHS`` (6) is never a finding regardless of
how low the index goes. Comparisons are strict ``<`` on the index. Boundaries
(locked by tests):

    (79, 6) → CONFIRMED   (79, 5) → None   (84, 6) → INDICATIVE
    (89, 6) → WATCH       (90, 6) → None

Suppression / data gaps (any → ``detect`` returns ``None``, no row)
===================================================================
    * **0 / NULL index** is a data gap (no usable normalisation that month), NOT
      underperformance — those rows are dropped upstream in
      ``ctx.load_norm_index_series()``.
    * **< 2 years of data** — fewer than ``MIN_MONTHS_OF_DATA`` (24) usable monthly
      points; a sustained-underperformance call needs at least two years of
      normalised history.
    * **No run** reaching the 6-month minimum below the tier floor.

The detector is pure: it reads only ``ctx.load_norm_index_series()`` and the
detection period, and returns a ``DetectorResult`` (or ``None``). Persistence is
the orchestrator's job.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# The norm-index tier floor used to count a "below" run, and the per-tier ceilings
# (strict ``<``). CONFIRMED is the same value the run is counted against (80).
OPS06_INDEX_CONFIRMED = 80.0
OPS06_INDEX_INDICATIVE = 85.0
OPS06_INDEX_WATCH = 90.0

# Run / history requirements.
MIN_CONSECUTIVE_MONTHS = 6  # months below the tier floor required to fire at all
MIN_MONTHS_OF_DATA = 24  # < 2 years of usable monthly points → suppress


# ─── Pure, DB-free helpers ────────────────────────────────────────────────────


def count_consecutive_months_below(
    norm_index_series: Sequence[Optional[float]],
    threshold: float,
) -> int:
    """Longest run of consecutive months strictly below ``threshold``.

    ``norm_index_series`` is a chronological sequence of monthly norm-index values
    (``None`` entries break a run — they neither count as "below" nor extend it).
    Returns the length of the longest contiguous stretch where every value is
    ``< threshold``; ``0`` when no value is below.

    Example: ``count_consecutive_months_below([95,79,78,77,81,76,75,74,73,72], 80)``
    is ``5`` (the final 76,75,74,73,72 run; the earlier 79,78,77 run is broken by
    81).
    """
    longest = 0
    current = 0
    for value in norm_index_series:
        if value is not None and value < threshold:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def classify_underperformance_severity(
    norm_index: Optional[float],
    consecutive_months: int,
) -> Optional[Severity]:
    """Classify OPS-06 severity from the representative norm-index + run length.

    Requires a sustained run: ``consecutive_months >= 6`` (otherwise ``None``).
    With the run requirement met, the index tiers (strict ``<``):
        * ``norm_index < 80`` → CONFIRMED
        * ``norm_index < 85`` → INDICATIVE
        * ``norm_index < 90`` → WATCH
        * ``norm_index >= 90`` → ``None`` (no finding)

    Returns ``None`` when ``norm_index`` is missing. Boundaries (locked by tests):
        (79, 6) → CONFIRMED, (79, 5) → None, (84, 6) → INDICATIVE,
        (89, 6) → WATCH, (90, 6) → None.
    """
    if consecutive_months < MIN_CONSECUTIVE_MONTHS:
        return None
    if norm_index is None:
        return None
    if norm_index < OPS06_INDEX_CONFIRMED:
        return Severity.CONFIRMED
    if norm_index < OPS06_INDEX_INDICATIVE:
        return Severity.INDICATIVE
    if norm_index < OPS06_INDEX_WATCH:
        return Severity.WATCH
    return None


def _extract_values(series: Any) -> List[Optional[float]]:
    """Normalise an injected/loaded series into a list of float-or-None values.

    Accepts either the list-of-dicts shape produced by
    ``ctx.load_norm_index_series()`` (each ``{"norm_index_p50": float, ...}``) or a
    bare list of numbers (convenient for tests). Non-numeric / missing entries
    become ``None`` so they break a consecutive run.
    """
    values: List[Optional[float]] = []
    for item in series:
        if isinstance(item, dict):
            raw = item.get("norm_index_p50")
        else:
            raw = item
        if raw is None:
            values.append(None)
            continue
        try:
            values.append(float(raw))
        except (TypeError, ValueError):
            values.append(None)
    return values


def _longest_run_below(values: Sequence[Optional[float]], threshold: float) -> List[float]:
    """Return the values of the longest run strictly below ``threshold``.

    Empty list when there is no below-threshold value. Ties keep the FIRST
    longest run encountered.
    """
    best: List[float] = []
    current: List[float] = []
    for value in values:
        if value is not None and value < threshold:
            current.append(value)
            if len(current) > len(best):
                best = list(current)
        else:
            current = []
    return best


# ─── Detector entrypoint ──────────────────────────────────────────────────────


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-06: persistent power-curve underperformance.

    Returns ``None`` (no finding) when:
        * there is no usable norm-index series for the windfarm,
        * fewer than ``MIN_MONTHS_OF_DATA`` (24 = 2 years) usable monthly points
          exist (0 / NULL months are already dropped upstream as data gaps),
        * the longest below-80 run is shorter than ``MIN_CONSECUTIVE_MONTHS`` (6), or
        * the representative index over that run is sub-threshold (>= 90).

    Otherwise emits a ``DetectorResult`` classified per
    ``classify_underperformance_severity``. The representative norm_index used for
    classification is the **mean of the longest below-80 run** (the sustained
    depth of underperformance), reported in ``data_slots["norm_index_p50"]``.
    """
    series = await ctx.load_norm_index_series()
    if not series:
        return None

    values = _extract_values(series)

    # < 2 years of usable data → suppress (count only real, non-gap points).
    usable = [v for v in values if v is not None]
    if len(usable) < MIN_MONTHS_OF_DATA:
        return None

    consecutive_months = count_consecutive_months_below(values, OPS06_INDEX_CONFIRMED)
    if consecutive_months < MIN_CONSECUTIVE_MONTHS:
        return None

    run_values = _longest_run_below(values, OPS06_INDEX_CONFIRMED)
    # Representative index over the run = its mean (sustained depth of the dip).
    norm_index = sum(run_values) / len(run_values) if run_values else None

    severity = classify_underperformance_severity(norm_index, consecutive_months)
    if severity is None:
        return None

    data_slots = {
        "norm_index_p50": round(norm_index, 2) if norm_index is not None else None,
        "consecutive_months_below_threshold": consecutive_months,
        "threshold": OPS06_INDEX_CONFIRMED,
        "months_observed": len(usable),
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }

    # Graceful-degradation: slots OPS-06 cannot yet populate (corroborating signals
    # that need other modules / external data).
    missing_slots = [
        "peer_norm_index_p50",
        "root_cause_category",
        "revenue_impact_eur",
    ]

    return DetectorResult(
        schema_code=SchemaCode.OPS_06,
        severity=severity,
        branch=None,
        data_slots=data_slots,
        missing_slots=missing_slots,
    )
