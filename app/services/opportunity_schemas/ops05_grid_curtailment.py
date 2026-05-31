"""OPS-05 · Grid curtailment — M3 new detector (issue #100).

Flags windfarms whose generation is being materially curtailed by the grid.
"True" curtailment is the share of *potential* output that was curtailed over the
detection window::

    curtailment_pct = curtailed / (curtailed + generation) * 100

where ``curtailed`` = ``SUM(generation_data.curtailed_mwh)`` and ``generation`` =
``SUM(generation_data.generation_mwh)``. The percentage is supplied by
``ctx.load_curtailment_pct()`` (added by #94), which runs that SUM and is
None-safe (returns ``None`` when there are no rows or the denominator is 0).

UK-ONLY — no proxy fallback
===========================
Metered curtailment (``curtailed_mwh``) is only reliably populated for UK farms
via the ELEXON / BOAV (Bid-Offer Acceptance Volume) pipeline. For every other
geography ``curtailed_mwh`` is absent or unreliable, so OPS-05 **does not fire**
for non-UK windfarms — there is deliberately no proxy estimate. "UK" is the GB
bidzone (``10YGB----------A`` / any ``10YGB*`` code) or the GB country
(ISO-3 ``GBR`` / ISO-2 ``GB``); see ``is_uk_bidzone``.

Severity tiers (spec thresholds, ``pct`` is the curtailment percentage):

    pct >= 10  →  CONFIRMED
    pct >=  5  →  INDICATIVE
    pct >=  3  →  WATCH
    otherwise  →  None (no finding)

Comparisons are inclusive ``>=`` on the percentage. Examples (locked by tests):
    10.0 → CONFIRMED   9.99 → INDICATIVE   5.0 → INDICATIVE
    4.99 → WATCH        3.0 → WATCH         2.99 → None

The detector is pure: it reads only ``ctx.windfarm`` (for the UK gate) and
``ctx.load_curtailment_pct()`` (for the percentage), and returns a
``DetectorResult`` (or ``None``). Persistence is the orchestrator's job.
"""

from __future__ import annotations

from typing import Any, Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Curtailment-percentage tier floors (inclusive ``>=``).
OPS05_PCT_CONFIRMED = 10.0
OPS05_PCT_INDICATIVE = 5.0
OPS05_PCT_WATCH = 3.0

# The GB / UK bidzone EIC code (ELEXON / ENTSOE). Any ``10YGB*`` code matches.
UK_BIDZONE_CODE = "10YGB----------A"


# ─── Pure, DB-free helpers ────────────────────────────────────────────────────


def compute_curtailment_pct(
    curtailed_mwh: Optional[float],
    generation_mwh: Optional[float],
) -> Optional[float]:
    """True curtailment percentage over the period.

    ``curtailed / (curtailed + generation) * 100``. Returns ``None`` when either
    input is missing or the denominator (curtailed + generation) is 0 — there is
    no meaningful percentage to report.

    Example: ``compute_curtailment_pct(100, 900) == 10.0``.
    """
    if curtailed_mwh is None or generation_mwh is None:
        return None
    denominator = curtailed_mwh + generation_mwh
    if denominator == 0:
        return None
    return curtailed_mwh / denominator * 100


def classify_curtailment_severity(pct: Optional[float]) -> Optional[Severity]:
    """Classify OPS-05 severity from the curtailment percentage.

    Tiers (inclusive ``>=``):
        * ``pct >= 10`` → CONFIRMED
        * ``pct >=  5`` → INDICATIVE
        * ``pct >=  3`` → WATCH
        * otherwise     → ``None`` (no finding)

    Returns ``None`` when ``pct`` is missing. Boundaries (locked by tests):
        10.0 → CONFIRMED, 9.99 → INDICATIVE, 5.0 → INDICATIVE,
        4.99 → WATCH, 3.0 → WATCH, 2.99 → None.
    """
    if pct is None:
        return None
    if pct >= OPS05_PCT_CONFIRMED:
        return Severity.CONFIRMED
    if pct >= OPS05_PCT_INDICATIVE:
        return Severity.INDICATIVE
    if pct >= OPS05_PCT_WATCH:
        return Severity.WATCH
    return None


def is_uk_bidzone(windfarm: Any) -> bool:
    """True when the windfarm is in the UK (GB) market.

    Metered curtailment is only reliable for UK farms (ELEXON / BOAV), so OPS-05
    is UK-only. A windfarm counts as UK when EITHER:
        * its bidzone code is ``10YGB----------A`` (or any ``10YGB*`` code), or
        * its country code is GB (ISO-2 ``GB``) / GBR (ISO-3 ``GBR``).

    Both are checked so the gate works whether the windfarm is geo-tagged by
    bidzone or only by country. A bare-int windfarm, a missing relationship, or
    any access failure (detached ORM relationship in a DB-free test) → ``False``
    (not UK), so the detector simply does not fire.
    """
    try:
        bidzone = getattr(windfarm, "bidzone", None)
        bidzone_code = (getattr(bidzone, "code", None) or "").upper()
        country = getattr(windfarm, "country", None)
        country_code = (getattr(country, "code", None) or "").upper()
    except Exception:
        return False

    if bidzone_code.startswith("10YGB"):
        return True
    if country_code in ("GB", "GBR"):
        return True
    return False


# ─── Detector entrypoint ──────────────────────────────────────────────────────


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """OPS-05: grid curtailment (UK only, no proxy).

    Returns ``None`` (no finding) when:
        * the windfarm is not in the UK (no proxy for other geographies),
        * curtailment data is unavailable for the period
          (``ctx.load_curtailment_pct()`` is ``None``), or
        * the curtailment percentage is sub-threshold (< 3%).

    Otherwise emits a ``DetectorResult`` classified per
    ``classify_curtailment_severity`` (CONFIRMED / INDICATIVE / WATCH).
    """
    # UK-only gate: non-UK farms have no reliable metered curtailment → no fire.
    if not is_uk_bidzone(ctx.windfarm):
        return None

    pct = await ctx.load_curtailment_pct()
    if pct is None:
        return None

    severity = classify_curtailment_severity(pct)
    if severity is None:
        return None

    data_slots = {
        "curtailment_pct": round(pct, 2),
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }

    return DetectorResult(
        schema_code=SchemaCode.OPS_05,
        severity=severity,
        branch=None,
        data_slots=data_slots,
    )
