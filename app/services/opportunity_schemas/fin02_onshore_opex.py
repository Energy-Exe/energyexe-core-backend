"""FIN-02 · Onshore OPEX overrun — M5 new detector (issue #108).

Flags **onshore** windfarms whose operating cost per MWh runs materially above a
*dynamically-computed* per-bidzone peer median. The companion FIN-03 detector
(``fin03_offshore_opex.py``) applies the identical logic to **offshore** farms;
both import the shared pure helpers from THIS module so the OPEX-per-MWh formula
and the severity thresholds live in exactly one place.

Cohort & median (``DetectionContext.compute_zone_opex_median``)
===============================================================
The benchmark is the median of peer OPEX-per-MWh over the windfarms that share
BOTH the subject's ``location_type`` (onshore / offshore) AND its ``bidzone_id``
(never cross-market), restricted to ``relationship_type = 'primary_asset'`` 1:1
links, **excluding the subject itself**, one vote per financial entity, and only
when at least ``OPEX_MIN_PEERS`` (3) peers qualify. Onshore and offshore form
**separate cohorts**: an offshore farm is never benchmarked against the onshore
median, and FIN-02 (onshore) does not even fire for an offshore farm (it gates on
``location_type``), nor FIN-03 for an onshore one.

OPEX-per-MWh (``app/services/financial_opex_metrics.py``)
=========================================================
Subject and peers use the platform's canonical definition (the one behind the
client Financial tab, ``FinancialDataService.calculate_financial_ratios``):

    opex_per_mwh = Σ opex_EUR / Σ metered_net_MWh   over the 3 most recent usable filings

* a filing is usable only after ramp-up (``period_start >= COD + 365 days``),
  with metered ``generation_data`` covering >= 50% of its own period (else its
  own ``reported_generation_gwh``; with neither it is dropped from BOTH sums);
* every filing is converted to **EUR** with the ECB period-average rate — peers
  file in NOK / GBP / DKK / EUR and the cohorts mix them;
* synthetic (estimated) filings are excluded — a finding must rest on filed data.

``compute_opex_per_mwh`` below is the pure last step: ``total_opex_eur /
generation_mwh``, generation supplied in **GWh** by the accessor and converted to
MWh here. ``3.18M€`` over ``100 GWh`` → ``31.8 €/MWh``. Returns ``None`` when
OPEX is missing or generation is missing / non-positive.

History: until 2026-08 the accessor divided by ``reported_generation_gwh`` and
summed OPEX over years that had no generation figure at all — Lutelandet (prod
report 41) showed 682 NOK/MWh, five years of OPEX over one year of output.

Overrun % + severity (``classify_opex_overrun_severity``)
=========================================================
    pct_over_median = (own_opex_per_mwh - median) / median * 100

    pct_over_median >= 100  →  CONFIRMED   (requires >= 2 full operating years)
    pct_over_median >= 70   →  INDICATIVE
    pct_over_median >= 30   →  WATCH
    otherwise (< 30)        →  None

The CONFIRMED tier is only reachable with two full operating years (the first
commissioning year is a ramp-up artefact and is excluded from the count by the
context accessor). A +100% overrun with only one full year **caps at
INDICATIVE**. Boundaries (locked by tests): +100→CONFIRMED(2yr), +70→INDICATIVE,
+30→WATCH, +29→None.

Consolidated-entity exclusion (v1)
==================================
Only ``primary_asset`` 1:1 windfarm↔entity links are assessed. A consolidated
(multi-asset) entity bundles several windfarms' costs, so attributing its OPEX to
a single farm would double-count — such links are skipped in v1 (the context
accessor returns ``None`` for them, and the detector returns ``None``).

Snapshot safety (CRITICAL — read before editing ``detect``)
===========================================================
The M1 characterization scenarios inject NEITHER own OPEX financials NOR a zone
OPEX median (and use a bare-int windfarm with no ``location_type``). So
``ctx.load_own_opex_financials()`` and ``ctx.compute_zone_opex_median()`` both
resolve to ``None``, the ``location_type`` gate fails, and ``detect`` returns
``None`` for every legacy scenario — keeping ``EXPECTED_SNAPSHOT`` /
``M1_LEGACY_BASELINE`` byte-identical.
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Overrun-percentage tier thresholds (>=, percent above the peer median).
FIN_OPEX_CONFIRMED_PCT = 100.0  # >= this → CONFIRMED (requires >= 2 full years)
FIN_OPEX_INDICATIVE_PCT = 70.0  # >= this → INDICATIVE
FIN_OPEX_WATCH_PCT = 30.0  # >= this → WATCH

# CONFIRMED requires at least this many full operating years (the commissioning
# ramp-up year is excluded). One full year caps a +100% overrun at INDICATIVE.
FIN_OPEX_CONFIRMED_MIN_FULL_YEARS = 2

# This detector's location-type cohort.
LOCATION_TYPE = "onshore"


# ─── Shared pure, DB-free helpers (imported by fin03_offshore_opex) ────────────


def compute_opex_per_mwh(
    total_opex_eur: Optional[float],
    generation_mwh: Optional[float],
) -> Optional[float]:
    """OPEX per MWh = ``total_opex_eur / generation_mwh``.

    The caller passes generation already converted to **MWh** (the context
    accessor stores it in GWh; :func:`detect` multiplies by 1000 before calling
    this). Returns ``None`` when OPEX is missing, or generation is missing / not
    positive (no meaningful per-MWh cost). Example with MWh input::

        compute_opex_per_mwh(3.18e6, 100_000) == 31.8
    """
    if total_opex_eur is None:
        return None
    if generation_mwh is None or generation_mwh <= 0:
        return None
    return total_opex_eur / generation_mwh


def classify_opex_overrun_severity(
    pct_over_median: Optional[float],
    full_years: Optional[int],
) -> Optional[Severity]:
    """Classify OPEX-overrun severity from the % above the peer median.

    Tiers (``>=``):

        pct_over_median >= 100 → CONFIRMED   (only if ``full_years >= 2``;
                                              otherwise caps at INDICATIVE)
        pct_over_median >= 70  → INDICATIVE
        pct_over_median >= 30  → WATCH
        otherwise              → None

    Returns ``None`` when ``pct_over_median`` is missing. Boundaries (locked by
    tests): +100 → CONFIRMED(2yr), +70 → INDICATIVE, +30 → WATCH, +29 → None.
    """
    if pct_over_median is None:
        return None

    if pct_over_median >= FIN_OPEX_CONFIRMED_PCT:
        # CONFIRMED needs two full operating years; otherwise the +100% overrun
        # is on too little history and caps at INDICATIVE.
        if full_years is not None and full_years >= FIN_OPEX_CONFIRMED_MIN_FULL_YEARS:
            return Severity.CONFIRMED
        return Severity.INDICATIVE
    if pct_over_median >= FIN_OPEX_INDICATIVE_PCT:
        return Severity.INDICATIVE
    if pct_over_median >= FIN_OPEX_WATCH_PCT:
        return Severity.WATCH
    return None


def compute_pct_over_median(
    own_opex_per_mwh: Optional[float],
    median_opex_per_mwh: Optional[float],
) -> Optional[float]:
    """Percent the subject's OPEX/MWh runs above the peer median.

    ``(own - median) / median * 100``. Returns ``None`` when either input is
    missing or the median is not positive (no meaningful comparison).
    """
    if own_opex_per_mwh is None or median_opex_per_mwh is None:
        return None
    if median_opex_per_mwh <= 0:
        return None
    return (own_opex_per_mwh - median_opex_per_mwh) / median_opex_per_mwh * 100


async def run_opex_overrun_detector(
    ctx: DetectionContext,
    *,
    schema_code: SchemaCode,
    location_type: str,
) -> Optional[DetectorResult]:
    """Shared FIN-02 / FIN-03 detection body, parameterised by location type.

    Both detectors call this with their own ``schema_code`` / ``location_type``.
    Steps:

      1. Gate on the windfarm's ``location_type`` — FIN-02 runs only for onshore
         farms, FIN-03 only for offshore. A mismatch (or unknown type) → ``None``,
         so an offshore farm is never benchmarked against the onshore median.
      2. Load the subject's own ``primary_asset`` OPEX financials (consolidated
         links are excluded by the accessor) → ``None`` → no finding.
      3. Compute own OPEX/MWh (GWh → MWh conversion) and the per-bidzone,
         per-location-type peer median; compute ``pct_over_median``.
      4. Classify severity (CONFIRMED requires >= 2 full operating years).

    Returns ``None`` whenever any required datum is absent — the snapshot-safety
    contract for the legacy scenarios.
    """
    wf_location = await _windfarm_location_type(ctx)
    if wf_location != location_type:
        # Wrong cohort (or unknown location) — do not fire / do not benchmark.
        return None

    financials = await ctx.load_own_opex_financials()
    if not financials:
        # No primary_asset OPEX data (or a consolidated-only link) → no finding.
        return None

    generation_gwh = financials.get("generation_gwh")
    generation_mwh = generation_gwh * 1000.0 if generation_gwh is not None else None
    own_opex_per_mwh = compute_opex_per_mwh(financials.get("total_opex_eur"), generation_mwh)
    if own_opex_per_mwh is None:
        return None

    median = await ctx.compute_zone_opex_median(location_type)
    if median is None or median <= 0:
        return None

    pct_over_median = compute_pct_over_median(own_opex_per_mwh, median)
    full_years = financials.get("full_years")
    severity = classify_opex_overrun_severity(pct_over_median, full_years)
    if severity is None:
        return None

    data_slots = {
        "opex_per_mwh": round(own_opex_per_mwh, 4),
        "zone_opex_median": round(median, 4),
        "pct_over_median": round(pct_over_median, 2) if pct_over_median is not None else None,
        "location_type": location_type,
        "full_years": full_years,
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
        # Provenance (additive — legacy prefetched dicts omit these; evidence
        # rendering skips None): comparison currency, the farm's own filing
        # currency + ratio, fiscal years pooled, denominator basis, cohort size.
        "currency": financials.get("currency"),
        "native_currency": financials.get("native_currency"),
        "native_opex_per_mwh": financials.get("native_opex_per_mwh"),
        "years_used": financials.get("years_used"),
        "generation_source": financials.get("generation_source"),
        "peer_count": ctx.peek(f"zone_opex_peer_count:{location_type}"),
    }

    return DetectorResult(
        schema_code=schema_code,
        severity=severity,
        branch=None,
        data_slots=data_slots,
    )


async def _windfarm_location_type(ctx: DetectionContext) -> Optional[str]:
    """The windfarm's ``location_type``, None-safe.

    Read off the ORM / snapshot object when present; for a bare-int windfarm
    (the nightly runner) the context resolves it with one cached DB lookup —
    before that fallback FIN-02/03 silently never fired in the nightly at all.
    """
    try:
        value = await ctx.windfarm_attr("location_type")
    except Exception:
        return None
    return value if isinstance(value, str) else None


# ─── Detector entrypoint ──────────────────────────────────────────────────────


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """FIN-02: onshore OPEX overrun vs the per-bidzone onshore peer median.

    Fires only for onshore farms (gates on ``location_type == "onshore"``); see
    :func:`run_opex_overrun_detector` for the shared logic. Returns ``None`` when
    the farm is not onshore, has no ``primary_asset`` OPEX data, or its overrun is
    below the WATCH threshold.
    """
    return await run_opex_overrun_detector(
        ctx, schema_code=SchemaCode.FIN_02, location_type=LOCATION_TYPE
    )


__all__ = [
    "FIN_OPEX_CONFIRMED_PCT",
    "FIN_OPEX_INDICATIVE_PCT",
    "FIN_OPEX_WATCH_PCT",
    "FIN_OPEX_CONFIRMED_MIN_FULL_YEARS",
    "LOCATION_TYPE",
    "compute_opex_per_mwh",
    "classify_opex_overrun_severity",
    "compute_pct_over_median",
    "run_opex_overrun_detector",
    "detect",
]
