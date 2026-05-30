"""MKT-06 · Negative-price hours exposure — M4 new detector (issue #105).

Flags windfarms that **generate into negative day-ahead prices** for a material
number of hours per year. When a farm produces while the day-ahead price is
below zero it is, in effect, paying to export — a direct revenue leak that
better curtailment / storage / contracting could avoid. The signal is the count
of hours in the detection window where the farm's net generation is positive AND
``price_data.day_ahead_price < 0`` (non-generating hours carry no
curtailment-avoided exposure and are excluded upstream — see
``PriceAnalyticsService.count_negative_price_hours``).

Per-year normalization
=======================
Severity tiers are expressed in **hours per year**, but a detection window is
not always exactly one year (backfills, partial years, multi-year windows). The
raw count over the window is therefore scaled to an annual rate::

    hours_per_year = negative_hours * (365.25 / window_days)

where ``window_days = (period_end - period_start).days`` (≥ 1, guarded). A
window of exactly one year leaves the count essentially unchanged
(365 days → ×1.0007), a two-year window halves it to an annual average, and a
half-year window doubles it. When the window length is unavailable / non-positive
the raw count is used as-is (treated as a one-year proxy) rather than crashing.

Severity tiers (``classify_negative_price_severity``)
=====================================================
Driven by ``hours_per_year`` (inclusive ``>=`` bands, descending)::

    >= 400  → CONFIRMED
    >= 250  → INDICATIVE
    >= 150  → WATCH
    otherwise → None

Boundaries (locked by tests): 400 → CONFIRMED, 399 → INDICATIVE, 250 →
INDICATIVE, 150 → WATCH, 149 → None.

Suppression / downgrade
=======================
Per spec, MKT-06 is suppressed when an active BESS dispatches in the negative
hours, or a fixed-price long-dated PPA insulates the revenue. Neither the
storage-dispatch signal nor the forward PPA economics are modelled yet, so those
are surfaced in ``missing_slots`` rather than gated here in v1. MKT-06 is also
**downgraded one tier** when MKT-03 (high cannibalisation) is CONFIRMED for the
same farm — but that overlap rule is a cross-schema post-pass owned by the
registry (issue #112 / #25), NOT this detector; ``detect`` stays pure and emits
its standalone tier.

The detector reads only ``ctx.load_negative_price_hours()`` (None/0-safe) and the
detection period; it is pure and returns a ``DetectorResult`` or ``None``.
"""

from __future__ import annotations

from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Negative-price-hours-per-year tier floors (inclusive ``>=``).
MKT06_CONFIRMED_HOURS = 400
MKT06_INDICATIVE_HOURS = 250
MKT06_WATCH_HOURS = 150

# Days per year used to annualise a non-one-year detection window.
DAYS_PER_YEAR = 365.25


# ─── Pure, DB-free helpers ────────────────────────────────────────────────────


def classify_negative_price_severity(
    hours_per_year: Optional[float],
) -> Optional[Severity]:
    """Classify MKT-06 severity from negative-price hours per year.

    Inclusive ``>=`` bands (descending):
        * ``>= 400`` → CONFIRMED
        * ``>= 250`` → INDICATIVE
        * ``>= 150`` → WATCH
        * otherwise  → ``None``

    Boundaries (locked by tests): 400 → CONFIRMED, 399 → INDICATIVE,
    250 → INDICATIVE, 150 → WATCH, 149 → None. ``None``/below the WATCH floor
    means "no finding".
    """
    if hours_per_year is None:
        return None
    if hours_per_year >= MKT06_CONFIRMED_HOURS:
        return Severity.CONFIRMED
    if hours_per_year >= MKT06_INDICATIVE_HOURS:
        return Severity.INDICATIVE
    if hours_per_year >= MKT06_WATCH_HOURS:
        return Severity.WATCH
    return None


def annualise_hours(negative_hours: int, window_days: Optional[float]) -> float:
    """Scale a raw window count to a per-year rate.

    ``negative_hours * (365.25 / window_days)``. When ``window_days`` is missing
    or non-positive the raw count is returned unchanged (one-year proxy).
    """
    if not window_days or window_days <= 0:
        return float(negative_hours)
    return negative_hours * (DAYS_PER_YEAR / window_days)


# ─── Detector entrypoint ──────────────────────────────────────────────────────


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """MKT-06: negative-price-hours exposure.

    Reads the count of hours the farm generates at a negative day-ahead price
    over the detection window (``ctx.load_negative_price_hours()``), normalizes
    it to a per-year rate (the window is not always exactly one year), and
    classifies severity.

    Returns ``None`` (no finding) when:
        * the accessor yields ``None`` or ``0`` (no data, or no qualifying
          negative-price generating hours), or
        * the annualised count falls below the WATCH floor (< 150 h/yr).

    Otherwise emits a ``DetectorResult`` classified per
    ``classify_negative_price_severity``. MKT-06's one-tier downgrade when MKT-03
    is CONFIRMED is applied by the registry post-pass (#112), not here.
    """
    negative_hours = await ctx.load_negative_price_hours()
    if not negative_hours:  # None or 0 → nothing producing at negative prices.
        return None

    window_days = _window_days(ctx)
    hours_per_year = annualise_hours(negative_hours, window_days)

    severity = classify_negative_price_severity(hours_per_year)
    if severity is None:
        return None

    data_slots = {
        "negative_price_hours": int(negative_hours),
        "negative_price_hours_per_year": round(hours_per_year, 1),
        "window_days": round(window_days, 1) if window_days else None,
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }

    # Graceful-degradation: slots MKT-06 cannot yet populate. The avg negative
    # price *depth* enrichment and the BESS-dispatch / fixed-price-PPA suppression
    # signals are not modelled yet.
    missing_slots = [
        "avg_negative_price_depth_eur_mwh",
        "negative_price_revenue_loss_eur",
        "bess_dispatches_in_negative_hours",
        "fixed_price_ppa_coverage",
    ]

    return DetectorResult(
        schema_code=SchemaCode.MKT_06,
        severity=severity,
        branch=None,
        data_slots=data_slots,
        missing_slots=missing_slots,
    )


def _window_days(ctx: DetectionContext) -> Optional[float]:
    """Length of the detection window in days, or ``None`` if not computable."""
    start = getattr(ctx, "period_start", None)
    end = getattr(ctx, "period_end", None)
    if start is None or end is None:
        return None
    try:
        return (end - start).total_seconds() / 86400.0
    except Exception:
        return None


__all__ = [
    "MKT06_CONFIRMED_HOURS",
    "MKT06_INDICATIVE_HOURS",
    "MKT06_WATCH_HOURS",
    "DAYS_PER_YEAR",
    "classify_negative_price_severity",
    "annualise_hours",
    "detect",
]
