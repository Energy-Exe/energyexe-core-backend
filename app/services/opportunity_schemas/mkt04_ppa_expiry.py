"""MKT-04 · PPA expiry risk — M4 new detector (issue #104).

A buyer-side market schema (distinct from OPS-03, which is about OEM/O&M
contracting). It flags windfarms whose **Power Purchase Agreement is approaching
expiry** with no confirmed successor, so the asset is about to roll onto merchant
exposure (or needs re-contracting). The signal is the time remaining until the
active PPA's ``ppa_end_date``; a secondary WATCH branch fires when an active PPA
exists but its price was never captured (``ppa_price_eur_mwh`` is NULL — the
spec's "price not captured" case).

Months-to-expiry measure (``months_until_expiry``)
==================================================
Months between ``as_of_date`` and ``ppa_end_date`` are measured as a
**calendar-month delta plus a day fraction over the average month length**::

    months = (end.year  - as_of.year) * 12
           + (end.month - as_of.month)
           + (end.day   - as_of.day) / 30.44

This makes whole-calendar-month spans exact (e.g. 2026-06-15 → 2026-12-15 is
exactly ``6.0`` months; → 2027-06-15 is exactly ``12.0``) while still ordering
mid-month dates sensibly. ``None`` is returned when there is no end date. A
negative result (already expired) is possible and is handled by the classifier
(``<= 6`` → CONFIRMED), matching "expired with no successor" being the most
urgent case.

Severity tiers (``classify_ppa_expiry_severity``)
=================================================
Driven by months-to-expiry (inclusive ``<=`` bands), or by the null-price flag::

    months_to_expiry <= 6   →  CONFIRMED
    months_to_expiry <= 12  →  INDICATIVE
    months_to_expiry <= 18  →  WATCH
    otherwise               →  None
    (price_is_null)         →  WATCH   (when months gives no finding / is None)

Boundaries (locked by tests): 6 → CONFIRMED, 6.1 → INDICATIVE, 12 → INDICATIVE,
18 → WATCH, 18.1 → None. An active PPA with a NULL price → WATCH regardless of
the (possibly absent) end date.

Suppression
===========
Per spec, suppress when a confirmed successor exists or a fixed-price contract
has > 5 years remaining. There is no successor field in the data model yet
(assume none), and the "> 5yr fixed-price remaining" case never reaches the
threshold bands (months > 18 → no finding anyway), so no explicit suppression
hook is needed in v1; the relevant slots are surfaced in ``missing_slots``.

The detector reads only ``ctx.load_ppa_info()`` (the latest PPA for the
windfarm) and the detection period; it is pure and returns a ``DetectorResult``
or ``None``. Persistence is the orchestrator's job.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Average days per month (Gregorian, 365.25 / 12) — the day-fraction denominator.
AVG_DAYS_PER_MONTH = 30.44

# Months-to-expiry tier ceilings (inclusive ``<=``).
MKT04_CONFIRMED_MONTHS = 6.0
MKT04_INDICATIVE_MONTHS = 12.0
MKT04_WATCH_MONTHS = 18.0

# The PPA statuses that count as "live" for the null-price WATCH branch.
ACTIVE_PPA_STATUS = "active"


# ─── Pure, DB-free helpers ────────────────────────────────────────────────────


def months_until_expiry(
    ppa_end_date: Optional[date],
    as_of_date: date,
) -> Optional[float]:
    """Months from ``as_of_date`` to ``ppa_end_date``.

    Calendar-month delta plus a day fraction over the average month length::

        (end.year - as_of.year) * 12 + (end.month - as_of.month)
            + (end.day - as_of.day) / 30.44

    Whole-calendar-month spans are therefore exact. Returns ``None`` when there
    is no end date. The result may be negative (PPA already expired).

    Examples:
        2026-06-15 → 2026-12-15  →  6.0   (six whole calendar months)
        2026-06-15 → 2027-06-15  →  12.0  (one whole year)
    """
    if ppa_end_date is None:
        return None

    end = _as_date(ppa_end_date)
    ref = _as_date(as_of_date)

    whole_months = (end.year - ref.year) * 12 + (end.month - ref.month)
    day_fraction = (end.day - ref.day) / AVG_DAYS_PER_MONTH
    return whole_months + day_fraction


def classify_ppa_expiry_severity(
    months_to_expiry: Optional[float],
    price_is_null: bool,
) -> Optional[Severity]:
    """Classify MKT-04 severity from months-to-expiry (and the null-price flag).

    Months-to-expiry bands (inclusive ``<=``):
        * ``<= 6``  → CONFIRMED
        * ``<= 12`` → INDICATIVE
        * ``<= 18`` → WATCH
        * otherwise → ``None``

    When the months bands yield no finding (or ``months_to_expiry`` is ``None``)
    but ``price_is_null`` is set (an active PPA whose price was never captured),
    the result is ``WATCH`` — the spec's "price not captured" case.

    Boundaries (locked by tests): 6 → CONFIRMED, 6.1 → INDICATIVE,
    12 → INDICATIVE, 18 → WATCH, 18.1 → None; null price → WATCH.
    """
    if months_to_expiry is not None:
        if months_to_expiry <= MKT04_CONFIRMED_MONTHS:
            return Severity.CONFIRMED
        if months_to_expiry <= MKT04_INDICATIVE_MONTHS:
            return Severity.INDICATIVE
        if months_to_expiry <= MKT04_WATCH_MONTHS:
            return Severity.WATCH

    # Null-price active PPA → WATCH (only when the months bands gave no finding;
    # a closer expiry tier always wins as the more urgent classification).
    if price_is_null:
        return Severity.WATCH

    return None


# ─── Detector entrypoint ──────────────────────────────────────────────────────


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """MKT-04: PPA expiry risk.

    Reads the latest PPA for the windfarm (``ctx.load_ppa_info()``), computes the
    months remaining until ``ppa_end_date`` against the detection period end (or
    today when no period end is available), and classifies severity. An active
    PPA whose ``ppa_price_eur_mwh`` is NULL fires WATCH even without a near-term
    end date (the spec's "price not captured" case).

    Returns ``None`` (no finding) when:
        * there is no PPA for the windfarm (``ppa_info`` is empty / has no
          ``ppa_buyer`` — i.e. no real contract), or
        * the PPA is not active, or
        * the active PPA has both a non-near-term (or absent) end date and a
          captured price (so neither the expiry bands nor the null-price branch
          fire).

    Otherwise emits a ``DetectorResult`` classified per
    ``classify_ppa_expiry_severity``.
    """
    ppa_info = await ctx.load_ppa_info()

    # No real PPA contract → nothing to assess. A genuine PPA from
    # ``load_ppa_info`` always carries ``ppa_buyer``; an empty dict (no PPA) does
    # not. This also keeps the M1 characterization snapshot byte-identical: the
    # legacy scenarios inject partial ppa_info dicts (no ``ppa_buyer``), so
    # MKT-04 does not fire on any of them.
    if not ppa_info or not ppa_info.get("ppa_buyer"):
        return None

    is_active = (ppa_info.get("ppa_status") or "").lower() == ACTIVE_PPA_STATUS
    if not is_active:
        return None

    as_of_date = _as_of_date(ctx)
    months = months_until_expiry(ppa_info.get("ppa_end_date"), as_of_date)
    price_is_null = ppa_info.get("ppa_price_eur_mwh") is None

    severity = classify_ppa_expiry_severity(months, price_is_null)
    if severity is None:
        return None

    data_slots = {
        "ppa_buyer": ppa_info.get("ppa_buyer"),
        "ppa_status": ppa_info.get("ppa_status"),
        "ppa_end_date": (
            ppa_info["ppa_end_date"].isoformat() if ppa_info.get("ppa_end_date") else None
        ),
        "months_until_expiry": round(months, 2) if months is not None else None,
        "contract_type": ppa_info.get("contract_type"),
        "ppa_price_captured": not price_is_null,
        "as_of_date": as_of_date.isoformat(),
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }

    # Graceful-degradation: slots MKT-04 cannot yet populate. No successor-PPA
    # field exists in the data model, and forward merchant-price / re-contracting
    # economics are not yet tracked.
    missing_slots = [
        "confirmed_successor_ppa",
        "forward_merchant_price_eur_mwh",
        "recontracting_revenue_impact_eur",
    ]

    return DetectorResult(
        schema_code=SchemaCode.MKT_04,
        severity=severity,
        branch=None,
        data_slots=data_slots,
        missing_slots=missing_slots,
    )


def _as_of_date(ctx: DetectionContext) -> date:
    """Derive the ``as_of_date`` for the months-to-expiry computation.

    Uses the detection period end (``ctx.period_end``) when present — the most
    recent point the analysis covers — falling back to today's date.
    """
    period_end = getattr(ctx, "period_end", None)
    return _as_date(period_end) if period_end is not None else date.today()


def _as_date(value) -> date:
    """Normalize a ``datetime`` or ``date`` to a ``date``."""
    if isinstance(value, datetime):
        return value.date()
    return value


__all__ = [
    "AVG_DAYS_PER_MONTH",
    "MKT04_CONFIRMED_MONTHS",
    "MKT04_INDICATIVE_MONTHS",
    "MKT04_WATCH_MONTHS",
    "months_until_expiry",
    "classify_ppa_expiry_severity",
    "detect",
]
