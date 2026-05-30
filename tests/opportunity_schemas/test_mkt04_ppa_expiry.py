"""MKT-04 detector tests (issue #104) — PPA expiry risk.

MKT-04 is a buyer-side market schema: it flags windfarms whose active PPA is
approaching expiry (no confirmed successor), or whose active PPA has a NULL
captured price.

Months-to-expiry measure (``months_until_expiry``): calendar-month delta plus a
day fraction over the average month length (30.44), so whole-calendar-month
spans are exact (2026-06-15 → 2026-12-15 = 6.0; → 2027-06-15 = 12.0).

Severity tiers (``classify_ppa_expiry_severity``), inclusive ``<=`` bands on
months-to-expiry, with a null-price WATCH fallback:
    <= 6 → CONFIRMED   <= 12 → INDICATIVE   <= 18 → WATCH   else → None
    active PPA, price NULL → WATCH

Boundaries (locked): 6 → CONFIRMED, 6.1 → INDICATIVE, 12 → INDICATIVE,
18 → WATCH, 18.1 → None.

All tests are DB-free: PPA info is injected via
``DetectionContext(prefetched={"ppa_info": {...}})``.
"""

from datetime import date, datetime

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.mkt04_ppa_expiry import (
    classify_ppa_expiry_severity,
    detect,
    months_until_expiry,
)

WF_ID = 104


def _ctx(ppa_info=None, *, period_end=datetime(2026, 1, 1)):
    """A DB-free DetectionContext with ``ppa_info`` injected.

    Passing the key (even ``{}``) short-circuits ``load_ppa_info``'s DB query.
    """
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=datetime(2024, 1, 1),
        period_end=period_end,
        prefetched={"ppa_info": ppa_info if ppa_info is not None else {}},
    )


# ─── classify_ppa_expiry_severity — the required boundary table ───────────────


def test_expiry_boundaries():
    """6 → CONFIRMED, 6.1 → INDICATIVE, 12 → INDICATIVE, 18 → WATCH, 18.1 → None."""
    assert classify_ppa_expiry_severity(6.0, False) == Severity.CONFIRMED
    assert classify_ppa_expiry_severity(6.1, False) == Severity.INDICATIVE
    assert classify_ppa_expiry_severity(12.0, False) == Severity.INDICATIVE
    assert classify_ppa_expiry_severity(18.0, False) == Severity.WATCH
    assert classify_ppa_expiry_severity(18.1, False) is None


def test_null_price_active_ppa_is_watch():
    """An active PPA whose price was never captured (NULL) → WATCH.

    Exercised both through the pure classifier (no near-term expiry, price NULL)
    and end-to-end through ``detect`` with an active, null-price PPA.
    """
    # Pure classifier: months gives no finding, but the null-price flag → WATCH.
    assert classify_ppa_expiry_severity(None, True) == Severity.WATCH
    assert classify_ppa_expiry_severity(99.0, True) == Severity.WATCH


def test_months_until_expiry_formula():
    """A concrete date pair → exact expected months.

    2026-06-15 → 2026-12-15 spans exactly six whole calendar months (day delta
    zero) → 6.0; 2026-06-15 → 2027-06-15 spans one whole year → 12.0.
    """
    assert months_until_expiry(date(2026, 12, 15), date(2026, 6, 15)) == pytest.approx(6.0)
    assert months_until_expiry(date(2027, 6, 15), date(2026, 6, 15)) == pytest.approx(12.0)
    # No end date → None.
    assert months_until_expiry(None, date(2026, 6, 15)) is None


@pytest.mark.asyncio
async def test_detect_none_when_no_ppa():
    """No PPA contract (empty ppa_info) → no finding."""
    result = await detect(_ctx({}))
    assert result is None


# ─── Additional detect() coverage ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_detect_confirmed_when_expiry_within_6_months():
    """Active PPA expiring in ~5 months (period end 2026-01-01) → CONFIRMED."""
    result = await detect(
        _ctx(
            {
                "ppa_buyer": "Acme Energy",
                "ppa_status": "active",
                "ppa_end_date": date(2026, 6, 1),
                "ppa_price_eur_mwh": 45.0,
                "contract_type": "fixed_price",
            }
        )
    )
    assert result is not None
    assert result.schema_code == SchemaCode.MKT_04
    assert result.severity == Severity.CONFIRMED


@pytest.mark.asyncio
async def test_detect_null_price_active_ppa_fires_watch():
    """Active PPA with NULL price but far-off end date → WATCH (price not captured)."""
    result = await detect(
        _ctx(
            {
                "ppa_buyer": "Acme Energy",
                "ppa_status": "active",
                "ppa_end_date": date(2031, 1, 1),  # > 18 months out
                "ppa_price_eur_mwh": None,
            }
        )
    )
    assert result is not None
    assert result.severity == Severity.WATCH


@pytest.mark.asyncio
async def test_detect_none_when_ppa_not_active():
    """A PPA that is not active (e.g. expired) → no finding, even if near-dated."""
    result = await detect(
        _ctx(
            {
                "ppa_buyer": "Acme Energy",
                "ppa_status": "expired",
                "ppa_end_date": date(2026, 3, 1),
                "ppa_price_eur_mwh": 45.0,
            }
        )
    )
    assert result is None


@pytest.mark.asyncio
async def test_detect_none_when_active_long_dated_with_price():
    """Active PPA far from expiry (> 18 months) with a captured price → None."""
    result = await detect(
        _ctx(
            {
                "ppa_buyer": "Acme Energy",
                "ppa_status": "active",
                "ppa_end_date": date(2030, 1, 1),
                "ppa_price_eur_mwh": 45.0,
            }
        )
    )
    assert result is None
