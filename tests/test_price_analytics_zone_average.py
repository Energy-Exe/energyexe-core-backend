"""Tests for ``PriceAnalyticsService.compute_zone_average_capture_rate`` (issue #94).

This is the root-cause fix that unblocks MKT-01: the generation-weighted mean
capture rate across a bidzone's windfarms, previously omitted from
``compare_capture_rates_by_bidzone``. Pure / DB-free — the function is a
staticmethod over the already-fetched ``windfarms`` list.
"""

import pytest

from app.services.price_analytics_service import PriceAnalyticsService

compute = PriceAnalyticsService.compute_zone_average_capture_rate


def test_zone_average_is_generation_weighted():
    """Weighted by generation, not a plain mean.

    Farms ``[(capture=0.80, gen=100), (capture=0.60, gen=300)]``::

        (0.80*100 + 0.60*300) / (100 + 300) = (80 + 180) / 400 = 0.65

    (a plain mean would be 0.70 — proving the weighting).
    """
    windfarms = [
        {"capture_rate": 0.80, "total_generation_mwh": 100},
        {"capture_rate": 0.60, "total_generation_mwh": 300},
    ]
    assert compute(windfarms) == pytest.approx(0.65)


def test_zone_average_none_when_zero_generation():
    """All farms have zero generation → no benchmark → None."""
    windfarms = [
        {"capture_rate": 0.80, "total_generation_mwh": 0},
        {"capture_rate": 0.60, "total_generation_mwh": 0},
    ]
    assert compute(windfarms) is None


def test_zone_average_none_when_empty_list():
    """No farms → None."""
    assert compute([]) is None


def test_zone_average_skips_farms_without_capture_rate():
    """A farm with capture_rate None is excluded from both numerator and
    denominator, so it neither contributes to nor dilutes the weighted mean."""
    windfarms = [
        {"capture_rate": 0.80, "total_generation_mwh": 100},
        {"capture_rate": None, "total_generation_mwh": 900},  # excluded
    ]
    assert compute(windfarms) == pytest.approx(0.80)
