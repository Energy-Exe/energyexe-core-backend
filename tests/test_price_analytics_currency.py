"""Tests for display-currency conversion on price analytics (EPR-93)."""

import copy
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.price_analytics_service import (
    PriceAnalyticsService,
    _bucket_bounds,
    apply_capture_rate_conversion,
    apply_compare_conversion,
    apply_revenue_metrics_conversion,
)


def _capture_rate_payload():
    return {
        "windfarm_id": 1,
        "windfarm_name": "Hamnefjell",
        "aggregation": "month",
        "native_currency": "EUR",
        "currency": "EUR",
        "display_currency": "NOK",
        "exchange_rate_used": None,
        "overall": {
            "total_generation_mwh": 30000.0,
            "total_revenue_eur": 1700000.0,
            "achieved_price": 1700000.0 / 30000.0,
            "market_average_price": 60.0,
            "capture_rate": 0.94,
        },
        "periods": [
            {
                "period": "2025-05-01T00:00:00+00:00",
                "total_generation_mwh": 10000.0,
                "revenue_eur": 500000.0,
                "achieved_price": 50.0,
                "market_average_price": 55.0,
                "hours_in_period": 744,
                "capture_rate": 0.91,
            },
            {
                "period": "2025-06-01T00:00:00+00:00",
                "total_generation_mwh": 20000.0,
                "revenue_eur": 1200000.0,
                "achieved_price": 60.0,
                "market_average_price": None,
                "hours_in_period": 720,
                "capture_rate": None,
            },
        ],
    }


class TestBucketBounds:
    def test_month_bucket(self):
        start, end = _bucket_bounds(
            "2025-06-01T00:00:00+00:00", "month", date(2025, 1, 1), date(2025, 12, 31)
        )
        assert start == date(2025, 6, 1)
        assert end == date(2025, 6, 30)

    def test_month_bucket_clamped_to_range_end(self):
        start, end = _bucket_bounds(
            "2025-06-01T00:00:00+00:00", "month", date(2025, 1, 1), date(2025, 6, 15)
        )
        assert start == date(2025, 6, 1)
        assert end == date(2025, 6, 15)

    def test_year_bucket(self):
        start, end = _bucket_bounds(
            "2024-01-01T00:00:00+00:00", "year", date(2024, 3, 1), date(2025, 12, 31)
        )
        assert start == date(2024, 3, 1)  # clamped to range start
        assert end == date(2024, 12, 31)

    def test_inverted_clamp_falls_back_to_bucket_bounds(self):
        # A DATE_TRUNC bucket that lies just before the UTC query range
        # (session-timezone spill) must not produce an inverted window.
        start, end = _bucket_bounds(
            "2023-12-01T00:00:00+00:00", "month", date(2024, 1, 1), date(2025, 1, 1)
        )
        assert start == date(2023, 12, 1)
        assert end == date(2023, 12, 31)
        assert start <= end

    def test_fine_aggregation_uses_full_range(self):
        start, end = _bucket_bounds(
            "2025-06-01T00:00:00+00:00", "day", date(2025, 1, 1), date(2025, 12, 31)
        )
        assert (start, end) == (date(2025, 1, 1), date(2025, 12, 31))


class TestCaptureRateConversion:
    def test_monetary_fields_scale_and_invariants_hold(self):
        payload = _capture_rate_payload()
        original = copy.deepcopy(payload)
        rates = [Decimal("11.5"), Decimal("11.7")]
        apply_capture_rate_conversion(payload, rates, Decimal("11.6"))

        # Periods: monetary values scaled by their own bucket rate
        assert payload["periods"][0]["revenue_eur"] == pytest.approx(500000.0 * 11.5)
        assert payload["periods"][0]["achieved_price"] == pytest.approx(50.0 * 11.5)
        assert payload["periods"][0]["market_average_price"] == pytest.approx(55.0 * 11.5)
        assert payload["periods"][1]["revenue_eur"] == pytest.approx(1200000.0 * 11.7)
        assert payload["periods"][1]["market_average_price"] is None

        # Currency-invariant fields untouched
        for i in range(2):
            assert (
                payload["periods"][i]["capture_rate"]
                == original["periods"][i]["capture_rate"]
            )
            assert (
                payload["periods"][i]["total_generation_mwh"]
                == original["periods"][i]["total_generation_mwh"]
            )
            assert (
                payload["periods"][i]["hours_in_period"]
                == original["periods"][i]["hours_in_period"]
            )
        assert payload["overall"]["capture_rate"] == original["overall"]["capture_rate"]
        assert (
            payload["overall"]["total_generation_mwh"]
            == original["overall"]["total_generation_mwh"]
        )

        # Overall total = sum of converted periods; achieved = total / generation
        expected_total = round(500000.0 * 11.5 + 1200000.0 * 11.7, 2)
        assert payload["overall"]["total_revenue_eur"] == pytest.approx(expected_total)
        assert payload["overall"]["achieved_price"] == pytest.approx(
            expected_total / 30000.0
        )
        # Overall market average uses the full-range rate
        assert payload["overall"]["market_average_price"] == pytest.approx(60.0 * 11.6)

    def test_revenue_metrics_conversion(self):
        payload = {
            "periods": [
                {
                    "period": "2025-06-01T00:00:00+00:00",
                    "total_generation_mwh": 1000.0,
                    "day_ahead_revenue_eur": 40000.0,
                    "total_revenue_eur": 41000.0,
                    "avg_day_ahead_price": 40.0,
                    "avg_intraday_price": None,
                    "hours_with_generation": 500,
                }
            ]
        }
        apply_revenue_metrics_conversion(payload, [Decimal("11.5")])
        p = payload["periods"][0]
        assert p["day_ahead_revenue_eur"] == pytest.approx(40000.0 * 11.5)
        assert p["total_revenue_eur"] == pytest.approx(41000.0 * 11.5)
        assert p["avg_day_ahead_price"] == pytest.approx(40.0 * 11.5)
        assert p["avg_intraday_price"] is None
        assert p["total_generation_mwh"] == 1000.0
        assert p["hours_with_generation"] == 500

    def test_compare_conversion(self):
        payload = {
            "windfarms": [
                {
                    "windfarm_id": 1,
                    "capture_rate": 0.95,
                    "achieved_price": 50.0,
                    "market_average_price": 52.0,
                    "total_generation_mwh": 100.0,
                    "total_revenue_eur": 5000.0,
                },
                {
                    "windfarm_id": 2,
                    "capture_rate": None,
                    "achieved_price": None,
                    "market_average_price": None,
                    "total_generation_mwh": 0.0,
                    "total_revenue_eur": 0.0,
                },
            ]
        }
        apply_compare_conversion(payload, Decimal("0.85"))
        wf = payload["windfarms"][0]
        assert wf["achieved_price"] == pytest.approx(50.0 * 0.85)
        assert wf["market_average_price"] == pytest.approx(52.0 * 0.85)
        assert wf["total_revenue_eur"] == pytest.approx(5000.0 * 0.85)
        assert wf["capture_rate"] == 0.95
        assert payload["windfarms"][1]["achieved_price"] is None


class TestResolveRates:
    @pytest.mark.asyncio
    async def test_missing_rate_returns_none(self):
        service = PriceAnalyticsService(MagicMock())
        with patch(
            "app.services.price_analytics_service.ExchangeRateService"
        ) as mock_cls:
            mock_cls.return_value.get_rate_for_period = AsyncMock(return_value=None)
            result = await service._resolve_rates(
                "EUR",
                "NOK",
                _capture_rate_payload()["periods"],
                "month",
                date(2025, 5, 1),
                date(2025, 6, 30),
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_rates_memoised_per_bucket(self):
        service = PriceAnalyticsService(MagicMock())
        mock_get = AsyncMock(return_value=Decimal("11.5"))
        with patch(
            "app.services.price_analytics_service.ExchangeRateService"
        ) as mock_cls:
            mock_cls.return_value.get_rate_for_period = mock_get
            result = await service._resolve_rates(
                "EUR",
                "NOK",
                _capture_rate_payload()["periods"],
                "month",
                date(2025, 5, 1),
                date(2025, 6, 30),
            )
        assert result is not None
        period_rates, overall_rate = result
        assert period_rates == [Decimal("11.5"), Decimal("11.5")]
        assert overall_rate == Decimal("11.5")
        # full range + May bucket + June bucket = 3 distinct windows
        assert mock_get.await_count == 3
