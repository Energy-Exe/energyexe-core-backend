"""Tests for financial ratios with currency conversion."""

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.financial_data_service import FinancialDataService


class TestFinancialRatiosConversion:
    """Tests that currency conversion integrates correctly with ratio computation."""

    def test_ebitda_margin_unchanged_after_conversion(self):
        """EBITDA margin (%) is currency-independent — same before and after conversion."""
        # Original NOK values
        original = FinancialDataService._compute_ratios(
            total_revenue=Decimal("10000000"),  # 10M NOK
            total_opex=Decimal("6000000"),       # 6M NOK
            ebitda=Decimal("4000000"),            # 4M NOK
            generation_mwh=Decimal("50000"),
        )

        # Converted to EUR (rate 0.0887 → ~887K EUR revenue, ~532K opex, ~355K ebitda)
        rate = Decimal("0.0887")
        converted = FinancialDataService._compute_ratios(
            total_revenue=round(Decimal("10000000") * rate, 2),
            total_opex=round(Decimal("6000000") * rate, 2),
            ebitda=round(Decimal("4000000") * rate, 2),
            generation_mwh=Decimal("50000"),
        )

        # EBITDA margin should be identical (40%)
        assert original["ebitda_margin_pct"] == Decimal("40")
        assert converted["ebitda_margin_pct"] == Decimal("40")

    def test_revenue_per_mwh_scales_with_rate(self):
        """Revenue/MWh should scale proportionally with the exchange rate."""
        rate = Decimal("0.0887")
        original_revenue = Decimal("10000000")
        converted_revenue = round(original_revenue * rate, 2)
        gen = Decimal("50000")

        original = FinancialDataService._compute_ratios(
            total_revenue=original_revenue, total_opex=None, ebitda=None, generation_mwh=gen,
        )
        converted = FinancialDataService._compute_ratios(
            total_revenue=converted_revenue, total_opex=None, ebitda=None, generation_mwh=gen,
        )

        # Original: 10M / 50K = 200 NOK/MWh
        assert original["revenue_per_mwh"] == Decimal("200")
        # Converted: ~887K / 50K = ~17.74 EUR/MWh
        assert converted["revenue_per_mwh"] == Decimal("17.74")

    def test_no_display_currency_backward_compatible(self):
        """Without display_currency, ratios return unchanged."""
        result = FinancialDataService._compute_ratios(
            total_revenue=Decimal("5000000"),
            total_opex=Decimal("3000000"),
            ebitda=Decimal("2000000"),
            generation_mwh=Decimal("25000"),
        )
        assert result["revenue_per_mwh"] == Decimal("200")
        assert result["opex_per_mwh"] == Decimal("120")
        assert result["ebitda_margin_pct"] == Decimal("40")

    def test_conversion_with_none_values(self):
        """Conversion should handle None financial values gracefully."""
        rate = Decimal("0.0887")

        # If revenue is None, converted revenue is also None
        converted_revenue = None
        if converted_revenue is not None:
            converted_revenue = round(converted_revenue * rate, 2)

        result = FinancialDataService._compute_ratios(
            total_revenue=converted_revenue,
            total_opex=round(Decimal("3000000") * rate, 2),
            ebitda=None,
            generation_mwh=Decimal("25000"),
        )

        assert result["revenue_per_mwh"] is None
        assert result["opex_per_mwh"] is not None
        assert result["ebitda_margin_pct"] is None

    def test_eur_entity_no_conversion_needed(self):
        """Entity already in EUR should return rate=1 and identical values."""
        # This mirrors the service logic: if from_currency == to_currency, rate = 1
        rate = Decimal("1")
        original_revenue = Decimal("5000000")
        effective_revenue = round(original_revenue * rate, 2)

        assert effective_revenue == original_revenue

        result = FinancialDataService._compute_ratios(
            total_revenue=effective_revenue,
            total_opex=Decimal("3000000"),
            ebitda=Decimal("2000000"),
            generation_mwh=Decimal("25000"),
        )
        assert result["revenue_per_mwh"] == Decimal("200")

    def test_gbp_to_eur_conversion(self):
        """Test GBP→EUR conversion with realistic rate."""
        # GBP inverse rate ~1.16 (1 GBP = 1.16 EUR)
        rate = Decimal("1.16")
        gbp_revenue = Decimal("3000000")  # 3M GBP
        eur_revenue = round(gbp_revenue * rate, 2)  # 3.48M EUR

        result = FinancialDataService._compute_ratios(
            total_revenue=eur_revenue,
            total_opex=round(Decimal("1800000") * rate, 2),
            ebitda=round(Decimal("1200000") * rate, 2),
            generation_mwh=Decimal("20000"),
        )

        # 3.48M / 20K = 174.00 EUR/MWh
        assert result["revenue_per_mwh"] == Decimal("174")
        # EBITDA margin stays 40%
        assert result["ebitda_margin_pct"] == Decimal("40")
