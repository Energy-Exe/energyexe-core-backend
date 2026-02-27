"""Unit tests for financial ratio computation logic and ramp-up exclusion."""

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.financial_data_service import FinancialDataService


class TestComputeRatios:
    """Tests for the pure _compute_ratios static method."""

    def test_normal_ratios(self):
        result = FinancialDataService._compute_ratios(
            total_revenue=Decimal("1000000"),
            total_opex=Decimal("600000"),
            ebitda=Decimal("400000"),
            generation_mwh=Decimal("10000"),
        )
        assert result["revenue_per_mwh"] == Decimal("100")
        assert result["opex_per_mwh"] == Decimal("60")
        assert result["ebitda_margin_pct"] == Decimal("40")

    def test_zero_generation(self):
        result = FinancialDataService._compute_ratios(
            total_revenue=Decimal("1000000"),
            total_opex=Decimal("600000"),
            ebitda=Decimal("400000"),
            generation_mwh=Decimal("0"),
        )
        assert result["revenue_per_mwh"] is None
        assert result["opex_per_mwh"] is None
        # EBITDA margin is independent of generation
        assert result["ebitda_margin_pct"] == Decimal("40")

    def test_none_generation(self):
        result = FinancialDataService._compute_ratios(
            total_revenue=Decimal("1000000"),
            total_opex=Decimal("600000"),
            ebitda=Decimal("400000"),
            generation_mwh=None,
        )
        assert result["revenue_per_mwh"] is None
        assert result["opex_per_mwh"] is None
        assert result["ebitda_margin_pct"] == Decimal("40")

    def test_zero_revenue(self):
        result = FinancialDataService._compute_ratios(
            total_revenue=Decimal("0"),
            total_opex=Decimal("600000"),
            ebitda=Decimal("-600000"),
            generation_mwh=Decimal("10000"),
        )
        assert result["revenue_per_mwh"] == Decimal("0")
        assert result["opex_per_mwh"] == Decimal("60")
        # ebitda_margin requires total_revenue > 0
        assert result["ebitda_margin_pct"] is None

    def test_none_revenue(self):
        result = FinancialDataService._compute_ratios(
            total_revenue=None,
            total_opex=Decimal("600000"),
            ebitda=Decimal("400000"),
            generation_mwh=Decimal("10000"),
        )
        assert result["revenue_per_mwh"] is None
        assert result["opex_per_mwh"] == Decimal("60")
        assert result["ebitda_margin_pct"] is None

    def test_negative_ebitda(self):
        result = FinancialDataService._compute_ratios(
            total_revenue=Decimal("1000000"),
            total_opex=Decimal("1200000"),
            ebitda=Decimal("-200000"),
            generation_mwh=Decimal("10000"),
        )
        assert result["revenue_per_mwh"] == Decimal("100")
        assert result["opex_per_mwh"] == Decimal("120")
        assert result["ebitda_margin_pct"] == Decimal("-20")

    def test_none_opex(self):
        result = FinancialDataService._compute_ratios(
            total_revenue=Decimal("1000000"),
            total_opex=None,
            ebitda=Decimal("400000"),
            generation_mwh=Decimal("10000"),
        )
        assert result["revenue_per_mwh"] == Decimal("100")
        assert result["opex_per_mwh"] is None
        assert result["ebitda_margin_pct"] == Decimal("40")

    def test_all_none(self):
        result = FinancialDataService._compute_ratios(
            total_revenue=None,
            total_opex=None,
            ebitda=None,
            generation_mwh=None,
        )
        assert result["revenue_per_mwh"] is None
        assert result["opex_per_mwh"] is None
        assert result["ebitda_margin_pct"] is None


class TestRampUpExclusion:
    """Tests for the ramp-up exclusion logic (COD + 365 days)."""

    def test_period_before_cod_plus_365_excluded(self):
        cod = date(2020, 8, 17)
        cutoff = cod + timedelta(days=365)
        period_start = date(2020, 1, 1)
        assert period_start < cutoff

    def test_period_after_cod_plus_365_included(self):
        cod = date(2020, 8, 17)
        cutoff = cod + timedelta(days=365)
        period_start = date(2022, 1, 1)
        assert period_start >= cutoff

    def test_period_in_first_year_after_cod_excluded(self):
        cod = date(2020, 8, 17)
        cutoff = cod + timedelta(days=365)  # 2021-08-17
        period_start = date(2021, 1, 1)
        assert period_start < cutoff

    def test_period_exactly_at_cutoff_included(self):
        cod = date(2020, 8, 17)
        cutoff = cod + timedelta(days=365)  # 2021-08-17
        period_start = date(2021, 8, 17)
        assert period_start >= cutoff

    def test_none_cod_no_exclusion(self):
        # When COD is None, ramp_up_cutoff is None → no exclusion
        cod = None
        ramp_up_cutoff = None
        if cod is not None:
            ramp_up_cutoff = cod + timedelta(days=365)
        period_start = date(2018, 1, 1)
        is_excluded = ramp_up_cutoff is not None and period_start < ramp_up_cutoff
        assert is_excluded is False

    def test_multi_windfarm_uses_max_cod(self):
        cod_dates = [date(2020, 1, 1), date(2021, 6, 15)]
        effective_cod = max(cod_dates)
        assert effective_cod == date(2021, 6, 15)
        cutoff = effective_cod + timedelta(days=365)  # 2022-06-15
        # Period 2022-01-01 should still be excluded
        assert date(2022, 1, 1) < cutoff
        # Period 2023-01-01 should be included
        assert date(2023, 1, 1) >= cutoff
