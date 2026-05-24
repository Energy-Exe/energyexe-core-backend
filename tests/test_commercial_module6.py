"""Unit tests for Module 6 commercial logic — D1 (contract revenue),
D2 (PPA scenario uplift vs base).

The Module 6 functions in PerformancePipelineService that touch the DB
(`_compute_commercial_metrics`, `run_ppa_scenarios`) are integration-shaped
so we test the pure math via small helpers replicating the spec formulas
(`energyexe_pipeline_full.py:1150-1194`).
"""

import math

import numpy as np
import pandas as pd
import pytest

# ─── D1 helpers ─────────────────────────────────────────────


def compute_contract_revenue(generation_mwh: pd.Series, price: pd.Series) -> float:
    """Reference replica: contract_revenue = sum(gen_mwh × price)."""
    return float((generation_mwh * price).sum(skipna=True))


def compute_contract_revenue_vs_p50(
    contract_revenue: float, p50_mwh: float, avg_price: float
) -> float:
    """target_revenue = p50_mwh × avg_price; result = contract - target."""
    return contract_revenue - p50_mwh * avg_price


class TestD1ContractRevenue:
    def test_revenue_with_constant_ppa(self):
        """Fixed PPA price: revenue = sum(gen) × ppa."""
        gen = pd.Series([10.0, 20.0, 30.0])
        price = pd.Series([50.0, 50.0, 50.0])
        rev = compute_contract_revenue(gen, price)
        assert math.isclose(rev, (10 + 20 + 30) * 50)

    def test_revenue_with_spot_prices(self):
        """Hourly market price: revenue = Σ gen × price."""
        gen = pd.Series([10.0, 20.0, 30.0])
        price = pd.Series([40.0, 50.0, 60.0])
        rev = compute_contract_revenue(gen, price)
        # 10×40 + 20×50 + 30×60 = 400 + 1000 + 1800 = 3200
        assert math.isclose(rev, 3200.0)

    def test_revenue_with_nan_price_filled(self):
        """NaN price filled with mean — matches spec :270-280."""
        gen = pd.Series([10.0, 20.0, 30.0])
        price = pd.Series([40.0, np.nan, 60.0])
        mean = float(price.mean())  # 50.0
        filled = price.fillna(mean)
        rev = compute_contract_revenue(gen, filled)
        # 10×40 + 20×50 + 30×60 = 3200 (NaN replaced with 50)
        assert math.isclose(rev, 3200.0)

    def test_revenue_vs_p50_target(self):
        """vs_p50 = revenue - (p50_mwh × avg_price)."""
        contract = 3200.0
        p50_mwh = 50.0
        avg_price = 50.0
        result = compute_contract_revenue_vs_p50(contract, p50_mwh, avg_price)
        assert math.isclose(result, 3200 - 50 * 50)  # 700.0


# ─── D2 helpers ─────────────────────────────────────────────


def build_ppa_scenarios(actual_mwh: float, p50_mwh: float, prices, base_price=None):
    """Reference replica of `PerformancePipelineService.run_ppa_scenarios`
    minus the DB lookups.
    """
    base_revenue = None
    if base_price is not None and base_price in prices:
        base_revenue = actual_mwh * float(base_price)

    scenarios = []
    for p in prices:
        revenue = actual_mwh * p
        scenarios.append(
            {
                "ppa_eur_per_mwh": p,
                "revenue_eur": revenue,
                "is_base": base_price is not None and p == base_price,
                "revenue_uplift_vs_base_eur": (
                    revenue - base_revenue if base_revenue is not None else None
                ),
            }
        )
    return scenarios


class TestD2PpaScenarios:
    def test_base_marked_and_uplift_populated(self):
        scenarios = build_ppa_scenarios(
            actual_mwh=100.0,
            p50_mwh=80.0,
            prices=[30.0, 40.0, 50.0],
            base_price=40.0,
        )
        base = [s for s in scenarios if s["is_base"]]
        assert len(base) == 1
        assert base[0]["ppa_eur_per_mwh"] == 40.0
        assert base[0]["revenue_uplift_vs_base_eur"] == 0.0
        # Higher price → positive uplift
        s50 = [s for s in scenarios if s["ppa_eur_per_mwh"] == 50.0][0]
        assert s50["revenue_uplift_vs_base_eur"] == (100 * 50) - (100 * 40)
        # Lower price → negative uplift
        s30 = [s for s in scenarios if s["ppa_eur_per_mwh"] == 30.0][0]
        assert s30["revenue_uplift_vs_base_eur"] == (100 * 30) - (100 * 40)

    def test_no_base_when_price_not_in_scenarios(self):
        scenarios = build_ppa_scenarios(
            actual_mwh=100.0,
            p50_mwh=80.0,
            prices=[30.0, 40.0, 50.0],
            base_price=42.0,  # not in list
        )
        assert all(not s["is_base"] for s in scenarios)
        assert all(s["revenue_uplift_vs_base_eur"] is None for s in scenarios)

    def test_no_base_when_windfarm_has_no_ppa(self):
        scenarios = build_ppa_scenarios(
            actual_mwh=100.0,
            p50_mwh=80.0,
            prices=[30.0, 40.0, 50.0],
            base_price=None,
        )
        assert all(not s["is_base"] for s in scenarios)
        assert all(s["revenue_uplift_vs_base_eur"] is None for s in scenarios)
