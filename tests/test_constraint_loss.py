"""Unit tests for Module 3f constraint loss computation (issue #82).

Pure pandas/numpy; no DB.
"""

import numpy as np
import pandas as pd

from app.services.constraint_loss_service import ConstraintLossService

RATED_MW = 100.0
# overall_clean Q50: bin 8 expects 0.50 p.u. (= 50 MWh at 100 MW rated).
OVERALL_CLEAN_Q50 = {8.0: 0.50, 9.0: 0.60}


def _period_df(*, n=300, gen_mwh=25.0, wind=8.5, price=30.0):
    return pd.DataFrame(
        {
            "hour": pd.date_range("2024-06-01", periods=n, freq="h"),
            "wind_speed": np.full(n, wind),
            "generation_mwh": np.full(n, gen_mwh),
            "market_price": np.full(n, price),
        }
    )


class TestComputePeriodLoss:
    def test_half_output_constraint(self):
        """25 MWh actual vs 50 MWh expected → 25 MWh lost/hr, priced at 30."""
        loss = ConstraintLossService.compute_period_loss(
            _period_df(n=300, gen_mwh=25.0), OVERALL_CLEAN_Q50, RATED_MW
        )
        assert loss["duration_hours"] == 300
        assert loss["expected_mwh"] == 15000.0  # 50 * 300
        assert loss["actual_mwh"] == 7500.0  # 25 * 300
        assert loss["lost_mwh"] == 7500.0
        assert loss["lost_eur"] == 225000.0  # 7500 * 30

    def test_overperformance_clipped_to_zero(self):
        """Actual above expected must not produce negative loss."""
        loss = ConstraintLossService.compute_period_loss(
            _period_df(n=100, gen_mwh=55.0), OVERALL_CLEAN_Q50, RATED_MW
        )
        assert loss["lost_mwh"] == 0.0
        assert loss["lost_eur"] == 0.0

    def test_ppa_price_overrides_market(self):
        loss = ConstraintLossService.compute_period_loss(
            _period_df(n=10, gen_mwh=25.0, price=30.0),
            OVERALL_CLEAN_Q50,
            RATED_MW,
            ppa_price=50.0,
        )
        # 25 lost/hr * 10 hr * 50 = 12500
        assert loss["lost_eur"] == 12500.0

    def test_hours_outside_curve_bins_skipped(self):
        """A bin with no overall_clean value yields no reference → None."""
        df = _period_df(n=50, wind=20.5)  # bin 20 not in lookup
        loss = ConstraintLossService.compute_period_loss(df, OVERALL_CLEAN_Q50, RATED_MW)
        assert loss is None

    def test_empty_period_returns_none(self):
        loss = ConstraintLossService.compute_period_loss(
            pd.DataFrame(columns=["hour", "wind_speed", "generation_mwh", "market_price"]),
            OVERALL_CLEAN_Q50,
            RATED_MW,
        )
        assert loss is None

    def test_lost_eur_none_when_no_price(self):
        df = _period_df(n=20, gen_mwh=25.0)
        df["market_price"] = np.nan
        loss = ConstraintLossService.compute_period_loss(df, OVERALL_CLEAN_Q50, RATED_MW)
        assert loss["lost_mwh"] == 500.0  # 25 * 20
        assert loss["lost_eur"] is None
