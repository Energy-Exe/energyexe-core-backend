"""Unit tests for PeerAggregateService._summarise (the pure-statistics core).

Validates that the percentile + mean computation matches numpy's defaults so
the cached `peer_group_aggregates` rows are interpretable in the same way as
ad-hoc analysis. No DB access, no async fixtures.
"""

import math

import numpy as np
import pytest

from app.services.peer_aggregate_service import (
    METRIC_SOURCES,
    SUPPORTED_GROUP_TYPES,
    PeerAggregateService,
)


class TestSummarise:
    def test_empty_returns_none_stats(self):
        s = PeerAggregateService._summarise([])
        assert s == {"n": 0, "avg": None, "p10": None, "p50": None, "p90": None}

    def test_single_value(self):
        s = PeerAggregateService._summarise([5.0])
        assert s["n"] == 1
        assert s["avg"] == 5.0
        # All percentiles equal the single value
        assert s["p10"] == s["p50"] == s["p90"] == 5.0

    def test_known_distribution_matches_numpy(self):
        values = list(range(1, 101))  # 1..100
        s = PeerAggregateService._summarise(values)
        assert s["n"] == 100
        # numpy default linear interpolation
        assert math.isclose(s["avg"], float(np.mean(values)), abs_tol=1e-3)
        assert math.isclose(s["p10"], float(np.percentile(values, 10)), abs_tol=1e-3)
        assert math.isclose(s["p50"], float(np.percentile(values, 50)), abs_tol=1e-3)
        assert math.isclose(s["p90"], float(np.percentile(values, 90)), abs_tol=1e-3)

    def test_unsorted_input_handled(self):
        s_sorted = PeerAggregateService._summarise([1.0, 2.0, 3.0, 4.0, 5.0])
        s_shuffled = PeerAggregateService._summarise([3.0, 1.0, 5.0, 2.0, 4.0])
        assert s_sorted == s_shuffled

    def test_negative_values(self):
        s = PeerAggregateService._summarise([-5.0, -3.0, 0.0, 3.0, 5.0])
        assert s["avg"] == 0.0
        assert s["p50"] == 0.0
        # p10 = first value, p90 = last
        assert s["p10"] < 0
        assert s["p90"] > 0


class TestRegistry:
    def test_supported_group_types_present(self):
        for g in ("bidzone", "country", "owner", "turbine_model"):
            assert g in SUPPORTED_GROUP_TYPES

    def test_metric_sources_cover_all_modules(self):
        # ODI metrics from Module 3
        assert "odi_pct_underperf" in METRIC_SOURCES
        assert "odi_pct_loss_mwh" in METRIC_SOURCES
        assert "odi_pct_loss_eur" in METRIC_SOURCES
        # Wind normalisation Module 4
        assert "wind_norm_index_p50" in METRIC_SOURCES
        assert "wind_norm_index_p10" in METRIC_SOURCES
        # Degradation Module 5
        assert "degradation_slope_pct_per_year_q50" in METRIC_SOURCES
        assert "degradation_slope_pct_per_year_q90" in METRIC_SOURCES
        # Generation Concentration spec item 3
        assert "concentration_capture_ratio" in METRIC_SOURCES

    def test_metric_sources_have_table_column_tuples(self):
        for k, v in METRIC_SOURCES.items():
            assert isinstance(v, tuple), f"{k} source must be a tuple"
            assert len(v) == 3, f"{k} source must be (table, column, ref_filter)"
            table, column, _ = v
            assert isinstance(table, str) and table
            assert isinstance(column, str) and column

    def test_validate_rejects_unknown_group_type(self):
        svc = PeerAggregateService(db=None)
        with pytest.raises(ValueError, match="Unsupported group_type"):
            svc._validate("unknown_type", "odi_pct_underperf")

    def test_validate_rejects_unknown_metric_key(self):
        svc = PeerAggregateService(db=None)
        with pytest.raises(ValueError, match="Unknown metric_key"):
            svc._validate("bidzone", "made_up_metric")
