"""Unit tests for tests.utils.snapshot_diff."""

import copy

from tests.utils.snapshot_diff import diff_snapshots


def _sample_snapshot() -> dict:
    """A minimal snapshot with one row per table — enough to exercise the diff logic."""
    return {
        "windfarm": {"code": "TESTWF", "id": 999, "nameplate_capacity_mw": 100.0},
        "captured_at": "2026-05-24T00:00:00Z",
        "power_curve_bins": [
            {
                "curve_type": "overall_clean",
                "year": None,
                "wind_bin": 10.0,
                "q50_pu": 0.5,
                "q90_pu": 0.7,
                "mean_pu": 0.5,
                "mad_pu": 0.05,
                "sample_count": 800,
            },
        ],
        "degradation_results": [
            {
                "reference_curve": "q50",
                "slope_pu_per_year": 0.001,
                "slope_pct_per_year": 0.3,
                "intercept": -2.0,
                "r_squared": 0.05,
                "p_value": 0.5,
                "ci_lower_95": -0.001,
                "ci_upper_95": 0.003,
                "baseline_cap_pu": 0.35,
                "data_points": 48,
                "analysis_start": "2022-01-01",
                "analysis_end": "2024-12-31",
                "pipeline_run_id": None,
            },
        ],
        "performance_summaries": [
            {
                "period_type": "year",
                "year": 2024,
                "month": None,
                "odi_pct_loss_mwh": 2.5,
                "odi_pct_loss_eur": 2.0,
                "lost_mwh": 1000.0,
                "lost_eur": 30000.0,
                "norm_index_p50": 100.0,
                "norm_index_p10": 100.0,
                "constraint_proxy_mwh": 5000.0,
                "lost_value_eur": 150000.0,
                "total_hours": 8784,
                "underperf_hours": 200,
                "overperf_hours": 10,
                "odi_pct_underperf": 2.3,
                "expected_mwh": 40000.0,
                "expected_revenue_eur": 1500000.0,
                "long_run_count": 2,
                "max_run_hours": 48,
                "norm_ratio_p50": 1.0,
                "norm_ratio_p10": 1.0,
            },
        ],
    }


def test_identical_snapshots_are_all_in_tolerance():
    snap = _sample_snapshot()
    report = diff_snapshots(snap, copy.deepcopy(snap))
    assert report.all_in_tolerance, report.to_dict()
    assert report.total_out_of_tolerance == 0
    assert report.total_cells_compared > 0


def test_tiny_drift_inside_tolerance():
    pre = _sample_snapshot()
    post = copy.deepcopy(pre)
    # tiny drifts well within configured tolerances
    post["degradation_results"][0]["slope_pu_per_year"] = 0.0010001  # +0.01%
    post["performance_summaries"][0]["norm_index_p50"] = 100.001     # +0.001%
    post["power_curve_bins"][0]["q50_pu"] = 0.5001                   # +0.02%
    report = diff_snapshots(pre, post)
    assert report.all_in_tolerance, report.to_dict()


def test_baseline_shift_flagged_as_out_of_tolerance():
    """Module 5 Bug C: baseline_cap_pu shifts from 0.35 → 0.27 (Lutelandet post-fix).

    Slope_pct will swing with it. Both should be flagged.
    """
    pre = _sample_snapshot()
    post = copy.deepcopy(pre)
    post["degradation_results"][0]["baseline_cap_pu"] = 0.27   # -23%
    post["degradation_results"][0]["slope_pct_per_year"] = 0.4  # +33%

    report = diff_snapshots(pre, post)
    assert not report.all_in_tolerance
    assert report.tables["degradation_results"].out_of_tolerance_count >= 2
    # slope_pu unchanged → should still be in tolerance
    slope_pu_diff = next(
        c
        for c in report.tables["degradation_results"].cell_diffs
        if c.column == "slope_pu_per_year"
    )
    assert slope_pu_diff.in_tolerance


def test_row_added_and_removed():
    pre = _sample_snapshot()
    post = copy.deepcopy(pre)
    post["power_curve_bins"].append(
        {
            "curve_type": "overall_clean",
            "year": None,
            "wind_bin": 11.0,
            "q50_pu": 0.6,
            "q90_pu": 0.8,
            "mean_pu": 0.6,
            "mad_pu": 0.05,
            "sample_count": 700,
        }
    )
    post["performance_summaries"] = []  # removed

    report = diff_snapshots(pre, post)
    assert len(report.tables["power_curve_bins"].rows_added) == 1
    assert len(report.tables["performance_summaries"].rows_removed) == 1
    assert not report.all_in_tolerance


def test_mismatched_windfarm_raises():
    pre = _sample_snapshot()
    post = copy.deepcopy(pre)
    post["windfarm"]["code"] = "OTHERWF"
    try:
        diff_snapshots(pre, post)
    except ValueError as exc:
        assert "windfarm mismatch" in str(exc).lower()
    else:
        raise AssertionError("expected ValueError")
