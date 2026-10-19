"""Pure helpers of the Comparison page service (EPR-188)."""

from app.services.comparison_analytics_service import cf_series, month_cf


def test_month_cf_percent_rounded():
    # 10 MW x 744 h = 7,440 MWh at 100 %; 2,976 MWh is 40 %.
    assert month_cf(2976.0, 10.0, 744, "2025-01") == 40.0


def test_month_cf_drops_poorly_covered_month():
    # January has 744 h; under half covered is left out, half is kept.
    assert month_cf(100.0, 10.0, 371, "2025-01") is None
    assert month_cf(100.0, 10.0, 372, "2025-01") is not None


def test_month_cf_drops_out_of_range_values():
    assert month_cf(-5.0, 10.0, 744, "2025-01") is None
    assert month_cf(8000.0, 10.0, 744, "2025-01") is None
    assert month_cf(100.0, None, 744, "2025-01") is None


def test_cf_series_aligns_to_months():
    months = ["2025-01", "2025-02", "2025-03"]
    by_month = {"2025-03": (2232.0, 744), "2025-01": (744.0, 744)}
    assert cf_series(months, by_month, 10.0) == [10.0, None, 30.0]
