"""EPR-213: the power-curve payload carries every turbine's curve and the farm median."""

from app.services.scada_service import power_curve_payload

ROWS = [
    # (turbine, year, ws_bin, mean kW)
    ("T01", 2024, 5.0, 300.0),
    ("T01", 2025, 5.0, 310.0),
    ("T01", 2025, 6.0, 500.0),
    ("T02", 2025, 5.0, 330.0),
    ("T02", 2025, 6.0, 540.0),
    ("T03", 2025, 5.0, 400.0),
]


def test_picked_turbine_years_and_mean_reference_unchanged():
    out = power_curve_payload(ROWS, turbine="T01", year=2025, rated_kw=2300.0)
    assert [y["year"] for y in out["years"]] == [2024, 2025]
    assert out["years"][1]["bins"] == [{"ws": 5.0, "p": 310.0}, {"ws": 6.0, "p": 500.0}]
    # mean of T01, T02, T03 at 5 m/s; T01 and T02 at 6 m/s
    assert out["farm_ref"] == [{"ws": 5.0, "p": 346.7}, {"ws": 6.0, "p": 520.0}]
    assert out["rated_kw"] == 2300.0


def test_every_turbine_and_the_median_for_the_year():
    out = power_curve_payload(ROWS, turbine="T01", year=2025, rated_kw=None)
    assert sorted(out["turbines"]) == ["T01", "T02", "T03"]
    assert out["turbines"]["T03"] == [{"ws": 5.0, "p": 400.0}]
    # 2024 rows never reach the whole-farm view
    assert all(b["p"] != 300.0 for b in out["turbines"]["T01"])
    assert out["farm_median"] == [{"ws": 5.0, "p": 330.0}, {"ws": 6.0, "p": 520.0}]
