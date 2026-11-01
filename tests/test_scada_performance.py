"""EPR-203 SCADA Performance page: the pure rules and the endpoint wiring.

The SQL runs against Postgres-only `scada.*` tables, so numbers are checked live on
staging; here the rules that turn sums into bars, the league and the P50 estimate are
pinned without a database, and the endpoints are wired with the service faked.
"""

from datetime import date

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints import scada as endpoint_module
from app.core.deps import get_current_active_user, get_db
from app.services import scada_performance_service as sps
from app.services import scada_service
from app.services.performance_analytics_service import PerformanceAnalyticsService
from app.services.scada_performance_service import Sums


def s(actual, potential, perf=144, reporting=144, ws=8.0, ws_n=144):
    return Sums(actual, potential, perf, reporting, ws * ws_n, ws_n)


# ─── baseline, index and gain ───────────────────────────────────────────────


def test_baseline_rescales_the_farm_long_run_index_to_100():
    days = [s(900.0, 1000.0), s(1100.0, 1000.0), s(940.0, 1000.0)]
    total = days[0] + days[1] + days[2]
    b = sps.baseline(total.actual_kwh, total.potential_kwh)
    assert b == pytest.approx(0.98)
    assert sps.index_point(total, b, "month")["index"] == pytest.approx(100.0)


def test_index_gain_and_expected_follow_the_definitions():
    p = sps.index_point(s(1050.0, 1000.0), 0.98, "month")
    expected = 1000.0 * 0.98
    assert p["expected_mwh"] == pytest.approx(expected / 1000)
    assert p["index"] == pytest.approx(1050.0 / expected * 100, abs=0.01)
    assert p["gain_mwh"] == pytest.approx((1050.0 - expected) / 1000, abs=1e-3)
    assert p["wind_ms"] == pytest.approx(8.0)


def test_low_coverage_bar_is_a_gap_but_keeps_its_wind():
    # 60 of 144 reporting intervals were performance: 42 % < the daily 50 %
    p = sps.index_point(s(400.0, 420.0, perf=60, reporting=144), 1.0, "day")
    assert p["index"] is None and p["gain_mwh"] is None
    assert p["coverage"] == pytest.approx(60 / 144, abs=1e-3)
    assert p["wind_ms"] == pytest.approx(8.0)
    # the same share is enough for a month (30 %)
    assert sps.index_point(s(400.0, 420.0, perf=60, reporting=144), 1.0, "month")["index"]


def test_no_baseline_or_potential_gives_no_index():
    assert sps.baseline(0.0, 0.0) is None
    assert sps.index_point(s(10.0, 0.0), 1.0, "month")["index"] is None
    assert sps.index_point(s(10.0, 10.0), None, "month")["index"] is None


# ─── windows ────────────────────────────────────────────────────────────────


def test_window_is_whole_months_ending_on_the_last_data_day():
    first, last = date(2015, 12, 31), date(2026, 4, 30)
    assert sps.resolve_window("24m", first, last) == (date(2024, 5, 1), last)
    assert sps.resolve_window("12m", first, last) == (date(2025, 5, 1), last)
    assert sps.resolve_window("all", first, last) == (first, last)
    # a short history clamps to the first data day
    assert sps.resolve_window("5y", date(2024, 6, 15), last) == (date(2024, 6, 15), last)


def test_daily_view_is_the_latest_three_months():
    assert sps.daily_start(date(2024, 5, 1), date(2026, 4, 30)) == date(2026, 2, 1)
    assert sps.daily_start(date(2026, 3, 10), date(2026, 4, 30)) == date(2026, 3, 10)


def test_years_are_ratio_of_sums_and_flag_partial_years_at_both_ends():
    start, end = sps.resolve_window("24m", date(2015, 12, 31), date(2026, 4, 30))
    months = sps.months_in_window(start, end)
    monthly = {m: s(100.0 if m[1] % 2 else 300.0, 200.0) for m in months}
    years = sps.roll_up_years(monthly, months)
    assert [(y, n) for y, _, n in years] == [(2024, 8), (2025, 12), (2026, 4)]
    y2025 = years[1][1]
    assert y2025.actual_kwh == pytest.approx(6 * 100.0 + 6 * 300.0)  # sums, not mean of ratios
    assert y2025.potential_kwh == pytest.approx(12 * 200.0)


# ─── league ─────────────────────────────────────────────────────────────────


def test_league_ranks_worst_first_and_nets_the_fleet():
    out = sps.league(
        {"T01": s(950.0, 1000.0), "T02": s(1030.0, 1000.0), "T03": s(990.0, 1000.0)}, 1.0
    )
    assert [r["t"] for r in out["turbines"]] == ["T01", "T03", "T02"]
    assert [r["gain_mwh"] for r in out["turbines"]] == [-0.05, -0.01, 0.03]
    assert out["fleet_net_mwh"] == pytest.approx(-0.03)
    assert out["n_above"] == 1 and out["n_turbines"] == 3


def test_league_skips_turbines_without_potential():
    out = sps.league({"T01": s(0.0, 0.0), "T02": s(10.0, 10.0)}, 1.0)
    assert [r["t"] for r in out["turbines"]] == ["T02"]


# ─── P50 ────────────────────────────────────────────────────────────────────


def test_estimate_uses_complete_years_only():
    full = 365 * 144 * 2  # two turbines, every interval known
    yearly = {
        2022: (10e6, full),
        2023: (12e6, int(full * 0.995)),
        2024: (30e6, int(366 * 144 * 2 * 0.90)),  # 90 % covered: rejected
    }
    annual, years = sps.estimate_annual_gwh(yearly, n_turbines=2)
    assert years == [2022, 2023]
    assert annual == pytest.approx(11.0)


def test_estimate_needs_two_complete_years():
    assert sps.estimate_annual_gwh({2022: (10e6, 365 * 144)}, 1) == (None, [])


def test_p50_months_drop_partial_first_and_last_months():
    start, end = date(2024, 5, 15), date(2026, 4, 20)
    months = sps.p50_months(sps.months_in_window(start, end), start, end)
    assert months[0] == (2024, 6) and months[-1] == (2026, 3)
    whole = sps.p50_months(
        sps.months_in_window(date(2024, 5, 1), date(2026, 4, 30)),
        date(2024, 5, 1),
        date(2026, 4, 30),
    )
    assert whole[0] == (2024, 5) and whole[-1] == (2026, 4)


def test_operator_target_turbine_share_feeds_monthly_p50():
    targets = [{"start": date(2010, 1, 1), "end": None, "annual_gwh": 120.0, "source": "x"}]
    share = 2300.0 / 48300.0
    scaled = [{**t, "annual_gwh": t["annual_gwh"] * share} for t in targets]
    by_month, _ = PerformanceAnalyticsService.monthly_p50(scaled, [(2025, 1)])
    assert by_month[(2025, 1)] == pytest.approx(120.0 * share / 12)


def test_rolling_needs_nine_of_twelve_months():
    hist = {(2025, m): 100.0 + m for m in range(1, 9)}  # 8 months
    assert PerformanceAnalyticsService.rolling_mean(hist, [(2025, 8)])[(2025, 8)] is None
    hist[(2025, 9)] = 110.0
    assert PerformanceAnalyticsService.rolling_mean(hist, [(2025, 9)])[(2025, 9)] is not None


# ─── endpoint wiring ────────────────────────────────────────────────────────


class _FakeUser:
    id = 42
    role = "client"
    is_active = True
    is_superuser = False


@pytest.fixture
def app_client(monkeypatch):
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/scada")

    async def _db():
        yield None

    async def _present(_db):
        return True

    async def _slugs(self):
        return ["hill_of_towie", "kelmarsh"]

    async def _rated(self, farm, turbine):
        return 2300.0 if turbine == "T01" else None

    monkeypatch.setattr(endpoint_module, "scada_schema_present", _present)
    monkeypatch.setattr(scada_service.ScadaService, "farm_slugs", _slugs)
    monkeypatch.setattr(sps.ScadaPerformanceService, "turbine_rated_kw", _rated)
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_active_user] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c


def test_bad_period_or_granularity_is_422(app_client):
    assert app_client.get("/scada/performance", params={"period": "7m"}).status_code == 422
    assert app_client.get("/scada/performance", params={"granularity": "week"}).status_code == 422
    assert app_client.get("/scada/performance-league", params={"period": "x"}).status_code == 422


def test_unknown_farm_or_turbine_is_404(app_client):
    assert app_client.get("/scada/performance", params={"farm": "nope"}).status_code == 404
    resp = app_client.get("/scada/performance", params={"turbine": "T99"})
    assert resp.status_code == 404


def test_missing_columns_is_503(app_client, monkeypatch):
    async def _missing(self, *a, **k):
        raise sps.PerformanceColumnsMissing()

    monkeypatch.setattr(sps.ScadaPerformanceService, "performance", _missing)
    monkeypatch.setattr(sps.ScadaPerformanceService, "league", _missing)
    assert app_client.get("/scada/performance").status_code == 503
    assert app_client.get("/scada/performance-league").status_code == 503


def test_parameters_reach_the_service(app_client, monkeypatch):
    seen = {}

    async def _perf(self, farm, period, granularity, turbine=None):
        seen.update(farm=farm, period=period, granularity=granularity, turbine=turbine)
        return {"periods": []}

    monkeypatch.setattr(sps.ScadaPerformanceService, "performance", _perf)
    resp = app_client.get(
        "/scada/performance",
        params={"farm": "kelmarsh", "period": "3y", "granularity": "year", "turbine": "T01"},
    )
    assert resp.status_code == 200
    assert seen == {"farm": "kelmarsh", "period": "3y", "granularity": "year", "turbine": "T01"}
