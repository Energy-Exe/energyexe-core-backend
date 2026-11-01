"""EPR-202 SCADA Degradation page: the pure rules, the assembled payload and the endpoint.

The SQL reads Postgres-only ``scada.*`` tables, so the numbers are checked live on staging;
here the rules (full years, within-period slope, chaining, fleet trend, band, loss, price)
are pinned without a database, the payload is assembled from faked query results, and the
endpoint is wired with the service faked.
"""

from datetime import date
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints import scada as endpoint_module
from app.core.deps import get_current_active_user, get_db
from app.services import scada_degradation_service as sds
from app.services import scada_performance_service as sps
from app.services import scada_service
from app.services.scada_degradation_service import Obs, Period

TODAY = date(2026, 11, 1)


def full_known(year, turbines=1):
    return sds.days_in_year(year) * sds.INTERVALS_PER_DAY * turbines


# ─── full years and periods ─────────────────────────────────────────────────

HOT_PERIODS = [
    Period("baseline", date(2012, 3, 1), date(2021, 6, 1)),
    Period("aeroup", date(2021, 9, 1), None),
]


def test_whole_year_period_follows_the_config_dates():
    assert sds.whole_year_period(HOT_PERIODS, 2020) == "baseline"
    assert sds.whole_year_period(HOT_PERIODS, 2021) is None  # works window inside the year
    assert sds.whole_year_period(HOT_PERIODS, 2022) == "aeroup"
    assert sds.whole_year_period(HOT_PERIODS, 2012) is None  # COD in March


def test_full_year_needs_coverage_wind_coverage_one_period_and_a_past_year():
    known = full_known(2020)
    assert sds.full_year(2020, known, 2026, "baseline", "baseline", 95.0)
    assert not sds.full_year(2020, int(known * 0.89), 2026, "baseline", "baseline", 95.0)
    assert not sds.full_year(2020, known, 2026, "baseline", "baseline", 89.9)
    assert not sds.full_year(2021, known, 2026, None, "baseline", 95.0)
    assert not sds.full_year(2026, full_known(2026), 2026, "aeroup", "aeroup", 95.0)
    # the row's own config must be the one covering the year
    assert not sds.full_year(2020, known, 2026, "baseline", "aeroup", 95.0)
    # no wind coverage given (mid-wind fade) is not a reason to refuse
    assert sds.full_year(2020, known, 2026, "default", "default")


def test_full_year_is_leap_aware():
    assert sds.days_in_year(2024) == 366
    just_under = int(0.9 * 366 * 144) - 1
    assert not sds.full_year(2024, just_under, 2026, "default", "default", 100.0)
    assert sds.full_year(2024, just_under + 2, 2026, "default", "default", 100.0)


# ─── within-period slope, chaining, re-centring ─────────────────────────────


def line_points(config, years, start, slope, origin=2016):
    return [(config, y, start + slope * (y - origin)) for y in years]


def test_slope_is_measured_within_each_period_so_an_upgrade_step_is_not_ageing():
    pts = line_points("baseline", range(2016, 2021), 1.00, -0.01) + line_points(
        "aeroup", range(2022, 2026), 1.12, -0.01
    )
    assert sds.within_period_slope(pts) == pytest.approx(-0.01)


def test_a_period_with_one_full_year_adds_nothing_and_three_points_are_needed():
    pts = line_points("baseline", range(2016, 2018), 1.0, -0.02) + [("aeroup", 2024, 1.3)]
    assert sds.within_period_slope(pts) is None  # only 2 usable points
    pts += [("baseline", 2018, 0.96)]
    assert sds.within_period_slope(pts) == pytest.approx(-0.02)


def test_chaining_removes_the_step_and_keeps_the_slope():
    raw = line_points("baseline", range(2016, 2021), 1.00, -0.01) + line_points(
        "aeroup", range(2022, 2026), 1.12, -0.01
    )
    obs = [Obs(c, y, v, 1.0) for c, y, v in raw]
    full = {(c, y) for c, y, _ in raw}
    chained, unchained = sds.chain(obs, full, -0.01, ["baseline", "aeroup"])
    assert not unchained
    years = sorted(chained, key=lambda k: k[1])
    values = [chained[k] for k in years]
    diffs = [b - a for a, b in zip(values, values[1:])]
    # 2020 -> 2022 is two years of ageing, every other step one
    assert diffs == pytest.approx([-0.01] * 4 + [-0.02] + [-0.01] * 3)
    # the latest period keeps its own level
    assert chained[("aeroup", 2025)] == pytest.approx(1.12 - 0.09)


def test_a_period_without_full_years_is_chained_on_its_shown_points():
    obs = [Obs("baseline", y, 1.0 - 0.01 * (y - 2016), 1.0) for y in range(2016, 2020)]
    obs.append(Obs("aeroup", 2024, 1.3, 1.0))
    full = {("baseline", y) for y in range(2016, 2020)}
    chained, unchained = sds.chain(obs, full, -0.01, ["baseline", "aeroup"])
    assert not unchained
    # the baseline line, carried to 2024, lands on the AeroUp point
    assert chained[("baseline", 2019)] + (-0.01) * 5 == pytest.approx(1.3)


def test_without_a_slope_other_periods_are_left_unchained_and_flagged():
    obs = [Obs("baseline", 2019, 1.0, 1.0), Obs("aeroup", 2024, 1.2, 1.0)]
    chained, unchained = sds.chain(obs, set(), None, ["baseline", "aeroup"])
    assert chained == {("baseline", 2019): 1.0, ("aeroup", 2024): 1.2}
    assert unchained == {"baseline"}
    series = sds.build_series(obs, set(), ["baseline", "aeroup"], recentred=True)
    assert series.unchained_years == {2019}
    assert series.slope is None and series.line is None


def test_a_straddling_year_is_the_interval_weighted_mean_of_its_periods():
    obs = [Obs("baseline", 2021, 1.0, 300.0), Obs("aeroup", 2021, 2.0, 100.0)]
    combined = sds.combine_years(obs, {("baseline", 2021): 1.0, ("aeroup", 2021): 2.0})
    assert combined[2021] == pytest.approx(1.25)


def test_recentring_makes_the_full_years_average_one():
    out = sds.recentre({2016: 0.9, 2017: 0.95, 2018: 1.0, 2026: 0.7}, {2016, 2017, 2018})
    assert (out[2016] + out[2017] + out[2018]) / 3 == pytest.approx(1.0)
    assert out[2026] == pytest.approx(0.75)


# ─── fleet ──────────────────────────────────────────────────────────────────


def series_for(slope, years=range(2017, 2026), partial=(2016, 2026)):
    obs = [Obs("default", y, 1 + slope * (y - 2021), 1.0) for y in [*years, *partial]]
    full = {("default", y) for y in years}
    return sds.build_series(obs, full, ["default"], recentred=True)


def test_fleet_slope_is_the_median_turbine_slope_and_worst_is_the_steepest():
    turbines = {
        "T1": series_for(-0.005),
        "T2": series_for(-0.010),
        "T3": series_for(-0.015),
        "T10": series_for(0.005),
    }
    block = sds.metric_block(turbines, 4, list(range(2016, 2051)), 2026, 4, pct_of_level=False)
    assert block["fleet"]["slope_pct"] == pytest.approx(-0.75)
    assert block["worst"] == "T3"
    assert [t["t"] for t in block["turbines"]] == ["T1", "T2", "T3", "T10"]  # natural order
    assert block["fleet"]["start_year"] == 2017
    trend = {p["year"]: p["v"] for p in block["fleet"]["trend"]}
    assert trend[2050] - trend[2049] == pytest.approx(-0.0075, abs=1e-4)
    assert block["fleet"]["fit_gap"] < 0.01


def test_band_holds_the_middle_half_of_turbine_rates_from_the_last_data_year():
    line = (2021.0, 1.0, -0.01)
    out = sds.band(line, [-0.02, -0.01, -0.01, 0.0], 2026, range(2016, 2031))
    assert out[0]["year"] == 2026
    assert out[0]["lo"] == pytest.approx(out[0]["hi"])  # zero width at the last data year
    lo_s = sds._percentile([-0.02, -0.01, -0.01, 0.0], 0.25)
    assert out[-1]["lo"] == pytest.approx(sds.at(line, 2030) + (lo_s + 0.01) * 4)
    assert out[-1]["hi"] > out[-1]["lo"]


def test_fleet_needs_two_years_where_half_the_turbines_are_full():
    lonely = {"T1": series_for(-0.01), "T2": series_for(-0.01, years=[], partial=(2020, 2021))}
    assert sds.qualifying_years({"a": {2017, 2018}, "b": {2018}}, 2) == [2017, 2018]
    assert sds.qualifying_years({"a": {2017}, "b": set(), "c": set()}, 3) == []
    block = sds.metric_block(lonely, 4, list(range(2016, 2051)), 2026, 4, pct_of_level=False)
    assert block["fleet"] is None  # one of four turbines is full in any year


def test_fade_slope_is_a_share_of_the_turbine_level():
    obs = [Obs("default", y, 800 - 8 * (y - 2017), 1.0) for y in range(2017, 2022)]
    full = {("default", y) for y in range(2017, 2022)}
    ts = sds.build_series(obs, full, ["default"], recentred=False)
    block = sds.metric_block({"T1": ts}, 1, list(range(2017, 2051)), 2021, 1, pct_of_level=True)
    # -8 kW/yr around a mean of 784 kW
    assert block["turbines"][0]["slope_pct"] == pytest.approx(-8 / 784 * 100, abs=1e-3)
    assert block["median_change_pct"] == pytest.approx((768 / 800 - 1) * 100, abs=0.01)
    assert ts.values[2021] == pytest.approx(768)  # kW stay real, never re-centred


# ─── loss and price ─────────────────────────────────────────────────────────


def test_loss_is_the_drop_since_the_start_year_clamped_to_zero_and_one():
    def falling(y):
        return 1.0 - 0.1 * (y - 2017)

    prices = {y: 50.0 for y in range(2016, 2031)}
    rows = sds.loss_rows(falling, 2017, 1000.0, range(2016, 2031), prices, 2026)
    by = {r["year"]: r for r in rows}
    assert by[2016]["lost_frac"] == 0 and by[2017]["lost_frac"] == 0
    assert by[2020]["lost_frac"] == pytest.approx(0.3)
    assert by[2020]["mwh"] == pytest.approx(300.0)
    assert by[2020]["revenue"] == pytest.approx(15000.0)
    assert by[2030]["lost_frac"] == 1.0  # the trend crosses zero; never more than everything
    assert by[2026]["projected"] is False and by[2027]["projected"] is True


def test_a_rising_trend_loses_nothing_and_no_trend_gives_no_rows():
    rows = sds.loss_rows(lambda y: 1 + 0.01 * (y - 2017), 2017, 1000.0, range(2017, 2051), {}, 2026)
    assert all(r["lost_frac"] == 0 and r["mwh"] == 0 for r in rows)
    assert all(r["revenue"] is None for r in rows)  # no price, no revenue
    assert sds.loss_rows(None, 2017, 1000.0, range(2017, 2020), {}, 2026) is None
    assert sds.loss_rows(lambda y: 0.0, 2017, 1000.0, range(2017, 2020), {}, 2026) is None
    no_base = sds.loss_rows(lambda y: 1 - 0.01 * (y - 2017), 2017, None, [2020], {}, 2026)
    assert no_base[0]["mwh"] is None and no_base[0]["lost_frac"] == pytest.approx(0.03)


def test_flat_price_averages_years_with_most_of_their_hours():
    by_year = {
        2016: (50.0, 2225),
        2023: (90.0, 8760),
        2024: (70.0, int(0.9 * 8784)),  # 7905 < 90 % of a leap year's 8784 hours
        2025: (80.0, 8760),
        2026: (105.0, 7008),
    }
    flat, years = sds.flat_price(by_year, 2026)
    assert years == [2023, 2025] and flat == pytest.approx(85.0)


def test_each_past_year_uses_its_own_price_when_half_its_hours_are_priced():
    by_year = {2016: (50.0, 2225), 2022: (198.0, 8760), 2026: (105.5, 7008)}
    prices = sds.year_prices(by_year, 81.6, [2016, 2022, 2026, 2027, 2050], 2026)
    assert prices == {2016: 81.6, 2022: 198.0, 2026: 105.5, 2027: 81.6, 2050: 81.6}


# ─── the assembled payload (queries faked) ──────────────────────────────────


class FakeResult:
    def __init__(self, scalar=None):
        self._scalar = scalar

    def scalar(self):
        return self._scalar


class FakeDb:
    """Answers each execute() with the next queued scalar; records the SQL."""

    def __init__(self, scalars=()):
        self.scalars = list(scalars)
        self.sql = []

    async def execute(self, statement, params=None):
        self.sql.append((str(statement), params))
        return FakeResult(self.scalars.pop(0) if self.scalars else None)


SLOPES = {"T1": -0.005, "T2": -0.010, "T3": -0.015, "T4": 0.005}


def fake_outlook(monkeypatch, *, periods=None, price=True, slopes=SLOPES):
    years = range(2016, 2027)
    ladder = [
        SimpleNamespace(turbine=t, config="default", year=y, v=1 + s * (y - 2021), w=1000, cov=99.0)
        for t, s in slopes.items()
        for y in years
    ]
    fade = [
        SimpleNamespace(turbine=t, config="default", year=y, v=800 * (1 + s * (y - 2021)), w=900)
        for t, s in slopes.items()
        for y in years
    ]
    energy = {
        t: {y: (5e6, full_known(y) if 2016 < y < 2026 else full_known(y) // 3) for y in years}
        for t in slopes
    }
    facts = SimpleNamespace(
        name="Kelmarsh",
        windfarm_id=None,
        bidzone="10YGB----------A",
        rated_kw=12300.0,
        zone_name="Great Britain",
    )
    hours = {y: sds.hours_in_year(y) for y in range(2017, 2026)}
    by_year = {y: (50.0, h) for y, h in hours.items()}
    by_year[2026] = (100.0, int(0.83 * 8760))
    by_year[2016] = (40.0, 100)

    async def ret(value):
        return value

    svc = sds.ScadaDegradationService(FakeDb())
    monkeypatch.setattr(sps.ScadaPerformanceService, "_farm", lambda self, farm: ret(facts))
    monkeypatch.setattr(svc, "_turbines", lambda farm: ret(list(slopes)))
    monkeypatch.setattr(svc, "_periods", lambda farm: ret(periods or {}))
    monkeypatch.setattr(svc, "_energy", lambda farm: ret(energy))
    monkeypatch.setattr(svc, "_ladder_rows", lambda farm: ret(ladder))
    monkeypatch.setattr(svc, "_fade_rows", lambda farm: ret(fade))
    monkeypatch.setattr(svc, "_price_windfarm", lambda wf, bz: ret(99 if price else None))
    monkeypatch.setattr(svc, "_prices_by_year", lambda wf, source: ret((by_year, "GBP")))
    return svc


async def test_outlook_runs_every_series_to_2050_and_prices_each_year(monkeypatch):
    out = await fake_outlook(monkeypatch).outlook("kelmarsh", today=TODAY)
    assert out["years"] == {
        "first": 2016,
        "last_data": 2026,
        "current": 2026,
        "horizon_min": 2027,
        "horizon_max": 2050,
    }
    assert out["farm"]["currency"] == "GBP" and out["farm"]["zone"] == "Great Britain"
    assert out["upgrade"] is None
    ladder = out["ladder"]
    assert ladder["fleet"]["trend"][-1]["year"] == 2050
    assert ladder["turbines"][0]["trend"][-1]["year"] == 2050
    assert ladder["fleet"]["band"][0]["year"] == 2026
    # 2016 and 2026 are partial: shown, not full
    pts = {p["year"]: p for p in ladder["turbines"][0]["points"]}
    assert not pts[2016]["full"] and not pts[2026]["full"] and pts[2020]["full"]

    price = out["loss"]["price"]
    assert price["flat"] == pytest.approx(50.0) and price["flat_years"] == list(range(2017, 2026))
    farm = out["loss"]["scopes"]["_farm"]
    assert farm["baseline_mwh"] == pytest.approx(20000.0)  # 4 turbines × 5 GWh
    assert farm["start_year"] == 2017 and farm["rising"] is False
    rows = {r["year"]: r for r in farm["rows"]}
    assert sorted(rows) == list(range(2016, 2051))
    assert rows[2016]["lost_frac"] == 0
    assert rows[2026]["price"] == pytest.approx(100.0)  # own partial year (83 % of hours)
    assert rows[2027]["price"] == pytest.approx(50.0) and rows[2027]["projected"]
    assert rows[2030]["revenue"] == pytest.approx(rows[2030]["mwh"] * 50.0, rel=1e-6)


async def test_a_turbine_scope_uses_its_own_trend_and_output(monkeypatch):
    out = await fake_outlook(monkeypatch).outlook("kelmarsh", today=TODAY)
    t3 = out["loss"]["scopes"]["T3"]
    assert t3["baseline_mwh"] == pytest.approx(5000.0)
    assert t3["slope_pct"] == pytest.approx(-1.5)
    rows = {r["year"]: r for r in t3["rows"]}
    t0 = 1 + (-0.015) * (2017 - 2021)
    assert rows[2025]["lost_frac"] == pytest.approx(0.015 * 8 / t0, rel=1e-3)
    t4 = out["loss"]["scopes"]["T4"]
    assert t4["rising"] and all(r["lost_frac"] == 0 for r in t4["rows"])


async def test_no_price_area_prices_leaves_revenue_empty(monkeypatch):
    out = await fake_outlook(monkeypatch, price=False).outlook("kelmarsh", today=TODAY)
    assert out["loss"]["price"] is None and out["farm"]["currency"] is None
    assert all(r["revenue"] is None for r in out["loss"]["scopes"]["_farm"]["rows"])


async def test_an_all_rising_farm_is_flagged_and_loses_nothing(monkeypatch):
    rising = {"T1": 0.003, "T2": 0.006, "T3": 0.01}
    out = await fake_outlook(monkeypatch, slopes=rising).outlook("kelmarsh", today=TODAY)
    farm = out["loss"]["scopes"]["_farm"]
    assert farm["rising"] and farm["slope_pct"] == pytest.approx(0.6)
    assert all(r["lost_frac"] == 0 for r in farm["rows"])
    assert out["ladder"]["worst"] == "T1"  # the lowest trend, still rising


async def test_upgrade_window_comes_from_the_config_dates(monkeypatch):
    periods = {
        "T1": [
            Period("baseline", date(2012, 1, 1), date(2021, 5, 3)),
            Period("aeroup", date(2021, 6, 1), None),
        ],
        "T2": [
            Period("baseline", date(2012, 1, 1), date(2022, 10, 1)),
            Period("aeroup", date(2023, 2, 1), None),
        ],
    }
    out = await fake_outlook(monkeypatch, periods=periods).outlook("hill_of_towie", today=TODAY)
    assert out["upgrade"] == {"label": "AeroUp", "from_year": 2021, "to_year": 2023}


async def test_empty_gold_gives_an_empty_payload(monkeypatch):
    out = await fake_outlook(monkeypatch, slopes={}).outlook("kelmarsh", today=TODAY)
    assert out["ladder"] is None and out["loss"] is None and out["years"] is None


async def test_zone_price_falls_back_to_another_farm_in_the_bidzone():
    # own wind farm has no rows for the zone's source -> lowest-id farm in the bidzone
    db = FakeDb([None, 7404])
    assert await sds.ScadaDegradationService(db)._price_windfarm(7309, "10YGB----------A") == 7404
    assert db.sql[0][1] == {"id": 7309, "source": "ELEXON"}
    assert db.sql[1][1]["code"] == "10YGB----------A"
    # own wind farm with prices wins
    assert await sds.ScadaDegradationService(FakeDb([1]))._price_windfarm(7309, "X") == 7309
    # no wind farm and no bidzone -> no prices
    assert await sds.ScadaDegradationService(FakeDb())._price_windfarm(None, None) is None
    # unlinked farm (Kelmarsh) goes straight to the zone
    db = FakeDb([7404])
    assert await sds.ScadaDegradationService(db)._price_windfarm(None, "10YGB----------A") == 7404
    assert len(db.sql) == 1


# ─── endpoint ───────────────────────────────────────────────────────────────


class _FakeUser:
    id = 1
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

    monkeypatch.setattr(endpoint_module, "scada_schema_present", _present)
    monkeypatch.setattr(scada_service.ScadaService, "farm_slugs", _slugs)
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_active_user] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c


def test_outlook_endpoint_passes_the_farm(app_client, monkeypatch):
    seen = {}

    async def _outlook(self, farm, today=None):
        seen["farm"] = farm
        return {"farm": {"slug": farm}}

    monkeypatch.setattr(sds.ScadaDegradationService, "outlook", _outlook)
    resp = app_client.get("/scada/degradation-outlook", params={"farm": "kelmarsh"})
    assert resp.status_code == 200 and seen == {"farm": "kelmarsh"}


def test_outlook_endpoint_unknown_farm_is_404(app_client):
    assert app_client.get("/scada/degradation-outlook", params={"farm": "nope"}).status_code == 404


def test_outlook_endpoint_without_the_schema_is_503(app_client, monkeypatch):
    async def _absent(_db):
        return False

    monkeypatch.setattr(endpoint_module, "scada_schema_present", _absent)
    assert app_client.get("/scada/degradation-outlook").status_code == 503


def test_outlook_endpoint_needs_a_signed_in_user():
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/scada")
    with TestClient(app) as c:
        assert c.get("/scada/degradation-outlook").status_code in (401, 403)
