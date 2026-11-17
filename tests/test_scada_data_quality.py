"""Data Quality page (EPR-230): coverage cards, triggers table, completeness grid."""

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints import scada as endpoint_module
from app.core.deps import get_current_active_user, get_db
from app.services import scada_service
from app.services.scada_data_quality import (
    completeness_grid,
    coverage_cards,
    load_trigger_config,
    trigger_table,
)


def test_cards_judge_power_and_wind_on_their_own():
    c = coverage_cards(
        expected=1000,
        present=900,
        power_null_pct=10.0,
        wind_null_pct=0.0,
        price_hours=90,
        price_expected=100,
    )
    assert c["timestamps"] == {"present": 900, "expected": 1000}
    assert c["power"] == {"usable": 810, "expected": 1000}
    assert c["wind"] == {"usable": 900, "expected": 1000}
    assert c["price"] == {"priced": 90, "expected": 100}


def test_cards_without_a_price_series_or_signal_summary():
    c = coverage_cards(1000, 1000, None, None, None, None)
    assert c["price"] is None
    assert c["power"]["usable"] is None and c["wind"]["usable"] is None


def test_the_config_is_consistent():
    cfg = load_trigger_config()
    assert cfg["provisional"] is True
    assert len(cfg["triggers"]) == 16
    for trig in cfg["triggers"]:
        assert set(trig["sources"]) <= set(cfg["sources"])
        assert "day_ahead_price" in trig["sources"]
    for keys in cfg["farms"].values():
        assert set(keys) <= set(cfg["sources"])


def test_a_trigger_is_met_only_when_every_source_is_available():
    cfg = {
        "provisional": True,
        "sources": {"a": "A", "b": "B", "day_ahead_price": "Price"},
        "triggers": [
            {"code": "X_1", "name": "One", "sources": ["a", "day_ahead_price"]},
            {"code": "X_2", "name": "Two", "sources": ["a", "b"]},
        ],
        "farms": {"f": ["a"]},
    }
    t = trigger_table(cfg, "f", price_ok=True)
    assert t["set_up"] is True and t["provisional"] is True
    assert [r["met"] for r in t["rows"]] == [True, False]
    assert [s["available"] for s in t["rows"][1]["sources"]] == [True, False]
    # measured price coverage overrides the list; unmeasured falls back to it
    assert trigger_table(cfg, "f", price_ok=False)["rows"][0]["met"] is False
    cfg["farms"]["f"].append("day_ahead_price")
    assert trigger_table(cfg, "f", price_ok=None)["rows"][0]["met"] is True


def test_a_farm_not_in_the_config_is_not_set_up():
    t = trigger_table(load_trigger_config(), "kelmarsh", price_ok=None)
    assert t["set_up"] is False
    assert not any(r["met"] for r in t["rows"])


def test_raggovidda_matches_the_pipeline_refusals():
    t = trigger_table(load_trigger_config(), "raggovidda", price_ok=None)
    not_met = {r["code"] for r in t["rows"] if not r["met"]}
    assert not_met == {"OPS_01", "OPS_02", "OPS_13", "FIN_04"}


def _row(t, mo, rp, rv, ex=100, pre=False):
    return SimpleNamespace(turbine=t, mo=mo, rp=rp, rv=rv, ex=ex, pre=pre)


def test_grid_spans_first_to_last_data_month_and_hatches_pre_cod():
    rows = [
        _row("T01", "2015-12", 100, 99),
        _row("T01", "2016-02", 100, 0),
        _row("T02", "2016-02", 0, 0),
    ]
    g = completeness_grid(rows, {"T02": "2016-02"})
    assert g["months"] == ["2015-12", "2016-01", "2016-02"]
    assert g["grid"][0] == [[99.0, 0, 100.0], None, [0.0, 0, 100.0]]
    # T02 before its COD: hatched, not missing
    assert g["grid"][1][:2] == [[0.0, 1, 0.0], [0.0, 1, 0.0]]
    assert g["grid"][1][2] == [0.0, 0, 0.0]


def test_grid_for_a_farm_without_data_is_empty():
    assert completeness_grid([], {}) == {"turbines": [], "months": [], "grid": []}


class _FakeUser:
    id = 42
    role = "client"
    is_active = True
    is_superuser = False


@pytest.fixture
def client(monkeypatch):
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/scada")

    async def _db():
        yield None

    async def _present(_db):
        return True

    async def _slugs(self):
        return ["hill_of_towie"]

    async def _dq(self, farm):
        return {"farm": farm}

    monkeypatch.setattr(endpoint_module, "scada_schema_present", _present)
    monkeypatch.setattr(scada_service.ScadaService, "farm_slugs", _slugs)
    monkeypatch.setattr(scada_service.ScadaService, "data_quality", _dq)
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_active_user] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c


def test_endpoint_wiring(client):
    assert client.get("/scada/data-quality?farm=hill_of_towie").json() == {"farm": "hill_of_towie"}
    assert client.get("/scada/data-quality?farm=nowhere").status_code == 404
