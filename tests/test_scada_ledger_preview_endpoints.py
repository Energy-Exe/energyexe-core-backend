"""Ledger preview endpoints: internal-only gate, off switch, 404, and the shipped files load."""

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints import scada_ledger_preview as endpoint_module
from app.core.config import Settings
from app.core.deps import get_current_active_user, get_current_internal_user, get_current_superuser
from app.services import scada_ledger_preview_service as svc

PREFIX = "/scada/ledger-preview"


class _FakeUser:
    id = 42
    is_active = True
    is_superuser = True
    is_internal = True


def _doc(farm="hill_of_towie", asset="HOT", stamp="SANDBOX — not for clients"):
    return {
        "schema": svc.SCHEMA,
        "meta": {
            "farm": farm,
            "asset": asset,
            "currency": "GBP",
            "stamp": stamp,
            "window_from": "2016-09-28",
            "window_to": "2024-12-31",
        },
        "headline": {"ledger": 1.0},
    }


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    (tmp_path / "hill_of_towie__HOT.json").write_text(json.dumps(_doc()))
    (tmp_path / "raggovidda__RG2.json").write_text(json.dumps(_doc("raggovidda", "RG2")))
    (tmp_path / "README.md").write_text("not a preview")
    monkeypatch.setattr(svc, "DATA_DIR", tmp_path)
    monkeypatch.setattr(svc.ScadaLedgerPreviewService.__init__, "__defaults__", (tmp_path,))
    return tmp_path


@pytest.fixture
def enabled(monkeypatch):
    s = Settings(SCADA_LEDGER_PREVIEW_ENABLED=True)
    monkeypatch.setattr(endpoint_module, "get_settings", lambda: s)
    return s


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix=PREFIX)
    app.dependency_overrides[get_current_internal_user] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c


def test_returns_the_asset_document(client, data_dir, enabled):
    r = client.get(f"{PREFIX}/", params={"farm": "raggovidda", "asset": "RG2"})
    assert r.status_code == 200
    assert r.json()["meta"]["asset"] == "RG2"


def test_lists_a_farms_assets(client, data_dir, enabled):
    r = client.get(f"{PREFIX}/assets", params={"farm": "raggovidda"})
    assert r.status_code == 200
    assert [a["asset_code"] for a in r.json()] == ["RG2"]
    assert client.get(f"{PREFIX}/assets", params={"farm": "nowhere"}).json() == []


def test_unknown_farm_or_asset_is_404(client, data_dir, enabled):
    assert client.get(f"{PREFIX}/", params={"farm": "nowhere"}).status_code == 404
    assert (
        client.get(f"{PREFIX}/", params={"farm": "raggovidda", "asset": "RG1"}).status_code == 404
    )


def test_off_switch_is_503(client, data_dir, monkeypatch):
    s = Settings(SCADA_LEDGER_PREVIEW_ENABLED=False)
    monkeypatch.setattr(endpoint_module, "get_settings", lambda: s)
    assert client.get(f"{PREFIX}/", params={"farm": "hill_of_towie"}).status_code == 503
    assert client.get(f"{PREFIX}/assets").status_code == 503


def test_default_is_off_on_production_only():
    assert Settings(SENTRY_ENVIRONMENT="production").scada_ledger_preview_enabled is False
    assert Settings(SENTRY_ENVIRONMENT="staging").scada_ledger_preview_enabled is True
    assert (
        Settings(
            SENTRY_ENVIRONMENT="production", SCADA_LEDGER_PREVIEW_ENABLED=True
        ).scada_ledger_preview_enabled
        is True
    )


def test_a_file_not_stamped_sandbox_is_refused(tmp_path):
    (tmp_path / "x__HOT.json").write_text(json.dumps(_doc("x", stamp="register")))
    with pytest.raises(svc.LedgerPreviewInvalid):
        svc.ScadaLedgerPreviewService(tmp_path).get("x")


def test_without_a_login_every_route_is_refused():
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix=PREFIX)
    with TestClient(app) as c:
        assert c.get(f"{PREFIX}/").status_code in (401, 403)
        assert c.get(f"{PREFIX}/assets").status_code in (401, 403)


def test_routes_use_the_internal_user_dependency():
    for route in endpoint_module.router.routes:
        deps = {d.call for d in route.dependant.dependencies}
        assert get_current_internal_user in deps, route.path
        assert get_current_superuser not in deps and get_current_active_user not in deps


def test_the_shipped_files_load_and_are_sandbox_stamped():
    s = svc.ScadaLedgerPreviewService()
    hot, rg2 = s.get("hill_of_towie", "HOT"), s.get("raggovidda", "RG2")
    assert hot["meta"]["currency"] == "GBP" and rg2["meta"]["currency"] == "NOK"
    for d in (hot, rg2):
        assert "SANDBOX" in d["meta"]["stamp"]
        assert abs(d["bridge"][-1]["headline_after"] - d["headline"]["ledger"]) < 1.0
        years = {r["year"] for r in d["by_year"]}
        assert max(years) <= int(d["meta"]["window_to"][:4])
