"""EPR-221 SCADA Builder: window helpers, the prompt → spec call and the endpoints."""

import json
from datetime import date
from types import SimpleNamespace

import anthropic
import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoints import scada as scada_endpoints
from app.api.v1.endpoints import scada_builder as endpoint_module
from app.core.deps import get_current_active_user, get_current_superuser, get_db
from app.services import scada_builder_service as builder
from app.services import scada_service

# The client catalogue (src/components/scada/builder/catalogue.test.ts) asserts the same list.
DATASET_IDS = [
    "generation",
    "generation_vs_p50",
    "capacity_factor",
    "availability",
    "wind_speed",
    "capture_price",
    "revenue",
    "p50_attainment",
    "performance_index",
    "alarm_events_per_year",
    "value_lost_per_year",
    "curtailment_per_year",
    "downtime_per_year",
    "turbine_lost_energy",
    "turbine_availability",
    "turbine_capacity_factor",
    "turbine_downtime_cost",
    "alarm_hours_by_category",
    "value_lost_by_cause",
]


def test_dataset_ids_are_frozen():
    assert list(builder.DATASETS) == DATASET_IDS
    assert builder.SPEC_SCHEMA["properties"]["dataset"]["enum"] == DATASET_IDS


@pytest.mark.asyncio
async def test_yearly_money_splits_part_years_from_unpriced_hours(monkeypatch):
    seen = {}

    async def rows(self, sql, **p):
        seen["sql"] = sql
        return [
            SimpleNamespace(y=2023, v=10.0, days=365, hp=8000, hu=12),  # whole year, some unpriced
            SimpleNamespace(y=2024, v=20.0, days=100, hp=2400, hu=0),  # part year, all priced
        ]

    monkeypatch.setattr(builder.ScadaBuilderService, "_rows", rows)
    out = {}
    await builder.ScadaBuilderService(None)._yearly(
        out, "hill_of_towie", "curtailment_per_year", date(2024, 4, 9)
    )
    assert "revenue_curtailment_gbp" in seen["sql"]
    assert [(r["key"], r["partial"], r["unpriced"]) for r in out["rows"]] == [
        ("2023", False, True),
        ("2024", True, False),
    ]


def test_window_is_whole_months_ending_on_the_last_data_day():
    last = date(2026, 4, 30)
    assert builder.window_start(last, 12) == date(2025, 5, 1)
    assert builder.window_start(last, 3) == date(2026, 2, 1)
    assert builder.window_start(date(2024, 12, 31), 24) == date(2023, 1, 1)
    assert builder.days_in_month(date(2024, 2, 10)) == 29
    assert builder.days_in_month(date(2025, 12, 1)) == 31


# ─── prompt → spec ──────────────────────────────────────────────────────────

SPEC = {
    "dataset": "generation_vs_p50",
    "chart": "column",
    "months": 12,
    "top": None,
    "title": "Monthly generation vs P50",
    "match": "exact",
    "note": None,
}


def _fake_client(monkeypatch, *, text=None, stop="end_turn", error=None):
    seen = {}

    class _Messages:
        async def create(self, **kwargs):
            seen.update(kwargs)
            if error is not None:
                raise error
            return SimpleNamespace(
                stop_reason=stop,
                content=[SimpleNamespace(type="text", text=text)] if text is not None else [],
                usage=SimpleNamespace(input_tokens=10, output_tokens=5),
            )

    class _Client:
        def __init__(self, **kwargs):
            seen["client"] = kwargs
            self.messages = _Messages()

    monkeypatch.setattr(builder.anthropic, "AsyncAnthropic", _Client)
    monkeypatch.setattr(
        builder,
        "get_settings",
        lambda: SimpleNamespace(ANTHROPIC_API_KEY="k", SCADA_BUILDER_MODEL="claude-opus-5-5"),
    )
    return seen


async def test_spec_returns_the_model_json_with_a_json_schema(monkeypatch):
    seen = _fake_client(monkeypatch, text=json.dumps(SPEC))
    assert await builder.spec_from_prompt("generation vs p50") == SPEC
    fmt = seen["extra_body"]["output_config"]["format"]
    assert fmt["type"] == "json_schema" and fmt["schema"] is builder.SPEC_SCHEMA
    assert seen["model"] == "claude-opus-5-5"
    assert seen["messages"] == [{"role": "user", "content": "generation vs p50"}]
    assert seen["client"]["max_retries"] == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"text": json.dumps(SPEC), "stop": "refusal"},
        {"text": "{", "stop": "max_tokens"},
        {"text": "not json"},
        {"text": "[1, 2]"},
        {"text": None},
        {
            "error": anthropic.APIConnectionError(
                request=httpx.Request("POST", "https://api.anthropic.com")
            )
        },
    ],
)
async def test_spec_failures_raise_unavailable(monkeypatch, kwargs):
    _fake_client(monkeypatch, **kwargs)
    with pytest.raises(builder.BuilderSpecUnavailable):
        await builder.spec_from_prompt("x")


async def test_spec_without_a_key_is_unavailable(monkeypatch):
    monkeypatch.setattr(builder, "get_settings", lambda: SimpleNamespace(ANTHROPIC_API_KEY=None))
    with pytest.raises(builder.BuilderSpecUnavailable):
        await builder.spec_from_prompt("x")


# ─── endpoints ──────────────────────────────────────────────────────────────


class _FakeUser:
    id = 1
    is_active = True
    is_superuser = True


@pytest.fixture
def app_client(monkeypatch):
    app = FastAPI()
    app.include_router(endpoint_module.router, prefix="/scada/builder")

    async def _db():
        yield None

    async def _present(_db):
        return True

    async def _slugs(self):
        return ["hill_of_towie", "kelmarsh"]

    monkeypatch.setattr(scada_endpoints, "scada_schema_present", _present)
    monkeypatch.setattr(scada_service.ScadaService, "farm_slugs", _slugs)
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_current_active_user] = lambda: _FakeUser()
    with TestClient(app) as c:
        yield c, app


def test_data_validates_its_parameters(app_client):
    client, _ = app_client
    get = client.get
    assert (
        get("/scada/builder/data", params={"farm": "hill_of_towie", "dataset": "x"}).status_code
        == 422
    )
    params = {"farm": "hill_of_towie", "dataset": "generation", "months": 5}
    assert get("/scada/builder/data", params=params).status_code == 422
    params = {"farm": "nope", "dataset": "generation"}
    assert get("/scada/builder/data", params=params).status_code == 404


def test_data_is_503_without_the_scada_schema(app_client, monkeypatch):
    client, _ = app_client

    async def _absent(_db):
        return False

    monkeypatch.setattr(scada_endpoints, "scada_schema_present", _absent)
    params = {"farm": "hill_of_towie", "dataset": "generation"}
    assert client.get("/scada/builder/data", params=params).status_code == 503


def test_data_parameters_reach_the_service(app_client, monkeypatch):
    client, _ = app_client
    seen = {}

    async def _data(self, farm, dataset, months):
        seen.update(farm=farm, dataset=dataset, months=months)
        return {"rows": []}

    monkeypatch.setattr(builder.ScadaBuilderService, "data", _data)
    params = {"farm": "kelmarsh", "dataset": "turbine_availability", "months": 24}
    assert client.get("/scada/builder/data", params=params).status_code == 200
    assert seen == {"farm": "kelmarsh", "dataset": "turbine_availability", "months": 24}


def test_spec_needs_a_superuser(app_client):
    client, _ = app_client
    resp = client.post("/scada/builder/spec", json={"prompt": "generation"})
    assert resp.status_code in (401, 403)


def test_spec_is_503_when_the_assistant_fails(app_client, monkeypatch):
    client, app = app_client
    app.dependency_overrides[get_current_superuser] = lambda: _FakeUser()

    async def _fail(prompt):
        raise builder.BuilderSpecUnavailable("down")

    async def _ok(prompt):
        return {**SPEC, "title": prompt}

    monkeypatch.setattr(endpoint_module, "spec_from_prompt", _fail)
    assert client.post("/scada/builder/spec", json={"prompt": "x"}).status_code == 503
    assert client.post("/scada/builder/spec", json={"prompt": ""}).status_code == 422
    monkeypatch.setattr(endpoint_module, "spec_from_prompt", _ok)
    resp = client.post("/scada/builder/spec", json={"prompt": "  hi  "})
    assert resp.status_code == 200 and resp.json()["title"] == "hi"
