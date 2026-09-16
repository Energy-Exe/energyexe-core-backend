"""Run with pytest preview_tests --noconftest (ordinary fixtures start app.main)."""

import copy
import subprocess
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from jose import jwt
from pydantic import ValidationError

from app.scada_preview.config import PreviewSettings
from app.scada_preview.main import create_preview_app, get_preview_db
from app.scada_preview.schemas import IngestionSummary
from app.scada_preview.service import IngestionSummaryService

DB = "postgresql+asyncpg://preview@127.0.0.1:5432/energyexe_sfe_preview"


@pytest.fixture
def summary():
    return {
        "schema_version": 1,
        "run_id": "run-first",
        "farm": {
            "slug": "lutelandet",
            "name": "Lutelandet",
            "windfarm_id": 7197,
            "supplied_turbines": ["T09"],
            "installed_turbine_count": 9,
        },
        "source": {
            "reporting_label": "2025 delivery",
            "timezone": "Europe/Oslo",
            "interval_seconds": 300,
            "interval_label": "unconfirmed",
            "first_label_utc": "2024-12-31T23:00:00Z",
            "last_label_utc": "2025-12-31T22:55:00Z",
            "window_start_utc": None,
            "window_end_utc": None,
            "files": [
                {
                    "name": name,
                    "sha256": "a" * 64,
                    "size_bytes": 1,
                    "s3_key": name,
                    "s3_version_id": "v1",
                }
                for name in ["averages", "status", "tags"]
            ],
        },
        "ingestion": {
            "processed_at": "2026-09-16T00:00:00Z",
            "pipeline_version": "v1",
            "mapping_version": "v1",
            "status": "validated",
        },
        "coverage": {
            "expected_rows": 105120,
            "observed_rows": 1,
            "duplicate_rows": 0,
            "signals": [],
            "power_valid_rows": 1,
            "wind_valid_rows": 0,
        },
        "energy": {
            "estimated_kwh": -1.0,
            "method": "mean_power_integral",
            "status": "estimated",
            "assumption": "Five-minute mean power",
            "valid_intervals": 1,
            "missing_intervals": 0,
            "negative_intervals": 1,
            "dated_status": "blocked",
            "dated_reason": "Timestamp convention unconfirmed",
        },
        "capabilities": [],
        "charts": {"daily_trends": [], "power_wind": [], "monthly_energy": []},
        "statuses": [],
        "limitations": ["Only T09"],
    }


def db_mock(summary=None, present=True):
    present_result = MagicMock()
    present_result.scalar_one_or_none.return_value = "scada.ingestion_run" if present else None
    row_result = MagicMock()
    row_result.mappings.return_value.one_or_none.return_value = (
        {"run_id": summary["run_id"], "summary": summary} if summary else None
    )
    return AsyncMock(execute=AsyncMock(side_effect=[present_result, row_result]))


@pytest.mark.asyncio
async def test_current_and_historical_reads_preserve_payload(summary):
    for run_id in [None, "run-first"]:
        db = db_mock(summary)
        result = await IngestionSummaryService(db).get("lutelandet", run_id)
        assert result.model_dump() == summary
        query, params = db.execute.call_args.args
        assert params["farm"] == "lutelandet"
        assert params["run_id"] == run_id
        assert ("is_current = true" in str(query)) == (run_id is None)
        assert db.commit.await_count == 0


@pytest.mark.asyncio
async def test_missing_table_and_missing_or_wrong_farm_run(summary):
    with pytest.raises(HTTPException) as error:
        await IngestionSummaryService(db_mock(present=False)).get("lutelandet")
    assert error.value.status_code == 503
    assert await IngestionSummaryService(db_mock(present=False)).farms() == {"farms": []}
    with pytest.raises(HTTPException) as error:
        await IngestionSummaryService(db_mock()).get("other", "run-first")
    assert error.value.status_code == 404
    with pytest.raises(HTTPException) as error:
        await IngestionSummaryService(db_mock(summary)).get("other", "run-first")
    assert error.value.status_code == 503


def test_invalid_nonfinite_unbounded_and_unconfirmed_payloads(summary):
    for modify in [
        lambda data: data["energy"].update(estimated_kwh=float("nan")),
        lambda data: data["source"].update(window_start_utc="2025-01-01"),
        lambda data: data["energy"].update(dated_status="available"),
        lambda data: data["charts"].update(power_wind=[{}] * 501),
        lambda data: data["coverage"].update(observed_rows=-1),
    ]:
        changed = copy.deepcopy(summary)
        modify(changed)
        with pytest.raises(ValidationError):
            IngestionSummary.model_validate(changed)


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+asyncpg://u@remote/energyexe_sfe_preview",
        "postgresql+asyncpg://u@127.0.0.1/energyexe",
        "postgresql+asyncpg://u@127.0.0.1/energyexe_sfe_preview?host=remote",
        "postgresql+asyncpg:///energyexe_sfe_preview",
        "sqlite+aiosqlite:///:memory:",
    ],
)
def test_remote_or_wrong_databases_rejected(url):
    with pytest.raises(ValueError):
        PreviewSettings(url)


def test_explicit_environment_and_bind_required():
    with pytest.raises(ValueError):
        PreviewSettings.from_env({})
    for key in ["AWS_PROFILE", "DATABASE_URL", "OPENAI_API_KEY", "PGHOST"]:
        with pytest.raises(ValueError):
            PreviewSettings.from_env({"SFE_PREVIEW_DATABASE_URL": DB, key: "unused"})
    with pytest.raises(ValueError):
        PreviewSettings(DB, host="0.0.0.0")


def test_authenticated_routes_logout_and_unsupported_features(summary):
    app = create_preview_app(PreviewSettings(DB))

    async def database():
        yield db_mock(summary)

    app.dependency_overrides[get_preview_db] = database
    with TestClient(app, base_url="http://127.0.0.1") as client:
        assert client.get("/health").json()["agents_enabled"] is False
        assert client.get("/api/v1/scada/ingestion-summary").status_code == 401
        assert (
            client.post(
                "/api/v1/auth/login", json={"username": "admin", "password": "bad"}
            ).status_code
            == 401
        )
        login = client.post(
            "/api/v1/auth/login", json={"username": "admin", "password": "adminenergyexe"}
        )
        token = login.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        assert client.get("/api/v1/users/me").json()["is_superuser"] is True
        assert client.get("/api/v1/scada/ingestion-summary?run_id=run-first").json() == summary
        for route in ["availability", "turbine/1", "revenue-waterfall"]:
            assert client.get("/api/v1/scada/" + route).status_code == 409
        for route in ["brain-agent/chat", "imports", "auth/register"]:
            assert client.post("/api/v1/" + route, json={}).status_code == 404
        assert client.post("/api/v1/auth/logout").status_code == 200
        assert client.get("/api/v1/users/me").status_code == 401


def test_tokens_cannot_cross_preview_instances():
    first = create_preview_app(PreviewSettings(DB))
    second = create_preview_app(PreviewSettings(DB))
    with TestClient(first, base_url="http://127.0.0.1") as client:
        token = client.post(
            "/api/v1/auth/login", json={"username": "admin", "password": "adminenergyexe"}
        ).json()["access_token"]
    with TestClient(second, base_url="http://127.0.0.1") as client:
        assert (
            client.get("/api/v1/users/me", headers={"Authorization": f"Bearer {token}"}).status_code
            == 401
        )


@pytest.mark.parametrize("subject, expiry", [("client", 4102444800), ("admin", 1)])
def test_nonadmin_and_expired_tokens_are_denied(subject, expiry):
    app = create_preview_app(PreviewSettings(DB))
    token = jwt.encode(
        {
            "sub": subject,
            "exp": expiry,
            "aud": "sfe-local-preview",
            "iss": "sfe-local-preview",
            "session": app.state.session_id,
        },
        app.state.token_key,
        algorithm="HS256",
    )
    with TestClient(app, base_url="http://127.0.0.1") as client:
        assert (
            client.get("/api/v1/users/me", headers={"Authorization": f"Bearer {token}"}).status_code
            == 401
        )


def test_import_does_not_load_application_config_jobs_or_credentials():
    code = (
        """
import sys
from app.scada_preview.main import create_preview_app
from app.scada_preview.config import PreviewSettings
create_preview_app(PreviewSettings(%r))
assert 'app.main' not in sys.modules
assert 'app.core.config' not in sys.modules
assert 'app.services' not in sys.modules
assert 'boto3' not in sys.modules
assert 'dotenv' not in sys.modules
"""
        % DB
    )
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True)
