"""Endpoint tests for /performance-analytics (EPR-186/187/188).

The services read Postgres-only views, so they are patched; these tests cover
validation, auth, caching and how parameters reach the services.
"""

from unittest.mock import AsyncMock, patch

import pytest

from app.api.v1.endpoints import performance_analytics as ep
from app.core.deps import get_current_active_user
from app.models.user import User

BASE = "/api/v1/performance-analytics"


@pytest.fixture
def api(client):
    async def _user():
        return User(id=1, email="a@b.c", username="a", hashed_password="x", is_active=True)

    client.app.dependency_overrides[get_current_active_user] = _user
    with patch.object(ep, "get_cached_report", new=AsyncMock(return_value=None)), patch.object(
        ep, "cache_report", new=AsyncMock()
    ):
        yield client
    client.app.dependency_overrides.pop(get_current_active_user, None)


def test_requires_auth(client):
    assert client.get(f"{BASE}/comparison?start=2024-01&end=2024-02").status_code in (401, 403)


@pytest.mark.parametrize(
    "query",
    ["start=2024-13&end=2024-12", "start=2024-05&end=2024-04", "start=1990-01&end=2024-01"],
)
def test_month_window_rejected(api, query):
    with patch.object(ep.ComparisonAnalyticsService, "get_comparison", new=AsyncMock()) as svc:
        resp = api.get(f"{BASE}/comparison?{query}")
    assert resp.status_code == 400
    svc.assert_not_called()


def test_comparison_passes_first_days_of_months(api):
    with patch.object(
        ep.ComparisonAnalyticsService, "get_comparison", new=AsyncMock(return_value={"ok": 1})
    ) as svc:
        resp = api.get(f"{BASE}/comparison?start=2024-1&end=2024-03")
    assert resp.status_code == 200
    start, end = svc.await_args.args
    assert (start.isoformat(), end.isoformat()) == ("2024-01-01", "2024-03-01")
    # Unpadded and padded months share one cache entry.
    assert ep.cache_report.await_args.args[0] == "perf-analytics:cmp:v4:2024-01:2024-03"


def test_cache_hit_skips_service(api):
    ep.get_cached_report.return_value = {"cached": True}
    with patch.object(ep.ComparisonAnalyticsService, "get_comparison", new=AsyncMock()) as svc:
        resp = api.get(f"{BASE}/comparison?start=2024-01&end=2024-02")
    assert resp.json() == {"cached": True}
    svc.assert_not_called()


def test_performance_anchor_reaches_the_service_and_the_cache_key(api):
    with patch.object(
        ep.PerformanceAnalyticsService, "get_performance", new=AsyncMock(return_value={"ok": 1})
    ) as svc:
        resp = api.get(f"{BASE}/windfarms/5/performance?start=2024-10&end=2026-09&anchor=true")
    assert resp.status_code == 200
    assert svc.await_args.args == (5, (2024, 10), (2026, 9), None, True)
    assert ep.cache_report.await_args.args[0] == "perf-analytics:v6:5:2024-10:2026-09:-:a"


def test_performance_404_when_farm_missing(api):
    with patch.object(
        ep.PerformanceAnalyticsService, "get_performance", new=AsyncMock(return_value=None)
    ):
        resp = api.get(f"{BASE}/windfarms/5/performance?start=2024-01&end=2024-02")
    assert resp.status_code == 404


def test_generation_rejects_seven_farms(api):
    resp = api.get(f"{BASE}/generation?windfarm_ids=1,2,3,4,5,6,7&start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 400


def test_generation_dedupes_ids_in_order(api):
    with patch.object(
        ep.GenerationAnalyticsService, "get_generation", new=AsyncMock(return_value={})
    ) as svc:
        resp = api.get(f"{BASE}/generation?windfarm_ids=3,3,1&start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 200
    assert svc.await_args.args[0] == [3, 1]


def test_generation_day_view_capped(api):
    resp = api.get(
        f"{BASE}/generation?windfarm_ids=1&granularity=day&start=2023-01-01&end=2024-06-30"
    )
    assert resp.status_code == 400
