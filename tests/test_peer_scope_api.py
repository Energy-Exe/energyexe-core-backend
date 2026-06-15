"""Live-server tests for the peer-scope filtering surface (EPR-10).

Covers the windfarm scope helper's wire behavior across:
- GET /windfarms/aggregate-stats (new)
- scope params on /generation/portfolio/* (extended)
- GET /generation/portfolio/normalised-timeseries (new)
- GET /financial-data/peer-summary (new)

Style matches tests/test_market_api.py: requires a running API + DB
(API_BASE_URL, defaults to the local dev server) and skips when auth fails.
"""

import os

import httpx
import pytest

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8002/api/v1")


@pytest.fixture
def api_client():
    return httpx.Client(base_url=API_BASE_URL, timeout=120.0)


@pytest.fixture
def auth_headers(api_client):
    response = api_client.post(
        "/auth/login",
        json={
            "username": os.getenv("TEST_USER_USERNAME", "admin"),
            "password": os.getenv("TEST_USER_PASSWORD", "adminenergyexe"),
        },
    )
    if response.status_code != 200:
        pytest.skip("Could not authenticate - skipping protected endpoint tests")
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


class TestWindfarmAggregateStats:
    def test_unfiltered_returns_dataset_totals(self, api_client, auth_headers):
        r = api_client.get("/windfarms/aggregate-stats", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert set(data) == {
            "farm_count",
            "total_capacity_mw",
            "operational_count",
            "countries_count",
        }
        assert data["farm_count"] > 0
        assert data["operational_count"] <= data["farm_count"]
        assert data["total_capacity_mw"] > 0

    def test_filters_reduce_counts_consistently(self, api_client, auth_headers):
        total = api_client.get("/windfarms/aggregate-stats", headers=auth_headers).json()
        onshore = api_client.get(
            "/windfarms/aggregate-stats",
            params={"location_type": "onshore"},
            headers=auth_headers,
        ).json()
        gbr_onshore = api_client.get(
            "/windfarms/aggregate-stats",
            params={"location_type": "onshore", "country_code": "GBR"},
            headers=auth_headers,
        ).json()
        assert onshore["farm_count"] <= total["farm_count"]
        assert gbr_onshore["farm_count"] <= onshore["farm_count"]
        assert gbr_onshore["countries_count"] in (0, 1)

    def test_impossible_filter_returns_zeros(self, api_client, auth_headers):
        r = api_client.get(
            "/windfarms/aggregate-stats",
            params={"capacity_min": 999999},
            headers=auth_headers,
        )
        assert r.status_code == 200
        assert r.json()["farm_count"] == 0

    def test_invalid_location_type_rejected(self, api_client, auth_headers):
        r = api_client.get(
            "/windfarms/aggregate-stats",
            params={"location_type": "floating"},
            headers=auth_headers,
        )
        assert r.status_code == 422


class TestPortfolioScopeParams:
    """The extended /generation/portfolio endpoints accept peer-scope params."""

    WINDOW = {"start_date": "2025-04-01T00:00:00", "end_date": "2025-04-30T00:00:00"}

    def test_stats_scoped_vs_unscoped(self, api_client, auth_headers):
        scoped = api_client.get(
            "/generation/portfolio/stats",
            params={**self.WINDOW, "location_type": "onshore", "country_code": "GBR"},
            headers=auth_headers,
        )
        assert scoped.status_code == 200
        data = scoped.json()
        assert data["farm_count"] > 0
        # Scoped totals must match the aggregate-stats farm universe or less
        # (only farms with generation rows count here).
        agg = api_client.get(
            "/windfarms/aggregate-stats",
            params={"location_type": "onshore", "country_code": "GBR"},
            headers=auth_headers,
        ).json()
        assert data["farm_count"] <= agg["farm_count"]

    def test_unmatched_scope_returns_empty_payload_not_global(self, api_client, auth_headers):
        r = api_client.get(
            "/generation/portfolio/stats",
            params={**self.WINDOW, "capacity_min": 999999},
            headers=auth_headers,
        )
        assert r.status_code == 200
        data = r.json()
        # Pre-fix behavior silently dropped unmatched filters and returned the
        # global dataset; the helper now returns the empty payload.
        assert data["farm_count"] == 0
        assert data["total_mwh"] == 0

    def test_availability_scoped(self, api_client, auth_headers):
        r = api_client.get(
            "/generation/portfolio/availability",
            params={"country_code": "GBR"},
            headers=auth_headers,
        )
        assert r.status_code == 200
        data = r.json()
        assert data["farm_count"] is not None and data["farm_count"] > 0


class TestNormalisedTimeseries:
    def test_uk_onshore_window(self, api_client, auth_headers):
        r = api_client.get(
            "/generation/portfolio/normalised-timeseries",
            params={
                "start_date": "2025-06-01T00:00:00",
                "end_date": "2025-12-31T00:00:00",
                "location_type": "onshore",
                "country_code": "GBR",
            },
            headers=auth_headers,
        )
        assert r.status_code == 200
        data = r.json()
        assert data["coverage"]["farm_count"] > 0
        assert data["coverage"]["farms_with_norm"] <= data["coverage"]["farm_count"]
        # Months stay inside the requested window.
        months = [p["month"] for p in data["timeseries"]]
        assert months == sorted(months)
        assert all("2025-06" <= m <= "2025-12" for m in months)
        # Apples-to-apples delta: actual_covered vs normalised.
        t = data["totals"]
        assert t["actual_covered_mwh"] <= t["actual_mwh"] + 0.01
        if t["normalised_mwh"] > 0 and t["delta_pct"] is not None:
            expected = (t["actual_covered_mwh"] - t["normalised_mwh"]) / t["normalised_mwh"] * 100
            assert abs(expected - t["delta_pct"]) < 0.05
        # Top-10 contribution sorted by actual desc, honest coverage fields.
        actuals = [f["actual_mwh"] for f in data["by_farm"]]
        assert actuals == sorted(actuals, reverse=True)
        assert len(data["by_farm"]) <= 10
        for farm in data["by_farm"]:
            assert farm["months_covered"] <= farm["months_total"]
            if farm["normalised_mwh"] is None:
                assert farm["delta_pct"] is None

    def test_empty_scope_returns_empty_payload(self, api_client, auth_headers):
        r = api_client.get(
            "/generation/portfolio/normalised-timeseries",
            params={
                "start_date": "2025-06-01T00:00:00",
                "end_date": "2025-12-31T00:00:00",
                "capacity_min": 999999,
            },
            headers=auth_headers,
        )
        assert r.status_code == 200
        data = r.json()
        assert data["coverage"]["farm_count"] == 0
        assert data["timeseries"] == []
        assert data["by_farm"] == []


class TestPeerFinancialSummary:
    def test_uk_onshore_summary(self, api_client, auth_headers):
        r = api_client.get(
            "/financial-data/peer-summary",
            params={"location_type": "onshore", "country_code": "GBR"},
            headers=auth_headers,
        )
        assert r.status_code == 200
        data = r.json()
        assert data["display_currency"] == "EUR"
        cov = data["coverage"]
        assert cov["farms_with_financials"] <= cov["farm_count"]
        assert len(data["farms"]) == cov["farms_with_financials"]
        for farm in data["farms"][:5]:
            assert farm["period_start"] and farm["period_end"]
            assert farm["period_start"] <= farm["period_end"]

    def test_farm_row_matches_per_farm_ratios_endpoint(self, api_client, auth_headers):
        summary = api_client.get(
            "/financial-data/peer-summary",
            params={"location_type": "onshore", "country_code": "GBR"},
            headers=auth_headers,
        ).json()
        rows = [f for f in summary["farms"] if f["currency"] == "EUR"]
        if not rows:
            pytest.skip("No EUR-converted rows to cross-check")
        row = rows[0]
        ratios = api_client.get(
            f"/financial-data/ratios/{row['windfarm_id']}",
            params={"display_currency": "EUR"},
            headers=auth_headers,
        ).json()
        periods = [
            p
            for entity in ratios
            for p in entity["periods"]
            if p["period_start"] == row["period_start"] and p["period_end"] == row["period_end"]
        ]
        assert periods, "peer-summary period must exist on the per-farm ratios endpoint"
        match = periods[0]
        assert float(match["revenue_per_mwh"]) == pytest.approx(
            row["revenue_per_mwh"], abs=0.05
        )
