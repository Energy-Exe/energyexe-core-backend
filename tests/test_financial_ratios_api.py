"""
Integration tests for financial ratios API endpoint.
These tests run against the actual API server with real data.
"""

import os

import httpx
import pytest

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000/api/v1")


@pytest.fixture
def api_client():
    """Create an HTTP client for API testing."""
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0)


class TestFinancialRatiosAPI:
    def test_ratios_for_spv_windfarm(self, api_client):
        """Midtfjellet (id=7201, COD=2012-09-22) should have ratio data."""
        response = api_client.get("/financial-data/ratios/7201")
        assert response.status_code == 200
        data = response.json()
        assert len(data) >= 1

        entity_resp = data[0]
        assert entity_resp["windfarm_id"] == 7201
        assert entity_resp["financial_entity_id"] > 0
        assert entity_resp["windfarm_name"] == "Midtfjellet"
        assert len(entity_resp["linked_windfarm_ids"]) >= 1

        periods = entity_resp["periods"]
        assert len(periods) > 0

        active = [p for p in periods if not p["is_ramp_up_excluded"]]
        assert len(active) > 0
        # Latest active period should have computed ratios
        latest = active[-1]
        assert latest["generation_hours_count"] > 0
        assert latest["generation_data_available"] is True
        assert latest["revenue_per_mwh"] is not None
        assert latest["opex_per_mwh"] is not None
        assert latest["ebitda_margin_pct"] is not None

    def test_ratios_ramp_up_exclusion(self, api_client):
        """Vardafjellet (id=7225, COD=2020-08-17) should exclude early periods."""
        response = api_client.get("/financial-data/ratios/7225")
        if response.status_code != 200:
            pytest.skip("Vardafjellet not available")
        data = response.json()
        if not data:
            pytest.skip("No financial entities linked to Vardafjellet")

        periods = data[0]["periods"]
        excluded = [p for p in periods if p["is_ramp_up_excluded"]]
        if excluded:
            # 2020-01-01 period should be excluded (before COD + 365 = 2021-08-17)
            assert any(p["period_start"] == "2020-01-01" for p in excluded)
            for p in excluded:
                assert p["ramp_up_exclusion_reason"] is not None
                # Ratios should be null for excluded periods
                assert p["revenue_per_mwh"] is None
                assert p["opex_per_mwh"] is None
                assert p["ebitda_margin_pct"] is None

    def test_ratios_holdco_multi_windfarm(self, api_client):
        """A holdco entity should have multiple linked windfarm IDs."""
        response = api_client.get("/financial-data/ratios/7201")
        assert response.status_code == 200
        data = response.json()
        # Check if any entity linked to this windfarm is a holdco
        for entity_resp in data:
            if entity_resp["entity_type"] in ("holdco", "fund", "joint_venture"):
                assert len(entity_resp["linked_windfarm_ids"]) > 1
                break

    def test_ratios_windfarm_with_no_financial_data(self, api_client):
        """Nonexistent windfarm should return empty list."""
        response = api_client.get("/financial-data/ratios/9999")
        assert response.status_code == 200
        assert response.json() == []

    def test_ratios_generation_coverage(self, api_client):
        """Active periods should have coverage data populated."""
        response = api_client.get("/financial-data/ratios/7201")
        assert response.status_code == 200
        data = response.json()
        if not data:
            pytest.skip("No data for Midtfjellet")

        periods = data[0]["periods"]
        active = [p for p in periods if not p["is_ramp_up_excluded"] and p["generation_data_available"]]
        if not active:
            pytest.skip("No active periods with generation data")

        latest = active[-1]
        assert latest["period_coverage_pct"] is not None
        assert float(latest["period_coverage_pct"]) > 0
        assert latest["generation_hours_count"] > 0
        assert latest["generation_mwh"] is not None
        assert float(latest["generation_mwh"]) > 0

    def test_ratios_response_structure(self, api_client):
        """Verify all expected fields are present in the response."""
        response = api_client.get("/financial-data/ratios/7201")
        assert response.status_code == 200
        data = response.json()
        if not data:
            pytest.skip("No data")

        entity_resp = data[0]
        assert "windfarm_id" in entity_resp
        assert "windfarm_name" in entity_resp
        assert "financial_entity_id" in entity_resp
        assert "financial_entity_name" in entity_resp
        assert "entity_type" in entity_resp
        assert "cod" in entity_resp
        assert "linked_windfarm_ids" in entity_resp
        assert "periods" in entity_resp

        if entity_resp["periods"]:
            p = entity_resp["periods"][0]
            expected_fields = [
                "financial_data_id", "period_start", "period_end", "currency",
                "total_revenue", "total_operating_expenses", "ebitda",
                "generation_mwh", "generation_hours_count",
                "revenue_per_mwh", "opex_per_mwh", "ebitda_margin_pct",
                "is_ramp_up_excluded", "ramp_up_exclusion_reason",
                "generation_data_available", "period_coverage_pct",
            ]
            for field in expected_fields:
                assert field in p, f"Missing field: {field}"
