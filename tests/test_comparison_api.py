"""
Integration tests for comparison API endpoints.
These tests run against the actual API server and test endpoint functionality.
"""

import pytest
import httpx
import os
from datetime import datetime, timedelta

# API configuration
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8001/api/v1")


@pytest.fixture
def api_client():
    """Create an HTTP client for API testing."""
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0)


@pytest.fixture
def auth_headers(api_client):
    """Get authentication headers for protected endpoints."""
    response = api_client.post("/auth/login", json={
        "email": os.getenv("TEST_USER_EMAIL", "admin@energyexe.com"),
        "password": os.getenv("TEST_USER_PASSWORD", "admin123")
    })

    if response.status_code != 200:
        pytest.skip("Could not authenticate - skipping protected endpoint tests")

    token = response.json().get("access_token")
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def test_windfarm_ids(api_client, auth_headers):
    """Get test windfarm IDs that have generation data."""
    response = api_client.get("/comparison/windfarms", headers=auth_headers)
    if response.status_code != 200:
        pytest.skip("Could not fetch windfarms for comparison")

    windfarms = response.json()
    # Filter windfarms that have data
    windfarms_with_data = [wf for wf in windfarms if wf.get("has_data")]

    if len(windfarms_with_data) < 2:
        pytest.skip("Not enough windfarms with data for comparison testing")

    # Return first 3 windfarm IDs for testing
    return [wf["id"] for wf in windfarms_with_data[:3]]


class TestComparisonWindfarms:
    """Tests for GET /comparison/windfarms endpoint."""

    def test_get_available_windfarms(self, api_client, auth_headers):
        """Test fetching windfarms available for comparison."""
        response = api_client.get("/comparison/windfarms", headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)
        if len(data) > 0:
            wf = data[0]
            assert "id" in wf
            assert "name" in wf
            assert "capacity_mw" in wf
            assert "has_data" in wf
            assert "data_range" in wf
            assert "record_count" in wf

    def test_windfarm_data_range(self, api_client, auth_headers):
        """Test that data range is properly returned."""
        response = api_client.get("/comparison/windfarms", headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        windfarms_with_data = [wf for wf in data if wf.get("has_data")]
        if windfarms_with_data:
            wf = windfarms_with_data[0]
            assert wf["data_range"]["start"] is not None
            assert wf["data_range"]["end"] is not None
            assert wf["record_count"] > 0


class TestComparisonCompare:
    """Tests for GET /comparison/compare endpoint."""

    def test_compare_two_windfarms_daily(self, api_client, auth_headers, test_windfarm_ids):
        """Test comparing two windfarms with daily granularity."""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        params = {
            "windfarm_ids": test_windfarm_ids[:2],
            "start_date": start_date,
            "end_date": end_date,
            "granularity": "daily"
        }

        response = api_client.get("/comparison/compare", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        assert "data" in data
        assert "summary" in data
        assert data["summary"]["windfarm_count"] == 2
        assert data["summary"]["date_range"]["start"] == start_date
        assert data["summary"]["date_range"]["end"] == end_date

    def test_compare_multiple_windfarms(self, api_client, auth_headers, test_windfarm_ids):
        """Test comparing three windfarms."""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        params = {
            "windfarm_ids": test_windfarm_ids[:3],
            "start_date": start_date,
            "end_date": end_date,
            "granularity": "daily"
        }

        response = api_client.get("/comparison/compare", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        assert data["summary"]["windfarm_count"] == min(3, len(test_windfarm_ids))

    def test_compare_hourly_granularity(self, api_client, auth_headers, test_windfarm_ids):
        """Test comparison with hourly granularity."""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

        params = {
            "windfarm_ids": test_windfarm_ids[:2],
            "start_date": start_date,
            "end_date": end_date,
            "granularity": "hourly"
        }

        response = api_client.get("/comparison/compare", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        assert "data" in data
        # Hourly data should have period in format with time
        if data["data"]:
            period = data["data"][0]["period"]
            assert " " in period or "T" in period  # Has time component

    def test_compare_monthly_granularity(self, api_client, auth_headers, test_windfarm_ids):
        """Test comparison with monthly granularity."""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

        params = {
            "windfarm_ids": test_windfarm_ids[:2],
            "start_date": start_date,
            "end_date": end_date,
            "granularity": "monthly"
        }

        response = api_client.get("/comparison/compare", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        assert "data" in data
        # Monthly data should have period in YYYY-MM format
        if data["data"]:
            period = data["data"][0]["period"]
            assert len(period) == 7  # YYYY-MM

    def test_compare_data_structure(self, api_client, auth_headers, test_windfarm_ids):
        """Test the structure of comparison data points."""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        params = {
            "windfarm_ids": test_windfarm_ids[:2],
            "start_date": start_date,
            "end_date": end_date,
            "granularity": "daily"
        }

        response = api_client.get("/comparison/compare", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        if data["data"]:
            point = data["data"][0]
            assert "period" in point
            assert "windfarm_id" in point
            assert "windfarm_name" in point
            assert "total_generation" in point
            assert "avg_generation" in point
            assert "avg_capacity_factor" in point
            assert "data_points" in point

    def test_compare_no_windfarms_error(self, api_client, auth_headers):
        """Test that comparing with no windfarms returns an error."""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "granularity": "daily"
        }

        response = api_client.get("/comparison/compare", params=params, headers=auth_headers)

        # Should return 422 (validation error) or error response
        assert response.status_code in [400, 422] or "error" in response.json()


class TestComparisonStatistics:
    """Tests for GET /comparison/statistics endpoint."""

    def test_get_statistics_default_period(self, api_client, auth_headers, test_windfarm_ids):
        """Test fetching statistics with default 30-day period."""
        params = {
            "windfarm_ids": test_windfarm_ids[:2]
        }

        response = api_client.get("/comparison/statistics", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)
        assert len(data) <= 2

        if data:
            stat = data[0]
            assert "windfarm_id" in stat
            assert "windfarm_name" in stat
            assert "capacity_mw" in stat
            assert "total_generation" in stat
            assert "avg_capacity_factor" in stat
            assert "period_days" in stat
            assert stat["period_days"] == 30

    def test_get_statistics_custom_period(self, api_client, auth_headers, test_windfarm_ids):
        """Test fetching statistics with custom period."""
        params = {
            "windfarm_ids": test_windfarm_ids[:2],
            "period_days": 7
        }

        response = api_client.get("/comparison/statistics", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        if data:
            stat = data[0]
            assert stat["period_days"] == 7

    def test_statistics_structure(self, api_client, auth_headers, test_windfarm_ids):
        """Test the structure of statistics response."""
        params = {
            "windfarm_ids": test_windfarm_ids[:2],
            "period_days": 30
        }

        response = api_client.get("/comparison/statistics", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        if data:
            stat = data[0]
            # Check all expected fields
            expected_fields = [
                "windfarm_id", "windfarm_name", "capacity_mw",
                "total_generation", "peak_generation", "min_generation",
                "avg_generation", "stddev_generation",
                "avg_capacity_factor", "max_capacity_factor", "min_capacity_factor",
                "data_points", "period_days", "availability_percent", "data_completeness"
            ]
            for field in expected_fields:
                assert field in stat, f"Missing field: {field}"

    def test_statistics_values_valid(self, api_client, auth_headers, test_windfarm_ids):
        """Test that statistics values are valid."""
        params = {
            "windfarm_ids": test_windfarm_ids[:2],
            "period_days": 30
        }

        response = api_client.get("/comparison/statistics", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        for stat in data:
            # Capacity factor should be between 0 and 1
            if stat["avg_capacity_factor"] is not None:
                assert 0 <= stat["avg_capacity_factor"] <= 1

            # Availability should be a percentage
            if stat["availability_percent"] is not None:
                assert 0 <= stat["availability_percent"] <= 100

            # Data completeness should be a percentage
            if stat["data_completeness"] is not None:
                assert 0 <= stat["data_completeness"] <= 100

            # Peak generation should be >= avg generation
            if stat["peak_generation"] > 0 and stat["avg_generation"] > 0:
                assert stat["peak_generation"] >= stat["avg_generation"]

    def test_statistics_no_windfarms_error(self, api_client, auth_headers):
        """Test that statistics with no windfarms returns an error."""
        response = api_client.get("/comparison/statistics", headers=auth_headers)

        # Should return 422 (validation error) or error response
        assert response.status_code in [400, 422] or "error" in response.json()
