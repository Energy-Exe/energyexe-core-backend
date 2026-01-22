"""
Integration tests for generation windfarm-stats API endpoint.
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

    if response.status_code == 404:
        pytest.skip("Server not running - skipping integration tests")

    if response.status_code != 200:
        pytest.skip("Could not authenticate - skipping protected endpoint tests")

    token = response.json().get("access_token")
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def sample_windfarm_id(api_client, auth_headers):
    """Get a sample windfarm ID for testing."""
    response = api_client.get(
        "/windfarms",
        params={"limit": 1},
        headers=auth_headers
    )

    if response.status_code == 404:
        pytest.skip("Server not running - skipping integration tests")

    if response.status_code != 200 or not response.json():
        pytest.skip("No windfarms available for testing")

    return response.json()[0]["id"]


class TestWindfarmStatsAPI:
    """Test suite for windfarm-stats endpoint."""

    def test_get_windfarm_stats_returns_valid_response(self, api_client, auth_headers, sample_windfarm_id):
        """Test GET /generation/windfarm-stats returns expected fields."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        response = api_client.get(
            "/generation/windfarm-stats",
            params={
                "windfarm_id": sample_windfarm_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # Verify expected fields exist
        expected_fields = [
            "total_generation_mwh",
            "avg_hourly_generation_mwh",
            "max_hourly_generation_mwh",
            "peak_hour",
            "capacity_factor_percent",
            "operating_hours",
            "total_hours",
            "avg_quality_score",
            "windfarm_name",
            "windfarm_code"
        ]
        for field in expected_fields:
            assert field in data, f"Missing field: {field}"

        # Verify types
        assert isinstance(data["total_generation_mwh"], (int, float))
        assert isinstance(data["avg_hourly_generation_mwh"], (int, float))
        assert isinstance(data["max_hourly_generation_mwh"], (int, float))
        assert isinstance(data["operating_hours"], int)
        assert isinstance(data["total_hours"], int)

    def test_get_windfarm_stats_not_found(self, api_client, auth_headers):
        """Test GET /generation/windfarm-stats returns 404 for non-existent windfarm."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        response = api_client.get(
            "/generation/windfarm-stats",
            params={
                "windfarm_id": 99999999,  # Non-existent ID
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            headers=auth_headers
        )

        assert response.status_code == 404

    def test_get_windfarm_stats_requires_auth(self, api_client):
        """Test GET /generation/windfarm-stats requires authentication."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        response = api_client.get(
            "/generation/windfarm-stats",
            params={
                "windfarm_id": 1,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        )

        # Skip if server is not running
        if response.status_code == 404:
            pytest.skip("Server not running - skipping integration tests")

        assert response.status_code == 401

    def test_get_windfarm_stats_missing_params(self, api_client, auth_headers):
        """Test GET /generation/windfarm-stats returns 422 for missing parameters."""
        response = api_client.get(
            "/generation/windfarm-stats",
            params={},  # Missing required params
            headers=auth_headers
        )

        assert response.status_code == 422  # Validation error

    def test_get_windfarm_stats_different_date_ranges(self, api_client, auth_headers, sample_windfarm_id):
        """Test GET /generation/windfarm-stats with different date ranges."""
        end_date = datetime.now()

        # Test 7 day range
        start_date_7d = end_date - timedelta(days=7)
        response_7d = api_client.get(
            "/generation/windfarm-stats",
            params={
                "windfarm_id": sample_windfarm_id,
                "start_date": start_date_7d.isoformat(),
                "end_date": end_date.isoformat()
            },
            headers=auth_headers
        )
        assert response_7d.status_code == 200

        # Test 90 day range
        start_date_90d = end_date - timedelta(days=90)
        response_90d = api_client.get(
            "/generation/windfarm-stats",
            params={
                "windfarm_id": sample_windfarm_id,
                "start_date": start_date_90d.isoformat(),
                "end_date": end_date.isoformat()
            },
            headers=auth_headers
        )
        assert response_90d.status_code == 200


class TestPriceProfileAPI:
    """Test suite for price-profile endpoint."""

    @pytest.fixture
    def sample_bidzone_id(self, api_client, auth_headers, sample_windfarm_id):
        """Get a sample bidzone ID for testing."""
        response = api_client.get(
            f"/windfarms/{sample_windfarm_id}",
            headers=auth_headers
        )

        if response.status_code != 200:
            pytest.skip("Could not get windfarm details")

        bidzone_id = response.json().get("bidzone_id")
        if not bidzone_id:
            pytest.skip("Windfarm has no bidzone - skipping price profile tests")

        return bidzone_id

    def test_get_price_profile_returns_valid_response(self, api_client, auth_headers, sample_bidzone_id):
        """Test GET /prices/analytics/price-profile/{bidzone_id} returns expected fields."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        response = api_client.get(
            f"/prices/analytics/price-profile/{sample_bidzone_id}",
            params={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "aggregation": "hourly"
            },
            headers=auth_headers
        )

        # May return 200 with data or 200 with empty profile if no price data
        assert response.status_code == 200
        data = response.json()

        # Verify structure
        expected_fields = ["bidzone_id", "start_date", "end_date", "aggregation", "profile"]
        for field in expected_fields:
            assert field in data, f"Missing field: {field}"

        assert isinstance(data["profile"], list)

    def test_get_price_profile_requires_auth(self, api_client):
        """Test GET /prices/analytics/price-profile/{bidzone_id} requires authentication."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        response = api_client.get(
            "/prices/analytics/price-profile/1",
            params={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "aggregation": "hourly"
            }
        )

        # Skip if server is not running
        if response.status_code == 404:
            pytest.skip("Server not running - skipping integration tests")

        assert response.status_code == 401
