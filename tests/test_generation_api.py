"""
Integration tests for generation API endpoints.
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
    # Login to get auth token
    response = api_client.post("/auth/login", json={
        "email": os.getenv("TEST_USER_EMAIL", "admin@energyexe.com"),
        "password": os.getenv("TEST_USER_PASSWORD", "admin123")
    })

    if response.status_code != 200:
        pytest.skip("Could not authenticate - skipping protected endpoint tests")

    token = response.json().get("access_token")
    return {"Authorization": f"Bearer {token}"}


class TestGenerationAPI:
    """Test suite for generation API endpoints."""

    def test_get_generation_stats(self, api_client, auth_headers):
        """Test GET /generation/stats returns statistics for all sources."""
        response = api_client.get("/generation/stats", headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "sources" in data
        assert isinstance(data["sources"], list)

        # Check each source has expected fields
        for source in data["sources"]:
            assert "source" in source
            assert "totalRecords" in source
            assert "dateRange" in source
            assert "coverage" in source
            assert "avgQuality" in source

    def test_get_generation_availability(self, api_client, auth_headers):
        """Test GET /generation/availability returns availability calendar."""
        # Use current month
        now = datetime.utcnow()
        params = {
            "year": now.year,
            "month": now.month
        }

        response = api_client.get("/generation/availability", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # The response structure may vary based on data availability
        # Just ensure it doesn't error

    def test_get_generation_availability_with_sources(self, api_client, auth_headers):
        """Test availability endpoint with source filter."""
        now = datetime.utcnow()
        params = {
            "year": now.year,
            "month": now.month,
            "sources": "NVE,ENTSOE"
        }

        response = api_client.get("/generation/availability", params=params, headers=auth_headers)

        assert response.status_code == 200

    def test_get_generation_raw_data(self, api_client, auth_headers):
        """Test GET /generation/raw returns raw generation data."""
        params = {
            "limit": 10,
            "offset": 0
        }

        response = api_client.get("/generation/raw", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "data" in data
        assert "pagination" in data
        assert isinstance(data["data"], list)

        # Check pagination structure
        pagination = data["pagination"]
        assert "total" in pagination
        assert "limit" in pagination
        assert "offset" in pagination
        assert "hasMore" in pagination

    def test_get_generation_raw_data_with_source_filter(self, api_client, auth_headers):
        """Test raw data endpoint with source filter."""
        params = {
            "source": "NVE",
            "limit": 5
        }

        response = api_client.get("/generation/raw", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Verify all returned records are from the specified source
        for record in data["data"]:
            assert record["source"] == "NVE"

    def test_get_generation_hourly(self, api_client, auth_headers):
        """Test GET /generation/hourly returns hourly generation data."""
        # Use a date range from the past month
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=7)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        response = api_client.get("/generation/hourly", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)

        # Check structure if there's data
        if data:
            record = data[0]
            assert "hour" in record
            assert "generation_mwh" in record
            assert "source" in record

    def test_get_generation_hourly_with_windfarm(self, api_client, auth_headers):
        """Test hourly endpoint filtered by windfarm."""
        # First get a windfarm ID
        wf_response = api_client.get("/windfarms", params={"limit": 1}, headers=auth_headers)
        if wf_response.status_code != 200 or not wf_response.json():
            pytest.skip("No windfarms available for testing")

        windfarm_id = wf_response.json()[0]["id"]

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=7)

        params = {
            "windfarm_id": windfarm_id,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        response = api_client.get("/generation/hourly", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_generation_hourly_with_quality_filter(self, api_client, auth_headers):
        """Test hourly endpoint with minimum quality score filter."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=7)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "min_quality_score": 0.8
        }

        response = api_client.get("/generation/hourly", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # If data returned, verify quality scores meet threshold
        for record in data:
            if record.get("quality_score") is not None:
                assert record["quality_score"] >= 0.8


class TestGenerationAPIValidation:
    """Validation tests for generation API endpoints."""

    def test_availability_invalid_month(self, api_client, auth_headers):
        """Test availability endpoint rejects invalid month."""
        params = {
            "year": 2024,
            "month": 13  # Invalid month
        }

        response = api_client.get("/generation/availability", params=params, headers=auth_headers)

        # Should return validation error
        assert response.status_code == 400

    def test_raw_data_pagination_limits(self, api_client, auth_headers):
        """Test raw data respects pagination limits."""
        params = {
            "limit": 1001  # Exceeds max limit
        }

        response = api_client.get("/generation/raw", params=params, headers=auth_headers)

        # Should either enforce max limit or return validation error
        if response.status_code == 200:
            data = response.json()
            assert len(data["data"]) <= 1000  # Max allowed


class TestGenerationAPIPerformance:
    """Performance tests for generation API."""

    def test_stats_response_time(self, api_client, auth_headers):
        """Test stats endpoint responds in reasonable time."""
        import time

        start = time.time()
        response = api_client.get("/generation/stats", headers=auth_headers)
        elapsed = time.time() - start

        assert response.status_code == 200
        # Should complete in less than 10 seconds
        assert elapsed < 10.0, f"Stats endpoint took too long: {elapsed:.2f}s"

    def test_hourly_large_range_response_time(self, api_client, auth_headers):
        """Test hourly endpoint handles large date ranges."""
        import time

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        start = time.time()
        response = api_client.get("/generation/hourly", params=params, headers=auth_headers)
        elapsed = time.time() - start

        assert response.status_code == 200
        # Should complete in less than 15 seconds for 30 days
        assert elapsed < 15.0, f"Hourly endpoint took too long: {elapsed:.2f}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
