"""
Integration tests for market/price analytics API endpoints.
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
def test_windfarm_id(api_client, auth_headers):
    """Get a test windfarm ID that has price data."""
    response = api_client.get("/windfarms", params={"limit": 1}, headers=auth_headers)
    if response.status_code != 200 or not response.json():
        pytest.skip("No windfarms available for testing")
    return response.json()[0]["id"]


@pytest.fixture
def test_bidzone_id(api_client, auth_headers):
    """Get a test bidzone ID."""
    response = api_client.get("/bidzones", params={"limit": 1}, headers=auth_headers)
    if response.status_code != 200 or not response.json():
        pytest.skip("No bidzones available for testing")
    return response.json()[0]["id"]


@pytest.fixture
def date_range():
    """Get a default date range for testing (last year)."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    return {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }


class TestPriceStatisticsAPI:
    """Test suite for price statistics endpoints."""

    def test_get_price_statistics(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test GET /prices/windfarms/{id}/statistics returns price statistics."""
        response = api_client.get(
            f"/prices/windfarms/{test_windfarm_id}/statistics",
            params=date_range,
            headers=auth_headers
        )

        # Should return 200 OK
        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "hours_with_data" in data
        assert "day_ahead" in data
        assert "intraday" in data

        # Check day_ahead structure
        if data["day_ahead"]:
            assert "average" in data["day_ahead"]
            assert "min" in data["day_ahead"]
            assert "max" in data["day_ahead"]

    def test_get_price_statistics_invalid_windfarm(self, api_client, auth_headers, date_range):
        """Test statistics endpoint with invalid windfarm ID."""
        response = api_client.get(
            "/prices/windfarms/99999999/statistics",
            params=date_range,
            headers=auth_headers
        )

        # Should return 404 or empty data
        assert response.status_code in [200, 404]


class TestPriceCoverageAPI:
    """Test suite for price coverage endpoints."""

    def test_get_price_coverage(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test GET /prices/windfarms/{id}/coverage returns coverage metrics."""
        response = api_client.get(
            f"/prices/windfarms/{test_windfarm_id}/coverage",
            params=date_range,
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "total_hours" in data
        assert "hours_with_data" in data
        assert "coverage_percent" in data


class TestCaptureRateAPI:
    """Test suite for capture rate analytics endpoints."""

    def test_get_capture_rate_monthly(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test GET /prices/analytics/capture-rate/{id} with monthly aggregation."""
        params = {**date_range, "aggregation": "month", "price_type": "day_ahead"}
        response = api_client.get(
            f"/prices/analytics/capture-rate/{test_windfarm_id}",
            params=params,
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "windfarm_id" in data
        assert "overall" in data
        assert "periods" in data

        # Check overall structure
        overall = data["overall"]
        assert "total_generation_mwh" in overall
        assert "total_revenue_eur" in overall
        assert "capture_rate" in overall

    def test_get_capture_rate_yearly(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test capture rate with yearly aggregation."""
        params = {**date_range, "aggregation": "year", "price_type": "day_ahead"}
        response = api_client.get(
            f"/prices/analytics/capture-rate/{test_windfarm_id}",
            params=params,
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()
        assert data["aggregation"] == "year"


class TestRevenueMetricsAPI:
    """Test suite for revenue metrics endpoints."""

    def test_get_revenue_metrics_monthly(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test GET /prices/analytics/revenue/{id} with monthly aggregation."""
        params = {**date_range, "aggregation": "month"}
        response = api_client.get(
            f"/prices/analytics/revenue/{test_windfarm_id}",
            params=params,
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "windfarm_id" in data
        assert "periods" in data

        # Check periods structure if data exists
        if data["periods"]:
            period = data["periods"][0]
            assert "period" in period
            assert "total_generation_mwh" in period
            assert "total_revenue_eur" in period
            assert "avg_day_ahead_price" in period

    def test_get_revenue_metrics_with_different_aggregations(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test revenue metrics with different aggregation levels."""
        for agg in ["day", "week", "month", "year"]:
            params = {**date_range, "aggregation": agg}
            response = api_client.get(
                f"/prices/analytics/revenue/{test_windfarm_id}",
                params=params,
                headers=auth_headers
            )

            assert response.status_code == 200
            data = response.json()
            assert data["aggregation"] == agg


class TestPriceProfileAPI:
    """Test suite for price profile endpoints."""

    def test_get_hourly_price_profile(self, api_client, auth_headers, test_bidzone_id, date_range):
        """Test GET /prices/analytics/price-profile/{bidzone_id} with hourly aggregation."""
        params = {**date_range, "aggregation": "hourly"}
        response = api_client.get(
            f"/prices/analytics/price-profile/{test_bidzone_id}",
            params=params,
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "bidzone_id" in data
        assert "profile" in data

        # Check profile entries
        if data["profile"]:
            entry = data["profile"][0]
            assert "hour_of_day" in entry or "day_of_week" in entry
            assert "avg_price" in entry
            assert "sample_count" in entry

    def test_get_daily_price_profile(self, api_client, auth_headers, test_bidzone_id, date_range):
        """Test price profile with daily aggregation."""
        params = {**date_range, "aggregation": "daily"}
        response = api_client.get(
            f"/prices/analytics/price-profile/{test_bidzone_id}",
            params=params,
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()
        assert data["aggregation"] == "daily"


class TestGenerationPriceCorrelationAPI:
    """Test suite for generation-price correlation endpoints."""

    def test_get_correlation(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test GET /prices/analytics/correlation/{id} returns correlation data."""
        response = api_client.get(
            f"/prices/analytics/correlation/{test_windfarm_id}",
            params=date_range,
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "windfarm_id" in data
        assert "correlation" in data
        assert "sample_size" in data

        # Correlation should be between -1 and 1 if present
        if data["correlation"] is not None:
            assert -1 <= data["correlation"] <= 1

    def test_correlation_interpretation(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test that correlation includes interpretation."""
        response = api_client.get(
            f"/prices/analytics/correlation/{test_windfarm_id}",
            params=date_range,
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # Should have interpretation or message
        assert "interpretation" in data or "message" in data


class TestMarketAPIPerformance:
    """Test suite for market API performance."""

    def test_capture_rate_response_time(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test that capture rate endpoint responds within acceptable time."""
        import time

        params = {**date_range, "aggregation": "month"}
        start_time = time.time()

        response = api_client.get(
            f"/prices/analytics/capture-rate/{test_windfarm_id}",
            params=params,
            headers=auth_headers
        )

        elapsed_time = time.time() - start_time

        assert response.status_code == 200
        # Should respond within 10 seconds for reasonable date ranges
        assert elapsed_time < 10.0, f"Response took {elapsed_time:.2f}s"

    def test_revenue_response_time(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test that revenue endpoint responds within acceptable time."""
        import time

        params = {**date_range, "aggregation": "month"}
        start_time = time.time()

        response = api_client.get(
            f"/prices/analytics/revenue/{test_windfarm_id}",
            params=params,
            headers=auth_headers
        )

        elapsed_time = time.time() - start_time

        assert response.status_code == 200
        assert elapsed_time < 10.0, f"Response took {elapsed_time:.2f}s"


class TestMarketAPIValidation:
    """Test suite for input validation on market endpoints."""

    def test_invalid_date_range(self, api_client, auth_headers, test_windfarm_id):
        """Test that invalid date ranges are handled gracefully."""
        params = {
            "start_date": "2025-12-31",  # End date before start date
            "end_date": "2025-01-01"
        }
        response = api_client.get(
            f"/prices/windfarms/{test_windfarm_id}/statistics",
            params=params,
            headers=auth_headers
        )

        # Should handle gracefully (either error or empty data)
        assert response.status_code in [200, 400, 422]

    def test_invalid_aggregation(self, api_client, auth_headers, test_windfarm_id, date_range):
        """Test that invalid aggregation values are rejected."""
        params = {**date_range, "aggregation": "invalid"}
        response = api_client.get(
            f"/prices/analytics/capture-rate/{test_windfarm_id}",
            params=params,
            headers=auth_headers
        )

        # Should return validation error
        assert response.status_code in [400, 422]

    def test_missing_required_params(self, api_client, auth_headers, test_windfarm_id):
        """Test that missing required parameters return appropriate error."""
        # Missing date range
        response = api_client.get(
            f"/prices/windfarms/{test_windfarm_id}/statistics",
            headers=auth_headers
        )

        assert response.status_code in [400, 422]
