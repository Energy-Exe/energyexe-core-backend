"""
Integration tests for weather data API endpoints.
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


@pytest.fixture
def test_windfarm_id(api_client, auth_headers):
    """Get a test windfarm ID that has weather data."""
    response = api_client.get("/windfarms", params={"limit": 1}, headers=auth_headers)
    if response.status_code != 200 or not response.json():
        pytest.skip("No windfarms available for testing")
    return response.json()[0]["id"]


class TestWeatherStatisticsAPI:
    """Test suite for weather statistics endpoints."""

    def test_get_weather_statistics(self, api_client, auth_headers, test_windfarm_id):
        """Test GET /weather-data/windfarms/{id}/statistics returns wind statistics."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=365)

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        response = api_client.get(
            f"/weather-data/windfarms/{test_windfarm_id}/statistics",
            params=params,
            headers=auth_headers
        )

        # If no data, skip; otherwise verify structure
        if response.status_code == 404:
            pytest.skip("No weather data available for this windfarm")

        assert response.status_code == 200
        data = response.json()

        # Check expected fields
        expected_fields = [
            "meanSpeed", "medianSpeed", "maxSpeed", "minSpeed",
            "stdDev", "meanTemperature", "prevailingDirection",
            "totalHours"
        ]
        for field in expected_fields:
            assert field in data, f"Missing field: {field}"


class TestWindRoseAPI:
    """Test suite for wind rose endpoints."""

    def test_get_wind_rose(self, api_client, auth_headers, test_windfarm_id):
        """Test GET /weather-data/windfarms/{id}/wind-rose returns wind rose data."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=365)

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        response = api_client.get(
            f"/weather-data/windfarms/{test_windfarm_id}/wind-rose",
            params=params,
            headers=auth_headers
        )

        if response.status_code == 404:
            pytest.skip("No wind rose data available for this windfarm")

        assert response.status_code == 200
        data = response.json()

        # Check expected fields
        assert "directionBins" in data
        assert "speedBins" in data
        assert "frequency" in data
        assert "totalHours" in data
        assert "calmPercentage" in data


class TestWindDistributionAPI:
    """Test suite for wind speed distribution endpoints."""

    def test_get_wind_distribution(self, api_client, auth_headers, test_windfarm_id):
        """Test GET /weather-data/windfarms/{id}/distribution returns distribution data."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=365)

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        response = api_client.get(
            f"/weather-data/windfarms/{test_windfarm_id}/distribution",
            params=params,
            headers=auth_headers
        )

        if response.status_code == 404:
            pytest.skip("No distribution data available for this windfarm")

        assert response.status_code == 200
        data = response.json()

        # Check expected fields
        assert "speedBins" in data
        assert "frequency" in data
        assert "weibullK" in data
        assert "weibullC" in data
        assert "meanSpeed" in data


class TestCorrelationAPI:
    """Test suite for wind-generation correlation endpoints."""

    def test_get_correlation(self, api_client, auth_headers, test_windfarm_id):
        """Test GET /weather-data/windfarms/{id}/correlation returns correlation data."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=365)

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        response = api_client.get(
            f"/weather-data/windfarms/{test_windfarm_id}/correlation",
            params=params,
            headers=auth_headers
        )

        if response.status_code == 404:
            pytest.skip("No correlation data available for this windfarm")

        assert response.status_code == 200
        data = response.json()

        # Check expected fields
        assert "windSpeedBins" in data
        assert "avgGenerationMw" in data
        assert "correlationCoefficient" in data
        assert "rSquared" in data


class TestPowerCurveAPI:
    """Test suite for power curve endpoints."""

    def test_get_power_curve(self, api_client, auth_headers, test_windfarm_id):
        """Test GET /weather-data/windfarms/{id}/power-curve returns power curve data."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=365)

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        response = api_client.get(
            f"/weather-data/windfarms/{test_windfarm_id}/power-curve",
            params=params,
            headers=auth_headers
        )

        if response.status_code == 404:
            pytest.skip("No power curve data available for this windfarm")

        assert response.status_code == 200
        data = response.json()

        # Check expected fields
        assert "windSpeed" in data
        assert "generationMw" in data
        assert "correlationCoefficient" in data
        assert "rSquared" in data


class TestDiurnalPatternAPI:
    """Test suite for diurnal pattern endpoints."""

    def test_get_diurnal_pattern(self, api_client, auth_headers, test_windfarm_id):
        """Test GET /weather-data/windfarms/{id}/diurnal-pattern returns diurnal data."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=365)

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        response = api_client.get(
            f"/weather-data/windfarms/{test_windfarm_id}/diurnal-pattern",
            params=params,
            headers=auth_headers
        )

        if response.status_code == 404:
            pytest.skip("No diurnal pattern data available for this windfarm")

        assert response.status_code == 200
        data = response.json()

        # Check expected fields
        assert "hours" in data
        assert "avgWindSpeed" in data
        assert len(data["hours"]) == 24  # 24 hours


class TestSeasonalPatternAPI:
    """Test suite for seasonal pattern endpoints."""

    def test_get_seasonal_pattern(self, api_client, auth_headers, test_windfarm_id):
        """Test GET /weather-data/windfarms/{id}/seasonal-pattern returns seasonal data."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=365)

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        response = api_client.get(
            f"/weather-data/windfarms/{test_windfarm_id}/seasonal-pattern",
            params=params,
            headers=auth_headers
        )

        if response.status_code == 404:
            pytest.skip("No seasonal pattern data available for this windfarm")

        assert response.status_code == 200
        data = response.json()

        # Check expected fields
        assert "months" in data
        assert "avgWindSpeed" in data
        assert len(data["months"]) == 12  # 12 months


class TestWeatherAPIPerformance:
    """Performance tests for weather API."""

    def test_statistics_response_time(self, api_client, auth_headers, test_windfarm_id):
        """Test statistics endpoint responds in reasonable time."""
        import time

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=365)

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        start = time.time()
        response = api_client.get(
            f"/weather-data/windfarms/{test_windfarm_id}/statistics",
            params=params,
            headers=auth_headers
        )
        elapsed = time.time() - start

        # Accept 404 (no data) or 200 (success)
        assert response.status_code in [200, 404]
        # Should complete in less than 15 seconds
        assert elapsed < 15.0, f"Statistics endpoint took too long: {elapsed:.2f}s"

    def test_wind_rose_response_time(self, api_client, auth_headers, test_windfarm_id):
        """Test wind rose endpoint responds in reasonable time."""
        import time

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=365)

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        start = time.time()
        response = api_client.get(
            f"/weather-data/windfarms/{test_windfarm_id}/wind-rose",
            params=params,
            headers=auth_headers
        )
        elapsed = time.time() - start

        # Accept 404 (no data) or 200 (success)
        assert response.status_code in [200, 404]
        # Should complete in less than 15 seconds
        assert elapsed < 15.0, f"Wind rose endpoint took too long: {elapsed:.2f}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
