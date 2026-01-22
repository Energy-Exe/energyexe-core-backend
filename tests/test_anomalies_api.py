"""
Integration tests for data anomalies API endpoints.
These tests run against the actual API server and test endpoint functionality.
"""

import pytest
import httpx
import os

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


class TestAnomaliesListAPI:
    """Test suite for anomalies list endpoint."""

    def test_list_anomalies_returns_paginated_response(self, api_client, auth_headers):
        """Test GET /anomalies returns a paginated list."""
        response = api_client.get(
            "/anomalies",
            params={"page": 1, "page_size": 10},
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # Verify pagination structure
        expected_fields = ["anomalies", "total", "page", "page_size", "total_pages"]
        for field in expected_fields:
            assert field in data, f"Missing field: {field}"

        assert isinstance(data["anomalies"], list)
        assert data["page"] == 1
        assert data["page_size"] == 10

    def test_list_anomalies_with_severity_filter(self, api_client, auth_headers):
        """Test GET /anomalies with severity filter."""
        response = api_client.get(
            "/anomalies",
            params={"severity": "critical"},
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # All returned anomalies should have critical severity
        for anomaly in data["anomalies"]:
            assert anomaly["severity"] == "critical"

    def test_list_anomalies_with_status_filter(self, api_client, auth_headers):
        """Test GET /anomalies with status filter."""
        response = api_client.get(
            "/anomalies",
            params={"status": "pending"},
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # All returned anomalies should have pending status
        for anomaly in data["anomalies"]:
            assert anomaly["status"] == "pending"

    def test_list_anomalies_with_is_active_filter(self, api_client, auth_headers):
        """Test GET /anomalies with is_active filter."""
        response = api_client.get(
            "/anomalies",
            params={"is_active": True},
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # All returned anomalies should be active
        for anomaly in data["anomalies"]:
            assert anomaly["is_active"] is True


class TestAnomalyDetailAPI:
    """Test suite for anomaly detail endpoint."""

    def test_get_anomaly_not_found(self, api_client, auth_headers):
        """Test GET /anomalies/{id} returns 404 for non-existent anomaly."""
        response = api_client.get(
            "/anomalies/99999999",
            headers=auth_headers
        )

        assert response.status_code == 404

    def test_get_anomaly_returns_detail(self, api_client, auth_headers):
        """Test GET /anomalies/{id} returns anomaly details."""
        # First get a list of anomalies to find an ID
        list_response = api_client.get(
            "/anomalies",
            params={"page": 1, "page_size": 1},
            headers=auth_headers
        )

        if list_response.status_code != 200 or not list_response.json()["anomalies"]:
            pytest.skip("No anomalies available for testing")

        anomaly_id = list_response.json()["anomalies"][0]["id"]

        # Get the specific anomaly
        response = api_client.get(
            f"/anomalies/{anomaly_id}",
            headers=auth_headers
        )

        assert response.status_code == 200
        data = response.json()

        # Verify expected fields
        expected_fields = [
            "id", "anomaly_type", "severity", "status",
            "period_start", "period_end", "detected_at",
            "is_active", "created_at", "updated_at"
        ]
        for field in expected_fields:
            assert field in data, f"Missing field: {field}"

        assert data["id"] == anomaly_id


class TestAnomalyStatusUpdateAPI:
    """Test suite for anomaly status update endpoint."""

    def test_update_status_not_found(self, api_client, auth_headers):
        """Test PATCH /anomalies/{id}/status returns 404 for non-existent anomaly."""
        response = api_client.patch(
            "/anomalies/99999999/status",
            json={"status": "investigating"},
            headers=auth_headers
        )

        assert response.status_code == 404


class TestAnomaliesAPIAuthentication:
    """Test authentication requirements for anomalies endpoints."""

    def test_list_anomalies_requires_auth(self, api_client):
        """Test GET /anomalies requires authentication."""
        response = api_client.get("/anomalies")

        # Skip if server is not running
        if response.status_code == 404:
            pytest.skip("Server not running - skipping integration tests")

        assert response.status_code == 401

    def test_get_anomaly_requires_auth(self, api_client):
        """Test GET /anomalies/{id} requires authentication."""
        response = api_client.get("/anomalies/1")

        # Skip if server is not running
        if response.status_code == 404:
            pytest.skip("Server not running - skipping integration tests")

        assert response.status_code == 401

    def test_update_anomaly_status_requires_auth(self, api_client):
        """Test PATCH /anomalies/{id}/status requires authentication."""
        response = api_client.patch(
            "/anomalies/1/status",
            json={"status": "investigating"}
        )

        # Skip if server is not running
        if response.status_code == 404:
            pytest.skip("Server not running - skipping integration tests")

        assert response.status_code == 401


class TestAnomaliesAPIValidation:
    """Test input validation for anomalies endpoints."""

    def test_list_anomalies_invalid_page(self, api_client, auth_headers):
        """Test GET /anomalies with invalid page number."""
        response = api_client.get(
            "/anomalies",
            params={"page": 0},  # Invalid - must be >= 1
            headers=auth_headers
        )

        assert response.status_code == 422  # Validation error

    def test_list_anomalies_invalid_page_size(self, api_client, auth_headers):
        """Test GET /anomalies with invalid page_size."""
        response = api_client.get(
            "/anomalies",
            params={"page_size": 500},  # Invalid - max is 200
            headers=auth_headers
        )

        assert response.status_code == 422  # Validation error

    def test_update_status_invalid_status(self, api_client, auth_headers):
        """Test PATCH /anomalies/{id}/status with invalid status."""
        # First get a list of anomalies to find an ID
        list_response = api_client.get(
            "/anomalies",
            params={"page": 1, "page_size": 1},
            headers=auth_headers
        )

        if list_response.status_code != 200 or not list_response.json()["anomalies"]:
            pytest.skip("No anomalies available for testing")

        anomaly_id = list_response.json()["anomalies"][0]["id"]

        response = api_client.patch(
            f"/anomalies/{anomaly_id}/status",
            json={"status": "invalid_status"},
            headers=auth_headers
        )

        # Should return validation error
        assert response.status_code in [400, 422]
