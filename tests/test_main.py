"""Tests for main application endpoints."""

import pytest
from fastapi.testclient import TestClient


def test_root_endpoint(client: TestClient):
    """Test the root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "EnergyExe Core Backend API"
    assert data["version"] == "0.1.0"
    assert data["status"] == "healthy"


def test_health_check(client: TestClient):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


def test_openapi_docs_available_in_debug(client: TestClient):
    """Test that OpenAPI docs are available when in debug mode."""
    # Note: This test assumes DEBUG=True in test environment
    response = client.get("/docs")
    assert response.status_code == 200
