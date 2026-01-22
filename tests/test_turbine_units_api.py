"""
Integration tests for turbine-units API endpoints.
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


class TestTurbineUnitsAPI:
    """Test suite for turbine-units API endpoints."""

    def test_get_turbine_units_list(self, api_client):
        """Test GET /turbine-units returns a paginated list."""
        response = api_client.get("/turbine-units", params={"limit": 10})

        assert response.status_code == 200
        data = response.json()

        # Check response is a list
        assert isinstance(data, list)

        # Check response size respects limit
        assert len(data) <= 10

        # If there's data, check structure
        if data:
            turbine = data[0]
            # Required fields
            assert "id" in turbine
            assert "code" in turbine
            assert "windfarm_id" in turbine
            assert "turbine_model_id" in turbine
            assert "lat" in turbine
            assert "lng" in turbine
            # Relationships should be loaded
            assert "windfarm" in turbine
            assert "turbine_model" in turbine

    def test_get_turbine_units_pagination(self, api_client):
        """Test pagination works correctly."""
        # Get first page
        page1 = api_client.get("/turbine-units", params={"limit": 5, "skip": 0}).json()

        # Get second page
        page2 = api_client.get("/turbine-units", params={"limit": 5, "skip": 5}).json()

        # Verify pages are different (if enough data)
        if len(page1) == 5 and len(page2) > 0:
            page1_ids = {t["id"] for t in page1}
            page2_ids = {t["id"] for t in page2}
            assert page1_ids.isdisjoint(page2_ids), "Pages should not overlap"

    def test_get_turbine_units_filter_by_windfarm(self, api_client):
        """Test filtering turbine units by windfarm_id."""
        # First, get a list to find a valid windfarm_id
        all_turbines = api_client.get("/turbine-units", params={"limit": 100}).json()

        if not all_turbines:
            pytest.skip("No turbine units in database")

        # Find a windfarm_id that has turbines
        windfarm_id = all_turbines[0]["windfarm_id"]

        # Filter by this windfarm
        response = api_client.get("/turbine-units", params={"windfarm_id": windfarm_id})

        assert response.status_code == 200
        data = response.json()

        # All returned turbines should be from this windfarm
        for turbine in data:
            assert turbine["windfarm_id"] == windfarm_id

    def test_get_turbine_units_filter_by_model(self, api_client):
        """Test filtering turbine units by model_id."""
        # First, get a list to find a valid model_id
        all_turbines = api_client.get("/turbine-units", params={"limit": 100}).json()

        if not all_turbines:
            pytest.skip("No turbine units in database")

        # Find a model_id that has turbines
        model_id = all_turbines[0]["turbine_model_id"]

        # Filter by this model
        response = api_client.get("/turbine-units", params={"model_id": model_id})

        assert response.status_code == 200
        data = response.json()

        # All returned turbines should have this model
        for turbine in data:
            assert turbine["turbine_model_id"] == model_id

    def test_get_turbine_units_filter_by_status(self, api_client):
        """Test filtering turbine units by status."""
        # Test filtering by operational status
        response = api_client.get("/turbine-units", params={"status": "operational"})

        assert response.status_code == 200
        data = response.json()

        # All returned turbines should have operational status
        for turbine in data:
            assert turbine["status"] == "operational"

    def test_get_turbine_units_search(self, api_client):
        """Test searching turbine units by code."""
        # First, get a turbine to know a valid code pattern
        all_turbines = api_client.get("/turbine-units", params={"limit": 1}).json()

        if not all_turbines:
            pytest.skip("No turbine units in database")

        # Use part of the code for search
        code = all_turbines[0]["code"]
        search_term = code[:3] if len(code) >= 3 else code

        response = api_client.get("/turbine-units", params={"search": search_term})

        assert response.status_code == 200
        data = response.json()

        # Results should contain the search term in their code
        for turbine in data:
            assert search_term.lower() in turbine["code"].lower()

    def test_get_turbine_units_stats(self, api_client):
        """Test GET /turbine-units/stats returns aggregate statistics."""
        response = api_client.get("/turbine-units/stats")

        assert response.status_code == 200
        data = response.json()

        # Check required fields in response
        assert "total_count" in data
        assert "total_capacity_mw" in data
        assert "windfarm_count" in data
        assert "status_breakdown" in data

        # Values should be reasonable
        assert isinstance(data["total_count"], int)
        assert data["total_count"] >= 0
        assert isinstance(data["total_capacity_mw"], (int, float))
        assert isinstance(data["windfarm_count"], int)
        assert isinstance(data["status_breakdown"], dict)

    def test_get_turbine_units_stats_with_filter(self, api_client):
        """Test stats endpoint with filters."""
        # First, get a turbine to find a valid windfarm_id
        all_turbines = api_client.get("/turbine-units", params={"limit": 1}).json()

        if not all_turbines:
            pytest.skip("No turbine units in database")

        windfarm_id = all_turbines[0]["windfarm_id"]

        # Get stats filtered by windfarm
        response = api_client.get("/turbine-units/stats", params={"windfarm_id": windfarm_id})

        assert response.status_code == 200
        data = response.json()

        # Stats should reflect filtered results
        assert "total_count" in data
        assert data["total_count"] >= 1  # At least the turbine we found

    def test_get_single_turbine_unit(self, api_client):
        """Test GET /turbine-units/{id} returns a single turbine unit."""
        # First, get a turbine to find a valid ID
        all_turbines = api_client.get("/turbine-units", params={"limit": 1}).json()

        if not all_turbines:
            pytest.skip("No turbine units in database")

        turbine_id = all_turbines[0]["id"]

        response = api_client.get(f"/turbine-units/{turbine_id}")

        assert response.status_code == 200
        data = response.json()

        assert data["id"] == turbine_id
        assert "code" in data
        assert "windfarm" in data
        assert "turbine_model" in data

    def test_get_nonexistent_turbine_unit(self, api_client):
        """Test GET /turbine-units/{id} returns 404 for non-existent ID."""
        response = api_client.get("/turbine-units/999999999")

        assert response.status_code == 404

    def test_get_turbine_unit_with_relations(self, api_client):
        """Test GET /turbine-units/{id}/with-relations returns full relations."""
        # First, get a turbine to find a valid ID
        all_turbines = api_client.get("/turbine-units", params={"limit": 1}).json()

        if not all_turbines:
            pytest.skip("No turbine units in database")

        turbine_id = all_turbines[0]["id"]

        response = api_client.get(f"/turbine-units/{turbine_id}/with-relations")

        assert response.status_code == 200
        data = response.json()

        # Check detailed response structure
        assert "id" in data
        assert "code" in data
        assert "windfarm" in data
        assert "turbine_model" in data
        assert "generation_units" in data

        # Windfarm should have detailed info
        if data["windfarm"]:
            assert "id" in data["windfarm"]
            assert "name" in data["windfarm"]

        # Turbine model should have detailed info
        if data["turbine_model"]:
            assert "id" in data["turbine_model"]
            assert "model" in data["turbine_model"]
            assert "supplier" in data["turbine_model"]

    def test_get_turbine_unit_by_code(self, api_client):
        """Test GET /turbine-units/code/{code} returns turbine by code."""
        # First, get a turbine to find a valid code
        all_turbines = api_client.get("/turbine-units", params={"limit": 1}).json()

        if not all_turbines:
            pytest.skip("No turbine units in database")

        code = all_turbines[0]["code"]

        response = api_client.get(f"/turbine-units/code/{code}")

        assert response.status_code == 200
        data = response.json()

        assert data["code"] == code

    def test_combined_filters(self, api_client):
        """Test combining multiple filters."""
        # Get initial data to find valid filter values
        all_turbines = api_client.get("/turbine-units", params={"limit": 100}).json()

        if not all_turbines:
            pytest.skip("No turbine units in database")

        # Find a turbine with a specific windfarm and model
        turbine = all_turbines[0]
        windfarm_id = turbine["windfarm_id"]
        model_id = turbine["turbine_model_id"]

        # Apply combined filters
        response = api_client.get(
            "/turbine-units",
            params={
                "windfarm_id": windfarm_id,
                "model_id": model_id,
            },
        )

        assert response.status_code == 200
        data = response.json()

        # All results should match both filters
        for t in data:
            assert t["windfarm_id"] == windfarm_id
            assert t["turbine_model_id"] == model_id
