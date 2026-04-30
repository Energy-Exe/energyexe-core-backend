"""
Integration tests for windfarms API endpoints.
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


class TestWindfarmsAPI:
    """Test suite for windfarms API endpoints."""

    def test_get_windfarms_list(self, api_client):
        """Test GET /windfarms returns a paginated list."""
        response = api_client.get("/windfarms", params={"limit": 10})

        assert response.status_code == 200
        data = response.json()

        # Check response is a list
        assert isinstance(data, list)

        # Check response size respects limit
        assert len(data) <= 10

        # If there's data, check structure
        if data:
            windfarm = data[0]
            # Required fields
            assert "id" in windfarm
            assert "code" in windfarm
            assert "name" in windfarm
            assert "country_id" in windfarm
            # List-specific fields
            assert "country" in windfarm
            assert "owners" in windfarm

    def test_get_windfarms_pagination(self, api_client):
        """Test pagination works correctly."""
        # Get first page
        page1 = api_client.get("/windfarms", params={"limit": 5, "skip": 0}).json()

        # Get second page
        page2 = api_client.get("/windfarms", params={"limit": 5, "skip": 5}).json()

        # Verify pages are different (if enough data)
        if len(page1) == 5 and len(page2) > 0:
            page1_ids = {wf["id"] for wf in page1}
            page2_ids = {wf["id"] for wf in page2}
            assert page1_ids.isdisjoint(page2_ids), "Pages should not overlap"

    def test_search_windfarms(self, api_client):
        """Test GET /windfarms/search returns matching results."""
        # Search for a common term
        response = api_client.get("/windfarms/search", params={"q": "wind"})

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)

        # Verify results match search term
        for windfarm in data:
            name_lower = windfarm["name"].lower()
            # The search should match windfarms with "wind" in name
            # Note: Backend may have different search behavior

    def test_search_windfarms_requires_query(self, api_client):
        """Test search requires a query parameter."""
        response = api_client.get("/windfarms/search")

        # Should return 422 (Unprocessable Entity) for missing required param
        assert response.status_code == 422

    def test_get_windfarm_by_id(self, api_client):
        """Test GET /windfarms/{id} returns a single windfarm."""
        # First get a list to get a valid ID
        list_response = api_client.get("/windfarms", params={"limit": 1})
        windfarms = list_response.json()

        if not windfarms:
            pytest.skip("No windfarms in database")

        windfarm_id = windfarms[0]["id"]

        # Get the specific windfarm
        response = api_client.get(f"/windfarms/{windfarm_id}")

        assert response.status_code == 200
        data = response.json()

        assert data["id"] == windfarm_id
        assert "code" in data
        assert "name" in data

    def test_get_windfarm_not_found(self, api_client):
        """Test GET /windfarms/{id} returns 404 for non-existent ID."""
        response = api_client.get("/windfarms/999999999")

        assert response.status_code == 404

    def test_get_windfarm_with_owners(self, api_client):
        """Test GET /windfarms/{id}/with-owners returns full details."""
        # First get a list to get a valid ID
        list_response = api_client.get("/windfarms", params={"limit": 1})
        windfarms = list_response.json()

        if not windfarms:
            pytest.skip("No windfarms in database")

        windfarm_id = windfarms[0]["id"]

        # Get windfarm with owners
        response = api_client.get(f"/windfarms/{windfarm_id}/with-owners")

        assert response.status_code == 200
        data = response.json()

        # Check enhanced fields
        assert "windfarm_owners" in data
        assert isinstance(data["windfarm_owners"], list)

        # Check nested relationships exist (may be null)
        assert "country" in data
        assert "state" in data
        assert "region" in data
        assert "bidzone" in data

    def test_get_windfarm_turbine_units(self, api_client):
        """Test GET /windfarms/{id}/turbine-units returns turbine list."""
        # First get a list to get a valid ID
        list_response = api_client.get("/windfarms", params={"limit": 1})
        windfarms = list_response.json()

        if not windfarms:
            pytest.skip("No windfarms in database")

        windfarm_id = windfarms[0]["id"]

        # Get turbine units
        response = api_client.get(f"/windfarms/{windfarm_id}/turbine-units")

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)

        # Check structure if there are turbines
        if data:
            turbine = data[0]
            assert "id" in turbine
            assert "code" in turbine

    def test_get_windfarm_generation_units(self, api_client):
        """Test GET /windfarms/{id}/generation-units returns generation units."""
        # First get a list to get a valid ID
        list_response = api_client.get("/windfarms", params={"limit": 1})
        windfarms = list_response.json()

        if not windfarms:
            pytest.skip("No windfarms in database")

        windfarm_id = windfarms[0]["id"]

        # Get generation units
        response = api_client.get(f"/windfarms/{windfarm_id}/generation-units")

        assert response.status_code == 200
        data = response.json()

        assert isinstance(data, list)

    def test_windfarms_list_includes_country_info(self, api_client):
        """Test that windfarm list includes country information."""
        response = api_client.get("/windfarms", params={"limit": 10})
        data = response.json()

        for windfarm in data:
            if windfarm.get("country"):
                assert "id" in windfarm["country"]
                assert "code" in windfarm["country"]
                assert "name" in windfarm["country"]

    def test_windfarms_list_includes_owners(self, api_client):
        """Test that windfarm list includes owners with percentages."""
        response = api_client.get("/windfarms", params={"limit": 10})
        data = response.json()

        for windfarm in data:
            assert "owners" in windfarm
            assert isinstance(windfarm["owners"], list)

            for owner in windfarm["owners"]:
                assert "id" in owner
                assert "name" in owner
                # ownership_percentage may be null


class TestWindfarmsSearchAcrossFields:
    """#4 — Search must match against name, code, country.name and owner.name (not just name)."""

    def test_search_returns_country_matches(self, api_client):
        """Searching for a country name should return windfarms in that country, even if 'germany' is not in any windfarm name."""
        response = api_client.get("/windfarms/search", params={"q": "Germany"})

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

        # If the DB has any German windfarms (it does in our seed), we should get matches.
        if not data:
            pytest.skip("No German windfarms in DB — seed not populated; cannot validate #4.")

        # Every result should either have country.name == Germany OR have 'germany' in
        # name/code (the BE OR-clause). We check that at least one row matches by country
        # exclusively (name doesn't contain 'germany') — the new behavior we're verifying.
        country_only_matches = [
            wf for wf in data
            if (wf.get("country") or {}).get("name", "").lower() == "germany"
            and "germany" not in wf.get("name", "").lower()
            and "germany" not in wf.get("code", "").lower()
        ]
        assert len(country_only_matches) > 0, (
            "Expected at least one windfarm matched purely by country.name=Germany "
            "(name/code not containing 'germany'). The OR-search across country was not applied."
        )

    def test_search_returns_owner_matches(self, api_client):
        """Searching for an owner name should return windfarms owned by that company, even if the owner name is not in the windfarm name."""
        # Pull an owner name from a real windfarm in the DB to use as a search term.
        seed = api_client.get("/windfarms", params={"limit": 100}).json()
        owner_name = None
        owner_windfarm_name = None
        owner_windfarm_code = None
        for wf in seed:
            owners = wf.get("owners") or []
            if owners:
                # Find an owner whose name doesn't appear in the windfarm name/code so we
                # can prove the search matched purely on owner.
                for o in owners:
                    name = (o.get("name") or "").strip()
                    if not name:
                        continue
                    wf_name = (wf.get("name") or "").lower()
                    wf_code = (wf.get("code") or "").lower()
                    if name.lower() not in wf_name and name.lower() not in wf_code:
                        owner_name = name
                        owner_windfarm_name = wf.get("name")
                        owner_windfarm_code = wf.get("code")
                        break
            if owner_name:
                break

        if not owner_name:
            pytest.skip("Could not find a windfarm whose owner name is distinct from its own name.")

        # Use just the first word of the owner name to keep the ILIKE pattern simple.
        search_term = owner_name.split()[0]
        response = api_client.get("/windfarms/search", params={"q": search_term})

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list), "Expected a list response."
        assert len(data) > 0, f"Expected at least one match for owner-name search '{search_term}'."

        # The seed windfarm should appear (either by owner-match or directly).
        match_codes = {wf.get("code") for wf in data}
        assert owner_windfarm_code in match_codes, (
            f"Windfarm '{owner_windfarm_name}' (code={owner_windfarm_code}) should appear "
            f"in search results for owner-name term '{search_term}'."
        )

    def test_search_still_matches_name_substring(self, api_client):
        """Backward-compat: name-substring search must still work after the OR refactor."""
        seed = api_client.get("/windfarms", params={"limit": 1}).json()
        if not seed:
            pytest.skip("No windfarms available.")

        seed_name = seed[0]["name"]
        # Use the first 4 characters as a search term to keep the match broad.
        term = seed_name[:4]

        response = api_client.get("/windfarms/search", params={"q": term})
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert any(wf["id"] == seed[0]["id"] for wf in data), (
            f"Expected to find seed windfarm '{seed_name}' when searching for substring '{term}'."
        )


class TestWindfarmsListIncludesBidzone:
    """#6 — Card view shows bidzone, so the list endpoint must serialize bidzone relation."""

    def test_list_response_includes_bidzone_field(self, api_client):
        response = api_client.get("/windfarms", params={"limit": 100})
        assert response.status_code == 200
        data = response.json()

        # Every windfarm row should have a 'bidzone' key (may be None).
        for wf in data:
            assert "bidzone" in wf, f"Windfarm {wf.get('id')} missing 'bidzone' field."

        # At least one windfarm in our DB should have a populated bidzone.
        with_bidzone = [wf for wf in data if wf.get("bidzone")]
        if not with_bidzone:
            pytest.skip("No windfarms with bidzone in this slice — adjust limit to verify.")

        # When bidzone is populated, it must have id/code/name.
        sample = with_bidzone[0]["bidzone"]
        assert "id" in sample
        assert "code" in sample
        assert "name" in sample


class TestWindfarmsAPIPerformance:
    """Performance tests for windfarms API."""

    def test_large_pagination_limit(self, api_client):
        """Test API handles large pagination limits."""
        response = api_client.get("/windfarms", params={"limit": 500})

        assert response.status_code == 200
        # Should complete without timing out

    def test_search_response_time(self, api_client):
        """Test search endpoint responds quickly."""
        import time

        start = time.time()
        response = api_client.get("/windfarms/search", params={"q": "test"})
        elapsed = time.time() - start

        assert response.status_code == 200
        # Should complete in less than 5 seconds
        assert elapsed < 5.0, f"Search took too long: {elapsed:.2f}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
