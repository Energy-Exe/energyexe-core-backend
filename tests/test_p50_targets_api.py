"""
Integration tests for P50 targets API endpoints.

Tests the full lifecycle of P50 target management and analysis:
- CRUD operations (create, read, update, delete)
- Date overlap validation
- Default start date computation from COD
- P50 analysis calculation (cumulative gap, monthly data, yearly gaps)
- Edge cases (no targets, invalid data, non-existent windfarms)

Runs against the actual API server (requires backend running on port 8001).
"""

import os
import time

import httpx
import pytest

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000/api/v1")


@pytest.fixture
def api_client():
    """Create an HTTP client for API testing."""
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0)


@pytest.fixture
def windfarm_id(api_client):
    """Get a valid windfarm ID from the database for testing."""
    response = api_client.get("/windfarms", params={"limit": 1})
    assert response.status_code == 200, f"Failed to get windfarms: {response.text}"
    data = response.json()
    if not data:
        pytest.skip("No windfarms in database")
    return data[0]["id"]


@pytest.fixture
def windfarm_with_cod(api_client):
    """Get a windfarm that has a commercial_operational_date set."""
    response = api_client.get("/windfarms", params={"limit": 50})
    assert response.status_code == 200
    for wf in response.json():
        if wf.get("commercial_operational_date"):
            return wf
    pytest.skip("No windfarm with commercial_operational_date found")


@pytest.fixture
def windfarm_with_generation(api_client):
    """Get a windfarm that has generation data available."""
    response = api_client.get("/windfarms", params={"limit": 50})
    assert response.status_code == 200
    windfarms = response.json()

    for wf in windfarms:
        # Check if this windfarm has generation units (proxy for having data)
        gu_resp = api_client.get(f"/windfarms/{wf['id']}/generation-units")
        if gu_resp.status_code == 200 and gu_resp.json():
            return wf

    pytest.skip("No windfarm with generation data found")


@pytest.fixture
def cleanup_targets(api_client):
    """Track created P50 targets and clean them up after the test."""
    created: list[tuple[int, int]] = []  # (windfarm_id, target_id)

    def register(windfarm_id: int, target_id: int):
        created.append((windfarm_id, target_id))

    yield register

    for wf_id, t_id in created:
        try:
            api_client.delete(f"/windfarms/{wf_id}/p50-targets/{t_id}")
        except Exception:
            pass


# ==============================================================================
# CRUD Tests
# ==============================================================================


class TestP50TargetsCRUD:
    """Test CRUD operations for P50 targets."""

    def test_list_targets_empty(self, api_client, windfarm_id):
        """GET /windfarms/{id}/p50-targets returns a list (possibly empty)."""
        response = api_client.get(f"/windfarms/{windfarm_id}/p50-targets")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_create_target(self, api_client, windfarm_id, cleanup_targets):
        """POST /windfarms/{id}/p50-targets creates a target and returns 201."""
        payload = {
            "p50_target_start_date": "2020-01-01",
            "p50_target_end_date": "2020-12-31",
            "p50_target_volume_gwh": 150.0,
            "source": "https://example.com/wind-resource-assessment",
            "comment": "Integration test target",
        }

        response = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets", json=payload
        )
        assert response.status_code == 201, f"Create failed: {response.text}"

        data = response.json()
        cleanup_targets(windfarm_id, data["id"])

        # Check response structure
        assert data["windfarm_id"] == windfarm_id
        assert data["p50_target_start_date"] == "2020-01-01"
        assert data["p50_target_end_date"] == "2020-12-31"
        assert data["p50_target_volume_gwh"] == 150.0
        assert data["source"] == "https://example.com/wind-resource-assessment"
        assert data["comment"] == "Integration test target"
        assert "id" in data
        assert "created_at" in data
        assert "updated_at" in data

    def test_create_target_returns_monthly_p50(self, api_client, windfarm_id, cleanup_targets):
        """Created target response includes computed monthly_p50_gwh = annual / 12."""
        payload = {
            "p50_target_start_date": "2019-01-01",
            "p50_target_end_date": "2019-12-31",
            "p50_target_volume_gwh": 120.0,
        }

        response = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets", json=payload
        )
        assert response.status_code == 201

        data = response.json()
        cleanup_targets(windfarm_id, data["id"])

        assert data["monthly_p50_gwh"] == 10.0  # 120 / 12

    def test_create_target_ongoing(self, api_client, windfarm_id, cleanup_targets):
        """Create a target with no end date (ongoing)."""
        payload = {
            "p50_target_start_date": "2018-01-01",
            "p50_target_volume_gwh": 200.0,
        }

        response = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets", json=payload
        )
        assert response.status_code == 201

        data = response.json()
        cleanup_targets(windfarm_id, data["id"])

        assert data["p50_target_end_date"] is None

    def test_list_targets_after_create(self, api_client, windfarm_id, cleanup_targets):
        """Listing targets returns newly created targets."""
        payload = {
            "p50_target_start_date": "2017-01-01",
            "p50_target_end_date": "2017-12-31",
            "p50_target_volume_gwh": 100.0,
        }

        create_resp = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets", json=payload
        )
        assert create_resp.status_code == 201
        created_id = create_resp.json()["id"]
        cleanup_targets(windfarm_id, created_id)

        # List and verify
        list_resp = api_client.get(f"/windfarms/{windfarm_id}/p50-targets")
        assert list_resp.status_code == 200
        targets = list_resp.json()

        ids = [t["id"] for t in targets]
        assert created_id in ids

    def test_update_target(self, api_client, windfarm_id, cleanup_targets):
        """PUT /windfarms/{id}/p50-targets/{tid} updates the target."""
        # Create
        create_resp = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets",
            json={
                "p50_target_start_date": "2016-01-01",
                "p50_target_end_date": "2016-12-31",
                "p50_target_volume_gwh": 100.0,
            },
        )
        assert create_resp.status_code == 201
        target_id = create_resp.json()["id"]
        cleanup_targets(windfarm_id, target_id)

        # Update
        update_resp = api_client.put(
            f"/windfarms/{windfarm_id}/p50-targets/{target_id}",
            json={
                "p50_target_volume_gwh": 180.0,
                "comment": "Updated in integration test",
            },
        )
        assert update_resp.status_code == 200

        updated = update_resp.json()
        assert updated["p50_target_volume_gwh"] == 180.0
        assert updated["monthly_p50_gwh"] == 15.0  # 180 / 12
        assert updated["comment"] == "Updated in integration test"
        # Original fields should be unchanged
        assert updated["p50_target_start_date"] == "2016-01-01"

    def test_delete_target(self, api_client, windfarm_id):
        """DELETE /windfarms/{id}/p50-targets/{tid} removes the target."""
        # Create a target to delete
        create_resp = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets",
            json={
                "p50_target_start_date": "2015-01-01",
                "p50_target_end_date": "2015-12-31",
                "p50_target_volume_gwh": 50.0,
            },
        )
        assert create_resp.status_code == 201
        target_id = create_resp.json()["id"]

        # Delete
        del_resp = api_client.delete(
            f"/windfarms/{windfarm_id}/p50-targets/{target_id}"
        )
        assert del_resp.status_code == 200
        assert del_resp.json()["ok"] is True

        # Verify it's gone
        list_resp = api_client.get(f"/windfarms/{windfarm_id}/p50-targets")
        ids = [t["id"] for t in list_resp.json()]
        assert target_id not in ids

    def test_get_active_target(self, api_client, windfarm_id, cleanup_targets):
        """GET /windfarms/{id}/p50-targets/active returns the currently active target."""
        # Create an ongoing target
        create_resp = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets",
            json={
                "p50_target_start_date": "2014-01-01",
                "p50_target_volume_gwh": 175.0,
            },
        )
        assert create_resp.status_code == 201
        target_id = create_resp.json()["id"]
        cleanup_targets(windfarm_id, target_id)

        # Get active
        active_resp = api_client.get(
            f"/windfarms/{windfarm_id}/p50-targets/active"
        )
        assert active_resp.status_code == 200
        assert active_resp.json()["id"] == target_id


# ==============================================================================
# Validation Tests
# ==============================================================================


class TestP50TargetsValidation:
    """Test validation and error handling."""

    def test_create_target_missing_volume(self, api_client, windfarm_id):
        """Creating a target without volume returns 422."""
        payload = {"p50_target_start_date": "2020-01-01"}

        response = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets", json=payload
        )
        assert response.status_code == 422

    def test_create_target_zero_volume(self, api_client, windfarm_id):
        """Creating a target with zero volume returns 422 (must be > 0)."""
        payload = {
            "p50_target_start_date": "2020-01-01",
            "p50_target_volume_gwh": 0,
        }

        response = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets", json=payload
        )
        assert response.status_code == 422

    def test_create_target_negative_volume(self, api_client, windfarm_id):
        """Creating a target with negative volume returns 422."""
        payload = {
            "p50_target_start_date": "2020-01-01",
            "p50_target_volume_gwh": -50,
        }

        response = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets", json=payload
        )
        assert response.status_code == 422

    def test_create_overlapping_targets_rejected(
        self, api_client, windfarm_id, cleanup_targets
    ):
        """Creating a target that overlaps with an existing one returns 400."""
        # Create first target: 2021-01-01 to 2021-12-31
        resp1 = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets",
            json={
                "p50_target_start_date": "2021-01-01",
                "p50_target_end_date": "2021-12-31",
                "p50_target_volume_gwh": 100.0,
            },
        )
        assert resp1.status_code == 201
        cleanup_targets(windfarm_id, resp1.json()["id"])

        # Try overlapping: 2021-06-01 to 2022-06-30
        resp2 = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets",
            json={
                "p50_target_start_date": "2021-06-01",
                "p50_target_end_date": "2022-06-30",
                "p50_target_volume_gwh": 120.0,
            },
        )
        assert resp2.status_code == 400
        body = resp2.json()
        # Error may be in "detail" (FastAPI default) or "error.message" (custom handler)
        error_msg = body.get("detail", "") or body.get("error", {}).get("message", "")
        assert "overlap" in error_msg.lower(), f"Expected overlap error, got: {body}"

    def test_create_non_overlapping_targets_ok(
        self, api_client, windfarm_id, cleanup_targets
    ):
        """Creating non-overlapping sequential targets succeeds."""
        # Target 1: 2010-01-01 to 2010-12-31
        resp1 = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets",
            json={
                "p50_target_start_date": "2010-01-01",
                "p50_target_end_date": "2010-12-31",
                "p50_target_volume_gwh": 100.0,
            },
        )
        assert resp1.status_code == 201
        cleanup_targets(windfarm_id, resp1.json()["id"])

        # Target 2: 2011-01-01 to 2011-12-31 (immediately after)
        resp2 = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets",
            json={
                "p50_target_start_date": "2011-01-01",
                "p50_target_end_date": "2011-12-31",
                "p50_target_volume_gwh": 110.0,
            },
        )
        assert resp2.status_code == 201
        cleanup_targets(windfarm_id, resp2.json()["id"])

    def test_delete_nonexistent_target(self, api_client, windfarm_id):
        """Deleting a non-existent target returns 404."""
        response = api_client.delete(
            f"/windfarms/{windfarm_id}/p50-targets/999999"
        )
        assert response.status_code == 404

    def test_update_nonexistent_target(self, api_client, windfarm_id):
        """Updating a non-existent target returns 404."""
        response = api_client.put(
            f"/windfarms/{windfarm_id}/p50-targets/999999",
            json={"p50_target_volume_gwh": 200.0},
        )
        assert response.status_code == 404

    def test_get_active_target_none_exists(self, api_client):
        """Getting active target when none exist returns 404."""
        # Use a windfarm unlikely to have P50 targets
        response = api_client.get("/windfarms", params={"limit": 50})
        if response.status_code != 200:
            pytest.skip("Cannot list windfarms")

        # Find a windfarm with no P50 targets
        for wf in response.json():
            targets_resp = api_client.get(f"/windfarms/{wf['id']}/p50-targets")
            if targets_resp.status_code == 200 and len(targets_resp.json()) == 0:
                active_resp = api_client.get(
                    f"/windfarms/{wf['id']}/p50-targets/active"
                )
                assert active_resp.status_code == 404
                return

        pytest.skip("All windfarms have P50 targets")

    def test_nonexistent_windfarm(self, api_client):
        """Endpoints return appropriate errors for non-existent windfarm IDs."""
        # List targets - should return empty list (no FK check on GET list)
        resp = api_client.get("/windfarms/999999/p50-targets")
        assert resp.status_code == 200
        assert resp.json() == []

        # Analysis - should return 404
        resp = api_client.get("/windfarms/999999/p50-analysis")
        assert resp.status_code == 404


# ==============================================================================
# Default Start Date Tests
# ==============================================================================


class TestP50DefaultStartDate:
    """Test default P50 start date computation from COD."""

    def test_default_start_date_from_cod(
        self, api_client, windfarm_with_cod, cleanup_targets
    ):
        """When no start date is given, it defaults to COD month + 2 months rounded up."""
        wf = windfarm_with_cod
        cod = wf["commercial_operational_date"]  # e.g. "2022-01-17"

        # Create without specifying start date
        response = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_end_date": "2099-12-31",
                "p50_target_volume_gwh": 200.0,
                "comment": "Test default start date from COD",
            },
        )

        if response.status_code == 400:
            # May overlap with existing target — skip
            pytest.skip(f"Could not create target: {response.json()['detail']}")

        assert response.status_code == 201, f"Failed: {response.text}"

        data = response.json()
        cleanup_targets(wf["id"], data["id"])

        # Verify the start date was computed:
        # COD month + 2 months, rounded up to 1st of month
        start = data["p50_target_start_date"]
        assert start is not None
        assert start.endswith("-01"), (
            f"Default start date {start} should be the 1st of a month"
        )

        # Parse and verify the month offset from COD
        from datetime import date as _date

        cod_date = _date.fromisoformat(cod)
        start_date = _date.fromisoformat(start)

        # Start should be at least 2 months after the 1st of COD's month
        # (rounded up if COD is not on the 1st, then + 2 months)
        assert start_date > cod_date, (
            f"P50 start {start_date} should be after COD {cod_date}"
        )


# ==============================================================================
# Analysis Tests
# ==============================================================================


class TestP50Analysis:
    """Test the P50 analysis endpoint and its calculations."""

    def test_analysis_returns_404_without_target(self, api_client):
        """GET /windfarms/{id}/p50-analysis returns 404 if no P50 target exists."""
        # Find a windfarm with no P50 targets
        response = api_client.get("/windfarms", params={"limit": 50})
        for wf in response.json():
            targets_resp = api_client.get(f"/windfarms/{wf['id']}/p50-targets")
            if targets_resp.status_code == 200 and len(targets_resp.json()) == 0:
                analysis_resp = api_client.get(
                    f"/windfarms/{wf['id']}/p50-analysis"
                )
                assert analysis_resp.status_code == 404
                return

        pytest.skip("All windfarms have P50 targets")

    def test_analysis_structure(
        self, api_client, windfarm_with_generation, cleanup_targets
    ):
        """Analysis response has the correct structure and fields."""
        wf = windfarm_with_generation

        # Ensure a P50 target exists
        target_resp = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_start_date": "2000-01-01",
                "p50_target_volume_gwh": 150.0,
                "comment": "Test analysis structure",
            },
        )
        if target_resp.status_code == 400:
            # May conflict with existing — try using the active one
            pass
        elif target_resp.status_code == 201:
            cleanup_targets(wf["id"], target_resp.json()["id"])

        response = api_client.get(f"/windfarms/{wf['id']}/p50-analysis")
        if response.status_code == 404:
            pytest.skip("No P50 analysis available for this windfarm")

        assert response.status_code == 200
        data = response.json()

        # Top-level fields
        assert data["windfarm_id"] == wf["id"]
        assert isinstance(data["windfarm_name"], str)
        assert "installed_capacity_mw" in data
        assert "p50_target" in data
        assert "p50_capacity_factor_pct" in data
        assert isinstance(data["avg_annual_generation_gwh"], (int, float))
        assert isinstance(data["avg_annual_gap_gwh"], (int, float))
        assert isinstance(data["gap_from_p50_gwh"], (int, float))
        assert isinstance(data["gap_in_months"], (int, float))

        # Nested P50 target
        target = data["p50_target"]
        assert "id" in target
        assert "p50_target_volume_gwh" in target
        assert "monthly_p50_gwh" in target

        # Monthly data array
        assert isinstance(data["monthly_data"], list)

        # Yearly gaps array
        assert isinstance(data["yearly_gaps"], list)

    def test_analysis_monthly_data_structure(
        self, api_client, windfarm_with_generation, cleanup_targets
    ):
        """Monthly data points have the correct fields."""
        wf = windfarm_with_generation

        # Ensure a P50 target exists
        target_resp = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_start_date": "2000-01-01",
                "p50_target_volume_gwh": 150.0,
            },
        )
        if target_resp.status_code == 201:
            cleanup_targets(wf["id"], target_resp.json()["id"])

        response = api_client.get(f"/windfarms/{wf['id']}/p50-analysis")
        if response.status_code == 404:
            pytest.skip("No P50 analysis available")

        data = response.json()
        monthly = data["monthly_data"]

        if not monthly:
            pytest.skip("No monthly data points")

        point = monthly[0]
        assert "month" in point  # "YYYY-MM"
        assert "monthly_p50_gwh" in point
        assert "actual_generation_gwh" in point
        assert "aggregated_p50_gwh" in point
        assert "aggregated_actual_gwh" in point
        assert "aggregated_gap_gwh" in point

        # Month format check
        assert len(point["month"]) == 7  # "YYYY-MM"
        assert point["month"][4] == "-"

    def test_analysis_monthly_p50_matches_target(
        self, api_client, windfarm_with_generation, cleanup_targets
    ):
        """Monthly P50 should equal annual / 12 for each data point."""
        wf = windfarm_with_generation
        annual_gwh = 120.0

        target_resp = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_start_date": "2000-01-01",
                "p50_target_volume_gwh": annual_gwh,
            },
        )
        if target_resp.status_code == 201:
            cleanup_targets(wf["id"], target_resp.json()["id"])

        response = api_client.get(f"/windfarms/{wf['id']}/p50-analysis")
        if response.status_code == 404:
            pytest.skip("No P50 analysis available")

        data = response.json()
        expected_monthly = round(annual_gwh / 12.0, 3)

        for point in data["monthly_data"]:
            assert point["monthly_p50_gwh"] == expected_monthly, (
                f"Monthly P50 should be {expected_monthly}, got {point['monthly_p50_gwh']}"
            )

    def test_analysis_cumulative_values_increase(
        self, api_client, windfarm_with_generation, cleanup_targets
    ):
        """Aggregated P50 and actual values should be monotonically increasing."""
        wf = windfarm_with_generation

        target_resp = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_start_date": "2000-01-01",
                "p50_target_volume_gwh": 150.0,
            },
        )
        if target_resp.status_code == 201:
            cleanup_targets(wf["id"], target_resp.json()["id"])

        response = api_client.get(f"/windfarms/{wf['id']}/p50-analysis")
        if response.status_code == 404:
            pytest.skip("No P50 analysis available")

        monthly = response.json()["monthly_data"]
        if len(monthly) < 2:
            pytest.skip("Need at least 2 months of data")

        for i in range(1, len(monthly)):
            # P50 cumulative must always increase (by monthly_p50 each month)
            assert monthly[i]["aggregated_p50_gwh"] > monthly[i - 1]["aggregated_p50_gwh"], (
                f"Aggregated P50 should increase: month {monthly[i]['month']}"
            )
            # Actual cumulative must be non-decreasing (generation can't be negative)
            assert monthly[i]["aggregated_actual_gwh"] >= monthly[i - 1]["aggregated_actual_gwh"], (
                f"Aggregated actual should not decrease: month {monthly[i]['month']}"
            )

    def test_analysis_gap_consistency(
        self, api_client, windfarm_with_generation, cleanup_targets
    ):
        """Gap = aggregated_p50 - aggregated_actual at each data point."""
        wf = windfarm_with_generation

        target_resp = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_start_date": "2000-01-01",
                "p50_target_volume_gwh": 150.0,
            },
        )
        if target_resp.status_code == 201:
            cleanup_targets(wf["id"], target_resp.json()["id"])

        response = api_client.get(f"/windfarms/{wf['id']}/p50-analysis")
        if response.status_code == 404:
            pytest.skip("No P50 analysis available")

        for point in response.json()["monthly_data"]:
            expected_gap = round(
                point["aggregated_p50_gwh"] - point["aggregated_actual_gwh"], 3
            )
            actual_gap = point["aggregated_gap_gwh"]
            assert abs(actual_gap - expected_gap) < 0.01, (
                f"Month {point['month']}: gap {actual_gap} != "
                f"p50 {point['aggregated_p50_gwh']} - actual {point['aggregated_actual_gwh']} = {expected_gap}"
            )

    def test_analysis_yearly_gaps_structure(
        self, api_client, windfarm_with_generation, cleanup_targets
    ):
        """Yearly gaps have correct structure and gap = p50_target - actual."""
        wf = windfarm_with_generation

        target_resp = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_start_date": "2000-01-01",
                "p50_target_volume_gwh": 150.0,
            },
        )
        if target_resp.status_code == 201:
            cleanup_targets(wf["id"], target_resp.json()["id"])

        response = api_client.get(f"/windfarms/{wf['id']}/p50-analysis")
        if response.status_code == 404:
            pytest.skip("No P50 analysis available")

        data = response.json()
        for gap in data["yearly_gaps"]:
            assert "year" in gap
            assert "actual_generation_gwh" in gap
            assert "p50_target_gwh" in gap
            assert "gap_gwh" in gap
            assert "gap_months" in gap

            # gap = p50_target - actual
            expected = round(gap["p50_target_gwh"] - gap["actual_generation_gwh"], 3)
            assert abs(gap["gap_gwh"] - expected) < 0.01, (
                f"Year {gap['year']}: gap_gwh {gap['gap_gwh']} != expected {expected}"
            )

    def test_analysis_p50_capacity_factor(
        self, api_client, windfarm_with_generation, cleanup_targets
    ):
        """P50 capacity factor = P50_gwh * 1000 / (capacity_mw * 8760) * 100."""
        wf = windfarm_with_generation
        annual_gwh = 150.0

        target_resp = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_start_date": "2000-01-01",
                "p50_target_volume_gwh": annual_gwh,
            },
        )
        if target_resp.status_code == 201:
            cleanup_targets(wf["id"], target_resp.json()["id"])

        response = api_client.get(f"/windfarms/{wf['id']}/p50-analysis")
        if response.status_code == 404:
            pytest.skip("No P50 analysis available")

        data = response.json()
        capacity_mw = data["installed_capacity_mw"]

        if capacity_mw and capacity_mw > 0:
            expected_cf = round(
                (annual_gwh * 1000) / (capacity_mw * 8760) * 100, 2
            )
            assert data["p50_capacity_factor_pct"] == expected_cf, (
                f"P50 CF should be {expected_cf}%, got {data['p50_capacity_factor_pct']}%"
            )

    def test_analysis_with_specific_target_id(
        self, api_client, windfarm_with_generation, cleanup_targets
    ):
        """GET /windfarms/{id}/p50-analysis?target_id=X uses the specified target."""
        wf = windfarm_with_generation

        # Create two non-overlapping targets
        resp1 = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_start_date": "2000-01-01",
                "p50_target_end_date": "2009-12-31",
                "p50_target_volume_gwh": 100.0,
            },
        )
        resp2 = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_start_date": "2012-01-01",
                "p50_target_end_date": "2012-12-31",
                "p50_target_volume_gwh": 200.0,
            },
        )

        if resp1.status_code == 201:
            cleanup_targets(wf["id"], resp1.json()["id"])
        if resp2.status_code == 201:
            cleanup_targets(wf["id"], resp2.json()["id"])

        if resp2.status_code != 201:
            pytest.skip("Could not create second target")

        target2_id = resp2.json()["id"]

        # Request analysis with specific target
        response = api_client.get(
            f"/windfarms/{wf['id']}/p50-analysis",
            params={"target_id": target2_id},
        )

        if response.status_code == 404:
            pytest.skip("No analysis data for target")

        assert response.status_code == 200
        data = response.json()
        assert data["p50_target"]["id"] == target2_id
        assert data["p50_target"]["p50_target_volume_gwh"] == 200.0


# ==============================================================================
# Multiple Targets Lifecycle Tests
# ==============================================================================


class TestP50TargetLifecycle:
    """Test full lifecycle scenarios with multiple targets."""

    def test_full_crud_cycle(self, api_client, windfarm_id):
        """Test create → read → update → delete cycle."""
        # Create
        resp = api_client.post(
            f"/windfarms/{windfarm_id}/p50-targets",
            json={
                "p50_target_start_date": "2008-01-01",
                "p50_target_end_date": "2008-12-31",
                "p50_target_volume_gwh": 100.0,
                "source": "https://example.com/v1",
                "comment": "Version 1",
            },
        )
        assert resp.status_code == 201
        target_id = resp.json()["id"]

        try:
            # Read
            list_resp = api_client.get(f"/windfarms/{windfarm_id}/p50-targets")
            assert target_id in [t["id"] for t in list_resp.json()]

            # Update
            update_resp = api_client.put(
                f"/windfarms/{windfarm_id}/p50-targets/{target_id}",
                json={
                    "p50_target_volume_gwh": 120.0,
                    "comment": "Version 2",
                },
            )
            assert update_resp.status_code == 200
            assert update_resp.json()["p50_target_volume_gwh"] == 120.0
            assert update_resp.json()["comment"] == "Version 2"
        finally:
            # Delete (always clean up)
            del_resp = api_client.delete(
                f"/windfarms/{windfarm_id}/p50-targets/{target_id}"
            )
            assert del_resp.status_code == 200

    def test_sequential_targets(self, api_client, windfarm_id, cleanup_targets):
        """Create sequential non-overlapping targets and list them ordered."""
        targets_data = [
            {"p50_target_start_date": "2005-01-01", "p50_target_end_date": "2005-12-31", "p50_target_volume_gwh": 100.0},
            {"p50_target_start_date": "2006-01-01", "p50_target_end_date": "2006-12-31", "p50_target_volume_gwh": 110.0},
            {"p50_target_start_date": "2007-01-01", "p50_target_end_date": "2007-12-31", "p50_target_volume_gwh": 120.0},
        ]

        created_ids = []
        for td in targets_data:
            resp = api_client.post(
                f"/windfarms/{windfarm_id}/p50-targets", json=td
            )
            assert resp.status_code == 201, f"Failed to create: {resp.text}"
            created_ids.append(resp.json()["id"])
            cleanup_targets(windfarm_id, resp.json()["id"])

        # List and verify ordering
        list_resp = api_client.get(f"/windfarms/{windfarm_id}/p50-targets")
        targets = list_resp.json()
        our_targets = [t for t in targets if t["id"] in created_ids]

        # Should be ordered by start_date
        dates = [t["p50_target_start_date"] for t in our_targets]
        assert dates == sorted(dates), "Targets should be ordered by start_date"


# ==============================================================================
# Performance Tests
# ==============================================================================


class TestP50Performance:
    """Performance tests for P50 endpoints."""

    def test_list_targets_response_time(self, api_client, windfarm_id):
        """List targets should respond within 2 seconds."""
        start = time.time()
        response = api_client.get(f"/windfarms/{windfarm_id}/p50-targets")
        elapsed = time.time() - start

        assert response.status_code == 200
        assert elapsed < 2.0, f"List targets took {elapsed:.2f}s"

    def test_analysis_response_time(self, api_client, windfarm_with_generation, cleanup_targets):
        """Analysis endpoint should respond within 10 seconds."""
        wf = windfarm_with_generation

        # Ensure a target exists
        target_resp = api_client.post(
            f"/windfarms/{wf['id']}/p50-targets",
            json={
                "p50_target_start_date": "2000-01-01",
                "p50_target_volume_gwh": 150.0,
            },
        )
        if target_resp.status_code == 201:
            cleanup_targets(wf["id"], target_resp.json()["id"])

        start = time.time()
        response = api_client.get(f"/windfarms/{wf['id']}/p50-analysis")
        elapsed = time.time() - start

        if response.status_code == 404:
            pytest.skip("No P50 analysis available")

        assert response.status_code == 200
        assert elapsed < 10.0, f"Analysis took {elapsed:.2f}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
