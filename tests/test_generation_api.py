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


class TestPortfolioGenerationAPI:
    """Test suite for portfolio-level generation API endpoints."""

    def test_get_portfolio_generation_stats(self, api_client, auth_headers):
        """Test GET /generation/portfolio/stats returns aggregated statistics."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        response = api_client.get("/generation/portfolio/stats", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "total_mwh" in data
        assert "avg_capacity_factor" in data
        assert "farm_count" in data
        assert "record_count" in data
        assert "avg_quality_score" in data
        assert "total_capacity_mw" in data
        assert "top_performers" in data
        assert "bottom_performers" in data

        # Verify data types
        assert isinstance(data["total_mwh"], (int, float))
        assert isinstance(data["avg_capacity_factor"], (int, float))
        assert isinstance(data["farm_count"], int)
        assert isinstance(data["record_count"], int)
        assert isinstance(data["top_performers"], list)
        assert isinstance(data["bottom_performers"], list)

    def test_get_portfolio_generation_stats_with_portfolio_filter(self, api_client, auth_headers):
        """Test portfolio stats endpoint with portfolio_id filter."""
        # First get a portfolio ID
        portfolio_response = api_client.get("/portfolios", headers=auth_headers)
        if portfolio_response.status_code != 200 or not portfolio_response.json():
            pytest.skip("No portfolios available for testing")

        portfolio_id = portfolio_response.json()[0]["id"]

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "portfolio_id": portfolio_id
        }

        response = api_client.get("/generation/portfolio/stats", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Verify response structure is correct even with filter
        assert "total_mwh" in data
        assert "farm_count" in data
        assert "top_performers" in data

    def test_get_portfolio_generation_stats_performer_structure(self, api_client, auth_headers):
        """Test that top/bottom performers have correct structure."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        response = api_client.get("/generation/portfolio/stats", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check top performers structure
        if data["top_performers"]:
            performer = data["top_performers"][0]
            assert "windfarm_id" in performer
            assert "name" in performer
            assert "total_mwh" in performer
            assert "capacity_factor" in performer
            assert "avg_quality" in performer

    def test_get_portfolio_generation_timeseries(self, api_client, auth_headers):
        """Test GET /generation/portfolio/timeseries returns timeseries data."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": "daily"
        }

        response = api_client.get("/generation/portfolio/timeseries", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "aggregation" in data
        assert "start_date" in data
        assert "end_date" in data
        assert "timeseries" in data
        assert "by_farm" in data

        assert data["aggregation"] == "daily"
        assert isinstance(data["timeseries"], list)
        assert isinstance(data["by_farm"], dict)

    def test_get_portfolio_generation_timeseries_structure(self, api_client, auth_headers):
        """Test timeseries data points have correct structure."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=7)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": "daily"
        }

        response = api_client.get("/generation/portfolio/timeseries", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check timeseries point structure
        if data["timeseries"]:
            point = data["timeseries"][0]
            assert "period" in point
            assert "total_mwh" in point
            assert "avg_quality" in point
            assert "farm_count" in point

        # Check farm breakdown structure
        if data["by_farm"]:
            farm_name = list(data["by_farm"].keys())[0]
            farm_data = data["by_farm"][farm_name]
            assert isinstance(farm_data, list)
            if farm_data:
                assert "period" in farm_data[0]
                assert "mwh" in farm_data[0]

    def test_get_portfolio_generation_timeseries_aggregations(self, api_client, auth_headers):
        """Test different aggregation levels for timeseries."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)

        aggregations = ["daily", "weekly", "monthly"]

        for agg in aggregations:
            params = {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "aggregation": agg
            }

            response = api_client.get("/generation/portfolio/timeseries", params=params, headers=auth_headers)

            assert response.status_code == 200, f"Failed for aggregation: {agg}"
            data = response.json()
            assert data["aggregation"] == agg

    def test_get_portfolio_generation_timeseries_with_country_filter(self, api_client, auth_headers):
        """Test timeseries endpoint with country_id filter."""
        # First get a country ID from a windfarm
        wf_response = api_client.get("/windfarms", params={"limit": 1}, headers=auth_headers)
        if wf_response.status_code != 200 or not wf_response.json():
            pytest.skip("No windfarms available for testing")

        windfarm = wf_response.json()[0]
        country_id = windfarm.get("country_id")
        if not country_id:
            pytest.skip("Windfarm has no country_id for testing")

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": "daily",
            "country_id": country_id
        }

        response = api_client.get("/generation/portfolio/timeseries", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()
        assert "timeseries" in data
        assert "by_farm" in data


class TestPortfolioGenerationAPIValidation:
    """Validation tests for portfolio generation API endpoints."""

    def test_portfolio_stats_requires_dates(self, api_client, auth_headers):
        """Test portfolio stats endpoint requires date parameters."""
        response = api_client.get("/generation/portfolio/stats", headers=auth_headers)

        # Should return validation error for missing required parameters
        assert response.status_code == 422

    def test_portfolio_timeseries_requires_dates(self, api_client, auth_headers):
        """Test timeseries endpoint requires date parameters."""
        response = api_client.get("/generation/portfolio/timeseries", headers=auth_headers)

        # Should return validation error for missing required parameters
        assert response.status_code == 422

    def test_portfolio_timeseries_invalid_aggregation(self, api_client, auth_headers):
        """Test timeseries endpoint rejects invalid aggregation."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": "invalid"
        }

        response = api_client.get("/generation/portfolio/timeseries", params=params, headers=auth_headers)

        # Should return validation error for invalid aggregation
        assert response.status_code == 422


class TestPortfolioGenerationAPIPerformance:
    """Performance tests for portfolio generation API."""

    def test_portfolio_stats_response_time(self, api_client, auth_headers):
        """Test portfolio stats endpoint responds in reasonable time."""
        import time

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        start = time.time()
        response = api_client.get("/generation/portfolio/stats", params=params, headers=auth_headers)
        elapsed = time.time() - start

        assert response.status_code == 200
        # Should complete in less than 15 seconds
        assert elapsed < 15.0, f"Portfolio stats endpoint took too long: {elapsed:.2f}s"

    def test_portfolio_timeseries_response_time(self, api_client, auth_headers):
        """Test portfolio timeseries endpoint responds in reasonable time."""
        import time

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": "daily"
        }

        start = time.time()
        response = api_client.get("/generation/portfolio/timeseries", params=params, headers=auth_headers)
        elapsed = time.time() - start

        assert response.status_code == 200
        # Should complete in less than 20 seconds for 90 days
        assert elapsed < 20.0, f"Portfolio timeseries endpoint took too long: {elapsed:.2f}s"


class TestPortfolioPerformanceAPI:
    """Test suite for portfolio performance endpoints."""

    def test_get_portfolio_performance(self, api_client, auth_headers):
        """Test GET /generation/portfolio/performance returns performance metrics."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        response = api_client.get("/generation/portfolio/performance", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check expected top-level fields
        expected_fields = [
            "start_date", "end_date", "hours_in_period", "farm_count",
            "cf_distribution", "performance_ranking", "performance_trend",
            "by_technology", "statistics"
        ]
        for field in expected_fields:
            assert field in data, f"Missing field: {field}"

        # Check statistics structure
        stats = data["statistics"]
        assert "avg_capacity_factor" in stats
        assert "max_capacity_factor" in stats
        assert "min_capacity_factor" in stats
        assert "total_capacity_mw" in stats
        assert "total_generation_mwh" in stats

    def test_portfolio_performance_cf_distribution(self, api_client, auth_headers):
        """Test CF distribution histogram structure."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        response = api_client.get("/generation/portfolio/performance", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check CF distribution structure
        cf_dist = data["cf_distribution"]
        assert isinstance(cf_dist, list)
        if cf_dist:
            bin_item = cf_dist[0]
            assert "bin_start" in bin_item
            assert "bin_end" in bin_item
            assert "bin_label" in bin_item
            assert "count" in bin_item

    def test_portfolio_performance_ranking(self, api_client, auth_headers):
        """Test performance ranking structure."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        response = api_client.get("/generation/portfolio/performance", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check ranking structure
        ranking = data["performance_ranking"]
        assert isinstance(ranking, list)
        if ranking:
            farm = ranking[0]
            expected_fields = [
                "windfarm_id", "windfarm_name", "windfarm_code",
                "country_name", "capacity_mw", "total_mwh",
                "capacity_factor", "avg_quality", "record_count"
            ]
            for field in expected_fields:
                assert field in farm, f"Missing ranking field: {field}"

    def test_portfolio_performance_trend(self, api_client, auth_headers):
        """Test performance trend structure."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=365)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        response = api_client.get("/generation/portfolio/performance", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check trend structure
        trend = data["performance_trend"]
        assert isinstance(trend, list)
        if trend:
            point = trend[0]
            assert "period" in point
            assert "total_mwh" in point
            assert "capacity_factor" in point
            assert "farm_count" in point

    def test_portfolio_performance_by_technology(self, api_client, auth_headers):
        """Test technology breakdown structure."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        response = api_client.get("/generation/portfolio/performance", params=params, headers=auth_headers)

        assert response.status_code == 200
        data = response.json()

        # Check technology breakdown structure
        by_tech = data["by_technology"]
        assert isinstance(by_tech, list)
        if by_tech:
            tech = by_tech[0]
            expected_fields = [
                "model_id", "manufacturer", "model_name", "rated_power_kw",
                "farm_count", "turbine_count", "total_capacity_mw",
                "total_mwh", "capacity_factor"
            ]
            for field in expected_fields:
                assert field in tech, f"Missing technology field: {field}"


class TestPortfolioPerformanceAPIValidation:
    """Validation tests for portfolio performance API."""

    def test_portfolio_performance_requires_dates(self, api_client, auth_headers):
        """Test portfolio performance endpoint requires date parameters."""
        response = api_client.get("/generation/portfolio/performance", headers=auth_headers)
        assert response.status_code == 422

    def test_portfolio_performance_requires_auth(self, api_client):
        """Test portfolio performance requires authentication."""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        try:
            response = api_client.get("/generation/portfolio/performance", params=params)
        except Exception:
            pytest.skip("API server not running")

        if response.status_code == 404:
            pytest.skip("API server not running or endpoint not registered")

        assert response.status_code == 401


class TestPortfolioPerformanceAPIPerformance:
    """Performance tests for portfolio performance API."""

    def test_portfolio_performance_response_time(self, api_client, auth_headers):
        """Test portfolio performance endpoint responds in reasonable time."""
        import time

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

        start = time.time()
        response = api_client.get("/generation/portfolio/performance", params=params, headers=auth_headers)
        elapsed = time.time() - start

        assert response.status_code == 200
        # Should complete in less than 30 seconds
        assert elapsed < 30.0, f"Portfolio performance endpoint took too long: {elapsed:.2f}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
