"""Tests for portfolio API endpoints."""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.portfolio import Portfolio, PortfolioItem, UserFavorite, PortfolioType


@pytest.mark.asyncio
async def test_list_portfolios_empty(
    client: AsyncClient,
    test_user_token: str,
):
    """Test listing portfolios when user has none."""
    response = await client.get(
        "/api/v1/portfolios/",
        headers={"Authorization": f"Bearer {test_user_token}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_create_portfolio(
    client: AsyncClient,
    test_user_token: str,
):
    """Test creating a new portfolio."""
    portfolio_data = {
        "name": "Test Watchlist",
        "description": "My test watchlist portfolio",
        "portfolio_type": "watchlist",
    }
    response = await client.post(
        "/api/v1/portfolios/",
        headers={"Authorization": f"Bearer {test_user_token}"},
        json=portfolio_data,
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "Test Watchlist"
    assert data["description"] == "My test watchlist portfolio"
    assert data["portfolio_type"] == "watchlist"
    assert data["item_count"] == 0
    assert data["total_capacity_mw"] == 0.0
    assert "id" in data
    assert "created_at" in data


@pytest.mark.asyncio
async def test_get_portfolio(
    client: AsyncClient,
    test_user_token: str,
):
    """Test getting a single portfolio with items."""
    # First create a portfolio
    create_response = await client.post(
        "/api/v1/portfolios/",
        headers={"Authorization": f"Bearer {test_user_token}"},
        json={
            "name": "My Portfolio",
            "description": "Test description",
            "portfolio_type": "owned",
        },
    )
    assert create_response.status_code == 201
    portfolio_id = create_response.json()["id"]

    # Now get it
    response = await client.get(
        f"/api/v1/portfolios/{portfolio_id}",
        headers={"Authorization": f"Bearer {test_user_token}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "My Portfolio"
    assert data["portfolio_type"] == "owned"
    assert "items" in data
    assert isinstance(data["items"], list)


@pytest.mark.asyncio
async def test_get_portfolio_not_found(
    client: AsyncClient,
    test_user_token: str,
):
    """Test getting a non-existent portfolio."""
    response = await client.get(
        "/api/v1/portfolios/99999",
        headers={"Authorization": f"Bearer {test_user_token}"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_portfolio(
    client: AsyncClient,
    test_user_token: str,
):
    """Test updating a portfolio."""
    # Create a portfolio
    create_response = await client.post(
        "/api/v1/portfolios/",
        headers={"Authorization": f"Bearer {test_user_token}"},
        json={
            "name": "Original Name",
            "portfolio_type": "custom",
        },
    )
    portfolio_id = create_response.json()["id"]

    # Update it
    response = await client.put(
        f"/api/v1/portfolios/{portfolio_id}",
        headers={"Authorization": f"Bearer {test_user_token}"},
        json={
            "name": "Updated Name",
            "description": "New description",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Updated Name"
    assert data["description"] == "New description"


@pytest.mark.asyncio
async def test_delete_portfolio(
    client: AsyncClient,
    test_user_token: str,
):
    """Test deleting a portfolio."""
    # Create a portfolio
    create_response = await client.post(
        "/api/v1/portfolios/",
        headers={"Authorization": f"Bearer {test_user_token}"},
        json={
            "name": "To Delete",
            "portfolio_type": "watchlist",
        },
    )
    portfolio_id = create_response.json()["id"]

    # Delete it
    response = await client.delete(
        f"/api/v1/portfolios/{portfolio_id}",
        headers={"Authorization": f"Bearer {test_user_token}"},
    )
    assert response.status_code == 204

    # Verify it's gone
    get_response = await client.get(
        f"/api/v1/portfolios/{portfolio_id}",
        headers={"Authorization": f"Bearer {test_user_token}"},
    )
    assert get_response.status_code == 404


@pytest.mark.asyncio
async def test_favorites_list_empty(
    client: AsyncClient,
    test_user_token: str,
):
    """Test listing favorites when user has none."""
    response = await client.get(
        "/api/v1/portfolios/favorites/list",
        headers={"Authorization": f"Bearer {test_user_token}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert "favorites" in data
    assert "total" in data
    assert isinstance(data["favorites"], list)


@pytest.mark.asyncio
async def test_check_favorite_false(
    client: AsyncClient,
    test_user_token: str,
):
    """Test checking if a windfarm is favorited when it's not."""
    response = await client.get(
        "/api/v1/portfolios/favorites/check/1",
        headers={"Authorization": f"Bearer {test_user_token}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["is_favorite"] == False


@pytest.mark.asyncio
async def test_check_multiple_favorites(
    client: AsyncClient,
    test_user_token: str,
):
    """Test checking multiple windfarms for favorite status."""
    response = await client.post(
        "/api/v1/portfolios/favorites/check-multiple",
        headers={"Authorization": f"Bearer {test_user_token}"},
        json=[1, 2, 3, 4, 5],
    )
    assert response.status_code == 200
    data = response.json()
    assert "favorited_ids" in data
    assert isinstance(data["favorited_ids"], list)


@pytest.mark.asyncio
async def test_portfolio_requires_auth(
    client: AsyncClient,
):
    """Test that portfolio endpoints require authentication."""
    response = await client.get("/api/v1/portfolios/")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_favorites_requires_auth(
    client: AsyncClient,
):
    """Test that favorites endpoints require authentication."""
    response = await client.get("/api/v1/portfolios/favorites/list")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_portfolio_all_types(
    client: AsyncClient,
    test_user_token: str,
):
    """Test creating portfolios of all types."""
    portfolio_types = ["watchlist", "owned", "competitor", "custom"]

    for ptype in portfolio_types:
        response = await client.post(
            "/api/v1/portfolios/",
            headers={"Authorization": f"Bearer {test_user_token}"},
            json={
                "name": f"Test {ptype}",
                "portfolio_type": ptype,
            },
        )
        assert response.status_code == 201
        data = response.json()
        assert data["portfolio_type"] == ptype
