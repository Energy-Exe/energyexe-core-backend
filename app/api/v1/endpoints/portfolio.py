"""Portfolio API endpoints for managing user portfolios and favorites."""

from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_user
from app.models.user import User
from app.services.portfolio_service import PortfolioService
from app.schemas.portfolio import (
    PortfolioCreate,
    PortfolioUpdate,
    PortfolioResponse,
    PortfolioWithItems,
    PortfolioItemCreate,
    PortfolioItemResponse,
    FavoriteCreate,
    FavoriteResponse,
    FavoriteListResponse,
)

router = APIRouter()


# ============================================================================
# PORTFOLIO ENDPOINTS
# ============================================================================

@router.get("/", response_model=List[PortfolioResponse])
async def list_portfolios(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List all portfolios for the current user."""
    service = PortfolioService(db)
    portfolios = await service.list_portfolios(current_user.id)
    return portfolios


@router.post("/", response_model=PortfolioResponse, status_code=status.HTTP_201_CREATED)
async def create_portfolio(
    data: PortfolioCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create a new portfolio."""
    service = PortfolioService(db)
    portfolio = await service.create_portfolio(current_user.id, data)
    return {
        "id": portfolio.id,
        "user_id": portfolio.user_id,
        "name": portfolio.name,
        "description": portfolio.description,
        "portfolio_type": portfolio.portfolio_type.value,
        "is_default": portfolio.is_default,
        "created_at": portfolio.created_at,
        "updated_at": portfolio.updated_at,
        "item_count": 0,
        "total_capacity_mw": 0.0,
    }


@router.get("/{portfolio_id}", response_model=PortfolioWithItems)
async def get_portfolio(
    portfolio_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get a portfolio with its items."""
    service = PortfolioService(db)
    portfolio = await service.get_portfolio_with_items(portfolio_id, current_user.id)
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Portfolio not found",
        )

    items = await service.get_portfolio_items(portfolio_id, current_user.id)
    total_capacity = sum(
        item["windfarm"]["nameplate_capacity_mw"] or 0
        for item in items
    )

    return {
        "id": portfolio.id,
        "user_id": portfolio.user_id,
        "name": portfolio.name,
        "description": portfolio.description,
        "portfolio_type": portfolio.portfolio_type.value,
        "is_default": portfolio.is_default,
        "created_at": portfolio.created_at,
        "updated_at": portfolio.updated_at,
        "item_count": len(items),
        "total_capacity_mw": total_capacity,
        "items": items,
    }


@router.put("/{portfolio_id}", response_model=PortfolioResponse)
async def update_portfolio(
    portfolio_id: int,
    data: PortfolioUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update a portfolio."""
    service = PortfolioService(db)
    portfolio = await service.update_portfolio(portfolio_id, current_user.id, data)
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Portfolio not found",
        )

    # Get updated counts
    portfolios = await service.list_portfolios(current_user.id)
    portfolio_data = next((p for p in portfolios if p["id"] == portfolio_id), None)

    return portfolio_data


@router.delete("/{portfolio_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_portfolio(
    portfolio_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Delete a portfolio."""
    service = PortfolioService(db)
    deleted = await service.delete_portfolio(portfolio_id, current_user.id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Portfolio not found",
        )


# ============================================================================
# PORTFOLIO ITEMS ENDPOINTS
# ============================================================================

@router.post("/{portfolio_id}/items", response_model=PortfolioItemResponse, status_code=status.HTTP_201_CREATED)
async def add_item_to_portfolio(
    portfolio_id: int,
    data: PortfolioItemCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Add a windfarm to a portfolio."""
    service = PortfolioService(db)
    item = await service.add_item_to_portfolio(portfolio_id, current_user.id, data)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not add item - portfolio not found or item already exists",
        )

    # Get item with windfarm details
    items = await service.get_portfolio_items(portfolio_id, current_user.id)
    item_data = next((i for i in items if i["windfarm_id"] == data.windfarm_id), None)
    return item_data


@router.get("/{portfolio_id}/items", response_model=List[PortfolioItemResponse])
async def get_portfolio_items(
    portfolio_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get all items in a portfolio."""
    service = PortfolioService(db)
    items = await service.get_portfolio_items(portfolio_id, current_user.id)
    return items


@router.delete("/{portfolio_id}/items/{windfarm_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_item_from_portfolio(
    portfolio_id: int,
    windfarm_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Remove a windfarm from a portfolio."""
    service = PortfolioService(db)
    removed = await service.remove_item_from_portfolio(portfolio_id, current_user.id, windfarm_id)
    if not removed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Item not found in portfolio",
        )


# ============================================================================
# FAVORITES ENDPOINTS
# ============================================================================

@router.get("/favorites/list", response_model=FavoriteListResponse)
async def list_favorites(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List all favorites for the current user."""
    service = PortfolioService(db)
    favorites = await service.list_favorites(current_user.id)
    return {
        "favorites": favorites,
        "total": len(favorites),
    }


@router.post("/favorites", response_model=FavoriteResponse, status_code=status.HTTP_201_CREATED)
async def add_favorite(
    data: FavoriteCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Add a windfarm to favorites."""
    service = PortfolioService(db)
    favorite = await service.add_favorite(current_user.id, data.windfarm_id)
    if not favorite:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Windfarm already in favorites",
        )

    # Get favorite with windfarm details
    favorites = await service.list_favorites(current_user.id)
    favorite_data = next((f for f in favorites if f["windfarm_id"] == data.windfarm_id), None)
    return favorite_data


@router.delete("/favorites/{windfarm_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_favorite(
    windfarm_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Remove a windfarm from favorites."""
    service = PortfolioService(db)
    removed = await service.remove_favorite(current_user.id, windfarm_id)
    if not removed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Windfarm not in favorites",
        )


@router.get("/favorites/check/{windfarm_id}")
async def check_favorite(
    windfarm_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Check if a windfarm is in favorites."""
    service = PortfolioService(db)
    is_favorite = await service.is_favorite(current_user.id, windfarm_id)
    return {"is_favorite": is_favorite}


@router.post("/favorites/check-multiple")
async def check_multiple_favorites(
    windfarm_ids: List[int],
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Check which windfarms from a list are in favorites."""
    service = PortfolioService(db)
    favorited_ids = await service.get_favorites_for_windfarms(current_user.id, windfarm_ids)
    return {"favorited_ids": favorited_ids}
