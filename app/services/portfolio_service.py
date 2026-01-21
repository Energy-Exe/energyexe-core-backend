"""Portfolio service for managing user portfolios and favorites."""

from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete, and_
from sqlalchemy.orm import joinedload

from app.models.portfolio import Portfolio, PortfolioItem, UserFavorite, PortfolioType
from app.models.windfarm import Windfarm
from app.models.country import Country
from app.schemas.portfolio import (
    PortfolioCreate,
    PortfolioUpdate,
    PortfolioItemCreate,
    PortfolioResponse,
    PortfolioWithItems,
    PortfolioItemResponse,
    FavoriteResponse,
)


class PortfolioService:
    """Service for managing user portfolios."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ========================================================================
    # PORTFOLIO CRUD
    # ========================================================================

    async def create_portfolio(self, user_id: int, data: PortfolioCreate) -> Portfolio:
        """Create a new portfolio for a user."""
        portfolio = Portfolio(
            user_id=user_id,
            name=data.name,
            description=data.description,
            portfolio_type=PortfolioType(data.portfolio_type.value),
        )
        self.db.add(portfolio)
        await self.db.commit()
        await self.db.refresh(portfolio)
        return portfolio

    async def get_portfolio(self, portfolio_id: int, user_id: int) -> Optional[Portfolio]:
        """Get a portfolio by ID for a specific user."""
        result = await self.db.execute(
            select(Portfolio)
            .where(and_(Portfolio.id == portfolio_id, Portfolio.user_id == user_id))
        )
        return result.scalar_one_or_none()

    async def get_portfolio_with_items(self, portfolio_id: int, user_id: int) -> Optional[Portfolio]:
        """Get a portfolio with its items loaded."""
        result = await self.db.execute(
            select(Portfolio)
            .options(
                joinedload(Portfolio.items).joinedload(PortfolioItem.windfarm).joinedload(Windfarm.country)
            )
            .where(and_(Portfolio.id == portfolio_id, Portfolio.user_id == user_id))
        )
        return result.unique().scalar_one_or_none()

    async def list_portfolios(self, user_id: int) -> List[Dict[str, Any]]:
        """List all portfolios for a user with item counts and capacity."""
        # Get portfolios with aggregated data
        result = await self.db.execute(
            select(
                Portfolio,
                func.count(PortfolioItem.id).label("item_count"),
                func.coalesce(func.sum(Windfarm.nameplate_capacity_mw), 0).label("total_capacity_mw"),
            )
            .outerjoin(PortfolioItem, Portfolio.id == PortfolioItem.portfolio_id)
            .outerjoin(Windfarm, PortfolioItem.windfarm_id == Windfarm.id)
            .where(Portfolio.user_id == user_id)
            .group_by(Portfolio.id)
            .order_by(Portfolio.created_at.desc())
        )

        portfolios = []
        for row in result.all():
            portfolio = row[0]
            portfolios.append({
                "id": portfolio.id,
                "user_id": portfolio.user_id,
                "name": portfolio.name,
                "description": portfolio.description,
                "portfolio_type": portfolio.portfolio_type.value,
                "is_default": portfolio.is_default,
                "created_at": portfolio.created_at,
                "updated_at": portfolio.updated_at,
                "item_count": row[1],
                "total_capacity_mw": float(row[2]) if row[2] else 0.0,
            })

        return portfolios

    async def update_portfolio(
        self, portfolio_id: int, user_id: int, data: PortfolioUpdate
    ) -> Optional[Portfolio]:
        """Update a portfolio."""
        portfolio = await self.get_portfolio(portfolio_id, user_id)
        if not portfolio:
            return None

        if data.name is not None:
            portfolio.name = data.name
        if data.description is not None:
            portfolio.description = data.description
        if data.portfolio_type is not None:
            portfolio.portfolio_type = PortfolioType(data.portfolio_type.value)

        await self.db.commit()
        await self.db.refresh(portfolio)
        return portfolio

    async def delete_portfolio(self, portfolio_id: int, user_id: int) -> bool:
        """Delete a portfolio."""
        portfolio = await self.get_portfolio(portfolio_id, user_id)
        if not portfolio:
            return False

        await self.db.delete(portfolio)
        await self.db.commit()
        return True

    # ========================================================================
    # PORTFOLIO ITEMS
    # ========================================================================

    async def add_item_to_portfolio(
        self, portfolio_id: int, user_id: int, data: PortfolioItemCreate
    ) -> Optional[PortfolioItem]:
        """Add a windfarm to a portfolio."""
        # Verify portfolio belongs to user
        portfolio = await self.get_portfolio(portfolio_id, user_id)
        if not portfolio:
            return None

        # Check if windfarm already in portfolio
        existing = await self.db.execute(
            select(PortfolioItem).where(
                and_(
                    PortfolioItem.portfolio_id == portfolio_id,
                    PortfolioItem.windfarm_id == data.windfarm_id,
                )
            )
        )
        if existing.scalar_one_or_none():
            return None  # Already exists

        item = PortfolioItem(
            portfolio_id=portfolio_id,
            windfarm_id=data.windfarm_id,
            notes=data.notes,
        )
        self.db.add(item)
        await self.db.commit()
        await self.db.refresh(item)
        return item

    async def remove_item_from_portfolio(
        self, portfolio_id: int, user_id: int, windfarm_id: int
    ) -> bool:
        """Remove a windfarm from a portfolio."""
        # Verify portfolio belongs to user
        portfolio = await self.get_portfolio(portfolio_id, user_id)
        if not portfolio:
            return False

        result = await self.db.execute(
            delete(PortfolioItem).where(
                and_(
                    PortfolioItem.portfolio_id == portfolio_id,
                    PortfolioItem.windfarm_id == windfarm_id,
                )
            )
        )
        await self.db.commit()
        return result.rowcount > 0

    async def get_portfolio_items(
        self, portfolio_id: int, user_id: int
    ) -> List[Dict[str, Any]]:
        """Get all items in a portfolio with windfarm details."""
        # Verify portfolio belongs to user
        portfolio = await self.get_portfolio(portfolio_id, user_id)
        if not portfolio:
            return []

        result = await self.db.execute(
            select(PortfolioItem, Windfarm, Country.name.label("country_name"))
            .join(Windfarm, PortfolioItem.windfarm_id == Windfarm.id)
            .outerjoin(Country, Windfarm.country_id == Country.id)
            .where(PortfolioItem.portfolio_id == portfolio_id)
            .order_by(PortfolioItem.added_at.desc())
        )

        items = []
        for row in result.all():
            item = row[0]
            windfarm = row[1]
            country_name = row[2]
            items.append({
                "id": item.id,
                "portfolio_id": item.portfolio_id,
                "windfarm_id": item.windfarm_id,
                "added_at": item.added_at,
                "notes": item.notes,
                "windfarm": {
                    "id": windfarm.id,
                    "name": windfarm.name,
                    "nameplate_capacity_mw": float(windfarm.nameplate_capacity_mw) if windfarm.nameplate_capacity_mw else None,
                    "country_name": country_name,
                },
            })

        return items

    # ========================================================================
    # FAVORITES
    # ========================================================================

    async def add_favorite(self, user_id: int, windfarm_id: int) -> Optional[UserFavorite]:
        """Add a windfarm to user's favorites."""
        # Check if already favorited
        existing = await self.db.execute(
            select(UserFavorite).where(
                and_(
                    UserFavorite.user_id == user_id,
                    UserFavorite.windfarm_id == windfarm_id,
                )
            )
        )
        if existing.scalar_one_or_none():
            return None  # Already favorited

        favorite = UserFavorite(
            user_id=user_id,
            windfarm_id=windfarm_id,
        )
        self.db.add(favorite)
        await self.db.commit()
        await self.db.refresh(favorite)
        return favorite

    async def remove_favorite(self, user_id: int, windfarm_id: int) -> bool:
        """Remove a windfarm from user's favorites."""
        result = await self.db.execute(
            delete(UserFavorite).where(
                and_(
                    UserFavorite.user_id == user_id,
                    UserFavorite.windfarm_id == windfarm_id,
                )
            )
        )
        await self.db.commit()
        return result.rowcount > 0

    async def is_favorite(self, user_id: int, windfarm_id: int) -> bool:
        """Check if a windfarm is in user's favorites."""
        result = await self.db.execute(
            select(UserFavorite).where(
                and_(
                    UserFavorite.user_id == user_id,
                    UserFavorite.windfarm_id == windfarm_id,
                )
            )
        )
        return result.scalar_one_or_none() is not None

    async def list_favorites(self, user_id: int) -> List[Dict[str, Any]]:
        """List all favorites for a user with windfarm details."""
        result = await self.db.execute(
            select(UserFavorite, Windfarm, Country.name.label("country_name"))
            .join(Windfarm, UserFavorite.windfarm_id == Windfarm.id)
            .outerjoin(Country, Windfarm.country_id == Country.id)
            .where(UserFavorite.user_id == user_id)
            .order_by(UserFavorite.added_at.desc())
        )

        favorites = []
        for row in result.all():
            favorite = row[0]
            windfarm = row[1]
            country_name = row[2]
            favorites.append({
                "id": favorite.id,
                "user_id": favorite.user_id,
                "windfarm_id": favorite.windfarm_id,
                "added_at": favorite.added_at,
                "windfarm": {
                    "id": windfarm.id,
                    "name": windfarm.name,
                    "nameplate_capacity_mw": float(windfarm.nameplate_capacity_mw) if windfarm.nameplate_capacity_mw else None,
                    "country_name": country_name,
                },
            })

        return favorites

    async def get_favorites_for_windfarms(
        self, user_id: int, windfarm_ids: List[int]
    ) -> List[int]:
        """Get list of windfarm IDs that are favorited from the given list."""
        result = await self.db.execute(
            select(UserFavorite.windfarm_id).where(
                and_(
                    UserFavorite.user_id == user_id,
                    UserFavorite.windfarm_id.in_(windfarm_ids),
                )
            )
        )
        return [row[0] for row in result.all()]
