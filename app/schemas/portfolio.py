"""Portfolio schemas for API serialization."""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from enum import Enum


class PortfolioTypeEnum(str, Enum):
    """Portfolio type enum for API."""
    WATCHLIST = "watchlist"
    OWNED = "owned"
    COMPETITOR = "competitor"
    CUSTOM = "custom"


# ============================================================================
# PORTFOLIO SCHEMAS
# ============================================================================

class PortfolioBase(BaseModel):
    """Base portfolio schema."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    portfolio_type: PortfolioTypeEnum = PortfolioTypeEnum.CUSTOM


class PortfolioCreate(PortfolioBase):
    """Schema for creating a portfolio."""
    pass


class PortfolioUpdate(BaseModel):
    """Schema for updating a portfolio."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    portfolio_type: Optional[PortfolioTypeEnum] = None


class WindfarmSummary(BaseModel):
    """Summary of a windfarm for portfolio views."""
    id: int
    name: str
    nameplate_capacity_mw: Optional[float] = None
    country_name: Optional[str] = None
    bidzone_name: Optional[str] = None

    class Config:
        from_attributes = True


class PortfolioItemBase(BaseModel):
    """Base portfolio item schema."""
    windfarm_id: int
    notes: Optional[str] = None


class PortfolioItemCreate(PortfolioItemBase):
    """Schema for adding item to portfolio."""
    pass


class PortfolioItemResponse(BaseModel):
    """Schema for portfolio item response."""
    id: int
    portfolio_id: int
    windfarm_id: int
    added_at: datetime
    notes: Optional[str] = None
    windfarm: Optional[WindfarmSummary] = None

    class Config:
        from_attributes = True


class PortfolioResponse(BaseModel):
    """Schema for portfolio response."""
    id: int
    user_id: int
    name: str
    description: Optional[str] = None
    portfolio_type: PortfolioTypeEnum
    is_default: bool
    created_at: datetime
    updated_at: datetime
    item_count: int = 0
    total_capacity_mw: float = 0.0

    class Config:
        from_attributes = True


class PortfolioWithItems(PortfolioResponse):
    """Portfolio response with items included."""
    items: List[PortfolioItemResponse] = []


# ============================================================================
# FAVORITES SCHEMAS
# ============================================================================

class FavoriteCreate(BaseModel):
    """Schema for adding a favorite."""
    windfarm_id: int


class FavoriteResponse(BaseModel):
    """Schema for favorite response."""
    id: int
    user_id: int
    windfarm_id: int
    added_at: datetime
    windfarm: Optional[WindfarmSummary] = None

    class Config:
        from_attributes = True


class FavoriteListResponse(BaseModel):
    """Schema for listing favorites."""
    favorites: List[FavoriteResponse]
    total: int


# ============================================================================
# PORTFOLIO ANALYTICS SCHEMAS
# ============================================================================

class PortfolioSummary(BaseModel):
    """Summary analytics for a portfolio."""
    portfolio_id: int
    portfolio_name: str
    total_windfarms: int
    total_capacity_mw: float
    countries: int
    offshore_farms: int
    onshore_farms: int


class PortfolioPerformance(BaseModel):
    """Performance metrics for a portfolio."""
    portfolio_id: int
    period_start: str
    period_end: str
    total_generation_mwh: float
    avg_capacity_factor: float
    top_performer_id: Optional[int] = None
    top_performer_name: Optional[str] = None
    bottom_performer_id: Optional[int] = None
    bottom_performer_name: Optional[str] = None
