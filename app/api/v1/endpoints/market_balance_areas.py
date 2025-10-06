from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas import MarketBalanceArea, MarketBalanceAreaCreate, MarketBalanceAreaUpdate
from app.services.market_balance_area import MarketBalanceAreaService

router = APIRouter()


@router.get("/", response_model=List[MarketBalanceArea])
async def get_market_balance_areas(
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Get all market balance areas with pagination"""
    return await MarketBalanceAreaService.get_market_balance_areas(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[MarketBalanceArea])
async def search_market_balance_areas(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Search market balance areas by name"""
    return await MarketBalanceAreaService.search_market_balance_areas(
        db, query=q, skip=skip, limit=limit
    )


@router.get("/{market_balance_area_id}", response_model=MarketBalanceArea)
async def get_market_balance_area(market_balance_area_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific market balance area by ID"""
    market_balance_area = await MarketBalanceAreaService.get_market_balance_area(
        db, market_balance_area_id
    )
    if not market_balance_area:
        raise HTTPException(status_code=404, detail="Market balance area not found")
    return market_balance_area


@router.get("/code/{code}", response_model=MarketBalanceArea)
async def get_market_balance_area_by_code(code: str, db: AsyncSession = Depends(get_db)):
    """Get a market balance area by its code"""
    market_balance_area = await MarketBalanceAreaService.get_market_balance_area_by_code(db, code)
    if not market_balance_area:
        raise HTTPException(status_code=404, detail="Market balance area not found")
    return market_balance_area


@router.post("/", response_model=MarketBalanceArea, status_code=201)
async def create_market_balance_area(
    market_balance_area: MarketBalanceAreaCreate, db: AsyncSession = Depends(get_db)
):
    """Create a new market balance area"""
    # Check if market balance area with same code already exists
    existing_market_balance_area = await MarketBalanceAreaService.get_market_balance_area_by_code(
        db, market_balance_area.code
    )
    if existing_market_balance_area:
        raise HTTPException(
            status_code=400, detail="Market balance area with this code already exists"
        )

    return await MarketBalanceAreaService.create_market_balance_area(db, market_balance_area)


@router.put("/{market_balance_area_id}", response_model=MarketBalanceArea)
async def update_market_balance_area(
    market_balance_area_id: int,
    market_balance_area_update: MarketBalanceAreaUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a market balance area"""
    # Check if market balance area with same code already exists (excluding current market balance area)
    if market_balance_area_update.code:
        existing_market_balance_area = (
            await MarketBalanceAreaService.get_market_balance_area_by_code(
                db, market_balance_area_update.code
            )
        )
        if (
            existing_market_balance_area
            and existing_market_balance_area.id != market_balance_area_id
        ):
            raise HTTPException(
                status_code=400, detail="Market balance area with this code already exists"
            )

    updated_market_balance_area = await MarketBalanceAreaService.update_market_balance_area(
        db, market_balance_area_id, market_balance_area_update
    )
    if not updated_market_balance_area:
        raise HTTPException(status_code=404, detail="Market balance area not found")
    return updated_market_balance_area


@router.delete("/{market_balance_area_id}", response_model=MarketBalanceArea)
async def delete_market_balance_area(
    market_balance_area_id: int, db: AsyncSession = Depends(get_db)
):
    """Delete a market balance area"""
    deleted_market_balance_area = await MarketBalanceAreaService.delete_market_balance_area(
        db, market_balance_area_id
    )
    if not deleted_market_balance_area:
        raise HTTPException(status_code=404, detail="Market balance area not found")
    return deleted_market_balance_area
