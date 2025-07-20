from typing import List, Optional
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from app.models.market_balance_area import MarketBalanceArea
from app.schemas.market_balance_area import MarketBalanceAreaCreate, MarketBalanceAreaUpdate


class MarketBalanceAreaService:
    
    @staticmethod
    async def get_market_balance_areas(
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100
    ) -> List[MarketBalanceArea]:
        result = await db.execute(
            select(MarketBalanceArea)
            .options(selectinload(MarketBalanceArea.country))
            .offset(skip)
            .limit(limit)
            .order_by(MarketBalanceArea.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def get_market_balance_area(db: AsyncSession, market_balance_area_id: int) -> Optional[MarketBalanceArea]:
        result = await db.execute(
            select(MarketBalanceArea)
            .options(selectinload(MarketBalanceArea.country))
            .where(MarketBalanceArea.id == market_balance_area_id)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_market_balance_area_by_code(db: AsyncSession, code: str) -> Optional[MarketBalanceArea]:
        result = await db.execute(
            select(MarketBalanceArea)
            .options(selectinload(MarketBalanceArea.country))
            .where(MarketBalanceArea.code == code)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def search_market_balance_areas(
        db: AsyncSession,
        query: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[MarketBalanceArea]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(MarketBalanceArea)
            .options(selectinload(MarketBalanceArea.country))
            .where(
                and_(
                    MarketBalanceArea.name.ilike(search_pattern)
                )
            )
            .offset(skip)
            .limit(limit)
            .order_by(MarketBalanceArea.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def create_market_balance_area(db: AsyncSession, market_balance_area: MarketBalanceAreaCreate) -> MarketBalanceArea:
        db_market_balance_area = MarketBalanceArea(**market_balance_area.model_dump())
        db.add(db_market_balance_area)
        await db.commit()
        await db.refresh(db_market_balance_area)
        return db_market_balance_area
    
    @staticmethod
    async def update_market_balance_area(
        db: AsyncSession,
        market_balance_area_id: int,
        market_balance_area_update: MarketBalanceAreaUpdate
    ) -> Optional[MarketBalanceArea]:
        result = await db.execute(select(MarketBalanceArea).where(MarketBalanceArea.id == market_balance_area_id))
        db_market_balance_area = result.scalar_one_or_none()
        
        if not db_market_balance_area:
            return None
        
        update_data = market_balance_area_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_market_balance_area, field, value)
        
        await db.commit()
        await db.refresh(db_market_balance_area)
        
        # Fetch with country relationship
        result = await db.execute(
            select(MarketBalanceArea)
            .options(selectinload(MarketBalanceArea.country))
            .where(MarketBalanceArea.id == market_balance_area_id)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def delete_market_balance_area(db: AsyncSession, market_balance_area_id: int) -> Optional[MarketBalanceArea]:
        result = await db.execute(select(MarketBalanceArea).where(MarketBalanceArea.id == market_balance_area_id))
        db_market_balance_area = result.scalar_one_or_none()
        
        if not db_market_balance_area:
            return None
        
        await db.delete(db_market_balance_area)
        await db.commit()
        return db_market_balance_area