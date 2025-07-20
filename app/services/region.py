from typing import List, Optional
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.region import Region
from app.schemas.region import RegionCreate, RegionUpdate


class RegionService:
    
    @staticmethod
    async def get_regions(
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100
    ) -> List[Region]:
        result = await db.execute(
            select(Region)
            .offset(skip)
            .limit(limit)
            .order_by(Region.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def get_region(db: AsyncSession, region_id: int) -> Optional[Region]:
        result = await db.execute(select(Region).where(Region.id == region_id))
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_region_by_code(db: AsyncSession, code: str) -> Optional[Region]:
        result = await db.execute(select(Region).where(Region.code == code))
        return result.scalar_one_or_none()
    
    @staticmethod
    async def search_regions(
        db: AsyncSession,
        query: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Region]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(Region)
            .where(
                and_(
                    Region.name.ilike(search_pattern)
                )
            )
            .offset(skip)
            .limit(limit)
            .order_by(Region.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def create_region(db: AsyncSession, region: RegionCreate) -> Region:
        db_region = Region(**region.model_dump())
        db.add(db_region)
        await db.commit()
        await db.refresh(db_region)
        return db_region
    
    @staticmethod
    async def update_region(
        db: AsyncSession,
        region_id: int,
        region_update: RegionUpdate
    ) -> Optional[Region]:
        result = await db.execute(select(Region).where(Region.id == region_id))
        db_region = result.scalar_one_or_none()
        
        if not db_region:
            return None
        
        update_data = region_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_region, field, value)
        
        await db.commit()
        await db.refresh(db_region)
        return db_region
    
    @staticmethod
    async def delete_region(db: AsyncSession, region_id: int) -> Optional[Region]:
        result = await db.execute(select(Region).where(Region.id == region_id))
        db_region = result.scalar_one_or_none()
        
        if not db_region:
            return None
        
        await db.delete(db_region)
        await db.commit()
        return db_region