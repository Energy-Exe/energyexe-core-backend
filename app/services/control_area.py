from typing import List, Optional
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from app.models.control_area import ControlArea
from app.schemas.control_area import ControlAreaCreate, ControlAreaUpdate


class ControlAreaService:
    
    @staticmethod
    async def get_control_areas(
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100
    ) -> List[ControlArea]:
        result = await db.execute(
            select(ControlArea)
            .options(selectinload(ControlArea.country))
            .offset(skip)
            .limit(limit)
            .order_by(ControlArea.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def get_control_area(db: AsyncSession, control_area_id: int) -> Optional[ControlArea]:
        result = await db.execute(
            select(ControlArea)
            .options(selectinload(ControlArea.country))
            .where(ControlArea.id == control_area_id)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_control_area_by_code(db: AsyncSession, code: str) -> Optional[ControlArea]:
        result = await db.execute(
            select(ControlArea)
            .options(selectinload(ControlArea.country))
            .where(ControlArea.code == code)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def search_control_areas(
        db: AsyncSession,
        query: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[ControlArea]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(ControlArea)
            .options(selectinload(ControlArea.country))
            .where(
                and_(
                    ControlArea.name.ilike(search_pattern)
                )
            )
            .offset(skip)
            .limit(limit)
            .order_by(ControlArea.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def create_control_area(db: AsyncSession, control_area: ControlAreaCreate) -> ControlArea:
        db_control_area = ControlArea(**control_area.model_dump())
        db.add(db_control_area)
        await db.commit()
        await db.refresh(db_control_area)
        return db_control_area
    
    @staticmethod
    async def update_control_area(
        db: AsyncSession,
        control_area_id: int,
        control_area_update: ControlAreaUpdate
    ) -> Optional[ControlArea]:
        result = await db.execute(select(ControlArea).where(ControlArea.id == control_area_id))
        db_control_area = result.scalar_one_or_none()
        
        if not db_control_area:
            return None
        
        update_data = control_area_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_control_area, field, value)
        
        await db.commit()
        await db.refresh(db_control_area)
        
        # Fetch with country relationship
        result = await db.execute(
            select(ControlArea)
            .options(selectinload(ControlArea.country))
            .where(ControlArea.id == control_area_id)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def delete_control_area(db: AsyncSession, control_area_id: int) -> Optional[ControlArea]:
        result = await db.execute(select(ControlArea).where(ControlArea.id == control_area_id))
        db_control_area = result.scalar_one_or_none()
        
        if not db_control_area:
            return None
        
        await db.delete(db_control_area)
        await db.commit()
        return db_control_area