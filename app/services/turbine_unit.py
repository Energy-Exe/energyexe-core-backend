from typing import List, Optional
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.turbine_unit import TurbineUnit
from app.schemas.turbine_unit import TurbineUnitCreate, TurbineUnitUpdate


class TurbineUnitService:
    
    @staticmethod
    async def get_turbine_units(
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100
    ) -> List[TurbineUnit]:
        result = await db.execute(
            select(TurbineUnit)
            .offset(skip)
            .limit(limit)
            .order_by(TurbineUnit.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def get_turbine_unit(db: AsyncSession, turbine_unit_id: int) -> Optional[TurbineUnit]:
        result = await db.execute(select(TurbineUnit).where(TurbineUnit.id == turbine_unit_id))
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_turbine_unit_by_code(db: AsyncSession, code: str) -> Optional[TurbineUnit]:
        result = await db.execute(select(TurbineUnit).where(TurbineUnit.code == code))
        return result.scalar_one_or_none()
    
    @staticmethod
    async def search_turbine_units(
        db: AsyncSession,
        query: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[TurbineUnit]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(TurbineUnit)
            .where(
                and_(
                    TurbineUnit.name.ilike(search_pattern)
                )
            )
            .offset(skip)
            .limit(limit)
            .order_by(TurbineUnit.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def create_turbine_unit(db: AsyncSession, turbine_unit: TurbineUnitCreate) -> TurbineUnit:
        db_turbine_unit = TurbineUnit(**turbine_unit.model_dump())
        db.add(db_turbine_unit)
        await db.commit()
        await db.refresh(db_turbine_unit)
        return db_turbine_unit
    
    @staticmethod
    async def update_turbine_unit(
        db: AsyncSession,
        turbine_unit_id: int,
        turbine_unit_update: TurbineUnitUpdate
    ) -> Optional[TurbineUnit]:
        result = await db.execute(select(TurbineUnit).where(TurbineUnit.id == turbine_unit_id))
        db_turbine_unit = result.scalar_one_or_none()
        
        if not db_turbine_unit:
            return None
        
        update_data = turbine_unit_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_turbine_unit, field, value)
        
        await db.commit()
        await db.refresh(db_turbine_unit)
        return db_turbine_unit
    
    @staticmethod
    async def delete_turbine_unit(db: AsyncSession, turbine_unit_id: int) -> Optional[TurbineUnit]:
        result = await db.execute(select(TurbineUnit).where(TurbineUnit.id == turbine_unit_id))
        db_turbine_unit = result.scalar_one_or_none()
        
        if not db_turbine_unit:
            return None
        
        await db.delete(db_turbine_unit)
        await db.commit()
        return db_turbine_unit