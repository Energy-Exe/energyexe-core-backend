from typing import List, Optional
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.substation import Substation
from app.schemas.substation import SubstationCreate, SubstationUpdate


class SubstationService:
    
    @staticmethod
    async def get_substations(
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100
    ) -> List[Substation]:
        result = await db.execute(
            select(Substation)
            .offset(skip)
            .limit(limit)
            .order_by(Substation.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def get_substation(db: AsyncSession, substation_id: int) -> Optional[Substation]:
        result = await db.execute(select(Substation).where(Substation.id == substation_id))
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_substation_by_code(db: AsyncSession, code: str) -> Optional[Substation]:
        result = await db.execute(select(Substation).where(Substation.code == code))
        return result.scalar_one_or_none()
    
    @staticmethod
    async def search_substations(
        db: AsyncSession,
        query: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Substation]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(Substation)
            .where(
                and_(
                    Substation.name.ilike(search_pattern)
                )
            )
            .offset(skip)
            .limit(limit)
            .order_by(Substation.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def create_substation(db: AsyncSession, substation: SubstationCreate) -> Substation:
        db_substation = Substation(**substation.model_dump())
        db.add(db_substation)
        await db.commit()
        await db.refresh(db_substation)
        return db_substation
    
    @staticmethod
    async def update_substation(
        db: AsyncSession,
        substation_id: int,
        substation_update: SubstationUpdate
    ) -> Optional[Substation]:
        result = await db.execute(select(Substation).where(Substation.id == substation_id))
        db_substation = result.scalar_one_or_none()
        
        if not db_substation:
            return None
        
        update_data = substation_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_substation, field, value)
        
        await db.commit()
        await db.refresh(db_substation)
        return db_substation
    
    @staticmethod
    async def delete_substation(db: AsyncSession, substation_id: int) -> Optional[Substation]:
        result = await db.execute(select(Substation).where(Substation.id == substation_id))
        db_substation = result.scalar_one_or_none()
        
        if not db_substation:
            return None
        
        await db.delete(db_substation)
        await db.commit()
        return db_substation