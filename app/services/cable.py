from typing import List, Optional

from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.cable import Cable
from app.schemas.cable import CableCreate, CableUpdate


class CableService:
    @staticmethod
    async def get_cables(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[Cable]:
        result = await db.execute(
            select(Cable).offset(skip).limit(limit).order_by(Cable.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def get_cable(db: AsyncSession, cable_id: int) -> Optional[Cable]:
        result = await db.execute(select(Cable).where(Cable.id == cable_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_cable_by_code(db: AsyncSession, code: str) -> Optional[Cable]:
        result = await db.execute(select(Cable).where(Cable.code == code))
        return result.scalar_one_or_none()

    @staticmethod
    async def search_cables(
        db: AsyncSession, query: str, skip: int = 0, limit: int = 100
    ) -> List[Cable]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(Cable)
            .where(and_(Cable.name.ilike(search_pattern)))
            .offset(skip)
            .limit(limit)
            .order_by(Cable.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def create_cable(db: AsyncSession, cable: CableCreate) -> Cable:
        db_cable = Cable(**cable.model_dump())
        db.add(db_cable)
        await db.commit()
        await db.refresh(db_cable)
        return db_cable

    @staticmethod
    async def update_cable(
        db: AsyncSession, cable_id: int, cable_update: CableUpdate
    ) -> Optional[Cable]:
        result = await db.execute(select(Cable).where(Cable.id == cable_id))
        db_cable = result.scalar_one_or_none()

        if not db_cable:
            return None

        update_data = cable_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_cable, field, value)

        await db.commit()
        await db.refresh(db_cable)
        return db_cable

    @staticmethod
    async def delete_cable(db: AsyncSession, cable_id: int) -> Optional[Cable]:
        result = await db.execute(select(Cable).where(Cable.id == cable_id))
        db_cable = result.scalar_one_or_none()

        if not db_cable:
            return None

        await db.delete(db_cable)
        await db.commit()
        return db_cable
