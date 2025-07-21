from typing import List, Optional

from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.windfarm import Windfarm
from app.schemas.windfarm import WindfarmCreate, WindfarmUpdate


class WindfarmService:
    @staticmethod
    async def get_windfarms(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[Windfarm]:
        result = await db.execute(
            select(Windfarm).offset(skip).limit(limit).order_by(Windfarm.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def get_windfarm(db: AsyncSession, windfarm_id: int) -> Optional[Windfarm]:
        result = await db.execute(select(Windfarm).where(Windfarm.id == windfarm_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_windfarm_by_code(db: AsyncSession, code: str) -> Optional[Windfarm]:
        result = await db.execute(select(Windfarm).where(Windfarm.code == code))
        return result.scalar_one_or_none()

    @staticmethod
    async def search_windfarms(
        db: AsyncSession, query: str, skip: int = 0, limit: int = 100
    ) -> List[Windfarm]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(Windfarm)
            .where(and_(Windfarm.name.ilike(search_pattern)))
            .offset(skip)
            .limit(limit)
            .order_by(Windfarm.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def create_windfarm(db: AsyncSession, windfarm: WindfarmCreate) -> Windfarm:
        db_windfarm = Windfarm(**windfarm.model_dump())
        db.add(db_windfarm)
        await db.commit()
        await db.refresh(db_windfarm)
        return db_windfarm

    @staticmethod
    async def update_windfarm(
        db: AsyncSession, windfarm_id: int, windfarm_update: WindfarmUpdate
    ) -> Optional[Windfarm]:
        result = await db.execute(select(Windfarm).where(Windfarm.id == windfarm_id))
        db_windfarm = result.scalar_one_or_none()

        if not db_windfarm:
            return None

        update_data = windfarm_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_windfarm, field, value)

        await db.commit()
        await db.refresh(db_windfarm)
        return db_windfarm

    @staticmethod
    async def delete_windfarm(db: AsyncSession, windfarm_id: int) -> Optional[Windfarm]:
        result = await db.execute(select(Windfarm).where(Windfarm.id == windfarm_id))
        db_windfarm = result.scalar_one_or_none()

        if not db_windfarm:
            return None

        await db.delete(db_windfarm)
        await db.commit()
        return db_windfarm
