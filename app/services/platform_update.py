from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.platform_update import PlatformUpdate
from app.schemas.platform_update import PlatformUpdateCreate, PlatformUpdateUpdate


class PlatformUpdateService:
    @staticmethod
    async def list_updates(
        db: AsyncSession, active_only: bool = False, limit: Optional[int] = None
    ) -> List[PlatformUpdate]:
        stmt = select(PlatformUpdate)
        if active_only:
            stmt = stmt.where(PlatformUpdate.is_active.is_(True))
        stmt = stmt.order_by(PlatformUpdate.published_at.desc(), PlatformUpdate.id.desc())
        if limit:
            stmt = stmt.limit(limit)
        result = await db.execute(stmt)
        return result.scalars().all()

    @staticmethod
    async def get_update(db: AsyncSession, update_id: int) -> Optional[PlatformUpdate]:
        result = await db.execute(select(PlatformUpdate).where(PlatformUpdate.id == update_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def create_update(db: AsyncSession, update: PlatformUpdateCreate) -> PlatformUpdate:
        db_update = PlatformUpdate(**update.model_dump())
        db.add(db_update)
        await db.commit()
        await db.refresh(db_update)
        return db_update

    @staticmethod
    async def update_update(
        db: AsyncSession,
        update_id: int,
        update_data: PlatformUpdateUpdate,
    ) -> Optional[PlatformUpdate]:
        result = await db.execute(select(PlatformUpdate).where(PlatformUpdate.id == update_id))
        db_update = result.scalar_one_or_none()
        if not db_update:
            return None

        data = update_data.model_dump(exclude_unset=True)
        for field, value in data.items():
            setattr(db_update, field, value)

        await db.commit()
        await db.refresh(db_update)
        return db_update

    @staticmethod
    async def delete_update(db: AsyncSession, update_id: int) -> Optional[PlatformUpdate]:
        result = await db.execute(select(PlatformUpdate).where(PlatformUpdate.id == update_id))
        db_update = result.scalar_one_or_none()
        if not db_update:
            return None

        await db.delete(db_update)
        await db.commit()
        return db_update
