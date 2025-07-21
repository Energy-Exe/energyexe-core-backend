from typing import List, Optional

from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.owner import Owner
from app.schemas.owner import OwnerCreate, OwnerUpdate


class OwnerService:
    @staticmethod
    async def get_owners(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[Owner]:
        result = await db.execute(
            select(Owner).offset(skip).limit(limit).order_by(Owner.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def get_owner(db: AsyncSession, owner_id: int) -> Optional[Owner]:
        result = await db.execute(select(Owner).where(Owner.id == owner_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_owner_by_code(db: AsyncSession, code: str) -> Optional[Owner]:
        result = await db.execute(select(Owner).where(Owner.code == code))
        return result.scalar_one_or_none()

    @staticmethod
    async def search_owners(
        db: AsyncSession, query: str, skip: int = 0, limit: int = 100
    ) -> List[Owner]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(Owner)
            .where(and_(Owner.name.ilike(search_pattern)))
            .offset(skip)
            .limit(limit)
            .order_by(Owner.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def create_owner(db: AsyncSession, owner: OwnerCreate) -> Owner:
        db_owner = Owner(**owner.model_dump())
        db.add(db_owner)
        await db.commit()
        await db.refresh(db_owner)
        return db_owner

    @staticmethod
    async def update_owner(
        db: AsyncSession, owner_id: int, owner_update: OwnerUpdate
    ) -> Optional[Owner]:
        result = await db.execute(select(Owner).where(Owner.id == owner_id))
        db_owner = result.scalar_one_or_none()

        if not db_owner:
            return None

        update_data = owner_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_owner, field, value)

        await db.commit()
        await db.refresh(db_owner)
        return db_owner

    @staticmethod
    async def delete_owner(db: AsyncSession, owner_id: int) -> Optional[Owner]:
        result = await db.execute(select(Owner).where(Owner.id == owner_id))
        db_owner = result.scalar_one_or_none()

        if not db_owner:
            return None

        await db.delete(db_owner)
        await db.commit()
        return db_owner
