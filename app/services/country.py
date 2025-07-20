from typing import List, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.country import Country
from app.schemas.country import CountryCreate, CountryUpdate


class CountryService:
    @staticmethod
    async def create(db: AsyncSession, *, obj_in: CountryCreate) -> Country:
        db_obj = Country(**obj_in.model_dump())
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    @staticmethod
    async def get(db: AsyncSession, *, id: int) -> Optional[Country]:
        result = await db.execute(select(Country).where(Country.id == id))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_by_code(db: AsyncSession, *, code: str) -> Optional[Country]:
        result = await db.execute(select(Country).where(Country.code == code))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_multi(db: AsyncSession, *, skip: int = 0, limit: int = 100) -> List[Country]:
        result = await db.execute(select(Country).offset(skip).limit(limit).order_by(Country.name))
        return result.scalars().all()

    @staticmethod
    async def get_count(db: AsyncSession) -> int:
        result = await db.execute(select(func.count(Country.id)))
        return result.scalar_one()

    @staticmethod
    async def update(db: AsyncSession, *, db_obj: Country, obj_in: CountryUpdate) -> Country:
        update_data = obj_in.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_obj, field, value)
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    @staticmethod
    async def delete(db: AsyncSession, *, id: int) -> Country:
        result = await db.execute(select(Country).where(Country.id == id))
        obj = result.scalar_one()
        await db.delete(obj)
        await db.commit()
        return obj

    @staticmethod
    async def search(
        db: AsyncSession, *, query: str, skip: int = 0, limit: int = 100
    ) -> List[Country]:
        result = await db.execute(
            select(Country)
            .where(Country.name.ilike(f"%{query}%") | Country.code.ilike(f"%{query}%"))
            .offset(skip)
            .limit(limit)
            .order_by(Country.name)
        )
        return result.scalars().all()


country = CountryService()
