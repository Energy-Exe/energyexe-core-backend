from typing import List, Optional

from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from app.models.bidzone import Bidzone
from app.models.country import Country
from app.schemas.bidzone import BidzoneCreate, BidzoneUpdate


class BidzoneService:
    @staticmethod
    async def get_bidzones(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[Bidzone]:
        result = await db.execute(
            select(Bidzone)
            .options(selectinload(Bidzone.countries))
            .offset(skip)
            .limit(limit)
            .order_by(Bidzone.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def get_bidzone(db: AsyncSession, bidzone_id: int) -> Optional[Bidzone]:
        result = await db.execute(
            select(Bidzone).options(selectinload(Bidzone.countries)).where(Bidzone.id == bidzone_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def get_bidzone_by_code(db: AsyncSession, code: str) -> Optional[Bidzone]:
        result = await db.execute(
            select(Bidzone).options(selectinload(Bidzone.countries)).where(Bidzone.code == code)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def search_bidzones(
        db: AsyncSession, query: str, skip: int = 0, limit: int = 100
    ) -> List[Bidzone]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(Bidzone)
            .options(selectinload(Bidzone.countries))
            .where(and_(Bidzone.name.ilike(search_pattern)))
            .offset(skip)
            .limit(limit)
            .order_by(Bidzone.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def create_bidzone(db: AsyncSession, bidzone: BidzoneCreate) -> Bidzone:
        # Extract country_ids from the create schema
        bidzone_data = bidzone.model_dump()
        country_ids = bidzone_data.pop("country_ids", [])

        # Create the bidzone
        db_bidzone = Bidzone(**bidzone_data)

        # Add countries if provided
        if country_ids:
            countries = await db.execute(select(Country).where(Country.id.in_(country_ids)))
            db_bidzone.countries = list(countries.scalars().all())

        db.add(db_bidzone)
        await db.commit()
        await db.refresh(db_bidzone)

        # Load countries relationship for response
        result = await db.execute(
            select(Bidzone)
            .options(selectinload(Bidzone.countries))
            .where(Bidzone.id == db_bidzone.id)
        )
        return result.scalar_one()

    @staticmethod
    async def update_bidzone(
        db: AsyncSession, bidzone_id: int, bidzone_update: BidzoneUpdate
    ) -> Optional[Bidzone]:
        result = await db.execute(select(Bidzone).where(Bidzone.id == bidzone_id))
        db_bidzone = result.scalar_one_or_none()

        if not db_bidzone:
            return None

        update_data = bidzone_update.model_dump(exclude_unset=True)

        # Handle country_ids separately
        country_ids = update_data.pop("country_ids", None)

        # Update regular fields
        for field, value in update_data.items():
            setattr(db_bidzone, field, value)

        # Update countries if provided
        if country_ids is not None:
            countries = await db.execute(select(Country).where(Country.id.in_(country_ids)))
            db_bidzone.countries = list(countries.scalars().all())

        await db.commit()
        await db.refresh(db_bidzone)

        # Load countries relationship for response
        result = await db.execute(
            select(Bidzone)
            .options(selectinload(Bidzone.countries))
            .where(Bidzone.id == db_bidzone.id)
        )
        return result.scalar_one()

    @staticmethod
    async def delete_bidzone(db: AsyncSession, bidzone_id: int) -> Optional[Bidzone]:
        result = await db.execute(select(Bidzone).where(Bidzone.id == bidzone_id))
        db_bidzone = result.scalar_one_or_none()

        if not db_bidzone:
            return None

        await db.delete(db_bidzone)
        await db.commit()
        return db_bidzone
