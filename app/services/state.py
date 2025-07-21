from typing import List, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.state import State
from app.schemas.state import StateCreate, StateUpdate


class StateService:
    @staticmethod
    async def create(db: AsyncSession, *, obj_in: StateCreate) -> State:
        db_obj = State(**obj_in.model_dump())
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    @staticmethod
    async def get(db: AsyncSession, *, id: int) -> Optional[State]:
        result = await db.execute(select(State).where(State.id == id))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_with_country(db: AsyncSession, *, id: int) -> Optional[State]:
        result = await db.execute(
            select(State).options(selectinload(State.country)).where(State.id == id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def get_by_code(db: AsyncSession, *, code: str) -> Optional[State]:
        result = await db.execute(select(State).where(State.code == code))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_multi(db: AsyncSession, *, skip: int = 0, limit: int = 100) -> List[State]:
        result = await db.execute(
            select(State)
            .options(selectinload(State.country))
            .offset(skip)
            .limit(limit)
            .order_by(State.name)
        )
        return result.scalars().all()

    @staticmethod
    async def get_by_country(
        db: AsyncSession, *, country_id: int, skip: int = 0, limit: int = 100
    ) -> List[State]:
        result = await db.execute(
            select(State)
            .options(selectinload(State.country))
            .where(State.country_id == country_id)
            .offset(skip)
            .limit(limit)
            .order_by(State.name)
        )
        return result.scalars().all()

    @staticmethod
    async def get_count(db: AsyncSession) -> int:
        result = await db.execute(select(func.count(State.id)))
        return result.scalar_one()

    @staticmethod
    async def update(db: AsyncSession, *, db_obj: State, obj_in: StateUpdate) -> State:
        update_data = obj_in.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_obj, field, value)
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    @staticmethod
    async def delete(db: AsyncSession, *, id: int) -> State:
        result = await db.execute(select(State).where(State.id == id))
        obj = result.scalar_one()
        await db.delete(obj)
        await db.commit()
        return obj

    @staticmethod
    async def search(
        db: AsyncSession, *, query: str, skip: int = 0, limit: int = 100
    ) -> List[State]:
        result = await db.execute(
            select(State)
            .options(selectinload(State.country))
            .where(State.name.ilike(f"%{query}%") | State.code.ilike(f"%{query}%"))
            .offset(skip)
            .limit(limit)
            .order_by(State.name)
        )
        return result.scalars().all()


state = StateService()
