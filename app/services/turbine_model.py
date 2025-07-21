from typing import List, Optional

from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.turbine_model import TurbineModel
from app.schemas.turbine_model import TurbineModelCreate, TurbineModelUpdate


class TurbineModelService:
    @staticmethod
    async def get_turbine_models(
        db: AsyncSession, skip: int = 0, limit: int = 100
    ) -> List[TurbineModel]:
        result = await db.execute(
            select(TurbineModel).offset(skip).limit(limit).order_by(TurbineModel.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def get_turbine_model(db: AsyncSession, turbine_model_id: int) -> Optional[TurbineModel]:
        result = await db.execute(select(TurbineModel).where(TurbineModel.id == turbine_model_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_turbine_model_by_model(db: AsyncSession, model: str) -> Optional[TurbineModel]:
        result = await db.execute(select(TurbineModel).where(TurbineModel.model == model))
        return result.scalar_one_or_none()

    @staticmethod
    async def search_turbine_models(
        db: AsyncSession, query: str, skip: int = 0, limit: int = 100
    ) -> List[TurbineModel]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(TurbineModel)
            .where(and_(TurbineModel.model.ilike(search_pattern)))
            .offset(skip)
            .limit(limit)
            .order_by(TurbineModel.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def create_turbine_model(
        db: AsyncSession, turbine_model: TurbineModelCreate
    ) -> TurbineModel:
        db_turbine_model = TurbineModel(**turbine_model.model_dump())
        db.add(db_turbine_model)
        await db.commit()
        await db.refresh(db_turbine_model)
        return db_turbine_model

    @staticmethod
    async def update_turbine_model(
        db: AsyncSession, turbine_model_id: int, turbine_model_update: TurbineModelUpdate
    ) -> Optional[TurbineModel]:
        result = await db.execute(select(TurbineModel).where(TurbineModel.id == turbine_model_id))
        db_turbine_model = result.scalar_one_or_none()

        if not db_turbine_model:
            return None

        update_data = turbine_model_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_turbine_model, field, value)

        await db.commit()
        await db.refresh(db_turbine_model)
        return db_turbine_model

    @staticmethod
    async def delete_turbine_model(
        db: AsyncSession, turbine_model_id: int
    ) -> Optional[TurbineModel]:
        result = await db.execute(select(TurbineModel).where(TurbineModel.id == turbine_model_id))
        db_turbine_model = result.scalar_one_or_none()

        if not db_turbine_model:
            return None

        await db.delete(db_turbine_model)
        await db.commit()
        return db_turbine_model
