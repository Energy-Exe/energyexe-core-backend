from typing import Any, Dict, List, Optional

from sqlalchemy import and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from app.models.turbine_unit import TurbineUnit
from app.models.turbine_model import TurbineModel
from app.schemas.turbine_unit import TurbineUnitCreate, TurbineUnitUpdate


class TurbineUnitService:
    @staticmethod
    async def get_turbine_units(
        db: AsyncSession, skip: int = 0, limit: int = 100
    ) -> List[TurbineUnit]:
        result = await db.execute(
            select(TurbineUnit)
            .options(selectinload(TurbineUnit.windfarm), selectinload(TurbineUnit.turbine_model))
            .offset(skip)
            .limit(limit)
            .order_by(TurbineUnit.created_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def get_turbine_unit(db: AsyncSession, turbine_unit_id: int) -> Optional[TurbineUnit]:
        result = await db.execute(
            select(TurbineUnit)
            .options(selectinload(TurbineUnit.windfarm), selectinload(TurbineUnit.turbine_model))
            .where(TurbineUnit.id == turbine_unit_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def get_turbine_unit_by_code(db: AsyncSession, code: str) -> Optional[TurbineUnit]:
        result = await db.execute(select(TurbineUnit).where(TurbineUnit.code == code))
        return result.scalar_one_or_none()

    @staticmethod
    async def search_turbine_units(
        db: AsyncSession, query: str, skip: int = 0, limit: int = 100
    ) -> List[TurbineUnit]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(TurbineUnit)
            .options(selectinload(TurbineUnit.windfarm), selectinload(TurbineUnit.turbine_model))
            .where(and_(TurbineUnit.code.ilike(search_pattern)))
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
        db: AsyncSession, turbine_unit_id: int, turbine_unit_update: TurbineUnitUpdate
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

    @staticmethod
    async def get_turbine_units_filtered(
        db: AsyncSession,
        windfarm_id: Optional[int] = None,
        model_id: Optional[int] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> List[TurbineUnit]:
        """Get turbine units with optional filters."""
        query = (
            select(TurbineUnit)
            .options(
                selectinload(TurbineUnit.windfarm),
                selectinload(TurbineUnit.turbine_model),
            )
        )

        # Build filter conditions
        conditions = []
        if windfarm_id is not None:
            conditions.append(TurbineUnit.windfarm_id == windfarm_id)
        if model_id is not None:
            conditions.append(TurbineUnit.turbine_model_id == model_id)
        if status is not None:
            conditions.append(TurbineUnit.status == status)
        if search:
            search_pattern = f"%{search}%"
            conditions.append(TurbineUnit.code.ilike(search_pattern))

        if conditions:
            query = query.where(and_(*conditions))

        query = query.offset(skip).limit(limit).order_by(TurbineUnit.code)

        result = await db.execute(query)
        return result.scalars().all()

    @staticmethod
    async def get_turbine_units_stats(
        db: AsyncSession,
        windfarm_id: Optional[int] = None,
        model_id: Optional[int] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get aggregate statistics for turbine units."""
        # Build base query conditions
        conditions = []
        if windfarm_id is not None:
            conditions.append(TurbineUnit.windfarm_id == windfarm_id)
        if model_id is not None:
            conditions.append(TurbineUnit.turbine_model_id == model_id)
        if status is not None:
            conditions.append(TurbineUnit.status == status)

        # Total count
        count_query = select(func.count(TurbineUnit.id))
        if conditions:
            count_query = count_query.where(and_(*conditions))
        count_result = await db.execute(count_query)
        total_count = count_result.scalar() or 0

        # Total capacity (join with turbine_model to get rated_power_kw)
        capacity_query = (
            select(func.sum(TurbineModel.rated_power_kw))
            .select_from(TurbineUnit)
            .join(TurbineModel, TurbineUnit.turbine_model_id == TurbineModel.id)
        )
        if conditions:
            capacity_query = capacity_query.where(and_(*conditions))
        capacity_result = await db.execute(capacity_query)
        total_capacity_kw = capacity_result.scalar() or 0
        total_capacity_mw = total_capacity_kw / 1000 if total_capacity_kw else 0

        # Average hub height
        hub_height_query = select(func.avg(TurbineUnit.hub_height_m))
        if conditions:
            hub_height_query = hub_height_query.where(and_(*conditions))
        hub_height_result = await db.execute(hub_height_query)
        avg_hub_height = hub_height_result.scalar()

        # Count by status
        status_query = (
            select(TurbineUnit.status, func.count(TurbineUnit.id))
            .group_by(TurbineUnit.status)
        )
        if conditions:
            # For status count, we exclude the status filter
            non_status_conditions = [c for c in conditions if "status" not in str(c)]
            if non_status_conditions:
                status_query = status_query.where(and_(*non_status_conditions))
        status_result = await db.execute(status_query)
        status_counts = {row[0] or "unknown": row[1] for row in status_result.fetchall()}

        # Unique windfarm count
        windfarm_query = select(func.count(func.distinct(TurbineUnit.windfarm_id)))
        if conditions:
            windfarm_query = windfarm_query.where(and_(*conditions))
        windfarm_result = await db.execute(windfarm_query)
        windfarm_count = windfarm_result.scalar() or 0

        return {
            "total_count": total_count,
            "total_capacity_mw": round(total_capacity_mw, 2),
            "avg_hub_height_m": round(float(avg_hub_height), 1) if avg_hub_height else None,
            "windfarm_count": windfarm_count,
            "status_breakdown": status_counts,
        }
