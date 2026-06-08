from datetime import datetime
from typing import Dict, Iterable, List, Optional

from sqlalchemy import and_, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from app.models.generation_data import GenerationData
from app.models.windfarm import Windfarm
from app.models.windfarm_owner import WindfarmOwner
from app.schemas.windfarm import WindfarmCreate, WindfarmUpdate


class WindfarmService:
    @staticmethod
    async def get_windfarms(
        db: AsyncSession, skip: int = 0, limit: int = 100, visible_only: bool = False
    ) -> List[Windfarm]:
        stmt = (
            select(Windfarm)
            .options(
                selectinload(Windfarm.windfarm_owners).selectinload(WindfarmOwner.owner),
                selectinload(Windfarm.country),
                selectinload(Windfarm.bidzone),
            )
            .offset(skip)
            .limit(limit)
            .order_by(Windfarm.created_at.desc())
        )
        if visible_only:
            stmt = stmt.where(Windfarm.is_deleted == False)  # noqa: E712
        result = await db.execute(stmt)
        return result.scalars().all()

    @staticmethod
    async def get_latest_generation_per_windfarm(
        db: AsyncSession, windfarm_ids: Iterable[int]
    ) -> Dict[int, datetime]:
        """Return {windfarm_id: max(generation_data.hour)} for the given IDs.

        Single aggregate query — avoids N+1 round-trips when populating the
        wind farms list (#18).
        """
        ids = [i for i in windfarm_ids if i is not None]
        if not ids:
            return {}
        result = await db.execute(
            select(
                GenerationData.windfarm_id,
                func.max(GenerationData.hour),
            )
            .where(GenerationData.windfarm_id.in_(ids))
            .group_by(GenerationData.windfarm_id)
        )
        return {row[0]: row[1] for row in result.all() if row[0] is not None}

    @staticmethod
    async def get_windfarm(db: AsyncSession, windfarm_id: int) -> Optional[Windfarm]:
        result = await db.execute(select(Windfarm).where(Windfarm.id == windfarm_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def get_windfarm_with_owners(db: AsyncSession, windfarm_id: int) -> Optional[Windfarm]:
        result = await db.execute(
            select(Windfarm)
            .where(Windfarm.id == windfarm_id)
            .options(
                selectinload(Windfarm.windfarm_owners).selectinload(WindfarmOwner.owner),
                selectinload(Windfarm.country),
                selectinload(Windfarm.state),
                selectinload(Windfarm.region),
                selectinload(Windfarm.bidzone),
                selectinload(Windfarm.market_balance_area),
                selectinload(Windfarm.control_area),
                selectinload(Windfarm.project),
            )
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def get_windfarm_with_generation_units(db: AsyncSession, windfarm_id: int) -> Optional[Windfarm]:
        result = await db.execute(
            select(Windfarm)
            .where(Windfarm.id == windfarm_id)
            .options(
                selectinload(Windfarm.generation_units),
                selectinload(Windfarm.country),
                selectinload(Windfarm.state),
                selectinload(Windfarm.region),
                selectinload(Windfarm.bidzone),
                selectinload(Windfarm.market_balance_area),
                selectinload(Windfarm.control_area),
                selectinload(Windfarm.project),
            )
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def get_windfarm_by_code(db: AsyncSession, code: str) -> Optional[Windfarm]:
        result = await db.execute(select(Windfarm).where(Windfarm.code == code))
        return result.scalar_one_or_none()

    @staticmethod
    async def search_windfarms(
        db: AsyncSession, query: str, skip: int = 0, limit: int = 100, visible_only: bool = False
    ) -> List[Windfarm]:
        search_pattern = f"%{query}%"
        # Item #4 — search across name, country, and owner names (not just name).
        from app.models.country import Country
        from app.models.owner import Owner

        stmt = (
            select(Windfarm)
            .options(
                selectinload(Windfarm.windfarm_owners).selectinload(WindfarmOwner.owner),
                selectinload(Windfarm.country),
                selectinload(Windfarm.bidzone),
            )
            .outerjoin(Windfarm.country)
            .outerjoin(Windfarm.windfarm_owners)
            .outerjoin(WindfarmOwner.owner)
            .where(
                or_(
                    Windfarm.name.ilike(search_pattern),
                    Windfarm.code.ilike(search_pattern),
                    Country.name.ilike(search_pattern),
                    Owner.name.ilike(search_pattern),
                )
            )
            .distinct()
            .offset(skip)
            .limit(limit)
            .order_by(Windfarm.created_at.desc())
        )
        if visible_only:
            stmt = stmt.where(Windfarm.is_deleted == False)  # noqa: E712
        result = await db.execute(stmt)
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
        from sqlalchemy import update

        from app.models.generation_unit import GenerationUnit
        from app.models.turbine_unit import TurbineUnit

        result = await db.execute(select(Windfarm).where(Windfarm.id == windfarm_id))
        db_windfarm = result.scalar_one_or_none()

        if not db_windfarm:
            return None

        # First, set windfarm_id to NULL for any generation units that reference this windfarm
        await db.execute(
            update(GenerationUnit)
            .where(GenerationUnit.windfarm_id == windfarm_id)
            .values(windfarm_id=None)
        )

        # Delete related turbine units (cascade delete is configured)
        # Delete related windfarm owners (cascade delete is configured)

        await db.delete(db_windfarm)
        await db.commit()
        return db_windfarm

    @staticmethod
    async def get_windfarms_by_substation(
        db: AsyncSession, substation_id: int
    ) -> List[Windfarm]:
        """Get all windfarms linked to a specific substation."""
        result = await db.execute(
            select(Windfarm)
            .where(Windfarm.substation_id == substation_id)
            .order_by(Windfarm.name)
        )
        return result.scalars().all()

    @staticmethod
    async def link_to_substation(
        db: AsyncSession, windfarm_id: int, substation_id: int
    ) -> Optional[Windfarm]:
        """Link a windfarm to a substation by setting its substation_id."""
        result = await db.execute(select(Windfarm).where(Windfarm.id == windfarm_id))
        db_windfarm = result.scalar_one_or_none()

        if not db_windfarm:
            return None

        db_windfarm.substation_id = substation_id
        await db.commit()
        await db.refresh(db_windfarm)
        return db_windfarm

    @staticmethod
    async def unlink_from_substation(
        db: AsyncSession, windfarm_id: int, substation_id: int
    ) -> Optional[Windfarm]:
        """Unlink a windfarm from a substation by removing its substation_id."""
        result = await db.execute(
            select(Windfarm).where(
                and_(
                    Windfarm.id == windfarm_id,
                    Windfarm.substation_id == substation_id
                )
            )
        )
        db_windfarm = result.scalar_one_or_none()

        if not db_windfarm:
            return None

        db_windfarm.substation_id = None
        await db.commit()
        await db.refresh(db_windfarm)
        return db_windfarm
