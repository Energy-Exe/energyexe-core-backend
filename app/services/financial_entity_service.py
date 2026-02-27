"""Service for FinancialEntity CRUD operations and windfarm linking."""

from typing import List, Optional

import structlog
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.financial_entity import FinancialEntity
from app.models.windfarm import Windfarm
from app.models.windfarm_financial_entity import WindfarmFinancialEntity
from app.schemas.financial_entity import FinancialEntityCreate, FinancialEntityUpdate

logger = structlog.get_logger()


class FinancialEntityService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_entities(
        self,
        skip: int = 0,
        limit: int = 100,
        entity_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> tuple[List[FinancialEntity], int]:
        """Get all financial entities with pagination and optional filters."""
        query = select(FinancialEntity)
        count_query = select(func.count(FinancialEntity.id))

        if entity_type:
            query = query.where(FinancialEntity.entity_type == entity_type)
            count_query = count_query.where(FinancialEntity.entity_type == entity_type)

        if search:
            search_filter = FinancialEntity.name.ilike(f"%{search}%")
            query = query.where(search_filter)
            count_query = count_query.where(search_filter)

        count_result = await self.db.execute(count_query)
        total = count_result.scalar() or 0

        result = await self.db.execute(
            query.offset(skip).limit(limit).order_by(FinancialEntity.name)
        )
        entities = list(result.scalars().all())
        return entities, total

    async def get_entity(self, entity_id: int) -> Optional[FinancialEntity]:
        """Get a single financial entity by ID with windfarm links."""
        result = await self.db.execute(
            select(FinancialEntity)
            .options(
                selectinload(FinancialEntity.windfarm_financial_entities).selectinload(
                    WindfarmFinancialEntity.windfarm
                )
            )
            .where(FinancialEntity.id == entity_id)
        )
        return result.scalar_one_or_none()

    async def get_entity_by_code(self, code: str) -> Optional[FinancialEntity]:
        """Get a financial entity by code."""
        result = await self.db.execute(
            select(FinancialEntity).where(FinancialEntity.code == code)
        )
        return result.scalar_one_or_none()

    async def create_entity(self, data: FinancialEntityCreate) -> FinancialEntity:
        """Create a new financial entity."""
        db_entity = FinancialEntity(**data.model_dump())
        self.db.add(db_entity)
        await self.db.commit()
        await self.db.refresh(db_entity)
        return db_entity

    async def update_entity(
        self, entity_id: int, data: FinancialEntityUpdate
    ) -> Optional[FinancialEntity]:
        """Update an existing financial entity."""
        result = await self.db.execute(
            select(FinancialEntity).where(FinancialEntity.id == entity_id)
        )
        db_entity = result.scalar_one_or_none()
        if not db_entity:
            return None

        update_data = data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_entity, field, value)

        await self.db.commit()
        await self.db.refresh(db_entity)
        return db_entity

    async def delete_entity(self, entity_id: int) -> Optional[FinancialEntity]:
        """Delete a financial entity by ID."""
        result = await self.db.execute(
            select(FinancialEntity).where(FinancialEntity.id == entity_id)
        )
        db_entity = result.scalar_one_or_none()
        if not db_entity:
            return None

        await self.db.delete(db_entity)
        await self.db.commit()
        return db_entity

    # --- Windfarm linking ---

    async def link_windfarm(
        self,
        entity_id: int,
        windfarm_id: int,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> WindfarmFinancialEntity:
        """Link a financial entity to a windfarm."""
        link = WindfarmFinancialEntity(
            financial_entity_id=entity_id,
            windfarm_id=windfarm_id,
            relationship_type=relationship_type,
            notes=notes,
        )
        self.db.add(link)
        await self.db.commit()
        await self.db.refresh(link)
        return link

    async def unlink_windfarm(self, entity_id: int, windfarm_id: int) -> bool:
        """Unlink a financial entity from a windfarm."""
        result = await self.db.execute(
            select(WindfarmFinancialEntity).where(
                and_(
                    WindfarmFinancialEntity.financial_entity_id == entity_id,
                    WindfarmFinancialEntity.windfarm_id == windfarm_id,
                )
            )
        )
        link = result.scalar_one_or_none()
        if not link:
            return False

        await self.db.delete(link)
        await self.db.commit()
        return True

    async def get_entities_by_windfarm(
        self, windfarm_id: int
    ) -> List[FinancialEntity]:
        """Get all financial entities linked to a windfarm."""
        result = await self.db.execute(
            select(FinancialEntity)
            .join(WindfarmFinancialEntity)
            .where(WindfarmFinancialEntity.windfarm_id == windfarm_id)
        )
        return list(result.scalars().all())
