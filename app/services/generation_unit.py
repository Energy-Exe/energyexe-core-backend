"""Generation unit service layer."""

from typing import List, Optional

import structlog
from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import NotFoundException, ValidationException
from app.models.generation_unit import GenerationUnit
from app.schemas.generation_unit import (
    GenerationUnitCreate,
    GenerationUnitSearchParams,
    GenerationUnitUpdate,
)

logger = structlog.get_logger()


class GenerationUnitService:
    """Service for generation unit-related operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_id(self, unit_id: int) -> Optional[GenerationUnit]:
        """Get a generation unit by ID."""
        try:
            result = await self.db.execute(
                select(GenerationUnit).where(
                    and_(GenerationUnit.id == unit_id, GenerationUnit.is_active == True)
                )
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error("Error getting generation unit by ID", unit_id=unit_id, error=str(e))
            raise

    async def get_by_code(self, code: str) -> Optional[GenerationUnit]:
        """Get a generation unit by code."""
        try:
            result = await self.db.execute(
                select(GenerationUnit).where(
                    and_(GenerationUnit.code == code, GenerationUnit.is_active == True)
                )
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error("Error getting generation unit by code", code=code, error=str(e))
            raise

    async def get_all(self, params: GenerationUnitSearchParams) -> List[GenerationUnit]:
        """Get all generation units with optional filtering."""
        try:
            query = select(GenerationUnit).where(GenerationUnit.is_active == params.is_active)

            # Apply search filter
            if params.search:
                search_term = f"%{params.search}%"
                query = query.where(
                    or_(
                        GenerationUnit.name.ilike(search_term),
                        GenerationUnit.code.ilike(search_term),
                    )
                )

            # Apply filters
            if params.source:
                query = query.where(GenerationUnit.source == params.source)

            if params.fuel_type:
                query = query.where(GenerationUnit.fuel_type == params.fuel_type)

            if params.technology_type:
                query = query.where(GenerationUnit.technology_type == params.technology_type)

            # Apply ordering
            query = query.order_by(GenerationUnit.name)

            # Apply pagination
            query = query.offset(params.offset).limit(params.limit)

            result = await self.db.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            logger.error("Error getting generation units", error=str(e))
            raise

    async def get_count(self, params: GenerationUnitSearchParams) -> int:
        """Get count of generation units matching the search criteria."""
        try:
            query = select(func.count(GenerationUnit.id)).where(
                GenerationUnit.is_active == params.is_active
            )

            # Apply search filter
            if params.search:
                search_term = f"%{params.search}%"
                query = query.where(
                    or_(
                        GenerationUnit.name.ilike(search_term),
                        GenerationUnit.code.ilike(search_term),
                    )
                )

            # Apply filters
            if params.source:
                query = query.where(GenerationUnit.source == params.source)

            if params.fuel_type:
                query = query.where(GenerationUnit.fuel_type == params.fuel_type)

            if params.technology_type:
                query = query.where(GenerationUnit.technology_type == params.technology_type)

            result = await self.db.execute(query)
            return result.scalar_one()
        except Exception as e:
            logger.error("Error getting generation units count", error=str(e))
            raise

    async def create(self, unit_data: GenerationUnitCreate) -> GenerationUnit:
        """Create a new generation unit."""
        try:
            # Check if code already exists
            existing_unit = await self.get_by_code(unit_data.code)
            if existing_unit:
                raise ValidationException(
                    f"Generation unit with code '{unit_data.code}' already exists"
                )

            unit = GenerationUnit(**unit_data.model_dump())
            self.db.add(unit)
            await self.db.commit()
            await self.db.refresh(unit)

            logger.info("Generation unit created", unit_id=unit.id, code=unit.code)
            return unit
        except IntegrityError as e:
            await self.db.rollback()
            logger.error("Integrity error creating generation unit", error=str(e))
            raise ValidationException("Failed to create generation unit: code must be unique")
        except Exception as e:
            await self.db.rollback()
            logger.error("Error creating generation unit", error=str(e))
            raise

    async def update(self, unit_id: int, unit_data: GenerationUnitUpdate) -> GenerationUnit:
        """Update an existing generation unit."""
        try:
            unit = await self.get_by_id(unit_id)
            if not unit:
                raise NotFoundException(f"Generation unit with ID {unit_id} not found")

            # Check if code is being changed and if it conflicts
            if unit_data.code and unit_data.code != unit.code:
                existing_unit = await self.get_by_code(unit_data.code)
                if existing_unit and existing_unit.id != unit_id:
                    raise ValidationException(
                        f"Generation unit with code '{unit_data.code}' already exists"
                    )

            # Update fields
            update_data = unit_data.model_dump(exclude_unset=True)
            for field, value in update_data.items():
                setattr(unit, field, value)

            await self.db.commit()
            await self.db.refresh(unit)

            logger.info("Generation unit updated", unit_id=unit.id, code=unit.code)
            return unit
        except (NotFoundException, ValidationException):
            await self.db.rollback()
            raise
        except IntegrityError as e:
            await self.db.rollback()
            logger.error("Integrity error updating generation unit", unit_id=unit_id, error=str(e))
            raise ValidationException("Failed to update generation unit: code must be unique")
        except Exception as e:
            await self.db.rollback()
            logger.error("Error updating generation unit", unit_id=unit_id, error=str(e))
            raise

    async def delete(self, unit_id: int) -> None:
        """Soft delete a generation unit."""
        try:
            unit = await self.get_by_id(unit_id)
            if not unit:
                raise NotFoundException(f"Generation unit with ID {unit_id} not found")

            unit.is_active = False
            await self.db.commit()

            logger.info("Generation unit deleted", unit_id=unit.id, code=unit.code)
        except NotFoundException:
            await self.db.rollback()
            raise
        except Exception as e:
            await self.db.rollback()
            logger.error("Error deleting generation unit", unit_id=unit_id, error=str(e))
            raise
