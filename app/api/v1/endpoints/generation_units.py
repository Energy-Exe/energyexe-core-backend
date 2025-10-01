"""Generation units API endpoints."""

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import DEFAULT_PAGINATION_LIMIT, MAX_PAGINATION_LIMIT, MIN_PAGINATION_LIMIT
from app.core.deps import get_current_active_user, get_db
from app.core.exceptions import NotFoundException, ValidationException
from app.models.user import User
from app.schemas.generation_unit import (
    GenerationUnitCreate,
    GenerationUnitResponse,
    GenerationUnitSearchParams,
    GenerationUnitUpdate,
    GenerationUnitWithWindfarm,
)
from app.services.generation_unit import GenerationUnitService

router = APIRouter()


@router.get("/", response_model=List[GenerationUnitResponse])
async def get_generation_units(
    search: str = Query(None, description="Search term for name or code"),
    source: str = Query(None, description="Filter by data source"),
    fuel_type: str = Query(None, description="Filter by fuel type"),
    technology_type: str = Query(None, description="Filter by technology type"),
    is_active: bool = Query(True, description="Filter by active status"),
    limit: int = Query(DEFAULT_PAGINATION_LIMIT, ge=MIN_PAGINATION_LIMIT, le=MAX_PAGINATION_LIMIT, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    db: AsyncSession = Depends(get_db),
):
    """Get all generation units with optional filtering."""
    try:
        params = GenerationUnitSearchParams(
            search=search,
            source=source,
            fuel_type=fuel_type,
            technology_type=technology_type,
            is_active=is_active,
            limit=limit,
            offset=offset,
        )

        service = GenerationUnitService(db)
        generation_units = await service.get_all(params)
        return generation_units
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving generation units: {str(e)}",
        )


@router.get("/count")
async def get_generation_units_count(
    search: str = Query(None, description="Search term for name or code"),
    source: str = Query(None, description="Filter by data source"),
    fuel_type: str = Query(None, description="Filter by fuel type"),
    technology_type: str = Query(None, description="Filter by technology type"),
    is_active: bool = Query(True, description="Filter by active status"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get count of generation units matching the search criteria."""
    try:
        params = GenerationUnitSearchParams(
            search=search,
            source=source,
            fuel_type=fuel_type,
            technology_type=technology_type,
            is_active=is_active,
            limit=1,  # Not used for count
            offset=0,  # Not used for count
        )

        service = GenerationUnitService(db)
        count = await service.get_count(params)
        return {"count": count}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving generation units count: {str(e)}",
        )


@router.get("/{unit_id}", response_model=GenerationUnitWithWindfarm)
async def get_generation_unit(
    unit_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a specific generation unit by ID with windfarm details."""
    try:
        service = GenerationUnitService(db)
        generation_unit = await service.get_by_id_with_windfarm(unit_id)

        if not generation_unit:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Generation unit with ID {unit_id} not found",
            )

        return generation_unit
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving generation unit: {str(e)}",
        )


@router.post("/", response_model=GenerationUnitResponse, status_code=status.HTTP_201_CREATED)
async def create_generation_unit(
    unit_data: GenerationUnitCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new generation unit."""
    try:
        service = GenerationUnitService(db)
        generation_unit = await service.create(unit_data)
        return generation_unit
    except ValidationException as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating generation unit: {str(e)}",
        )


@router.put("/{unit_id}", response_model=GenerationUnitResponse)
async def update_generation_unit(
    unit_id: int,
    unit_data: GenerationUnitUpdate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Update an existing generation unit."""
    try:
        service = GenerationUnitService(db)
        generation_unit = await service.update(unit_id, unit_data)
        return generation_unit
    except NotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except ValidationException as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating generation unit: {str(e)}",
        )


@router.delete("/{unit_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_generation_unit(
    unit_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a generation unit (soft delete)."""
    try:
        service = GenerationUnitService(db)
        await service.delete(unit_id)
    except NotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting generation unit: {str(e)}",
        )
