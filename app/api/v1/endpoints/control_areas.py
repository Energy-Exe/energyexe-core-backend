from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.schemas.control_area import ControlArea, ControlAreaCreate, ControlAreaUpdate
from app.services.control_area import ControlAreaService

router = APIRouter()


@router.get("/", response_model=List[ControlArea])
async def get_control_areas(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Get all control areas with pagination"""
    return await ControlAreaService.get_control_areas(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[ControlArea])
async def search_control_areas(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Search control areas by name"""
    return await ControlAreaService.search_control_areas(db, query=q, skip=skip, limit=limit)


@router.get("/{control_area_id}", response_model=ControlArea)
async def get_control_area(
    control_area_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get a specific control area by ID"""
    control_area = await ControlAreaService.get_control_area(db, control_area_id)
    if not control_area:
        raise HTTPException(status_code=404, detail="Control area not found")
    return control_area


@router.get("/code/{code}", response_model=ControlArea)
async def get_control_area_by_code(
    code: str,
    db: AsyncSession = Depends(get_db)
):
    """Get a control area by its code"""
    control_area = await ControlAreaService.get_control_area_by_code(db, code)
    if not control_area:
        raise HTTPException(status_code=404, detail="Control area not found")
    return control_area


@router.post("/", response_model=ControlArea, status_code=201)
async def create_control_area(
    control_area: ControlAreaCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new control area"""
    # Check if control area with same code already exists
    existing_control_area = await ControlAreaService.get_control_area_by_code(db, control_area.code)
    if existing_control_area:
        raise HTTPException(
            status_code=400,
            detail="Control area with this code already exists"
        )
    
    return await ControlAreaService.create_control_area(db, control_area)


@router.put("/{control_area_id}", response_model=ControlArea)
async def update_control_area(
    control_area_id: int,
    control_area_update: ControlAreaUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update a control area"""
    # Check if control area with same code already exists (excluding current control area)
    if control_area_update.code:
        existing_control_area = await ControlAreaService.get_control_area_by_code(db, control_area_update.code)
        if existing_control_area and existing_control_area.id != control_area_id:
            raise HTTPException(
                status_code=400,
                detail="Control area with this code already exists"
            )
    
    updated_control_area = await ControlAreaService.update_control_area(db, control_area_id, control_area_update)
    if not updated_control_area:
        raise HTTPException(status_code=404, detail="Control area not found")
    return updated_control_area


@router.delete("/{control_area_id}", response_model=ControlArea)
async def delete_control_area(
    control_area_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Delete a control area"""
    deleted_control_area = await ControlAreaService.delete_control_area(db, control_area_id)
    if not deleted_control_area:
        raise HTTPException(status_code=404, detail="Control area not found")
    return deleted_control_area