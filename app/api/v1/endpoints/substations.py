from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.substation import Substation, SubstationCreate, SubstationUpdate
from app.services.substation import SubstationService

router = APIRouter()


@router.get("/", response_model=List[Substation])
async def get_substations(
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Get all substations with pagination"""
    return await SubstationService.get_substations(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[Substation])
async def search_substations(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Search substations by name"""
    return await SubstationService.search_substations(db, query=q, skip=skip, limit=limit)


@router.get("/{substation_id}", response_model=Substation)
async def get_substation(substation_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific substation by ID"""
    substation = await SubstationService.get_substation(db, substation_id)
    if not substation:
        raise HTTPException(status_code=404, detail="Substation not found")
    return substation


@router.get("/code/{code}", response_model=Substation)
async def get_substation_by_code(code: str, db: AsyncSession = Depends(get_db)):
    """Get a substation by its code"""
    substation = await SubstationService.get_substation_by_code(db, code)
    if not substation:
        raise HTTPException(status_code=404, detail="Substation not found")
    return substation


@router.post("/", response_model=Substation, status_code=201)
async def create_substation(substation: SubstationCreate, db: AsyncSession = Depends(get_db)):
    """Create a new substation"""
    # Check if substation with same code already exists
    existing_substation = await SubstationService.get_substation_by_code(db, substation.code)
    if existing_substation:
        raise HTTPException(status_code=400, detail="Substation with this code already exists")

    return await SubstationService.create_substation(db, substation)


@router.put("/{substation_id}", response_model=Substation)
async def update_substation(
    substation_id: int, substation_update: SubstationUpdate, db: AsyncSession = Depends(get_db)
):
    """Update a substation"""
    # Check if substation with same code already exists (excluding current substation)
    if substation_update.code:
        existing_substation = await SubstationService.get_substation_by_code(
            db, substation_update.code
        )
        if existing_substation and existing_substation.id != substation_id:
            raise HTTPException(status_code=400, detail="Substation with this code already exists")

    updated_substation = await SubstationService.update_substation(
        db, substation_id, substation_update
    )
    if not updated_substation:
        raise HTTPException(status_code=404, detail="Substation not found")
    return updated_substation


@router.delete("/{substation_id}", response_model=Substation)
async def delete_substation(substation_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a substation"""
    deleted_substation = await SubstationService.delete_substation(db, substation_id)
    if not deleted_substation:
        raise HTTPException(status_code=404, detail="Substation not found")
    return deleted_substation
