from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.schemas.cable import Cable, CableCreate, CableUpdate
from app.services.cable import CableService

router = APIRouter()


@router.get("/", response_model=List[Cable])
async def get_cables(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Get all cables with pagination"""
    return await CableService.get_cables(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[Cable])
async def search_cables(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Search cables by name"""
    return await CableService.search_cables(db, query=q, skip=skip, limit=limit)


@router.get("/{cable_id}", response_model=Cable)
async def get_cable(
    cable_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get a specific cable by ID"""
    cable = await CableService.get_cable(db, cable_id)
    if not cable:
        raise HTTPException(status_code=404, detail="Cable not found")
    return cable


@router.get("/code/{code}", response_model=Cable)
async def get_cable_by_code(
    code: str,
    db: AsyncSession = Depends(get_db)
):
    """Get a cable by its code"""
    cable = await CableService.get_cable_by_code(db, code)
    if not cable:
        raise HTTPException(status_code=404, detail="Cable not found")
    return cable


@router.post("/", response_model=Cable, status_code=201)
async def create_cable(
    cable: CableCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new cable"""
    # Check if cable with same code already exists
    existing_cable = await CableService.get_cable_by_code(db, cable.code)
    if existing_cable:
        raise HTTPException(
            status_code=400,
            detail="Cable with this code already exists"
        )
    
    return await CableService.create_cable(db, cable)


@router.put("/{cable_id}", response_model=Cable)
async def update_cable(
    cable_id: int,
    cable_update: CableUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update a cable"""
    # Check if cable with same code already exists (excluding current cable)
    if cable_update.code:
        existing_cable = await CableService.get_cable_by_code(db, cable_update.code)
        if existing_cable and existing_cable.id != cable_id:
            raise HTTPException(
                status_code=400,
                detail="Cable with this code already exists"
            )
    
    updated_cable = await CableService.update_cable(db, cable_id, cable_update)
    if not updated_cable:
        raise HTTPException(status_code=404, detail="Cable not found")
    return updated_cable


@router.delete("/{cable_id}", response_model=Cable)
async def delete_cable(
    cable_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Delete a cable"""
    deleted_cable = await CableService.delete_cable(db, cable_id)
    if not deleted_cable:
        raise HTTPException(status_code=404, detail="Cable not found")
    return deleted_cable