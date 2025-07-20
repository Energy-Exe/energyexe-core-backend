from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.schemas.region import Region, RegionCreate, RegionUpdate
from app.services.region import RegionService

router = APIRouter()


@router.get("/", response_model=List[Region])
async def get_regions(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Get all regions with pagination"""
    return await RegionService.get_regions(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[Region])
async def search_regions(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Search regions by name"""
    return await RegionService.search_regions(db, query=q, skip=skip, limit=limit)


@router.get("/{region_id}", response_model=Region)
async def get_region(
    region_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get a specific region by ID"""
    region = await RegionService.get_region(db, region_id)
    if not region:
        raise HTTPException(status_code=404, detail="Region not found")
    return region


@router.get("/code/{code}", response_model=Region)
async def get_region_by_code(
    code: str,
    db: AsyncSession = Depends(get_db)
):
    """Get a region by its code"""
    region = await RegionService.get_region_by_code(db, code)
    if not region:
        raise HTTPException(status_code=404, detail="Region not found")
    return region


@router.post("/", response_model=Region, status_code=201)
async def create_region(
    region: RegionCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new region"""
    # Check if region with same code already exists
    existing_region = await RegionService.get_region_by_code(db, region.code)
    if existing_region:
        raise HTTPException(
            status_code=400,
            detail="Region with this code already exists"
        )
    
    return await RegionService.create_region(db, region)


@router.put("/{region_id}", response_model=Region)
async def update_region(
    region_id: int,
    region_update: RegionUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update a region"""
    # Check if region with same code already exists (excluding current region)
    if region_update.code:
        existing_region = await RegionService.get_region_by_code(db, region_update.code)
        if existing_region and existing_region.id != region_id:
            raise HTTPException(
                status_code=400,
                detail="Region with this code already exists"
            )
    
    updated_region = await RegionService.update_region(db, region_id, region_update)
    if not updated_region:
        raise HTTPException(status_code=404, detail="Region not found")
    return updated_region


@router.delete("/{region_id}", response_model=Region)
async def delete_region(
    region_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Delete a region"""
    deleted_region = await RegionService.delete_region(db, region_id)
    if not deleted_region:
        raise HTTPException(status_code=404, detail="Region not found")
    return deleted_region