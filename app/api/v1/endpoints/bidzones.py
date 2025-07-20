from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.schemas.bidzone import Bidzone, BidzoneCreate, BidzoneUpdate
from app.services.bidzone import BidzoneService

router = APIRouter()


@router.get("/", response_model=List[Bidzone])
async def get_bidzones(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Get all bidzones with pagination"""
    return await BidzoneService.get_bidzones(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[Bidzone])
async def search_bidzones(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Search bidzones by name"""
    return await BidzoneService.search_bidzones(db, query=q, skip=skip, limit=limit)


@router.get("/{bidzone_id}", response_model=Bidzone)
async def get_bidzone(
    bidzone_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get a specific bidzone by ID"""
    bidzone = await BidzoneService.get_bidzone(db, bidzone_id)
    if not bidzone:
        raise HTTPException(status_code=404, detail="Bidzone not found")
    return bidzone


@router.get("/code/{code}", response_model=Bidzone)
async def get_bidzone_by_code(
    code: str,
    db: AsyncSession = Depends(get_db)
):
    """Get a bidzone by its code"""
    bidzone = await BidzoneService.get_bidzone_by_code(db, code)
    if not bidzone:
        raise HTTPException(status_code=404, detail="Bidzone not found")
    return bidzone


@router.post("/", response_model=Bidzone, status_code=201)
async def create_bidzone(
    bidzone: BidzoneCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new bidzone"""
    # Check if bidzone with same code already exists
    existing_bidzone = await BidzoneService.get_bidzone_by_code(db, bidzone.code)
    if existing_bidzone:
        raise HTTPException(
            status_code=400,
            detail="Bidzone with this code already exists"
        )
    
    return await BidzoneService.create_bidzone(db, bidzone)


@router.put("/{bidzone_id}", response_model=Bidzone)
async def update_bidzone(
    bidzone_id: int,
    bidzone_update: BidzoneUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update a bidzone"""
    # Check if bidzone with same code already exists (excluding current bidzone)
    if bidzone_update.code:
        existing_bidzone = await BidzoneService.get_bidzone_by_code(db, bidzone_update.code)
        if existing_bidzone and existing_bidzone.id != bidzone_id:
            raise HTTPException(
                status_code=400,
                detail="Bidzone with this code already exists"
            )
    
    updated_bidzone = await BidzoneService.update_bidzone(db, bidzone_id, bidzone_update)
    if not updated_bidzone:
        raise HTTPException(status_code=404, detail="Bidzone not found")
    return updated_bidzone


@router.delete("/{bidzone_id}", response_model=Bidzone)
async def delete_bidzone(
    bidzone_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Delete a bidzone"""
    deleted_bidzone = await BidzoneService.delete_bidzone(db, bidzone_id)
    if not deleted_bidzone:
        raise HTTPException(status_code=404, detail="Bidzone not found")
    return deleted_bidzone