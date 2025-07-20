from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.schemas.windfarm import Windfarm, WindfarmCreate, WindfarmUpdate
from app.services.windfarm import WindfarmService

router = APIRouter()


@router.get("/", response_model=List[Windfarm])
async def get_windfarms(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Get all windfarms with pagination"""
    return await WindfarmService.get_windfarms(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[Windfarm])
async def search_windfarms(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """Search windfarms by name"""
    return await WindfarmService.search_windfarms(db, query=q, skip=skip, limit=limit)


@router.get("/{windfarm_id}", response_model=Windfarm)
async def get_windfarm(
    windfarm_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Get a specific windfarm by ID"""
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")
    return windfarm


@router.get("/code/{code}", response_model=Windfarm)
async def get_windfarm_by_code(
    code: str,
    db: AsyncSession = Depends(get_db)
):
    """Get a windfarm by its code"""
    windfarm = await WindfarmService.get_windfarm_by_code(db, code)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")
    return windfarm


@router.post("/", response_model=Windfarm, status_code=201)
async def create_windfarm(
    windfarm: WindfarmCreate,
    db: AsyncSession = Depends(get_db)
):
    """Create a new windfarm"""
    # Check if windfarm with same code already exists
    existing_windfarm = await WindfarmService.get_windfarm_by_code(db, windfarm.code)
    if existing_windfarm:
        raise HTTPException(
            status_code=400,
            detail="Windfarm with this code already exists"
        )
    
    return await WindfarmService.create_windfarm(db, windfarm)


@router.put("/{windfarm_id}", response_model=Windfarm)
async def update_windfarm(
    windfarm_id: int,
    windfarm_update: WindfarmUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update a windfarm"""
    # Check if windfarm with same code already exists (excluding current windfarm)
    if windfarm_update.code:
        existing_windfarm = await WindfarmService.get_windfarm_by_code(db, windfarm_update.code)
        if existing_windfarm and existing_windfarm.id != windfarm_id:
            raise HTTPException(
                status_code=400,
                detail="Windfarm with this code already exists"
            )
    
    updated_windfarm = await WindfarmService.update_windfarm(db, windfarm_id, windfarm_update)
    if not updated_windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")
    return updated_windfarm


@router.delete("/{windfarm_id}", response_model=Windfarm)
async def delete_windfarm(
    windfarm_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Delete a windfarm"""
    deleted_windfarm = await WindfarmService.delete_windfarm(db, windfarm_id)
    if not deleted_windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")
    return deleted_windfarm