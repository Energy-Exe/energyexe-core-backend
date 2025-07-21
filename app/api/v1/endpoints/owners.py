from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.owner import Owner, OwnerCreate, OwnerUpdate
from app.services.owner import OwnerService

router = APIRouter()


@router.get("/", response_model=List[Owner])
async def get_owners(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Get all owners with pagination"""
    return await OwnerService.get_owners(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[Owner])
async def search_owners(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Search owners by name"""
    return await OwnerService.search_owners(db, query=q, skip=skip, limit=limit)


@router.get("/{owner_id}", response_model=Owner)
async def get_owner(owner_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific owner by ID"""
    owner = await OwnerService.get_owner(db, owner_id)
    if not owner:
        raise HTTPException(status_code=404, detail="Owner not found")
    return owner


@router.get("/code/{code}", response_model=Owner)
async def get_owner_by_code(code: str, db: AsyncSession = Depends(get_db)):
    """Get an owner by its code"""
    owner = await OwnerService.get_owner_by_code(db, code)
    if not owner:
        raise HTTPException(status_code=404, detail="Owner not found")
    return owner


@router.post("/", response_model=Owner, status_code=201)
async def create_owner(owner: OwnerCreate, db: AsyncSession = Depends(get_db)):
    """Create a new owner"""
    # Check if owner with same code already exists
    existing_owner = await OwnerService.get_owner_by_code(db, owner.code)
    if existing_owner:
        raise HTTPException(status_code=400, detail="Owner with this code already exists")

    return await OwnerService.create_owner(db, owner)


@router.put("/{owner_id}", response_model=Owner)
async def update_owner(
    owner_id: int, owner_update: OwnerUpdate, db: AsyncSession = Depends(get_db)
):
    """Update an owner"""
    # Check if owner with same code already exists (excluding current owner)
    if owner_update.code:
        existing_owner = await OwnerService.get_owner_by_code(db, owner_update.code)
        if existing_owner and existing_owner.id != owner_id:
            raise HTTPException(status_code=400, detail="Owner with this code already exists")

    updated_owner = await OwnerService.update_owner(db, owner_id, owner_update)
    if not updated_owner:
        raise HTTPException(status_code=404, detail="Owner not found")
    return updated_owner


@router.delete("/{owner_id}", response_model=Owner)
async def delete_owner(owner_id: int, db: AsyncSession = Depends(get_db)):
    """Delete an owner"""
    deleted_owner = await OwnerService.delete_owner(db, owner_id)
    if not deleted_owner:
        raise HTTPException(status_code=404, detail="Owner not found")
    return deleted_owner
