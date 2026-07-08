from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_admin_user, get_current_user
from app.models.user import User
from app.schemas.platform_update import (
    PlatformUpdate,
    PlatformUpdateCreate,
    PlatformUpdateUpdate,
)
from app.services.platform_update import PlatformUpdateService

router = APIRouter()


@router.get("/", response_model=List[PlatformUpdate])
async def list_updates(
    active_only: bool = Query(False, description="Only return active updates"),
    limit: Optional[int] = Query(None, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """List platform updates, newest published first."""
    return await PlatformUpdateService.list_updates(db, active_only=active_only, limit=limit)


@router.get("/{update_id}", response_model=PlatformUpdate)
async def get_update(
    update_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Get a single platform update by id."""
    update = await PlatformUpdateService.get_update(db, update_id)
    if not update:
        raise HTTPException(status_code=404, detail="Platform update not found")
    return update


@router.post("/", response_model=PlatformUpdate, status_code=201)
async def create_update(
    update: PlatformUpdateCreate,
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin_user),
):
    """Create a new platform update (admin only)."""
    return await PlatformUpdateService.create_update(db, update)


@router.put("/{update_id}", response_model=PlatformUpdate)
async def update_update(
    update_id: int,
    update_data: PlatformUpdateUpdate,
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin_user),
):
    """Update a platform update (admin only). Partial — also serves the active toggle."""
    updated = await PlatformUpdateService.update_update(db, update_id, update_data)
    if not updated:
        raise HTTPException(status_code=404, detail="Platform update not found")
    return updated


@router.delete("/{update_id}", response_model=PlatformUpdate)
async def delete_update(
    update_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin_user),
):
    """Delete a platform update (admin only)."""
    deleted = await PlatformUpdateService.delete_update(db, update_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Platform update not found")
    return deleted
