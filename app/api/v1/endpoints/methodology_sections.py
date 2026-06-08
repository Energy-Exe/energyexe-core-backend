from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_admin_user, get_current_user
from app.models.user import User
from app.schemas.methodology_section import (
    MethodologySection,
    MethodologySectionCreate,
    MethodologySectionUpdate,
)
from app.services.methodology_section import MethodologySectionService

router = APIRouter()


@router.get("/", response_model=List[MethodologySection])
async def list_sections(
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """List all methodology sections ordered by sort_order."""
    return await MethodologySectionService.list_sections(db)


@router.get("/{section_id}", response_model=MethodologySection)
async def get_section(
    section_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Get a single methodology section by id."""
    section = await MethodologySectionService.get_section(db, section_id)
    if not section:
        raise HTTPException(status_code=404, detail="Section not found")
    return section


@router.post("/", response_model=MethodologySection, status_code=201)
async def create_section(
    section: MethodologySectionCreate,
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin_user),
):
    """Create a new methodology section (admin only)."""
    existing = await MethodologySectionService.get_section_by_key(db, section.section_key)
    if existing:
        raise HTTPException(
            status_code=400, detail="Section with this section_key already exists"
        )
    return await MethodologySectionService.create_section(db, section)


@router.put("/{section_id}", response_model=MethodologySection)
async def update_section(
    section_id: int,
    section_update: MethodologySectionUpdate,
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin_user),
):
    """Update a methodology section (admin only)."""
    if section_update.section_key:
        existing = await MethodologySectionService.get_section_by_key(
            db, section_update.section_key
        )
        if existing and existing.id != section_id:
            raise HTTPException(
                status_code=400, detail="Section with this section_key already exists"
            )

    updated = await MethodologySectionService.update_section(db, section_id, section_update)
    if not updated:
        raise HTTPException(status_code=404, detail="Section not found")
    return updated


@router.delete("/{section_id}", response_model=MethodologySection)
async def delete_section(
    section_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin_user),
):
    """Delete a methodology section (admin only)."""
    deleted = await MethodologySectionService.delete_section(db, section_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Section not found")
    return deleted
