"""API endpoints for Financial Entity management."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import DEFAULT_PAGINATION_LIMIT, MAX_PAGINATION_LIMIT, MIN_PAGINATION_LIMIT
from app.core.database import get_db
from app.schemas.financial_entity import (
    FinancialEntity,
    FinancialEntityCreate,
    FinancialEntityListResponse,
    FinancialEntityUpdate,
    FinancialEntityWithWindfarms,
    WindfarmFinancialEntityLink,
    WindfarmLinkCreate,
)
from app.services.financial_entity_service import FinancialEntityService

router = APIRouter()


@router.get("", response_model=FinancialEntityListResponse)
async def list_entities(
    skip: int = Query(0, ge=0),
    limit: int = Query(DEFAULT_PAGINATION_LIMIT, ge=MIN_PAGINATION_LIMIT, le=MAX_PAGINATION_LIMIT),
    entity_type: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Get all financial entities with pagination."""
    service = FinancialEntityService(db)
    entities, total = await service.get_entities(
        skip=skip, limit=limit, entity_type=entity_type, search=search
    )
    return FinancialEntityListResponse(
        items=entities,
        total=total,
        limit=limit,
        offset=skip,
        has_more=(skip + limit) < total,
    )


@router.get("/{entity_id}", response_model=FinancialEntityWithWindfarms)
async def get_entity(
    entity_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific financial entity by ID with windfarm links."""
    service = FinancialEntityService(db)
    entity = await service.get_entity(entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Financial entity not found")
    return entity


@router.post("", response_model=FinancialEntity, status_code=201)
async def create_entity(
    data: FinancialEntityCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new financial entity."""
    service = FinancialEntityService(db)
    try:
        return await service.create_entity(data)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail="A financial entity with this code already exists",
        )


@router.put("/{entity_id}", response_model=FinancialEntity)
async def update_entity(
    entity_id: int,
    data: FinancialEntityUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update an existing financial entity."""
    service = FinancialEntityService(db)
    try:
        updated = await service.update_entity(entity_id, data)
        if not updated:
            raise HTTPException(status_code=404, detail="Financial entity not found")
        return updated
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail="A financial entity with this code already exists",
        )


@router.delete("/{entity_id}", response_model=FinancialEntity)
async def delete_entity(
    entity_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Delete a financial entity."""
    service = FinancialEntityService(db)
    deleted = await service.delete_entity(entity_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Financial entity not found")
    return deleted


@router.post("/{entity_id}/windfarms", response_model=WindfarmFinancialEntityLink, status_code=201)
async def link_windfarm(
    entity_id: int,
    body: WindfarmLinkCreate,
    db: AsyncSession = Depends(get_db),
):
    """Link a financial entity to a windfarm."""
    service = FinancialEntityService(db)

    # Verify entity exists
    entity = await service.get_entity(entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail="Financial entity not found")

    try:
        return await service.link_windfarm(
            entity_id=entity_id,
            windfarm_id=body.windfarm_id,
            relationship_type=body.relationship_type,
            notes=body.notes,
        )
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail="This windfarm is already linked to this financial entity",
        )


@router.delete("/{entity_id}/windfarms/{windfarm_id}")
async def unlink_windfarm(
    entity_id: int,
    windfarm_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Unlink a financial entity from a windfarm."""
    service = FinancialEntityService(db)
    success = await service.unlink_windfarm(entity_id, windfarm_id)
    if not success:
        raise HTTPException(status_code=404, detail="Link not found")
    return {"detail": "Windfarm unlinked successfully"}
