from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.deps import get_current_admin_user, get_current_user
from app.models.user import User
from app.schemas.agent_question_template import (
    AgentQuestionTemplate,
    AgentQuestionTemplateCreate,
    AgentQuestionTemplateUpdate,
)
from app.services.agent_question_template import AgentQuestionTemplateService

router = APIRouter()


@router.get("/", response_model=List[AgentQuestionTemplate])
async def list_templates(
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """List all agent question templates."""
    return await AgentQuestionTemplateService.list_templates(db, skip=skip, limit=limit)


@router.get("/{template_id}", response_model=AgentQuestionTemplate)
async def get_template(
    template_id: int,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    """Get a single agent question template by id."""
    template = await AgentQuestionTemplateService.get_template(db, template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    return template


@router.post("/", response_model=AgentQuestionTemplate, status_code=201)
async def create_template(
    template: AgentQuestionTemplateCreate,
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin_user),
):
    """Create a new agent question template (admin only)."""
    existing = await AgentQuestionTemplateService.get_template_by_route(db, template.route_path)
    if existing:
        raise HTTPException(
            status_code=400, detail="Template with this route_path already exists"
        )
    return await AgentQuestionTemplateService.create_template(db, template)


@router.put("/{template_id}", response_model=AgentQuestionTemplate)
async def update_template(
    template_id: int,
    template_update: AgentQuestionTemplateUpdate,
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin_user),
):
    """Update an agent question template (admin only)."""
    if template_update.route_path:
        existing = await AgentQuestionTemplateService.get_template_by_route(
            db, template_update.route_path
        )
        if existing and existing.id != template_id:
            raise HTTPException(
                status_code=400, detail="Template with this route_path already exists"
            )

    updated = await AgentQuestionTemplateService.update_template(
        db, template_id, template_update
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Template not found")
    return updated


@router.delete("/{template_id}", response_model=AgentQuestionTemplate)
async def delete_template(
    template_id: int,
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin_user),
):
    """Delete an agent question template (admin only)."""
    deleted = await AgentQuestionTemplateService.delete_template(db, template_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Template not found")
    return deleted
