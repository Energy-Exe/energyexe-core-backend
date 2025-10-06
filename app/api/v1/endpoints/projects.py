from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.project import Project, ProjectCreate, ProjectUpdate
from app.services.project import ProjectService

router = APIRouter()


@router.get("/", response_model=List[Project])
async def get_projects(
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Get all projects with pagination"""
    return await ProjectService.get_projects(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[Project])
async def search_projects(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Search projects by name"""
    return await ProjectService.search_projects(db, query=q, skip=skip, limit=limit)


@router.get("/{project_id}", response_model=Project)
async def get_project(project_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific project by ID"""
    project = await ProjectService.get_project(db, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.get("/code/{code}", response_model=Project)
async def get_project_by_code(code: str, db: AsyncSession = Depends(get_db)):
    """Get a project by its code"""
    project = await ProjectService.get_project_by_code(db, code)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.post("/", response_model=Project, status_code=201)
async def create_project(project: ProjectCreate, db: AsyncSession = Depends(get_db)):
    """Create a new project"""
    # Check if project with same code already exists
    existing_project = await ProjectService.get_project_by_code(db, project.code)
    if existing_project:
        raise HTTPException(status_code=400, detail="Project with this code already exists")

    return await ProjectService.create_project(db, project)


@router.put("/{project_id}", response_model=Project)
async def update_project(
    project_id: int, project_update: ProjectUpdate, db: AsyncSession = Depends(get_db)
):
    """Update a project"""
    # Check if project with same code already exists (excluding current project)
    if project_update.code:
        existing_project = await ProjectService.get_project_by_code(db, project_update.code)
        if existing_project and existing_project.id != project_id:
            raise HTTPException(status_code=400, detail="Project with this code already exists")

    updated_project = await ProjectService.update_project(db, project_id, project_update)
    if not updated_project:
        raise HTTPException(status_code=404, detail="Project not found")
    return updated_project


@router.delete("/{project_id}", response_model=Project)
async def delete_project(project_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a project"""
    deleted_project = await ProjectService.delete_project(db, project_id)
    if not deleted_project:
        raise HTTPException(status_code=404, detail="Project not found")
    return deleted_project
