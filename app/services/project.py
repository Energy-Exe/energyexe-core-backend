from typing import List, Optional
from sqlalchemy import and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.project import Project
from app.schemas.project import ProjectCreate, ProjectUpdate


class ProjectService:
    
    @staticmethod
    async def get_projects(
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100
    ) -> List[Project]:
        result = await db.execute(
            select(Project)
            .offset(skip)
            .limit(limit)
            .order_by(Project.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def get_project(db: AsyncSession, project_id: int) -> Optional[Project]:
        result = await db.execute(select(Project).where(Project.id == project_id))
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_project_by_code(db: AsyncSession, code: str) -> Optional[Project]:
        result = await db.execute(select(Project).where(Project.code == code))
        return result.scalar_one_or_none()
    
    @staticmethod
    async def search_projects(
        db: AsyncSession,
        query: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Project]:
        search_pattern = f"%{query}%"
        result = await db.execute(
            select(Project)
            .where(
                and_(
                    Project.name.ilike(search_pattern)
                )
            )
            .offset(skip)
            .limit(limit)
            .order_by(Project.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def create_project(db: AsyncSession, project: ProjectCreate) -> Project:
        db_project = Project(**project.model_dump())
        db.add(db_project)
        await db.commit()
        await db.refresh(db_project)
        return db_project
    
    @staticmethod
    async def update_project(
        db: AsyncSession,
        project_id: int,
        project_update: ProjectUpdate
    ) -> Optional[Project]:
        result = await db.execute(select(Project).where(Project.id == project_id))
        db_project = result.scalar_one_or_none()
        
        if not db_project:
            return None
        
        update_data = project_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_project, field, value)
        
        await db.commit()
        await db.refresh(db_project)
        return db_project
    
    @staticmethod
    async def delete_project(db: AsyncSession, project_id: int) -> Optional[Project]:
        result = await db.execute(select(Project).where(Project.id == project_id))
        db_project = result.scalar_one_or_none()
        
        if not db_project:
            return None
        
        await db.delete(db_project)
        await db.commit()
        return db_project