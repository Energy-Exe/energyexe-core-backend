from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.agent_question_template import AgentQuestionTemplate
from app.schemas.agent_question_template import (
    AgentQuestionTemplateCreate,
    AgentQuestionTemplateUpdate,
)


class AgentQuestionTemplateService:
    @staticmethod
    async def list_templates(
        db: AsyncSession, skip: int = 0, limit: int = 1000
    ) -> List[AgentQuestionTemplate]:
        result = await db.execute(
            select(AgentQuestionTemplate)
            .offset(skip)
            .limit(limit)
            .order_by(AgentQuestionTemplate.route_path.asc())
        )
        return result.scalars().all()

    @staticmethod
    async def get_template(
        db: AsyncSession, template_id: int
    ) -> Optional[AgentQuestionTemplate]:
        result = await db.execute(
            select(AgentQuestionTemplate).where(AgentQuestionTemplate.id == template_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def get_template_by_route(
        db: AsyncSession, route_path: str
    ) -> Optional[AgentQuestionTemplate]:
        result = await db.execute(
            select(AgentQuestionTemplate).where(
                AgentQuestionTemplate.route_path == route_path
            )
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def create_template(
        db: AsyncSession, template: AgentQuestionTemplateCreate
    ) -> AgentQuestionTemplate:
        data = template.model_dump()
        # Pydantic models -> plain dicts for JSONB
        data["questions"] = [q for q in data["questions"]]
        db_template = AgentQuestionTemplate(**data)
        db.add(db_template)
        await db.commit()
        await db.refresh(db_template)
        return db_template

    @staticmethod
    async def update_template(
        db: AsyncSession,
        template_id: int,
        template_update: AgentQuestionTemplateUpdate,
    ) -> Optional[AgentQuestionTemplate]:
        result = await db.execute(
            select(AgentQuestionTemplate).where(AgentQuestionTemplate.id == template_id)
        )
        db_template = result.scalar_one_or_none()
        if not db_template:
            return None

        update_data = template_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_template, field, value)

        await db.commit()
        await db.refresh(db_template)
        return db_template

    @staticmethod
    async def delete_template(
        db: AsyncSession, template_id: int
    ) -> Optional[AgentQuestionTemplate]:
        result = await db.execute(
            select(AgentQuestionTemplate).where(AgentQuestionTemplate.id == template_id)
        )
        db_template = result.scalar_one_or_none()
        if not db_template:
            return None

        await db.delete(db_template)
        await db.commit()
        return db_template
