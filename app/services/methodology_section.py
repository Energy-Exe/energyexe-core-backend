from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.methodology_section import MethodologySection
from app.schemas.methodology_section import (
    MethodologySectionCreate,
    MethodologySectionUpdate,
)


class MethodologySectionService:
    @staticmethod
    async def list_sections(db: AsyncSession) -> List[MethodologySection]:
        result = await db.execute(
            select(MethodologySection).order_by(
                MethodologySection.sort_order.asc(), MethodologySection.id.asc()
            )
        )
        return result.scalars().all()

    @staticmethod
    async def get_section(db: AsyncSession, section_id: int) -> Optional[MethodologySection]:
        result = await db.execute(
            select(MethodologySection).where(MethodologySection.id == section_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def get_section_by_key(
        db: AsyncSession, section_key: str
    ) -> Optional[MethodologySection]:
        result = await db.execute(
            select(MethodologySection).where(MethodologySection.section_key == section_key)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def create_section(
        db: AsyncSession, section: MethodologySectionCreate
    ) -> MethodologySection:
        db_section = MethodologySection(**section.model_dump())
        db.add(db_section)
        await db.commit()
        await db.refresh(db_section)
        return db_section

    @staticmethod
    async def update_section(
        db: AsyncSession,
        section_id: int,
        section_update: MethodologySectionUpdate,
    ) -> Optional[MethodologySection]:
        result = await db.execute(
            select(MethodologySection).where(MethodologySection.id == section_id)
        )
        db_section = result.scalar_one_or_none()
        if not db_section:
            return None

        update_data = section_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_section, field, value)

        await db.commit()
        await db.refresh(db_section)
        return db_section

    @staticmethod
    async def delete_section(
        db: AsyncSession, section_id: int
    ) -> Optional[MethodologySection]:
        result = await db.execute(
            select(MethodologySection).where(MethodologySection.id == section_id)
        )
        db_section = result.scalar_one_or_none()
        if not db_section:
            return None

        await db.delete(db_section)
        await db.commit()
        return db_section

    @staticmethod
    async def compose_markdown(db: AsyncSession) -> str:
        """Compose all sections into one markdown document (for the brain-agent
        sandbox skill file). Returns an empty string when no sections exist."""
        sections = await MethodologySectionService.list_sections(db)
        if not sections:
            return ""
        parts = ["# Platform Methodology"]
        for s in sections:
            parts.append(f"## {s.title}")
            if s.description:
                parts.append(f"*{s.description}*")
            parts.append(s.content_md.strip())
        return "\n\n".join(parts) + "\n"
