"""
Service for managing substation ownership relationships.
"""

from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.substation_owner import SubstationOwner
from app.schemas.substation_owner import SubstationOwnerUpdate


class SubstationOwnerService:
    """Service for substation owner operations"""

    @staticmethod
    async def get_substation_owners(
        db: AsyncSession, substation_id: int
    ) -> List[SubstationOwner]:
        """Get all owners of a substation with owner details"""
        result = await db.execute(
            select(SubstationOwner)
            .where(SubstationOwner.substation_id == substation_id)
            .options(selectinload(SubstationOwner.owner))
        )
        return list(result.scalars().all())

    @staticmethod
    async def create_substation_owners(
        db: AsyncSession, substation_id: int, owners_data: List[dict]
    ) -> List[SubstationOwner]:
        """Create multiple substation owner relationships"""
        owners = []
        for owner_data in owners_data:
            owner = SubstationOwner(
                substation_id=substation_id,
                owner_id=owner_data["owner_id"],
                ownership_percentage=owner_data["ownership_percentage"],
            )
            db.add(owner)
            owners.append(owner)

        await db.commit()
        for owner in owners:
            await db.refresh(owner)

        return owners

    @staticmethod
    async def update_substation_owner(
        db: AsyncSession, owner_id: int, owner_update: SubstationOwnerUpdate
    ) -> Optional[SubstationOwner]:
        """Update a substation owner's ownership percentage"""
        result = await db.execute(
            select(SubstationOwner).where(SubstationOwner.id == owner_id)
        )
        owner = result.scalar_one_or_none()

        if not owner:
            return None

        owner.ownership_percentage = owner_update.ownership_percentage
        await db.commit()
        await db.refresh(owner)
        return owner

    @staticmethod
    async def delete_substation_owner(
        db: AsyncSession, owner_id: int
    ) -> Optional[SubstationOwner]:
        """Delete a substation owner relationship"""
        result = await db.execute(
            select(SubstationOwner).where(SubstationOwner.id == owner_id)
        )
        owner = result.scalar_one_or_none()

        if not owner:
            return None

        await db.delete(owner)
        await db.commit()
        return owner

    @staticmethod
    async def delete_all_substation_owners(db: AsyncSession, substation_id: int):
        """Delete all owners of a substation"""
        result = await db.execute(
            select(SubstationOwner).where(SubstationOwner.substation_id == substation_id)
        )
        owners = result.scalars().all()

        for owner in owners:
            await db.delete(owner)

        await db.commit()
