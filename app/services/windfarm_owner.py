from typing import List, Optional
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from app.models.windfarm import Windfarm
from app.models.windfarm_owner import WindfarmOwner
from app.schemas.windfarm_owner import WindfarmOwnerCreate, WindfarmOwnerUpdate


class WindfarmOwnerService:
    @staticmethod
    async def get_windfarm_owners(
        db: AsyncSession, windfarm_id: int
    ) -> List[WindfarmOwner]:
        result = await db.execute(
            select(WindfarmOwner)
            .where(WindfarmOwner.windfarm_id == windfarm_id)
            .options(selectinload(WindfarmOwner.owner))
        )
        return result.scalars().all()

    @staticmethod
    async def create_windfarm_owner(
        db: AsyncSession, windfarm_owner: WindfarmOwnerCreate
    ) -> WindfarmOwner:
        db_windfarm_owner = WindfarmOwner(**windfarm_owner.model_dump())
        db.add(db_windfarm_owner)
        await db.commit()
        await db.refresh(db_windfarm_owner)
        return db_windfarm_owner

    @staticmethod
    async def create_windfarm_owners(
        db: AsyncSession, windfarm_id: int, owners_data: List[dict]
    ) -> List[WindfarmOwner]:
        """Create multiple owners for a windfarm"""
        windfarm_owners = []
        
        for owner_data in owners_data:
            db_windfarm_owner = WindfarmOwner(
                windfarm_id=windfarm_id,
                owner_id=owner_data["owner_id"],
                ownership_percentage=Decimal(str(owner_data["ownership_percentage"]))
            )
            db.add(db_windfarm_owner)
            windfarm_owners.append(db_windfarm_owner)
        
        await db.commit()
        
        for owner in windfarm_owners:
            await db.refresh(owner)
        
        return windfarm_owners

    @staticmethod
    async def update_windfarm_owner(
        db: AsyncSession, windfarm_owner_id: int, windfarm_owner_update: WindfarmOwnerUpdate
    ) -> Optional[WindfarmOwner]:
        result = await db.execute(
            select(WindfarmOwner).where(WindfarmOwner.id == windfarm_owner_id)
        )
        db_windfarm_owner = result.scalar_one_or_none()

        if not db_windfarm_owner:
            return None

        update_data = windfarm_owner_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_windfarm_owner, field, value)

        await db.commit()
        await db.refresh(db_windfarm_owner)
        return db_windfarm_owner

    @staticmethod
    async def delete_windfarm_owner(
        db: AsyncSession, windfarm_owner_id: int
    ) -> Optional[WindfarmOwner]:
        result = await db.execute(
            select(WindfarmOwner).where(WindfarmOwner.id == windfarm_owner_id)
        )
        db_windfarm_owner = result.scalar_one_or_none()

        if not db_windfarm_owner:
            return None

        await db.delete(db_windfarm_owner)
        await db.commit()
        return db_windfarm_owner

    @staticmethod
    async def delete_all_windfarm_owners(
        db: AsyncSession, windfarm_id: int
    ) -> None:
        """Delete all owners for a windfarm"""
        result = await db.execute(
            select(WindfarmOwner).where(WindfarmOwner.windfarm_id == windfarm_id)
        )
        owners = result.scalars().all()
        
        for owner in owners:
            await db.delete(owner)
        
        await db.commit()

    @staticmethod
    async def validate_ownership_percentages(
        owners_data: List[dict]
    ) -> bool:
        """Validate that ownership percentages sum to 100%"""
        total_percentage = sum(
            Decimal(str(owner["ownership_percentage"])) for owner in owners_data
        )
        return total_percentage == Decimal("100.00")