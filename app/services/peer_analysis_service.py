"""Service for identifying and analyzing peer groups."""

from typing import List, Optional, Dict, Set
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.windfarm import Windfarm
from app.models.windfarm_owner import WindfarmOwner
from app.models.turbine_unit import TurbineUnit
from app.schemas.windfarm_report import PeerGroupInfo


class PeerAnalysisService:
    """Service for detecting and analyzing peer groups for windfarm comparison."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_windfarm_with_relations(self, windfarm_id: int) -> Optional[Windfarm]:
        """Get windfarm with all necessary relationships loaded."""
        stmt = (
            select(Windfarm)
            .options(
                selectinload(Windfarm.bidzone),
                selectinload(Windfarm.country),
                selectinload(Windfarm.windfarm_owners),
                selectinload(Windfarm.turbine_units).selectinload(TurbineUnit.turbine_model)
            )
            .where(Windfarm.id == windfarm_id)
        )
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_all_peer_groups(self, windfarm_id: int) -> Dict[str, PeerGroupInfo]:
        """
        Get all applicable peer groups for a windfarm.

        Returns dict keyed by peer group type: bidzone, country, owner, turbine
        """
        windfarm = await self.get_windfarm_with_relations(windfarm_id)
        if not windfarm:
            return {}

        peer_groups = {}

        # Bidzone peers
        if windfarm.bidzone_id and windfarm.bidzone:
            peer_groups['bidzone'] = await self._get_bidzone_peer_info(
                windfarm.bidzone_id,
                windfarm.bidzone.name,
                windfarm.bidzone.code
            )

        # Country peers (always available)
        if windfarm.country:
            peer_groups['country'] = await self._get_country_peer_info(
                windfarm.country_id,
                windfarm.country.name,
                windfarm.country.code
            )

        # Owner peers (if windfarm has owners)
        if windfarm.windfarm_owners:
            try:
                # Use first/primary owner for now
                from app.models.owner import Owner
                primary_ownership = windfarm.windfarm_owners[0]
                # Load owner separately if not already loaded
                if not primary_ownership.owner:
                    stmt = select(Owner).where(Owner.id == primary_ownership.owner_id)
                    result = await self.db.execute(stmt)
                    primary_owner = result.scalar_one_or_none()
                else:
                    primary_owner = primary_ownership.owner

                if primary_owner:
                    peer_groups['owner'] = await self._get_owner_peer_info(
                        primary_owner.id,
                        primary_owner.name,
                        primary_owner.code
                    )
            except Exception:
                # Skip owner peers if there's an issue
                pass

        # Turbine model peers (if windfarm has turbine units)
        if windfarm.turbine_units:
            # Get most common turbine model
            turbine_model = await self._get_primary_turbine_model(windfarm_id)
            if turbine_model:
                peer_groups['turbine'] = await self._get_turbine_peer_info(
                    turbine_model['id'],
                    turbine_model['name']
                )

        return peer_groups

    async def get_bidzone_peers(self, bidzone_id: int) -> List[int]:
        """Get all windfarm IDs in the same bidzone."""
        stmt = select(Windfarm.id).where(
            Windfarm.bidzone_id == bidzone_id
        )
        result = await self.db.execute(stmt)
        return [row[0] for row in result.all()]

    async def get_country_peers(self, country_id: int) -> List[int]:
        """Get all windfarm IDs in the same country."""
        stmt = select(Windfarm.id).where(
            Windfarm.country_id == country_id
        )
        result = await self.db.execute(stmt)
        return [row[0] for row in result.all()]

    async def get_owner_peers(self, owner_id: int) -> List[int]:
        """Get all windfarm IDs owned by the same owner."""
        stmt = (
            select(WindfarmOwner.windfarm_id)
            .where(WindfarmOwner.owner_id == owner_id)
            .distinct()
        )
        result = await self.db.execute(stmt)
        return [row[0] for row in result.all()]

    async def get_turbine_model_peers(self, turbine_model_id: int) -> List[int]:
        """Get all windfarm IDs using the same turbine model."""
        stmt = (
            select(TurbineUnit.windfarm_id)
            .where(
                and_(
                    TurbineUnit.turbine_model_id == turbine_model_id,
                    TurbineUnit.windfarm_id.isnot(None)
                )
            )
            .distinct()
        )
        result = await self.db.execute(stmt)
        return [row[0] for row in result.all()]

    async def _get_bidzone_peer_info(
        self,
        bidzone_id: int,
        bidzone_name: str,
        bidzone_code: str
    ) -> PeerGroupInfo:
        """Get peer group info for bidzone."""
        total = await self._count_windfarms_in_bidzone(bidzone_id)
        return PeerGroupInfo(
            group_type='bidzone',
            group_id=bidzone_id,
            group_name=bidzone_name,
            group_code=bidzone_code,
            total_windfarms=total
        )

    async def _get_country_peer_info(
        self,
        country_id: int,
        country_name: str,
        country_code: str
    ) -> PeerGroupInfo:
        """Get peer group info for country."""
        total = await self._count_windfarms_in_country(country_id)
        return PeerGroupInfo(
            group_type='country',
            group_id=country_id,
            group_name=country_name,
            group_code=country_code,
            total_windfarms=total
        )

    async def _get_owner_peer_info(
        self,
        owner_id: int,
        owner_name: str,
        owner_code: str
    ) -> PeerGroupInfo:
        """Get peer group info for owner."""
        total = await self._count_windfarms_for_owner(owner_id)
        return PeerGroupInfo(
            group_type='owner',
            group_id=owner_id,
            group_name=owner_name,
            group_code=owner_code,
            total_windfarms=total
        )

    async def _get_turbine_peer_info(
        self,
        turbine_model_id: int,
        turbine_model_name: str
    ) -> PeerGroupInfo:
        """Get peer group info for turbine model."""
        total = await self._count_windfarms_with_turbine_model(turbine_model_id)
        return PeerGroupInfo(
            group_type='turbine',
            group_id=turbine_model_id,
            group_name=turbine_model_name,
            group_code=None,
            total_windfarms=total
        )

    async def _count_windfarms_in_bidzone(self, bidzone_id: int) -> int:
        """Count windfarms in bidzone."""
        stmt = select(func.count(Windfarm.id)).where(
            Windfarm.bidzone_id == bidzone_id
        )
        result = await self.db.execute(stmt)
        return result.scalar_one()

    async def _count_windfarms_in_country(self, country_id: int) -> int:
        """Count windfarms in country."""
        stmt = select(func.count(Windfarm.id)).where(
            Windfarm.country_id == country_id
        )
        result = await self.db.execute(stmt)
        return result.scalar_one()

    async def _count_windfarms_for_owner(self, owner_id: int) -> int:
        """Count windfarms owned by owner."""
        stmt = select(func.count(func.distinct(WindfarmOwner.windfarm_id))).where(
            WindfarmOwner.owner_id == owner_id
        )
        result = await self.db.execute(stmt)
        return result.scalar_one()

    async def _count_windfarms_with_turbine_model(self, turbine_model_id: int) -> int:
        """Count windfarms using turbine model."""
        stmt = select(func.count(func.distinct(TurbineUnit.windfarm_id))).where(
            and_(
                TurbineUnit.turbine_model_id == turbine_model_id,
                TurbineUnit.windfarm_id.isnot(None)
            )
        )
        result = await self.db.execute(stmt)
        return result.scalar_one()

    async def _get_primary_turbine_model(self, windfarm_id: int) -> Optional[Dict]:
        """
        Get the most common turbine model for a windfarm.

        Returns dict with id and name, or None if no turbines.
        """
        stmt = (
            select(
                TurbineUnit.turbine_model_id,
                func.count(TurbineUnit.id).label('count')
            )
            .where(TurbineUnit.windfarm_id == windfarm_id)
            .group_by(TurbineUnit.turbine_model_id)
            .order_by(func.count(TurbineUnit.id).desc())
            .limit(1)
        )
        result = await self.db.execute(stmt)
        row = result.first()

        if not row or not row[0]:
            return None

        # Get turbine model details
        from app.models.turbine_model import TurbineModel
        stmt_model = select(TurbineModel).where(TurbineModel.id == row[0])
        result_model = await self.db.execute(stmt_model)
        model = result_model.scalar_one_or_none()

        if not model:
            return None

        return {
            'id': model.id,
            'name': model.model or f"Model {model.id}"
        }

    async def get_peer_windfarms_summary(
        self,
        peer_group_type: str,
        group_id: int
    ) -> List[Dict]:
        """
        Get basic summary of all windfarms in a peer group.

        Returns list of dicts with id, name, code, bidzone_code, country_code
        """
        if peer_group_type == 'bidzone':
            windfarm_ids = await self.get_bidzone_peers(group_id)
        elif peer_group_type == 'country':
            windfarm_ids = await self.get_country_peers(group_id)
        elif peer_group_type == 'owner':
            windfarm_ids = await self.get_owner_peers(group_id)
        elif peer_group_type == 'turbine':
            windfarm_ids = await self.get_turbine_model_peers(group_id)
        else:
            return []

        if not windfarm_ids:
            return []

        stmt = (
            select(Windfarm)
            .options(
                selectinload(Windfarm.bidzone),
                selectinload(Windfarm.country)
            )
            .where(Windfarm.id.in_(windfarm_ids))
        )
        result = await self.db.execute(stmt)
        windfarms = result.scalars().all()

        return [
            {
                'id': wf.id,
                'name': wf.name,
                'code': wf.code,
                'bidzone_code': wf.bidzone.code if wf.bidzone else None,
                'country_code': wf.country.code if wf.country else None
            }
            for wf in windfarms
        ]
