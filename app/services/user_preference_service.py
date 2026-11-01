"""Per-user saved settings (EPR-202). Only allowlisted keys, each with its own validator."""

from datetime import datetime
from typing import Any, Callable, Dict, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user_preference import UserPreference
from app.services.scada_degradation_service import HORIZON_MAX, HORIZON_MIN


class UnknownPreference(Exception):
    pass


class InvalidPreference(Exception):
    pass


def _horizon(value: Any) -> int:
    # bool is an int subclass; a JSON true must not pass as 1.
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidPreference("The horizon must be a whole year")
    if not HORIZON_MIN <= value <= HORIZON_MAX:
        raise InvalidPreference(f"The horizon must be between {HORIZON_MIN} and {HORIZON_MAX}")
    return value


VALIDATORS: Dict[str, Callable[[Any], Any]] = {"scada_degradation_horizon": _horizon}


def validate_key(key: str) -> None:
    if key not in VALIDATORS:
        raise UnknownPreference(key)


class UserPreferenceService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get(self, user_id: int, key: str) -> Optional[Any]:
        validate_key(key)
        result = await self.db.execute(
            select(UserPreference.value).where(
                UserPreference.user_id == user_id, UserPreference.key == key
            )
        )
        return result.scalar_one_or_none()

    async def set(self, user_id: int, key: str, value: Any) -> Any:
        validate_key(key)
        clean = VALIDATORS[key](value)
        row = await self.db.get(UserPreference, (user_id, key))
        if row is None:
            self.db.add(UserPreference(user_id=user_id, key=key, value=clean))
        else:
            row.value = clean
            row.updated_at = datetime.utcnow()
        await self.db.commit()
        return clean

    async def clear(self, user_id: int, key: str) -> None:
        validate_key(key)
        await self.db.execute(
            delete(UserPreference).where(
                UserPreference.user_id == user_id, UserPreference.key == key
            )
        )
        await self.db.commit()
