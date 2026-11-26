"""Per-user SCADA grant: portal access and an optional farm allowlist (demo / pilot logins).

Before this, the SCADA portal opened for superusers only (client-ui ``canAccessScada``) and every
active user could read any farm's charts by slug. A superuser can also manage users, read audit logs
and open every ``/admin`` route, so a demo or pilot login must NOT be one. Instead a plain client
account carries two kinds of ``user_features`` rows (no new table, no migration):

* ``scada_portal`` = true - the account may open the SCADA and Predict portals.
* ``scada_farm:<slug>`` = true - the farms it may read (e.g. ``scada_farm:hill_of_towie``).

Holding any ``scada_farm:*`` row makes the account *farm-limited*: every ``/scada`` request naming
another farm is a 404 (``enforce_scada_farm_scope``, mounted on each SCADA router in
``app/api/v1/router.py``), the farm list and the all-farm charts are filtered, and the value ledger
opens for its granted farms (``get_ledger_reader``). Superusers are never farm-limited, so staff
behaviour is unchanged. Grant and revoke: ``scripts/create_demo_user.py`` and
``docs/operations/scada-demo-access.md``.
"""

from dataclasses import dataclass
from typing import Any, Iterable, Optional

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.agent_access import is_internal_staff
from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.models.user_feature import UserFeature

SCADA_PORTAL_FEATURE = "scada_portal"
SCADA_FARM_FEATURE_PREFIX = "scada_farm:"

# Every SCADA endpoint that takes ``farm`` defaults it to this slug when the query omits it.
DEFAULT_SCADA_FARM = "hill_of_towie"


@dataclass(frozen=True)
class ScadaGrant:
    """``farms`` is None when the account is not farm-limited."""

    can_access: bool
    farms: Optional[frozenset[str]]


UNRESTRICTED = ScadaGrant(can_access=True, farms=None)
NO_GRANT = ScadaGrant(can_access=False, farms=None)


def grant_from_features(rows: Iterable[tuple[str, bool]]) -> ScadaGrant:
    enabled = {key for key, on in rows if on}
    farms = frozenset(
        key[len(SCADA_FARM_FEATURE_PREFIX) :]
        for key in enabled
        if key.startswith(SCADA_FARM_FEATURE_PREFIX) and len(key) > len(SCADA_FARM_FEATURE_PREFIX)
    )
    return ScadaGrant(can_access=SCADA_PORTAL_FEATURE in enabled, farms=farms or None)


async def load_scada_grant(db: AsyncSession, user: User) -> ScadaGrant:
    """The user's SCADA grant. Superusers skip the query: they are never farm-limited.

    Reads ``user_features`` with an explicit select - lazy-loading ``user.features`` on an async
    session raises MissingGreenlet.
    """
    if user.is_superuser:
        return UNRESTRICTED
    result = await db.execute(
        select(UserFeature.feature_key, UserFeature.enabled).where(
            UserFeature.user_id == user.id,
            (UserFeature.feature_key == SCADA_PORTAL_FEATURE)
            | UserFeature.feature_key.startswith(SCADA_FARM_FEATURE_PREFIX),
        )
    )
    return grant_from_features(result.all())


async def scada_farm_scope(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[frozenset[str]]:
    """The farms the caller may read on ``/scada``, or None for no farm limit."""
    return (await load_scada_grant(db, current_user)).farms


def _route_takes_farm(request: Request) -> bool:
    route = request.scope.get("route")
    dependant = getattr(route, "dependant", None)
    if dependant is None:
        return False
    return any(p.name == "farm" for p in dependant.query_params)


def check_farm(scope: Optional[frozenset[str]], farm: Optional[str]) -> None:
    """404 (not 403) for a farm outside the scope, so a limited login cannot probe other names."""
    if scope is not None and farm is not None and farm not in scope:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Unknown SCADA farm: {farm}"
        )


async def enforce_scada_farm_scope(
    request: Request,
    scope: Optional[frozenset[str]] = Depends(scada_farm_scope),
) -> None:
    """Router-level guard: a farm-limited caller may only name its own farms.

    Covers ``?farm=`` on every route of the router, including the implicit default farm when the
    route takes ``farm`` and the query leaves it out. Farms sent in a request body are checked by
    the endpoint itself (``PUT /scada/opportunities/actions``).
    """
    if scope is None:
        return
    farm = request.query_params.get("farm")
    if farm is None and _route_takes_farm(request):
        farm = DEFAULT_SCADA_FARM
    check_farm(scope, farm)


def filter_rows(rows: list[Any], scope: Optional[frozenset[str]], key: str = "farm") -> list[Any]:
    """Keep the rows of an all-farm payload whose ``key`` is in scope (all rows when unscoped)."""
    if scope is None:
        return rows
    return [r for r in rows if r.get(key) in scope]


async def get_ledger_reader(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Value ledger readers: internal staff, or a farm-limited SCADA grant (for its own farms only;
    ``enforce_scada_farm_scope`` on the ledger router holds it to them)."""
    if is_internal_staff(current_user):
        return current_user
    grant = await load_scada_grant(db, current_user)
    if grant.can_access and grant.farms is not None:
        return current_user
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN, detail="Internal EnergyExe access required"
    )
