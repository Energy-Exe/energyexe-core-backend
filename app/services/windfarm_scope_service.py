"""Windfarm peer-scope filtering shared by the portfolio analytics endpoints.

The client portal's Performance section scopes analytics to a "peer group" of
windfarms (location type + country + bidzone + capacity range). This module is
the single place that turns those filters — plus the legacy portfolio_id /
country_id params — into the windfarm-id list the per-table queries consume.

Semantics of the resolved id list:
- ``None``  -> no filters were given; queries run unrestricted (legacy behavior)
- ``[]``    -> filters were given but matched nothing; endpoints must return
               their empty payload instead of silently dropping the filter
               (the old inline blocks skipped the condition on empty matches,
               which returned the unfiltered global dataset)
"""

from typing import List, Optional

from fastapi import Query
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bidzone import Bidzone
from app.models.country import Country
from app.models.portfolio import PortfolioItem
from app.models.windfarm import Windfarm


class PeerScopeParams:
    """Peer-group query params, declared once and injected via Depends().

    ``country_id`` predates this class (the portfolio endpoints already accepted
    it) and stays on the wire unchanged; the code-based variants exist so the
    client can pass the same values it puts in its URLs (ISO3 / bidzone code).
    """

    def __init__(
        self,
        location_type: Optional[str] = Query(
            None, regex="^(onshore|offshore)$", description="Filter by windfarm location type"
        ),
        country_id: Optional[int] = Query(None, description="Filter by country ID"),
        country_code: Optional[str] = Query(
            None,
            min_length=3,
            max_length=3,
            description="Filter by ISO 3166-1 alpha-3 country code",
        ),
        bidzone_id: Optional[int] = Query(None, description="Filter by bidzone ID"),
        bidzone_code: Optional[str] = Query(None, description="Filter by bidzone code"),
        capacity_min: Optional[float] = Query(
            None, ge=0, description="Minimum nameplate capacity (MW), inclusive"
        ),
        capacity_max: Optional[float] = Query(
            None, ge=0, description="Maximum nameplate capacity (MW), inclusive"
        ),
    ):
        self.location_type = location_type
        self.country_id = country_id
        self.country_code = country_code
        self.bidzone_id = bidzone_id
        self.bidzone_code = bidzone_code
        self.capacity_min = capacity_min
        self.capacity_max = capacity_max

    def has_filters(self) -> bool:
        return any(
            v is not None
            for v in (
                self.location_type,
                self.country_id,
                self.country_code,
                self.bidzone_id,
                self.bidzone_code,
                self.capacity_min,
                self.capacity_max,
            )
        )


# Peer/portfolio analytics queries join generation_data (~25M rows) against
# price_data (~44M rows) at hourly grain over a year-wide, ~100+ farm scope. At
# the default work_mem (4MB) the generation-side bitmap spills to *lossy* mode and
# the planner abandons the index path for a full sequential scan of both tables —
# turning a ~4s query into a 60s+ timeout. Raising work_mem for just this
# statement keeps the bitmap exact and the hash joins in memory. SET LOCAL is
# scoped to the current transaction and auto-resets when the request's connection
# returns to the pool, so it never leaks to other queries.
ANALYTICS_WORK_MEM = "128MB"


async def apply_analytics_work_mem(db: AsyncSession) -> None:
    """Raise work_mem for the current request's heavy analytics query.

    Must run inside the request transaction (it is — every analytics endpoint
    authenticates first, which opens the transaction). The value is a hardcoded
    constant, so the literal interpolation below carries no injection risk; SET
    does not accept bind parameters for its value.
    """
    await db.execute(text(f"SET LOCAL work_mem = '{ANALYTICS_WORK_MEM}'"))


def windfarm_ids_sql_array(ids: List[int]) -> str:
    """Render windfarm ids as an inline ``int[]`` literal for raw-SQL filters.

    The portfolio analytics queries filter on ``windfarm_id = ANY(<this>)``.
    Passing the ids as a *bound* array parameter hides their cardinality from the
    planner, which then estimates a single row, picks a nested-loop plan that
    probes generation_data ~1M times, and blows past the 60s guard. Inlining the
    ids as a literal lets the planner see the real selectivity and choose the
    bitmap/hash plan instead (seconds, not minutes).

    ids come straight from ``resolve_windfarm_scope_ids`` (our own DB) and each is
    coerced through ``int()``, so there is no SQL-injection surface.
    """
    return "ARRAY[" + ",".join(str(int(i)) for i in ids) + "]::integer[]"


def build_windfarm_scope_conditions(scope: PeerScopeParams) -> list:
    """SQLAlchemy conditions on Windfarm for the given scope filters.

    Always excludes soft-deleted windfarms — these endpoints are client-facing
    and ghost farms must not contribute to analytics.
    """
    conditions = [Windfarm.is_deleted == False]  # noqa: E712 — SQLAlchemy comparison
    if scope.location_type:
        conditions.append(Windfarm.location_type == scope.location_type)
    if scope.country_id:
        conditions.append(Windfarm.country_id == scope.country_id)
    if scope.country_code:
        conditions.append(
            Windfarm.country_id.in_(
                select(Country.id).where(Country.code == scope.country_code.upper())
            )
        )
    if scope.bidzone_id:
        conditions.append(Windfarm.bidzone_id == scope.bidzone_id)
    if scope.bidzone_code:
        conditions.append(
            Windfarm.bidzone_id.in_(select(Bidzone.id).where(Bidzone.code == scope.bidzone_code))
        )
    if scope.capacity_min is not None:
        conditions.append(Windfarm.nameplate_capacity_mw >= scope.capacity_min)
    if scope.capacity_max is not None:
        conditions.append(Windfarm.nameplate_capacity_mw <= scope.capacity_max)
    return conditions


async def resolve_windfarm_scope_ids(
    db: AsyncSession,
    *,
    portfolio_id: Optional[int] = None,
    scope: Optional[PeerScopeParams] = None,
) -> Optional[List[int]]:
    """Resolve the windfarm ids a portfolio analytics query is scoped to.

    Returns None when no filters are given (unrestricted), otherwise the
    matching ids — possibly empty, in which case callers must return their
    empty payload rather than run unfiltered.
    """
    has_scope = scope is not None and scope.has_filters()

    if not portfolio_id and not has_scope:
        return None

    if portfolio_id and not has_scope:
        # Legacy portfolio-only path: the raw portfolio item list, exactly as
        # the endpoints resolved it before this helper existed.
        result = await db.execute(
            select(PortfolioItem.windfarm_id).where(PortfolioItem.portfolio_id == portfolio_id)
        )
        return [row[0] for row in result.fetchall()]

    query = select(Windfarm.id).where(*build_windfarm_scope_conditions(scope))
    if portfolio_id:
        query = query.where(
            Windfarm.id.in_(
                select(PortfolioItem.windfarm_id).where(PortfolioItem.portfolio_id == portfolio_id)
            )
        )
    result = await db.execute(query)
    return [row[0] for row in result.fetchall()]
