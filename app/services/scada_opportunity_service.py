"""SCADA opportunities service — read-only over the ``scada.*`` findings lane.

Serves the Revenue-at-Risk register the pipeline persists (energyexe-scada-pipeline
opportunities/persist.py → scada.opportunity_register / opportunity_run /
dim_opportunity_trigger). Same posture as ScadaService: schema-qualified raw SQL on the ordinary
session, no separate engine. The register row's ``trigger`` is LEFT-joined to the catalog dim for
its human name/domain/persona (register rows carry DET_aux / CHANGEPOINT / filename stems that are
not v7 codes, so it is not an FK).

The £ headline sums only the additive class totals (REALIZED + RECOVERABLE + CURTAILMENT); classes
are never cross-summed — CONDITIONAL is a per-event exposure, ALERT/CONTEXT carry no additive £.
"""

from typing import Any, Dict, List, Optional

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

_ADDITIVE_CLASSES = ("REALIZED", "RECOVERABLE", "CURTAILMENT")

# Register row columns returned to the API (dim fields joined on).
_REGISTER_SELECT = """
    r.farm, r.id, r.run_id, r.trigger, r.scope, r.scope_kind, r.item, r.status, r.cls, r.basis,
    r.gbp_year, r.cond_mean_lo, r.cond_mean_hi, r.cond_worst_hi, r.cond_worst_month,
    r.value_of_acting_early, r.additive, r.confidence, r.note, r.now_costing_gbp, r.now_floor_gbp,
    r.now_basis, r.now_available, r.now_confounded, r.rank_gbp,
    t.name AS trigger_name, t.domain, t.action_type, t.persona_primary, t.commercial_upside, t.lead_time,
    fa.status AS lifecycle_status, fa.notes AS lifecycle_notes, fa.acknowledged_at, fa.resolved_at,
    fa.updated_at AS lifecycle_updated_at,
    COALESCE(NULLIF(TRIM(CONCAT_WS(' ', u.first_name, u.last_name)), ''), u.username, u.email) AS lifecycle_actor
"""

# Phase 7b: attach the human lifecycle state (scada_finding_action, a public core table) to each
# register row by the stable natural key. LEFT joins → null lifecycle on an unworked finding. The
# register's nullable trigger is COALESCE(...,'')-d to match the '' the action table normalises to.
_LIFECYCLE_JOIN = """
    LEFT JOIN scada_finding_action fa
      ON fa.farm = r.farm AND fa.trigger = COALESCE(r.trigger, '')
      AND fa.scope = r.scope AND fa.cls = r.cls
    LEFT JOIN users u ON u.id = fa.updated_by
"""

# Once the findings tables are seen they never disappear mid-process; absence is rechecked per call
# so a fresh persist / migration lights the feature up without a backend restart.
_tables_seen = False


async def scada_opportunities_present(db: AsyncSession) -> bool:
    """True when the opportunities findings tables exist in the connected database."""
    global _tables_seen
    if _tables_seen:
        return True
    try:
        result = await db.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'scada' AND table_name = 'opportunity_register' LIMIT 1"
            )
        )
        present = result.scalar() is not None
    except Exception:
        logger.warning("scada_opportunities_check_failed", exc_info=True)
        return False
    if present:
        _tables_seen = True
    return present


class ScadaOpportunityService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def farm_slugs(self) -> List[str]:
        """Farms that have a persisted register (the ones this feature can serve)."""
        result = await self.db.execute(
            text("SELECT DISTINCT farm FROM scada.opportunity_register ORDER BY farm")
        )
        return [row.farm for row in result.fetchall()]

    def _filters(
        self, farm: str, cls, domain, trigger, additive_only, min_gbp
    ) -> tuple[str, Dict[str, Any]]:
        clauses = ["r.farm = :farm"]
        params: Dict[str, Any] = {"farm": farm}
        if cls:
            clauses.append("r.cls = :cls")
            params["cls"] = cls
        if domain:
            clauses.append("t.domain = :domain")
            params["domain"] = domain
        if trigger:
            clauses.append("r.trigger = :trigger")
            params["trigger"] = trigger
        if additive_only:
            clauses.append("r.additive IS TRUE")
        if min_gbp is not None:
            clauses.append("r.rank_gbp >= :min_gbp")
            params["min_gbp"] = min_gbp
        return " AND ".join(clauses), params

    async def list_opportunities(
        self,
        farm: str,
        *,
        cls: Optional[str] = None,
        domain: Optional[str] = None,
        trigger: Optional[str] = None,
        additive_only: bool = False,
        min_gbp: Optional[float] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        where, params = self._filters(farm, cls, domain, trigger, additive_only, min_gbp)
        join = "scada.opportunity_register r LEFT JOIN scada.dim_opportunity_trigger t ON t.code = r.trigger"
        rows = await self.db.execute(
            text(
                f"SELECT {_REGISTER_SELECT} FROM {join} {_LIFECYCLE_JOIN} WHERE {where} "
                "ORDER BY r.rank_gbp DESC, r.id ASC LIMIT :limit OFFSET :offset"
            ),
            {**params, "limit": limit, "offset": offset},
        )
        items = [dict(row._mapping) for row in rows.fetchall()]
        # COUNT stays on the base join — the lifecycle LEFT JOIN is 1:1 so it can't change the count.
        total = (
            await self.db.execute(text(f"SELECT COUNT(*) FROM {join} WHERE {where}"), params)
        ).scalar() or 0
        return {"items": items, "total": int(total), "summary": await self.summary(farm)}

    async def summary(self, farm: str) -> Optional[Dict[str, Any]]:
        """Farm-level register snapshot: run provenance + per-class counts/£ (filter-independent)."""
        run = (
            await self.db.execute(
                text(
                    "SELECT farm, run_id, site_name, generated_at, window_start_year, window_end_year, "
                    "ann_years, realized_gbp_year, recoverable_gbp_year, curtailment_gbp_year, "
                    "headline_gbp_year, n_rows, register_version "
                    "FROM scada.opportunity_run WHERE farm = :farm"
                ),
                {"farm": farm},
            )
        ).fetchone()
        if run is None:
            return None
        by_class_rows = await self.db.execute(
            text(
                "SELECT cls, COUNT(*) AS n, "
                "COALESCE(SUM(gbp_year) FILTER (WHERE additive), 0) AS gbp "
                "FROM scada.opportunity_register WHERE farm = :farm GROUP BY cls ORDER BY cls"
            ),
            {"farm": farm},
        )
        by_class = {
            row.cls: {
                "count": int(row.n),
                "gbp_year": float(row.gbp) if row.gbp is not None else 0.0,
            }
            for row in by_class_rows.fetchall()
        }
        out = dict(run._mapping)
        out["by_class"] = by_class
        return out

    async def get(self, farm: str, id: int) -> Optional[Dict[str, Any]]:
        row = await self.db.execute(
            text(
                f"SELECT {_REGISTER_SELECT} FROM scada.opportunity_register r "
                "LEFT JOIN scada.dim_opportunity_trigger t ON t.code = r.trigger "
                f"{_LIFECYCLE_JOIN} "
                "WHERE r.farm = :farm AND r.id = :id"
            ),
            {"farm": farm, "id": id},
        )
        found = row.fetchone()
        return dict(found._mapping) if found else None

    async def triggers(self) -> List[Dict[str, Any]]:
        rows = await self.db.execute(
            text(
                "SELECT code, name, domain, sub_domain, layer, action_type, persona_primary, "
                "persona_secondary, commercial_upside, confidence, lead_time, schema_status "
                "FROM scada.dim_opportunity_trigger ORDER BY code"
            )
        )
        return [dict(row._mapping) for row in rows.fetchall()]
