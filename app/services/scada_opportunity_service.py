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

import json
from typing import Any, Dict, List, Optional

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

_ADDITIVE_CLASSES = ("REALIZED", "RECOVERABLE", "CURTAILMENT")

# Register row columns returned to the API (dim fields joined on). Deploy pipeline migration
# f138a6b7c8d9 before this code: price_basis / offtake_regime are nullable on legacy rows.
_REGISTER_SELECT = """
    r.farm, r.id, r.run_id, r.trigger, r.scope, r.scope_kind, r.item, r.status, r.cls, r.basis,
    r.price_basis, r.offtake_regime,
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

# The interpretation records (pipeline migration a7d3e9c51b20, EPR-182): one row per line, finding and condition
# of the persisted run, in the export form (a reserved-window value is null with withheld = true). The lifecycle
# key a record shows is (farm, action_trigger, action_scope, action_cls): a line's and a condition's own register
# key, a finding's parent line key; actions are never transferred to findings.
_RECORD_SELECT = """
    x.record_kind, x.record_key, x.run_id, x.trigger, x.name, x.scope_kind, x.scope_id, x.parent_line,
    x.cls, x.additive, x.applicability, x.tier, x.magnitude_tier, x.branch, x.status, x.resolved, x.confounded,
    x.value_gbp, x.value_gbp_per_year, x.energy_mwh, x.value_basis, x.withheld,
    x.period_start, x.period_end, x.period_basis, x.issue, x.evidence, x.action_operational,
    x.action_commercial, x.wording_status, x.rank
"""
_RECORD_LIFECYCLE_JOIN = """
    LEFT JOIN scada_finding_action fa
      ON fa.farm = x.farm AND fa.trigger = x.action_trigger
      AND fa.scope = x.action_scope AND fa.cls = x.action_cls
"""
_RECORD_ORDER = (
    "ORDER BY x.rank ASC NULLS LAST, ABS(COALESCE(x.value_gbp_per_year, 0)) DESC, x.record_key ASC"
)

# Once the findings tables are seen they never disappear mid-process; absence is rechecked per call
# so a fresh persist / migration lights the feature up without a backend restart.
_tables_seen = False
_by_year_seen = False
_records_seen = False

_CLASS_SLOT = {"REALIZED": "realized", "RECOVERABLE": "recoverable", "CURTAILMENT": "curtailment"}
_BY_YEAR_SERIES = (("real", "gbp_real"), ("constant_price", "gbp_constant_price"))


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


async def scada_by_year_present(db: AsyncSession) -> bool:
    """True when the per-year tables (pipeline migration a9c3e7f1b2d4, EPR-131) exist here.

    Checked separately from the base register: an environment can carry the v1 register long
    before the by-year rows are materialised, and the client renders that as 'not yet loaded'."""
    global _by_year_seen
    if _by_year_seen:
        return True
    try:
        table = (
            await db.execute(
                text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'scada' AND table_name = 'opportunity_register_by_year' LIMIT 1"
                )
            )
        ).scalar() is not None
        column = (
            await db.execute(
                text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = 'scada' AND table_name = 'opportunity_run' "
                    "AND column_name = 'decade_only_triggers' LIMIT 1"
                )
            )
        ).scalar() is not None
        present = table and column
    except Exception:
        logger.warning("scada_by_year_check_failed", exc_info=True)
        return False
    if present:
        _by_year_seen = True
    return present


async def scada_records_present(db: AsyncSession) -> bool:
    """True when the interpretation tables (pipeline migration a7d3e9c51b20, EPR-182) exist here.

    Checked separately from the base register, like the per-year tables: the client renders absence as
    'not yet published' and never reports it."""
    global _records_seen
    if _records_seen:
        return True
    try:
        present = (
            await db.execute(
                text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'scada' AND table_name = 'opportunity_statement' LIMIT 1"
                )
            )
        ).scalar() is not None
    except Exception:
        logger.warning("scada_records_check_failed", exc_info=True)
        return False
    if present:
        _records_seen = True
    return present


def parse_statement(row: Dict[str, Any]) -> Dict[str, Any]:
    """The statement row with its JSON text parsed (stored as text so the pipeline writes it verbatim)."""
    out = dict(row)
    out["statement"] = json.loads(out.pop("statement_json") or "{}")
    return out


def split_csv(value: Optional[str]) -> List[str]:
    """'OPS_08,OPS_09' -> ['OPS_08', 'OPS_09'] (the run row stores the flat-line list as a comma string)."""
    return [p.strip() for p in (value or "").split(",") if p.strip()]


def summarise_by_year(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Class totals per (series, year), real series first then constant-price, years ascending.

    Done in Python rather than SQL: at most ~165 rows, the SQLite test suite cannot run scada.*
    SQL, and computing it from the same rows the client tabulates means chart, table and CSV
    reconcile by construction. Numeric columns arrive as Decimal -> float(v or 0); a class whose
    lines are all null (e.g. constant-price CURTAILMENT) sums to 0, the row basis says why."""
    acc: Dict[tuple, Dict[str, Any]] = {}
    for r in rows:
        year = int(r["year"])
        slot = _CLASS_SLOT.get(str(r.get("cls")))
        for series, key in _BY_YEAR_SERIES:
            d = acc.setdefault(
                (series, year),
                {
                    "year": year,
                    "series": series,
                    "realized": 0.0,
                    "recoverable": 0.0,
                    "curtailment": 0.0,
                    "total": 0.0,
                    "partial_year": False,
                },
            )
            d["partial_year"] = d["partial_year"] or bool(r.get("partial_year"))
            if slot is None:
                continue
            v = float(r.get(key) or 0)
            d[slot] += v
            d["total"] += v
    order = {series: i for i, (series, _) in enumerate(_BY_YEAR_SERIES)}
    return [acc[k] for k in sorted(acc, key=lambda k: (order[k[0]], k[1]))]


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

    async def by_year(self, farm: str) -> Optional[Dict[str, Any]]:
        """The per-year view (EPR-131): run provenance + (year, trigger) rows + totals + trend.
        None when the farm has no run or no by-year rows (a v1-only persist)."""
        run = (
            await self.db.execute(
                text(
                    "SELECT farm, run_id, generated_at, window_start_year, window_end_year, ann_years, "
                    "headline_gbp_year, price_basis_gbp_mwh, price_basis_note, decade_only_triggers "
                    "FROM scada.opportunity_run WHERE farm = :farm"
                ),
                {"farm": farm},
            )
        ).fetchone()
        if run is None:
            return None
        rows = [
            dict(r._mapping)
            for r in (
                await self.db.execute(
                    text(
                        "SELECT farm, year, trigger, run_id, cls, label, gbp_real, basis_real, "
                        "gbp_constant_price, basis_constant_price, energy_mwh, partial_year, "
                        "recon_status, recon_delta_gbp "
                        "FROM scada.opportunity_register_by_year WHERE farm = :farm "
                        "ORDER BY year, cls, trigger"
                    ),
                    {"farm": farm},
                )
            ).fetchall()
        ]
        if not rows:
            return None
        trend = [
            dict(r._mapping)
            for r in (
                await self.db.execute(
                    text(
                        "SELECT series, cls, n_full_years, slope_gbp_per_year, intercept_gbp, se_slope, "
                        "ci_lo, ci_hi, r2, year_first, year_last, note "
                        "FROM scada.opportunity_by_year_trend WHERE farm = :farm ORDER BY series, cls"
                    ),
                    {"farm": farm},
                )
            ).fetchall()
        ]
        out = dict(run._mapping)
        out["decade_only_triggers"] = split_csv(out.pop("decade_only_triggers", None))
        out["years"] = sorted({int(r["year"]) for r in rows})
        out["rows"] = rows
        out["summary"] = summarise_by_year(rows)
        out["trend"] = trend
        return out

    async def triggers(self) -> List[Dict[str, Any]]:
        rows = await self.db.execute(
            text(
                "SELECT code, name, domain, sub_domain, layer, action_type, persona_primary, "
                "persona_secondary, commercial_upside, confidence, lead_time, schema_status "
                "FROM scada.dim_opportunity_trigger ORDER BY code"
            )
        )
        return [dict(row._mapping) for row in rows.fetchall()]

    async def statement(self, farm: str) -> Optional[Dict[str, Any]]:
        """The asset statement (EPR-181): per value class sums, the overlap adjustment, the reporting floor, the
        ranked findings, the coverage section. None when this farm has none persisted."""
        row = (
            await self.db.execute(
                text(
                    "SELECT farm, run_id, asset_code, currency, statement_json, rendered_md, computed_at "
                    "FROM scada.opportunity_statement WHERE farm = :farm"
                ),
                {"farm": farm},
            )
        ).fetchone()
        return parse_statement(dict(row._mapping)) if row else None

    async def text_for(self, farm: str, id: int, *, limit: int = 50) -> Optional[Dict[str, Any]]:
        """The interpretation text of one register row: a FLEET line's own record and its findings (ranked as the
        statement ranks them), or a turbine row's condition records (one register key can carry several).
        """
        reg = await self.get(farm, id)
        if reg is None:
            return None
        key = {
            "farm": farm,
            "trigger": reg.get("trigger") or "",
            "scope": reg["scope"],
            "cls": reg["cls"],
        }
        match = "x.farm = :farm AND x.action_trigger = :trigger AND x.action_scope = :scope AND x.action_cls = :cls"
        out: Dict[str, Any] = {
            "register_id": id,
            "line": None,
            "findings": [],
            "findings_total": 0,
            "conditions": [],
        }
        if reg["scope"] == "FLEET":
            line = (
                await self.db.execute(
                    text(
                        f"SELECT {_RECORD_SELECT} FROM scada.opportunity_record x "
                        f"WHERE {match} AND x.record_kind = 'line'"
                    ),
                    key,
                )
            ).fetchone()
            out["line"] = dict(line._mapping) if line else None
            rows = await self.db.execute(
                text(
                    f"SELECT {_RECORD_SELECT} FROM scada.opportunity_record x "
                    f"WHERE {match} AND x.record_kind = 'finding' {_RECORD_ORDER} LIMIT :limit"
                ),
                {**key, "limit": limit},
            )
            out["findings"] = [dict(r._mapping) for r in rows.fetchall()]
            out["findings_total"] = int(
                (
                    await self.db.execute(
                        text(
                            f"SELECT COUNT(*) FROM scada.opportunity_record x "
                            f"WHERE {match} AND x.record_kind = 'finding'"
                        ),
                        key,
                    )
                ).scalar()
                or 0
            )
        else:
            rows = await self.db.execute(
                text(
                    f"SELECT {_RECORD_SELECT} FROM scada.opportunity_record x "
                    f"WHERE {match} AND x.record_kind = 'condition' ORDER BY x.record_key"
                ),
                key,
            )
            out["conditions"] = [dict(r._mapping) for r in rows.fetchall()]
        return out

    async def conditions(self, farm: str) -> List[Dict[str, Any]]:
        """Every condition record of the farm with the lifecycle state of its register key."""
        rows = await self.db.execute(
            text(
                f"SELECT {_RECORD_SELECT}, x.action_trigger, x.action_scope, x.action_cls, "
                "fa.status AS lifecycle_status FROM scada.opportunity_record x "
                f"{_RECORD_LIFECYCLE_JOIN} WHERE x.farm = :farm AND x.record_kind = 'condition' "
                "ORDER BY x.cls, x.action_scope, x.record_key"
            ),
            {"farm": farm},
        )
        return [dict(r._mapping) for r in rows.fetchall()]
