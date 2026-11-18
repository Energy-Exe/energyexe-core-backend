"""SCADA value ledger service: read-only over ``scada.opportunity_ledger_run`` and ``opportunity_ledger_cause``.

The pipeline writes both tables (``scada opportunities ledger-persist``); the format is energyexe-scada-pipeline
``docs/reference/value-ledger-format.md`` (version 2). Same posture as ScadaOpportunityService: schema-qualified
raw SQL on the ordinary session, behind a presence check.

Read rule: an asset's ledger is served only while the register run it was built beside (``register_run_id``) is
the register's current run for that farm and asset (``scada.opportunity_run``) and its rows are there. Otherwise
it is stale: the API says so and serves no document. Finding statuses are joined on read, never stored.
"""

import json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

FORMAT = "value-ledger/2"
FLEET = "FLEET"
JSON_FIELDS = (
    "years_detail",
    "turbines",
    "bridge",
    "bridge_basis",
    "ranges",
    "coverage",
    "checks",
    "lines",
    "realised",
    "catalog",
)
_NOT_RUN = (
    "pipeline_version",
    "computed_at",
    "current_register_run_id",
    "has_rows",
    "register_window_start_year",
    "register_window_end_year",
    "register_ann_years",
)

# One statement: the ledger run rows, the register's current run for the same (farm, asset) with its years and
# what it averages over (the portal's first bridge row, rollout Stage 8), and whether the ledger run's cause rows
# are there.
_RUNS_SQL = """
    SELECT l.*, o.run_id AS current_register_run_id,
           o.window_start_year AS register_window_start_year, o.window_end_year AS register_window_end_year,
           o.ann_years AS register_ann_years,
           EXISTS (SELECT 1 FROM scada.opportunity_ledger_cause c
                   WHERE c.farm = l.farm AND c.asset_code = l.asset_code AND c.run_id = l.run_id) AS has_rows
    FROM scada.opportunity_ledger_run l
    LEFT JOIN scada.opportunity_run o ON o.farm = l.farm AND o.asset_code = l.asset_code
    WHERE l.farm = :farm {asset}
    ORDER BY l.asset_code
"""
_ROWS_SQL = """
    SELECT year, turbine, cause, value, claimed_mwh, claims, summed
    FROM scada.opportunity_ledger_cause
    WHERE farm = :farm AND asset_code = :asset AND run_id = :run_id
"""
# The asset's finding statuses; the actor as ScadaOpportunityService derives it.
_STATUSES_SQL = """
    SELECT fa.trigger, fa.scope, fa.cls, fa.status, fa.notes, fa.acknowledged_at, fa.resolved_at, fa.updated_at,
           COALESCE(NULLIF(TRIM(CONCAT_WS(' ', u.first_name, u.last_name)), ''), u.username, u.email) AS actor
    FROM scada_finding_action fa
    LEFT JOIN users u ON u.id = fa.updated_by
    WHERE fa.farm = :farm AND fa.asset_code = :asset
"""

# Once the tables are seen they never disappear mid-process; absence is rechecked per call so a migration
# lights the feature up without a restart.
_tables_seen = False


async def scada_ledger_present(db: AsyncSession) -> bool:
    """True when both value-ledger tables exist in the connected database."""
    global _tables_seen
    if _tables_seen:
        return True
    try:
        n = (
            await db.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'scada' "
                    "AND table_name IN ('opportunity_ledger_run', 'opportunity_ledger_cause')"
                )
            )
        ).scalar()
    except Exception:
        logger.warning("scada_ledger_check_failed", exc_info=True)
        return False
    _tables_seen = n == 2
    return _tables_seen


def parse_run(row: Mapping[str, Any]) -> Dict[str, Any]:
    """A stored run row as the format's run row: JSON text parsed, dates as ISO strings."""
    run = {k: v for k, v in row.items() if k not in _NOT_RUN}
    for k in JSON_FIELDS:
        run[k] = json.loads(run[k])
    for k in ("window_from", "window_to"):
        run[k] = run[k].isoformat()
    return run


def register_basis(row: Mapping[str, Any]) -> Optional[Dict[str, int]]:
    """The register's current run: its first and last year and the years its figures are averaged over (today a
    fixed ten). None when the asset has no register run."""
    if row.get("current_register_run_id") is None:
        return None
    return {
        "window_start_year": int(row["register_window_start_year"]),
        "window_end_year": int(row["register_window_end_year"]),
        "ann_years": int(row["register_ann_years"]),
    }


def is_stale(row: Mapping[str, Any]) -> bool:
    return row["current_register_run_id"] != row["register_run_id"] or not row["has_rows"]


def status_keys(catalog: Mapping[str, Any]) -> Set[Tuple[str, str, str]]:
    """The (trigger, scope, cls) keys a run's causes hang their statuses on."""
    keys = (c["status_key"] for c in catalog["causes"])
    return {(k["trigger"], k["scope"], k["cls"]) for k in keys}


def _total(values: Iterable[Optional[float]]) -> Optional[float]:
    """The sum of the non-null values; null when every value is null."""
    xs = [v for v in values if v is not None]
    return sum(xs) if xs else None


def _grouped(rows: List[Dict[str, Any]], key) -> List[Tuple[Any, List[Dict[str, Any]]]]:
    out: Dict[Any, List[Dict[str, Any]]] = {}
    for r in rows:
        k = key(r)
        if k is not None:
            out.setdefault(k, []).append(r)
    return sorted(out.items(), key=lambda kv: kv[0])


def build_document(rows: List[Dict[str, Any]], run: Dict[str, Any]) -> Dict[str, Any]:
    """The document, from the two tables alone (the run row carries its cause list). A port of the pipeline's
    ``ledger.format.document``."""
    years, cat = run["years"], run["catalog"]
    by_year = [
        {
            "year": y,
            "cause": c,
            "value": sum(r["value"] for r in g),
            "claimed_mwh": _total(r["claimed_mwh"] for r in g),
            "claims": _total(r["claims"] for r in g),
        }
        for (y, c), g in _grouped(rows, lambda r: (r["year"], r["cause"]))
    ]
    by_cause = []
    for c, g in _grouped(rows, lambda r: r["cause"]):
        mwh = _total(r["claimed_mwh"] for r in g)
        by_cause.append(
            {
                "cause": c,
                "value_year": sum(r["value"] for r in g) / years,
                "claimed_mwh_year": None if mwh is None else mwh / years,
            }
        )
    by_turbine = [
        {"turbine": t, "cause": c, "value_year": sum(r["value"] for r in g) / years}
        for (t, c), g in _grouped(
            rows, lambda r: None if r["turbine"] == FLEET else (r["turbine"], r["cause"])
        )
    ]
    return {
        "format": FORMAT,
        "run": {k: v for k, v in run.items() if k != "catalog"},
        "groups": cat["groups"],
        "causes": cat["causes"],
        "by_cause": by_cause,
        "by_year": by_year,
        "by_turbine": by_turbine,
    }


class ScadaLedgerService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _runs(self, farm: str, asset: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = _RUNS_SQL.format(asset="AND l.asset_code = :asset" if asset else "")
        params = {"farm": farm, **({"asset": asset} if asset else {})}
        return [dict(r._mapping) for r in await self.db.execute(text(sql), params)]

    async def assets(self, farm: str) -> List[Dict[str, Any]]:
        """The farm's assets with a ledger run, and whether each is current."""
        return [
            {
                "asset_code": r["asset_code"],
                "currency": r["currency"],
                "window_from": r["window_from"].isoformat(),
                "window_to": r["window_to"].isoformat(),
                "status": "stale" if is_stale(r) else "ok",
            }
            for r in await self._runs(farm)
        ]

    async def current(self, farm: str, asset: str) -> Optional[Dict[str, Any]]:
        """The asset's ledger run row when it is current (not stale), else None."""
        runs = await self._runs(farm, asset)
        return parse_run(runs[0]) if runs and not is_stale(runs[0]) else None

    async def get(self, farm: str, asset: str) -> Optional[Dict[str, Any]]:
        """The asset's ledger: its read status, the document when current, and the statuses on its causes.
        None when the asset has no ledger run."""
        runs = await self._runs(farm, asset)
        if not runs:
            return None
        row, run = runs[0], parse_run(runs[0])
        document = None
        if is_stale(row):
            logger.warning(
                "scada_ledger_stale",
                farm=farm,
                asset_code=asset,
                run_id=run["run_id"],
                register_run_id=run["register_run_id"],
                current_register_run_id=row["current_register_run_id"],
                has_rows=row["has_rows"],
            )
        else:
            result = await self.db.execute(
                text(_ROWS_SQL), {"farm": farm, "asset": asset, "run_id": run["run_id"]}
            )
            document = build_document([dict(r._mapping) for r in result], run)
        return {
            "status": "ok" if document else "stale",
            "register_run_id": run["register_run_id"],
            "current_register_run_id": row["current_register_run_id"],
            "register": register_basis(row),
            "document": document,
            "statuses": await self._statuses(farm, asset, run["catalog"]),
        }

    async def _statuses(
        self, farm: str, asset: str, catalog: Mapping[str, Any]
    ) -> List[Dict[str, Any]]:
        """The asset's finding statuses whose key is one of the run's causes' status keys (filtered here)."""
        keys = status_keys(catalog)
        result = await self.db.execute(text(_STATUSES_SQL), {"farm": farm, "asset": asset})
        rows = [dict(r._mapping) for r in result]
        return sorted(
            (r for r in rows if (r["trigger"], r["scope"], r["cls"]) in keys),
            key=lambda r: (r["trigger"], r["scope"], r["cls"]),
        )
