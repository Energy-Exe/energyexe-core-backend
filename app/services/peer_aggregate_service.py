"""Peer-aggregate computation service (PRE-B for spec items 3, 4, 5, 6).

Computes (avg, p10, p50, p90, windfarm_count) of a metric over a peer group
(bidzone / country / owner / turbine_model) for a given year (or year+month).

Results are cached in `peer_group_aggregates` to avoid scanning all peer
windfarms' performance_summaries / degradation_results rows on every API
request. Refreshed by the daily pipeline cron after each module run.

Metric keys handled:
- ODI metrics (from performance_summaries):
    odi_pct_underperf, odi_pct_loss_mwh, odi_pct_loss_eur
- Wind normalisation (from performance_summaries):
    wind_norm_index_p50, wind_norm_index_p10
- Degradation (from degradation_results):
    degradation_slope_pct_per_year_q50, degradation_slope_pct_per_year_q90
- Generation concentration (from generation_concentration_summaries — added by
  item 3):
    capture_ratio, top_decile_share_pct, bottom_decile_share_pct
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import structlog
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.peer_group_aggregate import PeerGroupAggregate
from app.services.peer_analysis_service import PeerAnalysisService

logger = structlog.get_logger(__name__)


# Maps metric_key → (table, column, ref_curve_filter or None)
METRIC_SOURCES: Dict[str, Tuple[str, str, Optional[str]]] = {
    # performance_summaries — yearly only
    "odi_pct_underperf": ("performance_summaries", "odi_pct_underperf", None),
    "odi_pct_loss_mwh": ("performance_summaries", "odi_pct_loss_mwh", None),
    "odi_pct_loss_eur": ("performance_summaries", "odi_pct_loss_eur", None),
    "wind_norm_index_p50": ("performance_summaries", "norm_index_p50", None),
    "wind_norm_index_p10": ("performance_summaries", "norm_index_p10", None),
    # degradation_results
    "degradation_slope_pct_per_year_q50": (
        "degradation_results", "slope_pct_per_year", "q50"
    ),
    "degradation_slope_pct_per_year_q90": (
        "degradation_results", "slope_pct_per_year", "q90"
    ),
    # generation_concentration_summaries (item 3 — registered here even though
    # the table is added by 2026041702)
    "concentration_capture_ratio": (
        "generation_concentration_summaries", "capture_ratio", None
    ),
    "concentration_top_decile_share_pct": (
        "generation_concentration_summaries", "top_decile_share_pct", None
    ),
    "concentration_bottom_decile_share_pct": (
        "generation_concentration_summaries", "bottom_decile_share_pct", None
    ),
}


SUPPORTED_GROUP_TYPES = ("bidzone", "country", "owner", "turbine_model")


class PeerAggregateService:
    """Compute and cache peer-group aggregates for performance comparisons."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self._peer_svc = PeerAnalysisService(db)

    # ─── Public API ────────────────────────────────────────────

    async def get_or_compute(
        self,
        group_type: str,
        group_id: int,
        metric_key: str,
        year: int,
        month: Optional[int] = None,
        force_refresh: bool = False,
        max_age_seconds: int = 86_400,
    ) -> Optional[PeerGroupAggregate]:
        """Return a cached aggregate, recomputing if missing or stale.

        Args:
            group_type: One of SUPPORTED_GROUP_TYPES.
            group_id: ID of the bidzone/country/owner/turbine_model.
            metric_key: One of METRIC_SOURCES.
            year: Period year.
            month: Optional period month (None for yearly aggregates).
            force_refresh: If True, ignore cache and recompute.
            max_age_seconds: Recompute if cached row is older than this.
        """
        self._validate(group_type, metric_key)

        if not force_refresh:
            cached = await self._read_cache(group_type, group_id, metric_key, year, month)
            if cached is not None:
                age = (
                    datetime.now(timezone.utc).replace(tzinfo=None) - cached.computed_at
                ).total_seconds()
                if age < max_age_seconds:
                    return cached

        return await self.compute_and_cache(group_type, group_id, metric_key, year, month)

    async def compute_and_cache(
        self,
        group_type: str,
        group_id: int,
        metric_key: str,
        year: int,
        month: Optional[int] = None,
    ) -> Optional[PeerGroupAggregate]:
        """Recompute the aggregate from source tables and upsert into cache."""
        self._validate(group_type, metric_key)

        windfarm_ids = await self._get_peer_windfarm_ids(group_type, group_id)
        if not windfarm_ids:
            logger.info(
                "peer_aggregate_no_windfarms",
                group_type=group_type, group_id=group_id,
            )
            return None

        values = await self._fetch_metric_values(
            metric_key, windfarm_ids, year, month
        )
        if not values:
            logger.info(
                "peer_aggregate_no_values",
                group_type=group_type, group_id=group_id,
                metric=metric_key, year=year, month=month,
            )
            return None

        stats = self._summarise(values)

        # UPSERT — Postgres ON CONFLICT
        period_type = "month" if month else "year"
        await self.db.execute(
            text("""
                INSERT INTO peer_group_aggregates
                  (group_type, group_id, metric_key, period_type, year, month,
                   windfarm_count, avg_value, p10_value, p50_value, p90_value,
                   computed_at)
                VALUES
                  (:group_type, :group_id, :metric_key, :period_type, :year, :month,
                   :n, :avg, :p10, :p50, :p90, NOW())
                ON CONFLICT ON CONSTRAINT uq_peer_group_aggregate DO UPDATE SET
                  windfarm_count = EXCLUDED.windfarm_count,
                  avg_value = EXCLUDED.avg_value,
                  p10_value = EXCLUDED.p10_value,
                  p50_value = EXCLUDED.p50_value,
                  p90_value = EXCLUDED.p90_value,
                  computed_at = NOW()
            """),
            {
                "group_type": group_type,
                "group_id": group_id,
                "metric_key": metric_key,
                "period_type": period_type,
                "year": year,
                "month": month,
                "n": stats["n"],
                "avg": stats["avg"],
                "p10": stats["p10"],
                "p50": stats["p50"],
                "p90": stats["p90"],
            },
        )

        return await self._read_cache(group_type, group_id, metric_key, year, month)

    async def refresh_for_windfarm(
        self,
        windfarm_id: int,
        years: List[int],
    ) -> int:
        """Refresh all peer aggregates that include `windfarm_id` for these years.

        Called by the pipeline orchestrator after a per-windfarm run completes,
        so the windfarm's metric updates are reflected in its zone/country
        averages on the next API request.

        Returns: number of (group, metric, year) combos refreshed.
        """
        wf = await self._peer_svc.get_windfarm_with_relations(windfarm_id)
        if not wf:
            return 0

        groups = []
        if wf.bidzone_id:
            groups.append(("bidzone", wf.bidzone_id))
        if wf.country_id:
            groups.append(("country", wf.country_id))

        # Owner / turbine_model aggregates rebuild less often — skip in the
        # per-windfarm hot path. Cron job covers them.

        refreshed = 0
        for group_type, group_id in groups:
            for metric_key in METRIC_SOURCES.keys():
                for year in years:
                    try:
                        await self.compute_and_cache(
                            group_type, group_id, metric_key, year, month=None
                        )
                        refreshed += 1
                    except Exception as exc:
                        logger.warning(
                            "peer_aggregate_refresh_failed",
                            group_type=group_type, group_id=group_id,
                            metric=metric_key, year=year, error=str(exc),
                        )
        return refreshed

    # ─── Helpers ───────────────────────────────────────────────

    def _validate(self, group_type: str, metric_key: str) -> None:
        if group_type not in SUPPORTED_GROUP_TYPES:
            raise ValueError(
                f"Unsupported group_type {group_type!r} "
                f"(allowed: {SUPPORTED_GROUP_TYPES})"
            )
        if metric_key not in METRIC_SOURCES:
            raise ValueError(
                f"Unknown metric_key {metric_key!r} "
                f"(allowed: {sorted(METRIC_SOURCES)})"
            )

    async def _read_cache(
        self,
        group_type: str,
        group_id: int,
        metric_key: str,
        year: int,
        month: Optional[int],
    ) -> Optional[PeerGroupAggregate]:
        stmt = select(PeerGroupAggregate).where(
            PeerGroupAggregate.group_type == group_type,
            PeerGroupAggregate.group_id == group_id,
            PeerGroupAggregate.metric_key == metric_key,
            PeerGroupAggregate.year == year,
        )
        # NULL month must compare with `IS NULL`, not `=`
        if month is None:
            stmt = stmt.where(PeerGroupAggregate.month.is_(None))
        else:
            stmt = stmt.where(PeerGroupAggregate.month == month)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def _get_peer_windfarm_ids(
        self, group_type: str, group_id: int
    ) -> List[int]:
        if group_type == "bidzone":
            return await self._peer_svc.get_bidzone_peers(group_id)
        if group_type == "country":
            return await self._peer_svc.get_country_peers(group_id)
        if group_type == "owner":
            return await self._peer_svc.get_owner_peers(group_id)
        if group_type == "turbine_model":
            return await self._peer_svc.get_turbine_model_peers(group_id)
        raise ValueError(f"Unsupported group_type {group_type!r}")

    async def _fetch_metric_values(
        self,
        metric_key: str,
        windfarm_ids: List[int],
        year: int,
        month: Optional[int],
    ) -> List[float]:
        """Read the metric value for each peer windfarm from its source table."""
        table, column, ref_filter = METRIC_SOURCES[metric_key]
        period_type = "month" if month else "year"

        # degradation_results doesn't have period_type/year/month — its
        # analysis_start/analysis_end columns implicitly cover all available
        # data. We treat the most-recent row matching the reference_curve as
        # "the result for the most recent year". This matches how the pipeline
        # writes one row per (windfarm, reference_curve, run).
        if table == "degradation_results":
            sql = text(f"""
                SELECT {column}::float AS v
                FROM degradation_results
                WHERE windfarm_id = ANY(:ids)
                  AND reference_curve = :ref
                  AND {column} IS NOT NULL
                  AND EXTRACT(YEAR FROM analysis_end) = :year
            """)
            rows = await self.db.execute(
                sql, {"ids": windfarm_ids, "ref": ref_filter, "year": year}
            )
            return [float(r.v) for r in rows.fetchall() if r.v is not None]

        # generation_concentration_summaries (item 3) — table may not exist yet
        # at the time this service is first imported, so probe defensively.
        if table == "generation_concentration_summaries":
            exists = await self.db.scalar(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'generation_concentration_summaries'
                )
            """))
            if not exists:
                return []
            month_clause = "AND month IS NULL" if month is None else "AND month = :month"
            sql = text(f"""
                SELECT {column}::float AS v
                FROM generation_concentration_summaries
                WHERE windfarm_id = ANY(:ids)
                  AND period_type = :period_type
                  AND year = :year
                  {month_clause}
                  AND {column} IS NOT NULL
            """)
            params = {
                "ids": windfarm_ids,
                "period_type": period_type,
                "year": year,
            }
            if month is not None:
                params["month"] = month
            rows = await self.db.execute(sql, params)
            return [float(r.v) for r in rows.fetchall() if r.v is not None]

        # performance_summaries (default)
        month_clause = "AND month IS NULL" if month is None else "AND month = :month"
        sql = text(f"""
            SELECT {column}::float AS v
            FROM performance_summaries
            WHERE windfarm_id = ANY(:ids)
              AND period_type = :period_type
              AND year = :year
              {month_clause}
              AND {column} IS NOT NULL
        """)
        params = {
            "ids": windfarm_ids,
            "period_type": period_type,
            "year": year,
        }
        if month is not None:
            params["month"] = month
        rows = await self.db.execute(sql, params)
        return [float(r.v) for r in rows.fetchall() if r.v is not None]

    @staticmethod
    def _summarise(values: List[float]) -> Dict[str, float]:
        """Compute (n, avg, p10, p50, p90) without pulling in numpy."""
        from statistics import mean

        n = len(values)
        if n == 0:
            return {"n": 0, "avg": None, "p10": None, "p50": None, "p90": None}

        sorted_vals = sorted(values)

        def _pct(p: float) -> float:
            # Linear-interpolation percentile, matching numpy's default.
            if n == 1:
                return sorted_vals[0]
            k = (n - 1) * (p / 100.0)
            f = int(k)
            c = min(f + 1, n - 1)
            if f == c:
                return sorted_vals[f]
            return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)

        return {
            "n": n,
            "avg": round(mean(values), 4),
            "p10": round(_pct(10), 4),
            "p50": round(_pct(50), 4),
            "p90": round(_pct(90), 4),
        }
