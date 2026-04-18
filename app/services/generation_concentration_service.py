"""Generation Concentration analysis (spec item 3 — new module).

For each windfarm-period, partition all generation hours into price deciles
(D1 = lowest 10% of hourly prices, D10 = highest), then compute the share
of total MWh that fell into each decile, plus the volume-weighted capture
price and the capture ratio (weighted vs time-weighted average price).

Capture ratio interpretation:
  >1  windfarm generates more in higher-price hours (positive correlation,
      commercially good)
  =1  generation is uncorrelated with price
  <1  windfarm generates more in lower-price hours (commercially bad)

A turbine in an oversupplied bidzone or with a poor wind/price seasonal
match can have capture_ratio < 0.9, even with strong production.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.generation_concentration_summary import GenerationConcentrationSummary

logger = structlog.get_logger(__name__)


class GenerationConcentrationService:
    """Compute generation-vs-price concentration metrics per windfarm-period."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ─── Public API ────────────────────────────────────────────

    async def compute_for_windfarm(
        self,
        windfarm_id: int,
        year: int,
        df_preloaded=None,
        pipeline_run_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Compute and persist yearly concentration summary for one windfarm.

        Args:
            windfarm_id: Windfarm to analyse.
            year: Calendar year to analyse.
            df_preloaded: Optional pre-loaded hourly DataFrame (from
                PowerCurveService._load_hourly_data) — avoids redoing the
                3-table SQL join when the orchestrator is running modules in
                sequence.
            pipeline_run_id: Optional ImportJobExecution.id for tracking.

        Returns:
            Dict with the computed metrics + 'rows_persisted' count.
        """
        df = await self._get_hourly_df(windfarm_id, year, df_preloaded)
        if df is None or df.empty:
            logger.info("genconc_no_data", windfarm_id=windfarm_id, year=year)
            return {"windfarm_id": windfarm_id, "year": year, "error": "no data"}

        metrics = self._compute_metrics(df)
        if metrics is None:
            return {
                "windfarm_id": windfarm_id, "year": year,
                "error": "no qualifying hours after price filter",
            }

        # Look up zone average for vs-comparison (best effort)
        vs_zone = await self._compute_vs_zone(windfarm_id, year, metrics)

        await self._upsert_yearly(
            windfarm_id, year, metrics, vs_zone, pipeline_run_id
        )
        return {
            "windfarm_id": windfarm_id, "year": year,
            "rows_persisted": 1,
            **{k: v for k, v in metrics.items() if k != "decile_shares_full"},
            "decile_shares": metrics["decile_shares_full"],
            **vs_zone,
        }

    async def compute_for_windfarm_monthly(
        self,
        windfarm_id: int,
        year: int,
        df_preloaded=None,
        pipeline_run_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Compute and persist monthly concentration summaries for one year.

        Per-month deciles are computed within that month's hours — so D10 in
        January is the top 10% of January prices (not the year's top 10%).
        This isolates seasonal price patterns from generation patterns.
        """
        df = await self._get_hourly_df(windfarm_id, year, df_preloaded)
        if df is None or df.empty:
            return []

        df["month"] = df["hour"].dt.month
        results = []
        for month in range(1, 13):
            month_df = df[df["month"] == month]
            if len(month_df) < 100:  # need enough hours for stable deciles
                continue
            metrics = self._compute_metrics(month_df.drop(columns=["month"]))
            if metrics is None:
                continue
            await self._upsert_monthly(
                windfarm_id, year, month, metrics, pipeline_run_id
            )
            results.append({"year": year, "month": month, **metrics})
        return results

    # ─── Computation ───────────────────────────────────────────

    def _compute_metrics(self, df) -> Optional[Dict[str, Any]]:
        """Compute capture ratio + decile breakdown for a price/generation df.

        Required columns: 'market_price', 'generation_mwh'. Returns None if
        no qualifying rows remain after dropping nulls and zero-price rows.
        """
        import numpy as np
        import pandas as pd

        valid = df.dropna(subset=["market_price", "generation_mwh"])
        # Drop rows where price is non-finite or generation < 0 (treated as bad data)
        valid = valid[
            np.isfinite(valid["market_price"].astype(float))
            & (valid["generation_mwh"].astype(float) >= 0)
        ]
        if len(valid) < 100:
            return None

        prices = valid["market_price"].astype(float).to_numpy()
        gens = valid["generation_mwh"].astype(float).to_numpy()
        total_mwh = float(gens.sum())
        total_hours = int(len(valid))

        if total_mwh <= 0:
            return None

        time_weighted_avg = float(prices.mean())
        weighted_avg_capture = float((prices * gens).sum() / total_mwh)
        capture_ratio = (
            weighted_avg_capture / time_weighted_avg
            if time_weighted_avg != 0
            else None
        )

        # Assign deciles (1..10) by price rank — pandas qcut handles ties
        # via rank-then-cut.
        try:
            decile_labels = pd.qcut(
                prices, q=10, labels=False, duplicates="drop"
            )
        except ValueError:
            # Too few unique price values to make 10 buckets
            return None

        # decile_labels can have <10 distinct values if there were ties; align
        # to 1..10 anyway by computing share for each present label.
        decile_shares: Dict[int, float] = {}
        for d in range(10):
            mask = decile_labels == d
            mwh = float(gens[mask].sum())
            decile_shares[d + 1] = round(100.0 * mwh / total_mwh, 3) if total_mwh else 0.0

        # Top decile = D10 (highest price), bottom decile = D1.
        top_decile_share = decile_shares.get(10, 0.0)
        bottom_decile_share = decile_shares.get(1, 0.0)

        # Quartiles = sums of 3 deciles each (Q4 = D8+D9+D10, Q1 = D1+D2+D3),
        # rounded to 3 dp. (Decile-based quartiles slightly differ from true
        # quartiles but match the "thirds-of-deciles" intuition the spec uses.)
        top_quartile_share = round(
            sum(decile_shares.get(d, 0.0) for d in (8, 9, 10)), 3
        )
        bottom_quartile_share = round(
            sum(decile_shares.get(d, 0.0) for d in (1, 2, 3)), 3
        )

        # JSON-serialisable form
        decile_shares_full = {f"d{d}": share for d, share in decile_shares.items()}

        return {
            "total_mwh": round(total_mwh, 3),
            "total_hours": total_hours,
            "weighted_avg_capture_price_eur": round(weighted_avg_capture, 4),
            "time_weighted_avg_price_eur": round(time_weighted_avg, 4),
            "capture_ratio": round(capture_ratio, 4) if capture_ratio is not None else None,
            "top_decile_share_pct": top_decile_share,
            "top_quartile_share_pct": top_quartile_share,
            "bottom_decile_share_pct": bottom_decile_share,
            "bottom_quartile_share_pct": bottom_quartile_share,
            "decile_shares_full": decile_shares_full,
        }

    async def _compute_vs_zone(
        self, windfarm_id: int, year: int, metrics: Dict[str, Any]
    ) -> Dict[str, Optional[float]]:
        """Compute vs-zone-average diffs. Best effort — returns Nones if no peer data."""
        # Lookup the windfarm's bidzone
        bidzone_id = await self.db.scalar(
            text("SELECT bidzone_id FROM windfarms WHERE id = :id"),
            {"id": windfarm_id},
        )
        if not bidzone_id:
            return {
                "vs_zone_capture_ratio_diff": None,
                "vs_zone_top_decile_diff": None,
            }

        # Use peer aggregate service to get/compute zone averages.
        # Wrap in try/except — peer aggregates table may be empty for this
        # zone+year combo on first run; we don't want to crash the pipeline.
        try:
            from app.services.peer_aggregate_service import PeerAggregateService

            agg_svc = PeerAggregateService(self.db)
            cap_agg = await agg_svc.get_or_compute(
                "bidzone", bidzone_id, "concentration_capture_ratio", year
            )
            top_agg = await agg_svc.get_or_compute(
                "bidzone", bidzone_id, "concentration_top_decile_share_pct", year
            )

            cap_diff = None
            if cap_agg and cap_agg.avg_value is not None and metrics.get("capture_ratio") is not None:
                cap_diff = round(
                    float(metrics["capture_ratio"]) - float(cap_agg.avg_value), 4
                )
            top_diff = None
            if top_agg and top_agg.avg_value is not None and metrics.get("top_decile_share_pct") is not None:
                top_diff = round(
                    float(metrics["top_decile_share_pct"]) - float(top_agg.avg_value), 3
                )

            return {
                "vs_zone_capture_ratio_diff": cap_diff,
                "vs_zone_top_decile_diff": top_diff,
            }
        except Exception as exc:
            logger.warning(
                "genconc_vs_zone_failed",
                windfarm_id=windfarm_id, year=year, error=str(exc),
            )
            return {
                "vs_zone_capture_ratio_diff": None,
                "vs_zone_top_decile_diff": None,
            }

    # ─── Persistence ───────────────────────────────────────────

    async def _upsert_yearly(
        self,
        windfarm_id: int,
        year: int,
        metrics: Dict[str, Any],
        vs_zone: Dict[str, Optional[float]],
        pipeline_run_id: Optional[int],
    ) -> None:
        await self._upsert(
            windfarm_id=windfarm_id, year=year, month=None, period_type="year",
            metrics=metrics, vs_zone=vs_zone, pipeline_run_id=pipeline_run_id,
        )

    async def _upsert_monthly(
        self,
        windfarm_id: int,
        year: int,
        month: int,
        metrics: Dict[str, Any],
        pipeline_run_id: Optional[int],
    ) -> None:
        # Monthly rows don't yet do vs-zone comparison (would need monthly peer
        # aggregates which are noisy). Persist as-is.
        await self._upsert(
            windfarm_id=windfarm_id, year=year, month=month, period_type="month",
            metrics=metrics,
            vs_zone={"vs_zone_capture_ratio_diff": None, "vs_zone_top_decile_diff": None},
            pipeline_run_id=pipeline_run_id,
        )

    async def _upsert(
        self,
        windfarm_id: int,
        year: int,
        month: Optional[int],
        period_type: str,
        metrics: Dict[str, Any],
        vs_zone: Dict[str, Optional[float]],
        pipeline_run_id: Optional[int],
    ) -> None:
        import json

        params = {
            "wf_id": windfarm_id,
            "period_type": period_type,
            "year": year,
            "month": month,
            "total_mwh": metrics["total_mwh"],
            "total_hours": metrics["total_hours"],
            "wac_price": metrics["weighted_avg_capture_price_eur"],
            "tw_price": metrics["time_weighted_avg_price_eur"],
            "capture_ratio": metrics["capture_ratio"],
            "top_decile": metrics["top_decile_share_pct"],
            "top_quartile": metrics["top_quartile_share_pct"],
            "bot_decile": metrics["bottom_decile_share_pct"],
            "bot_quartile": metrics["bottom_quartile_share_pct"],
            "decile_shares": json.dumps(metrics["decile_shares_full"]),
            "vs_zone_cap": vs_zone["vs_zone_capture_ratio_diff"],
            "vs_zone_top": vs_zone["vs_zone_top_decile_diff"],
            "run_id": pipeline_run_id,
        }

        # Two-step UPDATE-then-INSERT: NULL month defeats ON CONFLICT
        # (uniqueness with NULL behaves oddly in PG when NULLs aren't NOT
        # DISTINCT). This matches the pattern used in
        # performance_pipeline_service._compute_commercial_metrics.
        if month is None:
            update_sql = text("""
                UPDATE generation_concentration_summaries
                SET total_mwh = :total_mwh,
                    total_hours = :total_hours,
                    weighted_avg_capture_price_eur = :wac_price,
                    time_weighted_avg_price_eur = :tw_price,
                    capture_ratio = :capture_ratio,
                    top_decile_share_pct = :top_decile,
                    top_quartile_share_pct = :top_quartile,
                    bottom_decile_share_pct = :bot_decile,
                    bottom_quartile_share_pct = :bot_quartile,
                    decile_shares = CAST(:decile_shares AS JSONB),
                    vs_zone_capture_ratio_diff = :vs_zone_cap,
                    vs_zone_top_decile_diff = :vs_zone_top,
                    pipeline_run_id = COALESCE(:run_id, pipeline_run_id),
                    updated_at = NOW()
                WHERE windfarm_id = :wf_id
                  AND period_type = :period_type
                  AND year = :year
                  AND month IS NULL
            """)
            res = await self.db.execute(update_sql, params)
            if res.rowcount == 0:
                await self.db.execute(
                    text("""
                        INSERT INTO generation_concentration_summaries
                          (windfarm_id, period_type, year, month,
                           total_mwh, total_hours, weighted_avg_capture_price_eur,
                           time_weighted_avg_price_eur, capture_ratio,
                           top_decile_share_pct, top_quartile_share_pct,
                           bottom_decile_share_pct, bottom_quartile_share_pct,
                           decile_shares,
                           vs_zone_capture_ratio_diff, vs_zone_top_decile_diff,
                           pipeline_run_id)
                        VALUES
                          (:wf_id, :period_type, :year, NULL,
                           :total_mwh, :total_hours, :wac_price,
                           :tw_price, :capture_ratio,
                           :top_decile, :top_quartile,
                           :bot_decile, :bot_quartile,
                           CAST(:decile_shares AS JSONB),
                           :vs_zone_cap, :vs_zone_top,
                           :run_id)
                    """),
                    params,
                )
            return

        # Monthly path: ON CONFLICT works because (year, month) tuple is
        # fully NOT NULL.
        await self.db.execute(
            text("""
                INSERT INTO generation_concentration_summaries
                  (windfarm_id, period_type, year, month,
                   total_mwh, total_hours, weighted_avg_capture_price_eur,
                   time_weighted_avg_price_eur, capture_ratio,
                   top_decile_share_pct, top_quartile_share_pct,
                   bottom_decile_share_pct, bottom_quartile_share_pct,
                   decile_shares,
                   vs_zone_capture_ratio_diff, vs_zone_top_decile_diff,
                   pipeline_run_id)
                VALUES
                  (:wf_id, :period_type, :year, :month,
                   :total_mwh, :total_hours, :wac_price,
                   :tw_price, :capture_ratio,
                   :top_decile, :top_quartile,
                   :bot_decile, :bot_quartile,
                   CAST(:decile_shares AS JSONB),
                   :vs_zone_cap, :vs_zone_top,
                   :run_id)
                ON CONFLICT ON CONSTRAINT uq_generation_concentration_wf_period
                DO UPDATE SET
                    total_mwh = EXCLUDED.total_mwh,
                    total_hours = EXCLUDED.total_hours,
                    weighted_avg_capture_price_eur = EXCLUDED.weighted_avg_capture_price_eur,
                    time_weighted_avg_price_eur = EXCLUDED.time_weighted_avg_price_eur,
                    capture_ratio = EXCLUDED.capture_ratio,
                    top_decile_share_pct = EXCLUDED.top_decile_share_pct,
                    top_quartile_share_pct = EXCLUDED.top_quartile_share_pct,
                    bottom_decile_share_pct = EXCLUDED.bottom_decile_share_pct,
                    bottom_quartile_share_pct = EXCLUDED.bottom_quartile_share_pct,
                    decile_shares = EXCLUDED.decile_shares,
                    pipeline_run_id = COALESCE(EXCLUDED.pipeline_run_id, generation_concentration_summaries.pipeline_run_id),
                    updated_at = NOW()
            """),
            params,
        )

    # ─── Data loading ──────────────────────────────────────────

    async def _get_hourly_df(
        self,
        windfarm_id: int,
        year: int,
        df_preloaded=None,
    ):
        """Return a DataFrame with [hour, generation_mwh, market_price] for the year.

        Reuses the orchestrator-supplied df when available, else delegates to
        PowerCurveService._load_hourly_data (same loader the rest of the
        pipeline uses).
        """
        import pandas as pd

        if df_preloaded is not None and not df_preloaded.empty:
            df = df_preloaded
            if "year" in df.columns:
                df = df[df["year"] == year]
            return df

        from app.services.power_curve_service import PowerCurveService

        # rated_mw is irrelevant for concentration (we don't use p_pu) — pass
        # a placeholder.
        pcs = PowerCurveService(self.db)
        return await pcs._load_hourly_data(windfarm_id, year, year, 1.0)

    # ─── Read API ──────────────────────────────────────────────

    async def get_summary(
        self,
        windfarm_id: int,
        year: Optional[int] = None,
        period: str = "year",
    ) -> List[Dict[str, Any]]:
        """Return persisted concentration summaries for a windfarm."""
        sql = text("""
            SELECT id, windfarm_id, period_type, year, month,
                   total_mwh, total_hours,
                   weighted_avg_capture_price_eur,
                   time_weighted_avg_price_eur,
                   capture_ratio,
                   top_decile_share_pct, top_quartile_share_pct,
                   bottom_decile_share_pct, bottom_quartile_share_pct,
                   decile_shares,
                   vs_zone_capture_ratio_diff, vs_zone_top_decile_diff,
                   computed_at, updated_at
            FROM generation_concentration_summaries
            WHERE windfarm_id = :wf_id
              AND period_type = :period_type
              AND (:year IS NULL OR year = :year)
            ORDER BY year DESC, COALESCE(month, 0)
        """)
        rows = await self.db.execute(
            sql, {"wf_id": windfarm_id, "period_type": period, "year": year}
        )
        out = []
        for r in rows.fetchall():
            row_dict = dict(r._mapping)
            # Coerce Decimals to float for JSON serialisation
            for k in (
                "total_mwh", "weighted_avg_capture_price_eur",
                "time_weighted_avg_price_eur", "capture_ratio",
                "top_decile_share_pct", "top_quartile_share_pct",
                "bottom_decile_share_pct", "bottom_quartile_share_pct",
                "vs_zone_capture_ratio_diff", "vs_zone_top_decile_diff",
            ):
                if row_dict.get(k) is not None:
                    row_dict[k] = float(row_dict[k])
            out.append(row_dict)
        return out
