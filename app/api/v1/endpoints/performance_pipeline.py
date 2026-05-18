"""API endpoints for the performance analysis pipeline."""

from typing import Any, Dict, List, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.degradation_result import DegradationResult
from app.models.performance_anomaly import PerformanceAnomaly
from app.models.performance_summary import PerformanceSummary
from app.models.power_curve_bin import PowerCurveBin
from app.models.user import User
from app.models.windfarm import Windfarm
from app.schemas.performance_pipeline import (
    DegradationResponse,
    GenerationConcentrationResponse,
    NormalisationResponse,
    ODIMetricsResponse,
    PeerAggregateResponse,
    PerformanceAnomalyResponse,
    PerformanceSummaryResponse,
    PipelineRunRequest,
    PipelineRunResponse,
    PowerCurveBinResponse,
    PowerCurveResponse,
    PPAScenarioRequest,
    PPAScenarioResponse,
    WindNormalisedHourPoint,
    WindNormalisedHourlyResponse,
)

logger = structlog.get_logger()
router = APIRouter()


@router.post("/run", response_model=PipelineRunResponse)
async def trigger_pipeline(
    request: PipelineRunRequest = PipelineRunRequest(),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Trigger the performance analysis pipeline."""
    from app.services.performance_pipeline_service import PerformancePipelineService

    service = PerformancePipelineService(db)
    result = await service.run_pipeline_batch(windfarm_ids=request.windfarm_ids)
    return PipelineRunResponse(**result)


@router.get("/power-curves/{windfarm_id}", response_model=PowerCurveResponse)
async def get_power_curves(
    windfarm_id: int,
    year: Optional[int] = Query(None),
    curve_type: str = Query("overall_clean"),
    include_zone_comparison: bool = Query(
        True,
        description="If true, also return per-bin bidzone-average q50/q90 "
                    "computed across all peer windfarms with the same curve_type.",
    ),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get stored power curve bins for a windfarm.

    Spec item 2: when `include_zone_comparison` is true, each bin is enriched
    with the bidzone average q50/q90 computed across all peer windfarms with
    the same `curve_type` (and matching `year` for yearly curves).
    """
    query = select(PowerCurveBin).where(
        PowerCurveBin.windfarm_id == windfarm_id,
        PowerCurveBin.curve_type == curve_type,
    )
    if year is not None:
        query = query.where(PowerCurveBin.year == year)
    elif curve_type == "overall_clean":
        query = query.where(PowerCurveBin.year.is_(None))
    query = query.order_by(PowerCurveBin.wind_bin)

    result = await db.execute(query)
    bins = result.scalars().all()
    if not bins:
        raise HTTPException(status_code=404, detail="No power curve found")

    # Per-bin zone aggregates (best-effort — never block the primary response)
    zone_by_bin: dict = {}
    bidzone_id: Optional[int] = None
    bidzone_name: Optional[str] = None
    if include_zone_comparison:
        from app.models.bidzone import Bidzone
        from app.models.windfarm import Windfarm
        from sqlalchemy import func

        wf_row = (await db.execute(
            select(Windfarm.bidzone_id).where(Windfarm.id == windfarm_id)
        )).first()
        bidzone_id = wf_row[0] if wf_row else None

        if bidzone_id is not None:
            bz_row = (await db.execute(
                select(Bidzone.name).where(Bidzone.id == bidzone_id)
            )).first()
            bidzone_name = bz_row[0] if bz_row else None

            # Get peer windfarm IDs in the same bidzone (excluding this windfarm)
            peer_ids_q = select(Windfarm.id).where(
                Windfarm.bidzone_id == bidzone_id,
                Windfarm.id != windfarm_id,
            )
            peer_ids = [r[0] for r in (await db.execute(peer_ids_q)).all()]

            if peer_ids:
                # Aggregate peer power curves by bin
                agg_q = select(
                    PowerCurveBin.wind_bin,
                    func.avg(PowerCurveBin.q50_pu).label("avg_q50"),
                    func.avg(PowerCurveBin.q90_pu).label("avg_q90"),
                    func.count(func.distinct(PowerCurveBin.windfarm_id)).label("n_wf"),
                ).where(
                    PowerCurveBin.windfarm_id.in_(peer_ids),
                    PowerCurveBin.curve_type == curve_type,
                )
                if year is not None:
                    agg_q = agg_q.where(PowerCurveBin.year == year)
                elif curve_type == "overall_clean":
                    agg_q = agg_q.where(PowerCurveBin.year.is_(None))
                agg_q = agg_q.group_by(PowerCurveBin.wind_bin)

                for r in (await db.execute(agg_q)).all():
                    zone_by_bin[float(r.wind_bin)] = {
                        "zone_avg_q50_pu": float(r.avg_q50) if r.avg_q50 is not None else None,
                        "zone_avg_q90_pu": float(r.avg_q90) if r.avg_q90 is not None else None,
                        "zone_windfarm_count": int(r.n_wf),
                    }

    return PowerCurveResponse(
        windfarm_id=windfarm_id,
        curve_type=curve_type,
        year=year,
        bidzone_id=bidzone_id,
        bidzone_name=bidzone_name,
        bins=[
            PowerCurveBinResponse(
                wind_bin=float(b.wind_bin),
                q50_pu=float(b.q50_pu) if b.q50_pu else None,
                q90_pu=float(b.q90_pu) if b.q90_pu else None,
                mean_pu=float(b.mean_pu) if b.mean_pu else None,
                mad_pu=float(b.mad_pu) if b.mad_pu else None,
                sample_count=b.sample_count,
                **(zone_by_bin.get(float(b.wind_bin), {})),
            )
            for b in bins
        ],
    )


@router.get("/odi/{windfarm_id}", response_model=List[ODIMetricsResponse])
async def get_odi_metrics(
    windfarm_id: int,
    year: Optional[int] = Query(None),
    period_type: Optional[str] = Query(None),
    include_zone_comparison: bool = Query(
        True,
        description="If true, attach bidzone-average ODI metrics + vs-zone diffs.",
    ),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get ODI metrics for a windfarm.

    Spec item 5: when `include_zone_comparison` is true and the windfarm has
    a bidzone, each yearly row is enriched with the bidzone-average
    odi_pct_underperf / odi_pct_loss_mwh / odi_pct_loss_eur and the
    differences vs zone (positive = worse than zone, negative = better).
    Monthly rows are not enriched (peer aggregates are computed yearly).
    """
    from app.services.performance_anomaly_service import PerformanceAnomalyService

    service = PerformanceAnomalyService(db)
    metrics = await service.get_odi_metrics(windfarm_id, year)
    if period_type:
        metrics = [m for m in metrics if m["period_type"] == period_type]

    responses = [ODIMetricsResponse(**m) for m in metrics]

    if include_zone_comparison:
        from app.models.windfarm import Windfarm
        from app.services.peer_aggregate_service import PeerAggregateService

        wf_row = (await db.execute(
            select(Windfarm.bidzone_id).where(Windfarm.id == windfarm_id)
        )).first()
        bidzone_id = wf_row[0] if wf_row else None

        if bidzone_id is not None:
            agg_svc = PeerAggregateService(db)
            for resp in responses:
                if resp.period_type != "year":
                    continue  # Skip monthly rows
                try:
                    for metric_key, attr_avg, attr_diff, this_attr in [
                        ("odi_pct_underperf",
                         "zone_avg_odi_pct_underperf", "vs_zone_diff_underperf",
                         "odi_pct_underperf"),
                        ("odi_pct_loss_mwh",
                         "zone_avg_odi_pct_loss_mwh", "vs_zone_diff_loss_mwh",
                         "odi_pct_loss_mwh"),
                        ("odi_pct_loss_eur",
                         "zone_avg_odi_pct_loss_eur", "vs_zone_diff_loss_eur",
                         "odi_pct_loss_eur"),
                    ]:
                        agg = await agg_svc.get_or_compute(
                            "bidzone", bidzone_id, metric_key, year=resp.year,
                        )
                        if agg is not None and agg.avg_value is not None:
                            zone_avg = float(agg.avg_value)
                            setattr(resp, attr_avg, round(zone_avg, 4))
                            resp.zone_windfarm_count = agg.windfarm_count
                            this_val = getattr(resp, this_attr)
                            if this_val is not None:
                                setattr(
                                    resp, attr_diff,
                                    round(float(this_val) - zone_avg, 4),
                                )
                except Exception as exc:
                    logger.warning(
                        "odi_zone_comparison_failed",
                        windfarm_id=windfarm_id, year=resp.year, error=str(exc),
                    )

            await db.commit()

    return responses


@router.get("/normalisation/{windfarm_id}", response_model=List[NormalisationResponse])
async def get_normalisation(
    windfarm_id: int,
    year: Optional[int] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get wind-normalised performance indices."""
    query = select(PerformanceSummary).where(
        PerformanceSummary.windfarm_id == windfarm_id,
    )
    if year:
        query = query.where(PerformanceSummary.year == year)
    query = query.order_by(PerformanceSummary.year, PerformanceSummary.month.nullslast())

    result = await db.execute(query)
    return [
        NormalisationResponse(
            period_type=s.period_type,
            year=s.year,
            month=s.month,
            norm_ratio_p50=float(s.norm_ratio_p50) if s.norm_ratio_p50 else None,
            norm_index_p50=float(s.norm_index_p50) if s.norm_index_p50 else None,
            norm_ratio_p10=float(s.norm_ratio_p10) if s.norm_ratio_p10 else None,
            norm_index_p10=float(s.norm_index_p10) if s.norm_index_p10 else None,
        )
        for s in result.scalars().all()
        if s.norm_ratio_p50 is not None or s.norm_ratio_p10 is not None
    ]


@router.get("/degradation/{windfarm_id}", response_model=List[DegradationResponse])
async def get_degradation(
    windfarm_id: int,
    include_zone_comparison: bool = Query(
        True,
        description="If true, attach bidzone-average slope and vs-zone diff "
                    "(uses cached peer aggregates when available).",
    ),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get degradation analysis results, optionally enriched with zone comparison.

    Spec item 4: returns vs-bidzone-average slope (across peer windfarms) plus
    the difference. Falls back gracefully if no peer aggregate is cached for
    the analysis_end year — diff fields are then None.
    """
    result = await db.execute(
        select(DegradationResult).where(
            DegradationResult.windfarm_id == windfarm_id,
        ).order_by(DegradationResult.reference_curve)
    )
    results = result.scalars().all()
    if not results:
        raise HTTPException(status_code=404, detail="No degradation results found")

    responses = [DegradationResponse.model_validate(r) for r in results]

    if include_zone_comparison:
        from app.models.windfarm import Windfarm
        from app.services.peer_aggregate_service import PeerAggregateService

        wf_row = (await db.execute(
            select(Windfarm.bidzone_id).where(Windfarm.id == windfarm_id)
        )).first()
        bidzone_id = wf_row[0] if wf_row else None

        if bidzone_id is not None:
            agg_svc = PeerAggregateService(db)
            for r, resp in zip(results, responses):
                metric_key = (
                    "degradation_slope_pct_per_year_q50"
                    if r.reference_curve == "q50"
                    else "degradation_slope_pct_per_year_q90"
                )
                try:
                    agg = await agg_svc.get_or_compute(
                        "bidzone", bidzone_id, metric_key,
                        year=r.analysis_end.year,
                    )
                    if agg is not None and agg.avg_value is not None:
                        zone_avg = float(agg.avg_value)
                        resp.zone_avg_slope_pct_per_year = round(zone_avg, 4)
                        resp.zone_windfarm_count = agg.windfarm_count
                        if resp.slope_pct_per_year is not None:
                            resp.vs_zone_diff_pct = round(
                                float(resp.slope_pct_per_year) - zone_avg, 4
                            )
                except Exception as exc:
                    logger.warning(
                        "degradation_zone_comparison_failed",
                        windfarm_id=windfarm_id, ref=r.reference_curve, error=str(exc),
                    )

            # Persist any newly-computed peer aggregate cache rows.
            await db.commit()

    return responses


@router.get("/anomalies/recent", response_model=List[Dict[str, Any]])
async def get_recent_anomalies(
    limit: int = Query(10, le=100),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Most-recent flagged hours across all windfarms (for dashboard widgets)."""
    query = (
        select(
            PerformanceAnomaly.id,
            PerformanceAnomaly.windfarm_id,
            PerformanceAnomaly.hour,
            PerformanceAnomaly.anomaly_type,
            PerformanceAnomaly.actual_p_pu,
            PerformanceAnomaly.expected_p_pu,
            PerformanceAnomaly.lost_mwh,
            PerformanceAnomaly.lost_eur,
            PerformanceAnomaly.run_id,
            Windfarm.name.label("windfarm_name"),
        )
        .join(Windfarm, PerformanceAnomaly.windfarm_id == Windfarm.id)
        .order_by(PerformanceAnomaly.hour.desc())
        .limit(limit)
    )
    result = await db.execute(query)
    return [
        {
            "id": r.id,
            "windfarm_id": r.windfarm_id,
            "windfarm_name": r.windfarm_name,
            "hour": r.hour.isoformat() if r.hour else None,
            "anomaly_type": r.anomaly_type,
            "actual_p_pu": float(r.actual_p_pu) if r.actual_p_pu else None,
            "expected_p_pu": float(r.expected_p_pu) if r.expected_p_pu else None,
            "lost_mwh": float(r.lost_mwh) if r.lost_mwh else None,
            "lost_eur": float(r.lost_eur) if r.lost_eur else None,
            "run_id": r.run_id,
        }
        for r in result.fetchall()
    ]


@router.get("/anomalies/{windfarm_id}", response_model=List[PerformanceAnomalyResponse])
async def get_anomalies(
    windfarm_id: int,
    year: Optional[int] = Query(None),
    anomaly_type: Optional[str] = Query(None),
    min_run_hours: Optional[int] = Query(None),
    limit: int = Query(1000, le=10000),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get hourly performance anomaly records."""
    from sqlalchemy import extract, func

    query = select(PerformanceAnomaly).where(
        PerformanceAnomaly.windfarm_id == windfarm_id,
    )
    if year:
        query = query.where(extract("year", PerformanceAnomaly.hour) == year)
    if anomaly_type:
        query = query.where(PerformanceAnomaly.anomaly_type == anomaly_type)
    query = query.order_by(PerformanceAnomaly.hour.desc()).limit(limit)

    result = await db.execute(query)
    anomalies = result.scalars().all()

    # Filter by run length if requested
    if min_run_hours and anomalies:
        # Get run sizes
        run_ids = {a.run_id for a in anomalies if a.run_id is not None}
        if run_ids:
            run_counts = await db.execute(
                select(
                    PerformanceAnomaly.run_id,
                    func.count(PerformanceAnomaly.id).label("cnt"),
                ).where(
                    PerformanceAnomaly.windfarm_id == windfarm_id,
                    PerformanceAnomaly.run_id.in_(run_ids),
                ).group_by(PerformanceAnomaly.run_id)
            )
            valid_runs = {r.run_id for r in run_counts.fetchall() if r.cnt >= min_run_hours}
            anomalies = [a for a in anomalies if a.run_id in valid_runs]

    return [
        PerformanceAnomalyResponse(
            hour=a.hour,
            anomaly_type=a.anomaly_type,
            actual_p_pu=float(a.actual_p_pu) if a.actual_p_pu else None,
            expected_p_pu=float(a.expected_p_pu) if a.expected_p_pu else None,
            wind_speed=float(a.wind_speed) if a.wind_speed else None,
            lost_mwh=float(a.lost_mwh) if a.lost_mwh else None,
            lost_eur=float(a.lost_eur) if a.lost_eur else None,
            run_id=a.run_id,
        )
        for a in anomalies
    ]


@router.get("/summary/{windfarm_id}", response_model=List[PerformanceSummaryResponse])
async def get_summary(
    windfarm_id: int,
    year: Optional[int] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get full performance summary (ODI + normalisation + commercial)."""
    query = select(PerformanceSummary).where(
        PerformanceSummary.windfarm_id == windfarm_id,
    )
    if year:
        query = query.where(PerformanceSummary.year == year)
    query = query.order_by(PerformanceSummary.year, PerformanceSummary.month.nullslast())

    result = await db.execute(query)
    return [PerformanceSummaryResponse.model_validate(s) for s in result.scalars().all()]


@router.post("/ppa-scenarios/{windfarm_id}", response_model=List[PPAScenarioResponse])
async def run_ppa_scenarios(
    windfarm_id: int,
    request: PPAScenarioRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Run PPA price scenario analysis."""
    from app.services.performance_pipeline_service import PerformancePipelineService

    service = PerformancePipelineService(db)
    results = await service.run_ppa_scenarios(
        windfarm_id, request.year, request.price_scenarios
    )
    return [PPAScenarioResponse(**r) for r in results]


# ─── Spec item 3: Generation Concentration ────────────────────


@router.get(
    "/generation-concentration/{windfarm_id}",
    response_model=List[GenerationConcentrationResponse],
)
async def get_generation_concentration(
    windfarm_id: int,
    year: Optional[int] = Query(None, description="Filter to a specific year"),
    period: str = Query("year", description="'year' or 'month'"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Distribution of generation by price decile + capture ratio + zone diff.

    Spec item 3 (Prioritisation 2026-03-30). Returns one row per persisted
    (windfarm, period). For period='year', month is NULL.
    """
    if period not in ("year", "month"):
        raise HTTPException(status_code=400, detail="period must be 'year' or 'month'")
    from app.services.generation_concentration_service import (
        GenerationConcentrationService,
    )

    svc = GenerationConcentrationService(db)
    rows = await svc.get_summary(windfarm_id, year=year, period=period)
    return [GenerationConcentrationResponse(**r) for r in rows]


@router.post(
    "/generation-concentration/{windfarm_id}/compute",
    response_model=GenerationConcentrationResponse,
)
async def compute_generation_concentration(
    windfarm_id: int,
    year: int = Query(..., description="Year to compute"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """On-demand recompute of yearly concentration for a windfarm."""
    from app.services.generation_concentration_service import (
        GenerationConcentrationService,
    )

    svc = GenerationConcentrationService(db)
    result = await svc.compute_for_windfarm(windfarm_id, year)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    await db.commit()
    rows = await svc.get_summary(windfarm_id, year=year, period="year")
    if not rows:
        raise HTTPException(status_code=500, detail="Compute succeeded but no row returned")
    return GenerationConcentrationResponse(**rows[0])


# ─── Cross-cutting: Peer aggregates ───────────────────────────


@router.get(
    "/peer-aggregates/{group_type}/{group_id}/{metric_key}",
    response_model=PeerAggregateResponse,
)
async def get_peer_aggregate(
    group_type: str,
    group_id: int,
    metric_key: str,
    year: int = Query(..., description="Period year"),
    month: Optional[int] = Query(None, description="Optional month for monthly aggregates"),
    force_refresh: bool = Query(False, description="Recompute ignoring cache"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get the peer-group aggregate (avg/p10/p50/p90/n) for a metric.

    Used by frontend "vs zone average" cards. Recomputes lazily if no cached
    row exists or cache is stale (>24h).
    """
    from app.services.peer_aggregate_service import (
        METRIC_SOURCES,
        SUPPORTED_GROUP_TYPES,
        PeerAggregateService,
    )

    if group_type not in SUPPORTED_GROUP_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"group_type must be one of {SUPPORTED_GROUP_TYPES}",
        )
    if metric_key not in METRIC_SOURCES:
        raise HTTPException(
            status_code=400,
            detail=f"unknown metric_key {metric_key} (allowed: {sorted(METRIC_SOURCES)})",
        )

    svc = PeerAggregateService(db)
    agg = await svc.get_or_compute(
        group_type, group_id, metric_key, year, month, force_refresh=force_refresh
    )
    if agg is None:
        raise HTTPException(status_code=404, detail="No peer aggregate available")

    await db.commit()  # persist any newly-computed cache row
    return PeerAggregateResponse(
        group_type=agg.group_type,
        group_id=agg.group_id,
        metric_key=agg.metric_key,
        period_type=agg.period_type,
        year=agg.year,
        month=agg.month,
        windfarm_count=agg.windfarm_count,
        avg_value=float(agg.avg_value) if agg.avg_value is not None else None,
        p10_value=float(agg.p10_value) if agg.p10_value is not None else None,
        p50_value=float(agg.p50_value) if agg.p50_value is not None else None,
        p90_value=float(agg.p90_value) if agg.p90_value is not None else None,
        computed_at=agg.computed_at,
    )


# ─── Spec item 6: Wind-norm monthly time series (client-facing) ─


@router.get(
    "/wind-normalisation/{windfarm_id}/monthly-time-series",
    response_model=List[NormalisationResponse],
)
async def get_wind_normalisation_monthly(
    windfarm_id: int,
    years: Optional[str] = Query(
        None,
        description="Comma-separated list of years to include, e.g. '2020,2021,2022'",
    ),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Monthly wind-normalised performance index time series.

    Spec item 6 — client-facing chart endpoint. Returns one row per (year,
    month) where wind normalisation was computed. Sorted oldest→newest so the
    frontend can plot directly.
    """
    query = select(PerformanceSummary).where(
        PerformanceSummary.windfarm_id == windfarm_id,
        PerformanceSummary.period_type == "month",
    )
    if years:
        try:
            year_list = [int(y.strip()) for y in years.split(",") if y.strip()]
            query = query.where(PerformanceSummary.year.in_(year_list))
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="years must be comma-separated integers, e.g. '2020,2021'",
            )
    query = query.order_by(PerformanceSummary.year, PerformanceSummary.month)

    result = await db.execute(query)
    rows = result.scalars().all()
    return [
        NormalisationResponse(
            period_type=s.period_type,
            year=s.year,
            month=s.month,
            norm_ratio_p50=float(s.norm_ratio_p50) if s.norm_ratio_p50 else None,
            norm_index_p50=float(s.norm_index_p50) if s.norm_index_p50 else None,
            norm_ratio_p10=float(s.norm_ratio_p10) if s.norm_ratio_p10 else None,
            norm_index_p10=float(s.norm_index_p10) if s.norm_index_p10 else None,
        )
        for s in rows
        if s.norm_ratio_p50 is not None or s.norm_ratio_p10 is not None
    ]


@router.get(
    "/wind-normalisation/{windfarm_id}/hourly",
    response_model=WindNormalisedHourlyResponse,
)
async def get_wind_normalisation_hourly(
    windfarm_id: int,
    start_year: Optional[int] = Query(
        None, description="Inclusive start year (defaults to all available)."
    ),
    end_year: Optional[int] = Query(
        None, description="Inclusive end year (defaults to all available)."
    ),
    reference: str = Query(
        "q50",
        regex="^(q50|q90)$",
        description="Power-curve reference quantile: q50 (median) or q90 (P10).",
    ),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Per-hour wind-normalised generation series for a windfarm.

    Unblocks the client portal's Actual / Wind-normalised scatter toggle
    (faisal-energyexe/energyexe-client-ui#25). Computes per-hour values
    on demand from the stored hourly generation, ERA5 wind, and the
    windfarm's `overall_clean` power curve — no extra storage required.

    For each qualifying hour (wind ≥ 4 m/s, curve value exists,
    expected_mwh > 0):

      expected_mwh = curve(wind_speed) × rated_mw
      norm_ratio   = actual_mwh / expected_mwh
      wind_normalised_mwh = norm_ratio × curve(mean_wind_speed) × rated_mw

    `wind_normalised_mwh` is the value users should plot to "factor out
    wind variability" — it is each hour's measured efficiency projected
    onto the windfarm's long-run-mean wind speed, so the remaining
    spread reflects performance, not weather.
    """
    from app.models.windfarm import Windfarm
    from app.services.power_curve_service import PowerCurveService
    from app.services.wind_normalisation_service import WindNormalisationService

    # Resolve rated capacity.
    wf_row = (
        await db.execute(
            select(Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
        )
    ).first()
    rated_mw = float(wf_row[0]) if wf_row and wf_row[0] is not None else None
    if not rated_mw or rated_mw <= 0:
        raise HTTPException(
            status_code=404,
            detail="Windfarm has no rated capacity recorded.",
        )

    norm_svc = WindNormalisationService(db)
    curve_lookup = await norm_svc._load_curve_lookup(windfarm_id, reference)
    if not curve_lookup:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No overall_clean power curve fitted for this windfarm at the "
                f"'{reference}' reference. Run the performance pipeline first."
            ),
        )

    pcs = PowerCurveService(db)
    df = await pcs._load_hourly_data(windfarm_id, start_year, end_year, rated_mw)
    if df.empty:
        return WindNormalisedHourlyResponse(
            windfarm_id=windfarm_id,
            reference_curve=reference,
            reference_wind_speed_mps=0.0,
            long_run_avg_norm_ratio=0.0,
            qualifying_hours=0,
            points=[],
        )

    hourly = WindNormalisationService.compute_hourly_ratios(df, curve_lookup, rated_mw)
    if hourly.empty:
        return WindNormalisedHourlyResponse(
            windfarm_id=windfarm_id,
            reference_curve=reference,
            reference_wind_speed_mps=float(df["wind_speed"].mean()) if "wind_speed" in df.columns else 0.0,
            long_run_avg_norm_ratio=0.0,
            qualifying_hours=0,
            points=[],
        )

    # Reference wind = long-run mean across qualifying hours. Snap to nearest
    # 1 m/s bin (matches the curve_lookup grid) so curve(reference_wind) is
    # always defined.
    ref_wind = float(hourly["wind_speed"].mean())
    ref_bin = float(int(round(ref_wind)))
    # Walk down if the rounded bin missing from the curve (rare — e.g. wind
    # bin 6 m/s outside the fitted band).
    while ref_bin not in curve_lookup and ref_bin > 0:
        ref_bin -= 1.0
    ref_expected_pu = curve_lookup.get(ref_bin, 0.0)
    ref_expected_mw = ref_expected_pu * rated_mw
    long_run_norm_ratio = float(hourly["norm_ratio"].mean())

    points = [
        WindNormalisedHourPoint(
            hour=row.hour,
            actual_mwh=float(row.actual_mw),
            expected_mwh=float(row.expected_mw),
            wind_normalised_mwh=float(row.norm_ratio) * ref_expected_mw,
            norm_ratio=float(row.norm_ratio),
            wind_speed=float(row.wind_speed),
        )
        for row in hourly.itertuples()
    ]

    return WindNormalisedHourlyResponse(
        windfarm_id=windfarm_id,
        reference_curve=reference,
        reference_wind_speed_mps=ref_wind,
        long_run_avg_norm_ratio=long_run_norm_ratio,
        qualifying_hours=len(points),
        points=points,
    )
