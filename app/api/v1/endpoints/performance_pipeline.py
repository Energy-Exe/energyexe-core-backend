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
    NormalisationResponse,
    ODIMetricsResponse,
    PerformanceAnomalyResponse,
    PerformanceSummaryResponse,
    PipelineRunRequest,
    PipelineRunResponse,
    PowerCurveBinResponse,
    PowerCurveResponse,
    PPAScenarioRequest,
    PPAScenarioResponse,
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
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get stored power curve bins for a windfarm."""
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

    return PowerCurveResponse(
        windfarm_id=windfarm_id,
        curve_type=curve_type,
        year=year,
        bins=[
            PowerCurveBinResponse(
                wind_bin=float(b.wind_bin),
                q50_pu=float(b.q50_pu) if b.q50_pu else None,
                q90_pu=float(b.q90_pu) if b.q90_pu else None,
                mean_pu=float(b.mean_pu) if b.mean_pu else None,
                mad_pu=float(b.mad_pu) if b.mad_pu else None,
                sample_count=b.sample_count,
            )
            for b in bins
        ],
    )


@router.get("/odi/{windfarm_id}", response_model=List[ODIMetricsResponse])
async def get_odi_metrics(
    windfarm_id: int,
    year: Optional[int] = Query(None),
    period_type: Optional[str] = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get ODI metrics for a windfarm."""
    from app.services.performance_anomaly_service import PerformanceAnomalyService

    service = PerformanceAnomalyService(db)
    metrics = await service.get_odi_metrics(windfarm_id, year)
    if period_type:
        metrics = [m for m in metrics if m["period_type"] == period_type]
    return [ODIMetricsResponse(**m) for m in metrics]


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
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get degradation analysis results."""
    result = await db.execute(
        select(DegradationResult).where(
            DegradationResult.windfarm_id == windfarm_id,
        ).order_by(DegradationResult.reference_curve)
    )
    results = result.scalars().all()
    if not results:
        raise HTTPException(status_code=404, detail="No degradation results found")
    return [DegradationResponse.model_validate(r) for r in results]


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
