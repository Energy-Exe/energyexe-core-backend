"""API endpoints for windfarm performance reports."""

import hashlib
import json
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.core.redis import get_cached_report, cache_report, invalidate_report_cache
from app.models.user import User
from app.services.windfarm_report_service import WindfarmReportService
from app.schemas.windfarm_report import (
    WindfarmReportData,
    ReportGenerationRequest,
    PeerComparisonRequest,
    PeerComparisonData,
    CapacityFactorDistributionRequest,
    BoxPlotData
)

router = APIRouter()


def _generate_cache_key(
    windfarm_id: int,
    start_date: datetime,
    end_date: datetime,
    include_peer_groups: Optional[str],
    generate_commentary: bool
) -> str:
    """
    Generate cache key for report data.

    Normalizes dates to day-level precision to ensure cache hits across refreshes.
    Frontend sends slightly different timestamps each time (milliseconds vary),
    but we want the same cache entry for reports covering the same date range.
    """
    # Normalize dates to just YYYY-MM-DD (ignore time component)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    key_parts = [
        f"report:v1:{windfarm_id}",
        start_date_str,
        end_date_str,
        include_peer_groups or "all",
        str(generate_commentary)
    ]
    # Create hash to keep key length reasonable
    key_string = ":".join(key_parts)
    key_hash = hashlib.sha256(key_string.encode()).hexdigest()[:16]
    return f"windfarm_report:{windfarm_id}:{key_hash}"


@router.get("/windfarms/{windfarm_id}/report-data", response_model=WindfarmReportData)
async def get_windfarm_report_data(
    windfarm_id: int,
    start_date: datetime = Query(..., description="Start date for analysis period"),
    end_date: datetime = Query(..., description="End date for analysis period"),
    include_peer_groups: Optional[str] = Query(
        None,
        description="Comma-separated list of peer groups: bidzone,country,owner,turbine. If not provided, includes all available"
    ),
    generate_commentary: bool = Query(
        default=False,
        description="Generate AI commentary (costs money, requires API key)"
    ),
    force_regenerate: bool = Query(
        default=False,
        description="Force regeneration, bypassing cache"
    ),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> WindfarmReportData:
    """
    Generate complete report data for a windfarm.

    Returns all sections needed for the performance report:
    - Windfarm details and context
    - Performance summary metrics
    - Rankings within peer groups
    - Peer comparison data (timeseries, distributions, heatmaps)
    - Performance highlights

    This is the main endpoint called when the Report tab is opened.

    **Caching**: Results are cached in Redis for 1 hour. Use force_regenerate=true to bypass cache.
    """
    import structlog
    logger = structlog.get_logger(__name__)

    # Generate cache key
    cache_key = _generate_cache_key(
        windfarm_id,
        start_date,
        end_date,
        include_peer_groups,
        generate_commentary
    )

    # Check cache unless force regenerate
    if not force_regenerate:
        cached_data = await get_cached_report(cache_key)
        if cached_data:
            logger.info("report_served_from_cache", windfarm_id=windfarm_id, cache_key=cache_key)
            return WindfarmReportData(**cached_data)

    try:
        service = WindfarmReportService(db)

        # Parse peer groups if provided
        peer_groups = None
        if include_peer_groups:
            peer_groups = [g.strip() for g in include_peer_groups.split(',')]

        logger.info("generating_report", windfarm_id=windfarm_id, from_cache=False, force_regenerate=force_regenerate)

        report_data = await service.generate_report_data(
            windfarm_id=windfarm_id,
            start_date=start_date,
            end_date=end_date,
            include_peer_groups=peer_groups,
            generate_commentary=generate_commentary
        )

        # Cache the result (1 hour TTL)
        report_dict = report_data.model_dump()
        await cache_report(cache_key, report_dict, ttl_seconds=3600)
        logger.info("report_cached", windfarm_id=windfarm_id, cache_key=cache_key)

        return report_data

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        import traceback
        logger.error("report_generation_failed", error=str(e), traceback=traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate report: {str(e)}"
        )


@router.post("/windfarms/{windfarm_id}/peer-comparison", response_model=PeerComparisonData)
async def get_peer_comparison(
    windfarm_id: int,
    request: PeerComparisonRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> PeerComparisonData:
    """
    Get detailed peer comparison data for a specific peer group.

    This endpoint can be called individually if the frontend wants to
    load peer comparisons one at a time or refresh specific sections.
    """
    try:
        service = WindfarmReportService(db)

        # Get windfarm and peer group info
        from app.services.peer_analysis_service import PeerAnalysisService
        peer_service = PeerAnalysisService(db)

        windfarm = await peer_service.get_windfarm_with_relations(windfarm_id)
        if not windfarm:
            raise HTTPException(status_code=404, detail="Windfarm not found")

        peer_groups = await peer_service.get_all_peer_groups(windfarm_id)

        if request.peer_group not in peer_groups:
            raise HTTPException(
                status_code=400,
                detail=f"Peer group '{request.peer_group}' not available for this windfarm"
            )

        peer_info = peer_groups[request.peer_group]

        peer_comparison = await service._generate_peer_comparison(
            windfarm_id=windfarm_id,
            windfarm_name=windfarm.name,
            peer_type=request.peer_group,
            peer_info=peer_info,
            start_date=request.start_date,
            end_date=request.end_date
        )

        return peer_comparison

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate peer comparison: {str(e)}"
        )


@router.post("/windfarms/{windfarm_id}/capacity-factor-distribution", response_model=List[BoxPlotData])
async def get_capacity_factor_distribution(
    windfarm_id: int,
    request: CapacityFactorDistributionRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> List[BoxPlotData]:
    """
    Get box plot distribution data for capacity factors.

    Returns statistical distribution for:
    - Target windfarm
    - Peer group aggregate
    """
    try:
        service = WindfarmReportService(db)
        from app.services.peer_analysis_service import PeerAnalysisService
        from app.services.statistical_analysis import StatisticalAnalysis

        peer_service = PeerAnalysisService(db)
        stats = StatisticalAnalysis()

        windfarm = await peer_service.get_windfarm_with_relations(windfarm_id)
        if not windfarm:
            raise HTTPException(status_code=404, detail="Windfarm not found")

        # Get target windfarm monthly CFs
        target_monthly_cfs = await service._get_monthly_capacity_factors(
            windfarm_id,
            request.start_date,
            request.end_date
        )

        # Get peer group data
        peer_groups = await peer_service.get_all_peer_groups(windfarm_id)
        if request.peer_group not in peer_groups:
            raise HTTPException(
                status_code=400,
                detail=f"Peer group '{request.peer_group}' not available"
            )

        peer_info = peer_groups[request.peer_group]
        peer_monthly_data = await service._get_peer_group_monthly_data(
            request.peer_group,
            peer_info.group_id,
            request.start_date,
            request.end_date
        )

        # Flatten peer data
        all_peer_values = [
            v for month_data in peer_monthly_data.values()
            for v in month_data.values()
        ]

        # Calculate box plots
        distribution = [
            stats.calculate_box_plot_data(target_monthly_cfs, windfarm.name),
            stats.calculate_box_plot_data(all_peer_values, f"{peer_info.group_name} Peers")
        ]

        return distribution

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate distribution: {str(e)}"
        )


@router.get("/windfarms/{windfarm_id}/rankings")
async def get_windfarm_rankings(
    windfarm_id: int,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get windfarm rankings within all peer groups.

    Returns ranking position and total count for:
    - Bidzone (if applicable)
    - Country
    - Owner portfolio (if applicable)
    - Turbine model group (if applicable)
    """
    try:
        service = WindfarmReportService(db)
        from app.services.peer_analysis_service import PeerAnalysisService

        peer_service = PeerAnalysisService(db)

        windfarm = await peer_service.get_windfarm_with_relations(windfarm_id)
        if not windfarm:
            raise HTTPException(status_code=404, detail="Windfarm not found")

        peer_groups = await peer_service.get_all_peer_groups(windfarm_id)
        rankings = await service._calculate_rankings(
            windfarm_id,
            start_date,
            end_date,
            peer_groups
        )

        return rankings

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate rankings: {str(e)}"
        )


@router.get("/windfarms/{windfarm_id}/peer-groups")
async def get_available_peer_groups(
    windfarm_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all available peer groups for a windfarm.

    Returns info about which peer group comparisons are possible.
    Useful for UI to show/hide peer group options.
    """
    try:
        from app.services.peer_analysis_service import PeerAnalysisService

        peer_service = PeerAnalysisService(db)
        peer_groups = await peer_service.get_all_peer_groups(windfarm_id)

        return {
            'windfarm_id': windfarm_id,
            'available_peer_groups': list(peer_groups.keys()),
            'peer_group_details': peer_groups
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get peer groups: {str(e)}"
        )


@router.get("/health")
async def health_check():
    """Health check endpoint for report service."""
    return {"status": "ok", "service": "windfarm_reports"}
