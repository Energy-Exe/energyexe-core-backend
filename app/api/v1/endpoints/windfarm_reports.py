"""API endpoints for windfarm performance reports."""

from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
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


@router.get("/windfarms/{windfarm_id}/report-data", response_model=WindfarmReportData)
async def get_windfarm_report_data(
    windfarm_id: int,
    start_date: datetime = Query(..., description="Start date for analysis period"),
    end_date: datetime = Query(..., description="End date for analysis period"),
    include_peer_groups: Optional[str] = Query(
        None,
        description="Comma-separated list of peer groups: bidzone,country,owner,turbine. If not provided, includes all available"
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
    """
    try:
        service = WindfarmReportService(db)

        # Parse peer groups if provided
        peer_groups = None
        if include_peer_groups:
            peer_groups = [g.strip() for g in include_peer_groups.split(',')]

        report_data = await service.generate_report_data(
            windfarm_id=windfarm_id,
            start_date=start_date,
            end_date=end_date,
            include_peer_groups=peer_groups
        )

        return report_data

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
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
