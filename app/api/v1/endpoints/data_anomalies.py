"""API endpoints for data anomaly detection and management."""

import logging
from typing import Any, Dict
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.services.data_anomaly_service import DataAnomalyService
from app.schemas.data_anomaly import (
    DataAnomalyResponse,
    DataAnomalyUpdate,
    DataAnomalyStatusUpdate,
    AnomalyDetectionRequest,
    AnomalyDetectionResponse,
    ReaggregationRequest,
    ReaggregationResponse,
    AnomalyListFilters,
    AnomalyListResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/detect", response_model=AnomalyDetectionResponse)
async def detect_anomalies(
    request: AnomalyDetectionRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> AnomalyDetectionResponse:
    """
    Detect data anomalies for specified windfarms and date range.

    This endpoint scans generation data for anomalies like capacity factor > threshold.
    It groups consecutive problematic periods into single anomaly entries.

    NOTE: This is a read-only operation - anomalies are NOT saved to the database.
    Results are returned for review only. Use the save endpoint if you want to persist them.

    Args:
        request: Detection parameters (windfarm_ids, date range, thresholds)
        current_user: Authenticated user
        db: Database session

    Returns:
        Detection results including all detected anomalies (not saved to DB)
    """
    service = DataAnomalyService(db)

    try:
        anomaly_dicts, summary = await service.detect_anomalies(request)

        return AnomalyDetectionResponse(
            anomalies_detected=len(anomaly_dicts),
            anomalies_created=0,  # Not saved to database
            anomalies=anomaly_dicts,
            detection_summary=summary
        )

    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to detect anomalies: {str(e)}"
        )


@router.get("", response_model=AnomalyListResponse)
async def list_anomalies(
    windfarm_id: int | None = Query(None, description="Filter by windfarm ID"),
    generation_unit_id: int | None = Query(None, description="Filter by generation unit ID"),
    anomaly_type: str | None = Query(None, description="Filter by anomaly type"),
    status: str | None = Query(None, description="Filter by status"),
    severity: str | None = Query(None, description="Filter by severity"),
    start_date: str | None = Query(None, description="Filter by period start date (ISO format)"),
    end_date: str | None = Query(None, description="Filter by period end date (ISO format)"),
    is_active: bool = Query(True, description="Filter by active status"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Items per page"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> AnomalyListResponse:
    """
    Get a paginated list of data anomalies with optional filtering.

    Args:
        Various filter parameters
        current_user: Authenticated user
        db: Database session

    Returns:
        Paginated list of anomalies
    """
    from datetime import datetime

    # Build filters object
    filters = AnomalyListFilters(
        windfarm_id=windfarm_id,
        generation_unit_id=generation_unit_id,
        anomaly_type=anomaly_type,
        status=status,
        severity=severity,
        start_date=datetime.fromisoformat(start_date) if start_date else None,
        end_date=datetime.fromisoformat(end_date) if end_date else None,
        is_active=is_active,
        page=page,
        page_size=page_size
    )

    service = DataAnomalyService(db)

    try:
        anomalies, total = await service.get_anomalies(filters)

        total_pages = (total + page_size - 1) // page_size  # Ceiling division

        return AnomalyListResponse(
            anomalies=anomalies,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    except Exception as e:
        logger.error(f"Error listing anomalies: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list anomalies: {str(e)}"
        )


@router.get("/{anomaly_id}", response_model=DataAnomalyResponse)
async def get_anomaly(
    anomaly_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> DataAnomalyResponse:
    """
    Get detailed information about a specific anomaly.

    Args:
        anomaly_id: Anomaly ID
        current_user: Authenticated user
        db: Database session

    Returns:
        Anomaly details
    """
    service = DataAnomalyService(db)

    anomaly = await service.get_anomaly_by_id(anomaly_id)

    if not anomaly:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anomaly {anomaly_id} not found"
        )

    return anomaly


@router.patch("/{anomaly_id}/status", response_model=DataAnomalyResponse)
async def update_anomaly_status(
    anomaly_id: int,
    status_update: DataAnomalyStatusUpdate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> DataAnomalyResponse:
    """
    Update the status of an anomaly.

    Common workflow:
    - pending → investigating (while investigating the issue)
    - investigating → resolved (after fixing the data or determining it's correct)
    - investigating → false_positive (if it's not actually an anomaly)
    - investigating → ignored (if we decide not to fix it)

    Args:
        anomaly_id: Anomaly ID
        status_update: New status and optional resolution notes
        current_user: Authenticated user
        db: Database session

    Returns:
        Updated anomaly
    """
    service = DataAnomalyService(db)

    anomaly = await service.update_anomaly_status(
        anomaly_id=anomaly_id,
        status=status_update.status,
        resolution_notes=status_update.resolution_notes,
        user_id=current_user.id
    )

    if not anomaly:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anomaly {anomaly_id} not found"
        )

    return anomaly


@router.patch("/{anomaly_id}", response_model=DataAnomalyResponse)
async def update_anomaly(
    anomaly_id: int,
    update_data: DataAnomalyUpdate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> DataAnomalyResponse:
    """
    Update anomaly fields (severity, description, metadata, etc.).

    Args:
        anomaly_id: Anomaly ID
        update_data: Fields to update
        current_user: Authenticated user
        db: Database session

    Returns:
        Updated anomaly
    """
    service = DataAnomalyService(db)

    anomaly = await service.update_anomaly(anomaly_id, update_data)

    if not anomaly:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anomaly {anomaly_id} not found"
        )

    return anomaly


@router.delete("/{anomaly_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_anomaly(
    anomaly_id: int,
    hard_delete: bool = Query(False, description="Permanently delete instead of soft delete"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> None:
    """
    Delete an anomaly.

    By default performs a soft delete (sets is_active=False).
    Use hard_delete=true to permanently remove the record.

    Args:
        anomaly_id: Anomaly ID
        hard_delete: Whether to permanently delete
        current_user: Authenticated user
        db: Database session
    """
    service = DataAnomalyService(db)

    if hard_delete:
        success = await service.hard_delete_anomaly(anomaly_id)
    else:
        success = await service.delete_anomaly(anomaly_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anomaly {anomaly_id} not found"
        )


@router.post("/{anomaly_id}/reaggregate", response_model=ReaggregationResponse)
async def reaggregate_anomaly_period(
    anomaly_id: int,
    sources: list[str] | None = Query(None, description="Specific sources to re-aggregate"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> ReaggregationResponse:
    """
    Re-aggregate generation data for the period affected by this anomaly.

    This will:
    1. Delete existing aggregated data for the anomaly's period
    2. Re-process raw data using the aggregation pipeline
    3. Recalculate capacity factors and other metrics

    After re-aggregation, you should re-run anomaly detection to verify the fix.

    Args:
        anomaly_id: Anomaly ID
        sources: Optional list of specific sources to re-aggregate (e.g., ["ENTSOE", "ELEXON"])
        current_user: Authenticated user
        db: Database session

    Returns:
        Re-aggregation results
    """
    service = DataAnomalyService(db)

    # Get the anomaly to determine the period
    anomaly = await service.get_anomaly_by_id(anomaly_id)
    if not anomaly:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anomaly {anomaly_id} not found"
        )

    try:
        result = await service.reaggregate_period(
            start_date=anomaly.period_start,
            end_date=anomaly.period_end,
            sources=sources,
            windfarm_id=anomaly.windfarm_id,
            generation_unit_id=anomaly.generation_unit_id
        )

        return ReaggregationResponse(**result)

    except Exception as e:
        logger.error(f"Error re-aggregating period for anomaly {anomaly_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Re-aggregation failed: {str(e)}"
        )


@router.post("/reaggregate", response_model=ReaggregationResponse)
async def reaggregate_custom_period(
    request: ReaggregationRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
) -> ReaggregationResponse:
    """
    Re-aggregate generation data for a custom period and windfarm/unit.

    This is useful when you want to re-process data without an associated anomaly,
    or for multiple windfarms/units at once.

    Args:
        request: Re-aggregation parameters
        current_user: Authenticated user
        db: Database session

    Returns:
        Re-aggregation results
    """
    service = DataAnomalyService(db)

    try:
        # Combine single source and multiple sources
        sources = None
        if request.source:
            sources = [request.source]
        elif request.sources:
            sources = request.sources

        result = await service.reaggregate_period(
            start_date=request.start_date,
            end_date=request.end_date,
            sources=sources,
            windfarm_id=request.windfarm_id,
            generation_unit_id=request.generation_unit_id
        )

        return ReaggregationResponse(**result)

    except Exception as e:
        logger.error(f"Error re-aggregating custom period: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Re-aggregation failed: {str(e)}"
        )
