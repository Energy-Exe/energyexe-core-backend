"""API endpoints for data export."""

from datetime import date
from typing import List, Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.services.generation_export_service import GenerationExportService

router = APIRouter()

# Maximum date range for exports (in days)
MAX_EXPORT_DATE_RANGE_DAYS = 730  # 2 years
# Maximum number of windfarms per export
MAX_WINDFARMS_PER_EXPORT = 500


@router.get("/generation/csv")
async def export_generation_csv(
    # Windfarm filters
    windfarm_ids: Optional[List[int]] = Query(
        None,
        description="Specific windfarm IDs to export"
    ),
    country_id: Optional[int] = Query(
        None,
        description="Filter by country ID"
    ),
    region_id: Optional[int] = Query(
        None,
        description="Filter by region ID"
    ),
    state_id: Optional[int] = Query(
        None,
        description="Filter by state ID"
    ),
    bidzone_id: Optional[int] = Query(
        None,
        description="Filter by bidzone ID (energy market zone)"
    ),
    market_balance_area_id: Optional[int] = Query(
        None,
        description="Filter by market balance area ID"
    ),
    control_area_id: Optional[int] = Query(
        None,
        description="Filter by control area ID"
    ),
    location_type: Optional[Literal["onshore", "offshore"]] = Query(
        None,
        description="Filter by location type"
    ),
    status: Optional[Literal["operational", "decommissioned", "under_installation", "expanded"]] = Query(
        None,
        description="Filter by windfarm status"
    ),
    foundation_type: Optional[Literal["fixed", "floating"]] = Query(
        None,
        description="Filter by foundation type (for offshore)"
    ),
    min_capacity_mw: Optional[float] = Query(
        None,
        description="Minimum nameplate capacity (MW)"
    ),
    max_capacity_mw: Optional[float] = Query(
        None,
        description="Maximum nameplate capacity (MW)"
    ),
    # Data source filter
    source: Optional[Literal["ENTSOE", "ELEXON", "EIA", "TAIPOWER", "NVE"]] = Query(
        None,
        description="Filter by data source"
    ),
    # Time range (required)
    start_date: date = Query(..., description="Start date for export (inclusive)"),
    end_date: date = Query(..., description="End date for export (inclusive)"),
    # Granularity
    granularity: Literal["daily", "monthly"] = Query(
        "daily",
        description="Aggregation granularity"
    ),
    # Options
    include_metadata: bool = Query(
        True,
        description="Include windfarm metadata columns in output"
    ),
    # Dependencies
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Export generation data as CSV file.

    Supports filtering windfarms by:
    - Specific windfarm IDs
    - Country, region, state, bidzone
    - Market balance area, control area
    - Location type (onshore/offshore)
    - Status (operational, decommissioned, etc.)
    - Foundation type (fixed/floating)
    - Capacity range (min/max MW)

    Data is aggregated to daily or monthly granularity.
    Returns a streaming CSV download.
    """

    # Validate date range
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date must be before or equal to end_date"
        )

    # Validate date range is not too large
    date_range_days = (end_date - start_date).days
    if date_range_days > MAX_EXPORT_DATE_RANGE_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Date range cannot exceed {MAX_EXPORT_DATE_RANGE_DAYS} days ({MAX_EXPORT_DATE_RANGE_DAYS // 365} years). "
                   f"Your range is {date_range_days} days."
        )

    # Validate at least one filter is provided
    has_filter = any([
        windfarm_ids,
        country_id,
        region_id,
        state_id,
        bidzone_id,
        market_balance_area_id,
        control_area_id,
        location_type,
        status,
        foundation_type,
        min_capacity_mw is not None,
        max_capacity_mw is not None,
    ])

    if not has_filter:
        raise HTTPException(
            status_code=400,
            detail="At least one filter parameter is required (e.g., country_id, windfarm_ids, location_type)"
        )

    service = GenerationExportService(db)

    # Get filtered windfarm IDs
    windfarm_ids_filtered = await service.get_filtered_windfarm_ids(
        windfarm_ids=windfarm_ids,
        country_id=country_id,
        region_id=region_id,
        state_id=state_id,
        bidzone_id=bidzone_id,
        market_balance_area_id=market_balance_area_id,
        control_area_id=control_area_id,
        location_type=location_type,
        status=status,
        foundation_type=foundation_type,
        min_capacity_mw=min_capacity_mw,
        max_capacity_mw=max_capacity_mw,
    )

    if not windfarm_ids_filtered:
        raise HTTPException(
            status_code=404,
            detail="No windfarms found matching the filter criteria"
        )

    # Validate not too many windfarms
    if len(windfarm_ids_filtered) > MAX_WINDFARMS_PER_EXPORT:
        raise HTTPException(
            status_code=400,
            detail=f"Too many windfarms match your filter ({len(windfarm_ids_filtered)}). "
                   f"Maximum is {MAX_WINDFARMS_PER_EXPORT}. Please narrow your filter criteria."
        )

    # Generate filename
    filename = service.generate_filename(granularity, start_date, end_date)

    # Create streaming response
    async def csv_generator():
        async for chunk in service.stream_csv_export(
            windfarm_ids=windfarm_ids_filtered,
            start_date=start_date,
            end_date=end_date,
            granularity=granularity,
            source=source,
            include_metadata=include_metadata,
        ):
            yield chunk

    return StreamingResponse(
        csv_generator(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-cache",
        }
    )
