"""API endpoints for ENTSOE integration."""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.schemas.entsoe import (
    AreaCodeResponse,
    FetchHistoryResponse,
    GenerationDataRequest,
    GenerationDataResponse,
)
from app.services.entsoe_client import ENTSOEClient
from app.services.entsoe_historical_service import ENTSOEHistoricalService
from app.services.entsoe_service import ENTSOEService

router = APIRouter()


@router.post("/generation/fetch", response_model=GenerationDataResponse)
async def fetch_generation_data(
    request: GenerationDataRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch real-time generation data from ENTSOE API.

    - **start_date**: Start date for data (ISO format)
    - **end_date**: End date for data (ISO format)
    - **area_codes**: List of area codes (e.g., ['DE_LU', 'FR'])
    - **production_types**: List of production types (['wind', 'solar'])
    """
    service = ENTSOEService(db)

    try:
        result = await service.fetch_real_time_generation(
            start_date=request.start_date,
            end_date=request.end_date,
            area_codes=request.area_codes,
            production_types=request.production_types,
            current_user=current_user,
        )
        return GenerationDataResponse(**result)

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/generation/areas", response_model=List[AreaCodeResponse])
async def get_available_areas(current_user: User = Depends(get_current_active_user)):
    """Get list of available area codes."""
    client = ENTSOEClient()
    areas = client.get_available_areas()

    return [AreaCodeResponse(code=code, name=name) for code, name in areas.items()]


@router.get("/fetch-history", response_model=List[FetchHistoryResponse])
async def get_fetch_history(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None, pattern="^(pending|success|failed|partial)$"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get history of ENTSOE data fetch operations."""
    service = ENTSOEService(db)
    history = await service.get_fetch_history(limit, offset, status)
    return history


@router.get("/generation/historical")
async def get_historical_generation_data(
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    area_codes: str = Query(..., description="Comma-separated area codes"),
    production_types: str = Query("wind,solar", description="Comma-separated production types"),
    aggregation: str = Query(
        "hourly", pattern="^(raw|hourly|daily)$", description="Aggregation level"
    ),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get historical generation data from TimescaleDB."""
    service = ENTSOEHistoricalService(db)

    # Parse comma-separated values
    area_list = [code.strip() for code in area_codes.split(",")]
    type_list = [type.strip() for type in production_types.split(",")]

    data = await service.get_stored_generation_data(
        start_date=start_date,
        end_date=end_date,
        area_codes=area_list,
        production_types=type_list,
        aggregation=aggregation,
    )

    return {
        "data": data,
        "metadata": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "area_codes": area_list,
            "production_types": type_list,
            "aggregation": aggregation,
            "record_count": len(data),
        },
    }


@router.get("/generation/availability")
async def get_data_availability(
    area_codes: Optional[str] = Query(None, description="Comma-separated area codes (optional)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get information about available historical data."""
    service = ENTSOEHistoricalService(db)

    # Parse area codes if provided
    area_list = None
    if area_codes:
        area_list = [code.strip() for code in area_codes.split(",")]

    availability = await service.get_data_availability(area_list)

    return {
        "availability": availability,
        "summary": {"total_areas": len(availability), "areas": list(availability.keys())},
    }


@router.post("/generation/backfill")
async def trigger_backfill(
    days_back: int = Query(30, ge=1, le=365, description="Number of days to backfill"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Trigger historical data backfill (admin only)."""
    # Check if user is admin (you may want to implement proper role checking)
    if not current_user.is_superuser:
        raise HTTPException(status_code=403, detail="Admin access required")

    import asyncio

    from app.cron.entsoe_scheduler import backfill_historical_data

    # Run backfill in background
    asyncio.create_task(backfill_historical_data(days_back))

    return {"message": f"Backfill started for {days_back} days", "status": "running"}


@router.get("/generation/windfarm/{windfarm_id}")
async def get_windfarm_generation_data(
    windfarm_id: int,
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get generation data for a specific windfarm from ENTSOE API.

    This endpoint:
    1. Fetches the windfarm and its control area
    2. Uses the control area code to query ENTSOE API
    3. Returns generation data and attempts to match with generation units
    """
    from app.services.windfarm import WindfarmService

    # Get windfarm with control area
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")

    # Check if windfarm has a control area
    if not windfarm.control_area_id:
        raise HTTPException(
            status_code=400, detail="Windfarm does not have a control area assigned"
        )

    # Get control area code
    from app.models.control_area import ControlArea
    from sqlalchemy import select

    stmt = select(ControlArea).where(ControlArea.id == windfarm.control_area_id)
    result = await db.execute(stmt)
    control_area = result.scalar_one_or_none()

    if not control_area:
        raise HTTPException(status_code=404, detail="Control area not found")

    # Fetch generation data from ENTSOE
    service = ENTSOEService(db)

    try:
        # Use control area code as the ENTSOE area code
        result = await service.fetch_real_time_generation(
            start_date=start_date,
            end_date=end_date,
            area_codes=[control_area.code],
            production_types=["wind"],  # For windfarms, we're interested in wind data
            current_user=current_user,
        )

        # Get generation units for this windfarm
        from app.models.generation_unit import GenerationUnit

        gen_units_stmt = select(GenerationUnit).where(
            GenerationUnit.windfarm_id == windfarm_id, GenerationUnit.is_active == True
        )
        gen_units_result = await db.execute(gen_units_stmt)
        generation_units = gen_units_result.scalars().all()

        # Prepare response with windfarm and generation unit info
        response = {
            "windfarm": {
                "id": windfarm.id,
                "code": windfarm.code,
                "name": windfarm.name,
                "control_area": {
                    "id": control_area.id,
                    "code": control_area.code,
                    "name": control_area.name,
                },
            },
            "generation_units": [
                {
                    "id": unit.id,
                    "code": unit.code,
                    "name": unit.name,
                    "capacity_mw": float(unit.capacity_mw) if unit.capacity_mw else None,
                }
                for unit in generation_units
            ],
            "generation_data": result,
            "metadata": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "area_code": control_area.code,
                "production_type": "wind",
            },
        }

        return response

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
