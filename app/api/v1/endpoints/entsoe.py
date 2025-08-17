"""API endpoints for ENTSOE integration."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.schemas.entsoe import (
    AreaCodeResponse,
    GenerationDataRequest,
    GenerationDataResponse,
)
from app.services.entsoe_client import ENTSOEClient
from app.services.entsoe_service import ENTSOEService
from app.services.entsoe_storage_service import ENTSOEStorageService

logger = structlog.get_logger()
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
            store_data=request.store_data,
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
            GenerationUnit.windfarm_id == windfarm_id,
            GenerationUnit.is_active == True,
            GenerationUnit.source == "ENTSOE",
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


@router.post("/generation/windfarm/{windfarm_id}/store")
async def store_windfarm_generation_data(
    windfarm_id: int,
    request: Dict[str, Any],
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Store fetched windfarm generation data to database.
    
    This endpoint stores previously fetched ENTSOE data for a specific windfarm.
    It handles overlapping data intelligently by using upsert operations.
    """
    from app.services.windfarm import WindfarmService
    
    # Validate windfarm exists
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")
    
    # Extract data from request
    data = request.get("data", [])
    
    if not data:
        raise HTTPException(status_code=400, detail="No data to store")
    
    storage_service = ENTSOEStorageService(db)
    
    try:
        # Store the data
        storage_result = await storage_service.store_generation_data(
            data=data,
            user=current_user,
        )
        
        if not storage_result.get("success"):
            raise HTTPException(
                status_code=500,
                detail=storage_result.get("error") or "Failed to store data"
            )
        
        # Add windfarm info to response
        storage_result["windfarm_id"] = windfarm_id
        storage_result["windfarm_name"] = windfarm.name
        
        return storage_result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error storing windfarm generation data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/generation/history")
async def get_stored_generation_data(
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    area_codes: Optional[List[str]] = Query(None, description="Filter by area codes"),
    production_types: Optional[List[str]] = Query(None, description="Filter by production types"),
    limit: int = Query(10000, description="Maximum number of records", le=50000),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieve stored historical generation data from database.
    
    - **start_date**: Start date for data query
    - **end_date**: End date for data query
    - **area_codes**: Optional filter by area codes
    - **production_types**: Optional filter by production types
    - **limit**: Maximum number of records to return (max 50000)
    """
    storage_service = ENTSOEStorageService(db)
    
    try:
        data = await storage_service.get_stored_data(
            start_date=start_date,
            end_date=end_date,
            area_codes=area_codes,
            production_types=production_types,
            limit=limit,
        )
        
        return {
            "data": data,
            "metadata": {
                "total_records": len(data),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "area_codes": area_codes,
                "production_types": production_types,
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/generation/windfarm/{windfarm_id}/availability")
async def get_windfarm_data_availability(
    windfarm_id: int,
    year: int = Query(..., description="Year to check availability"),
    month: int = Query(..., description="Month to check availability (1-12)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get data availability for a specific windfarm and month.
    Returns a list of dates that have data stored in the database.
    """
    from app.services.windfarm import WindfarmService
    from sqlalchemy import func, cast, Date
    from calendar import monthrange
    
    # Validate windfarm exists
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")
    
    # Get control area for the windfarm
    if not windfarm.control_area_id:
        return {
            "windfarm_id": windfarm_id,
            "windfarm_name": windfarm.name,
            "year": year,
            "month": month,
            "dates_with_data": [],
            "message": "Windfarm has no control area assigned"
        }
    
    from app.models.control_area import ControlArea
    stmt = select(ControlArea).where(ControlArea.id == windfarm.control_area_id)
    result = await db.execute(stmt)
    control_area = result.scalar_one_or_none()
    
    if not control_area:
        raise HTTPException(status_code=404, detail="Control area not found")
    
    try:
        # Calculate date range for the month
        _, last_day = monthrange(year, month)
        start_date = datetime(year, month, 1)
        end_date = datetime(year, month, last_day, 23, 59, 59)
        
        # Query distinct dates that have data for this area
        from app.models.entsoe_generation_data import ENTSOEGenerationData
        
        stmt = (
            select(cast(ENTSOEGenerationData.timestamp, Date))
            .distinct()
            .where(
                ENTSOEGenerationData.area_code == control_area.code,
                ENTSOEGenerationData.timestamp >= start_date,
                ENTSOEGenerationData.timestamp <= end_date,
                ENTSOEGenerationData.production_type == "wind"  # Windfarms are wind
            )
            .order_by(cast(ENTSOEGenerationData.timestamp, Date))
        )
        
        result = await db.execute(stmt)
        dates_with_data = [date_val for (date_val,) in result.all()]
        
        # Also get summary statistics for the month
        stmt_stats = (
            select(
                func.count(ENTSOEGenerationData.id).label("total_records"),
                func.min(ENTSOEGenerationData.timestamp).label("earliest"),
                func.max(ENTSOEGenerationData.timestamp).label("latest")
            )
            .where(
                ENTSOEGenerationData.area_code == control_area.code,
                ENTSOEGenerationData.timestamp >= start_date,
                ENTSOEGenerationData.timestamp <= end_date,
                ENTSOEGenerationData.production_type == "wind"
            )
        )
        
        result_stats = await db.execute(stmt_stats)
        stats = result_stats.one()
        
        return {
            "windfarm_id": windfarm_id,
            "windfarm_name": windfarm.name,
            "control_area": {
                "code": control_area.code,
                "name": control_area.name
            },
            "year": year,
            "month": month,
            "dates_with_data": [date.isoformat() for date in dates_with_data],
            "statistics": {
                "total_records": stats.total_records or 0,
                "earliest_data": stats.earliest.isoformat() if stats.earliest else None,
                "latest_data": stats.latest.isoformat() if stats.latest else None,
                "days_with_data": len(dates_with_data),
                "days_in_month": last_day
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting data availability: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/generation/windfarm/{windfarm_id}/stored")
async def get_stored_windfarm_data(
    windfarm_id: int,
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get stored generation data for a specific windfarm from the database.
    Also identifies gaps in the data for the specified date range.
    """
    from app.services.windfarm import WindfarmService
    from app.models.entsoe_generation_data import ENTSOEGenerationData
    from app.models.control_area import ControlArea
    from sqlalchemy import func
    from datetime import timedelta
    
    # Validate windfarm exists
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")
    
    # Get control area for the windfarm
    if not windfarm.control_area_id:
        raise HTTPException(
            status_code=400, detail="Windfarm does not have a control area assigned"
        )
    
    stmt = select(ControlArea).where(ControlArea.id == windfarm.control_area_id)
    result = await db.execute(stmt)
    control_area = result.scalar_one_or_none()
    
    if not control_area:
        raise HTTPException(status_code=404, detail="Control area not found")
    
    try:
        # Query stored data for the windfarm's control area
        stmt = (
            select(ENTSOEGenerationData)
            .where(
                ENTSOEGenerationData.area_code == control_area.code,
                ENTSOEGenerationData.timestamp >= start_date,
                ENTSOEGenerationData.timestamp <= end_date,
                ENTSOEGenerationData.production_type == "wind"
            )
            .order_by(ENTSOEGenerationData.timestamp)
        )
        
        result = await db.execute(stmt)
        stored_data = result.scalars().all()
        
        # Convert to list of dicts
        data_list = []
        timestamps_set = set()
        
        for record in stored_data:
            data_list.append({
                "timestamp": record.timestamp.isoformat(),
                "area_code": record.area_code,
                "production_type": record.production_type,
                "value": float(record.value) if record.value else 0,
                "unit": record.unit
            })
            # Track hourly timestamps (normalize to hour)
            hour_timestamp = record.timestamp.replace(minute=0, second=0, microsecond=0)
            timestamps_set.add(hour_timestamp)
        
        # Identify gaps in hourly data
        gaps = []
        expected_hours = []
        current = start_date.replace(minute=0, second=0, microsecond=0)
        end_normalized = end_date.replace(minute=0, second=0, microsecond=0)
        current_gap_start = None
        
        while current <= end_normalized:
            expected_hours.append(current)
            if current not in timestamps_set:
                # Start a new gap if we're not in one
                if current_gap_start is None:
                    current_gap_start = current
            else:
                # We have data, so close any open gap
                if current_gap_start is not None:
                    gap_end = current - timedelta(hours=1)  # Last hour without data
                    hours_missing = int((gap_end - current_gap_start).total_seconds() / 3600) + 1
                    gaps.append({
                        "start": current_gap_start.isoformat(),
                        "end": gap_end.isoformat(),
                        "hours": hours_missing
                    })
                    current_gap_start = None
            current += timedelta(hours=1)
        
        # Close any remaining gap at the end
        if current_gap_start is not None:
            gap_end = end_normalized
            hours_missing = int((gap_end - current_gap_start).total_seconds() / 3600) + 1
            gaps.append({
                "start": current_gap_start.isoformat(),
                "end": gap_end.isoformat(),
                "hours": hours_missing
            })
        
        # Calculate statistics
        total_expected_hours = len(expected_hours)
        hours_with_data = len(timestamps_set)
        coverage_percentage = (hours_with_data / total_expected_hours * 100) if total_expected_hours > 0 else 0
        
        return {
            "windfarm": {
                "id": windfarm.id,
                "code": windfarm.code,
                "name": windfarm.name,
                "control_area": {
                    "id": control_area.id,
                    "code": control_area.code,
                    "name": control_area.name
                }
            },
            "data": data_list,
            "gaps": gaps,
            "statistics": {
                "total_records": len(data_list),
                "expected_hours": total_expected_hours,
                "hours_with_data": hours_with_data,
                "missing_hours": total_expected_hours - hours_with_data,
                "coverage_percentage": round(coverage_percentage, 2),
                "gap_count": len(gaps)
            },
            "metadata": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "area_code": control_area.code,
                "production_type": "wind"
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting stored windfarm data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
