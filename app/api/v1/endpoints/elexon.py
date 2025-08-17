"""API endpoints for Elexon integration with data storage."""

from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.models.generation_unit import GenerationUnit
from app.schemas.elexon import (
    ElexonGenerationDataRequest,
    ElexonGenerationDataResponse,
    ElexonDataPoint,
)
from app.services.elexon_client import ElexonClient
from app.services.elexon_storage import ElexonStorageService
from app.services.windfarm import WindfarmService

router = APIRouter()


@router.post("/generation/fetch", response_model=ElexonGenerationDataResponse)
async def fetch_generation_data(
    request: ElexonGenerationDataRequest,
    store_data: bool = Query(False, description="Store fetched data in database"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch real-time generation data from Elexon API and optionally store it.

    - **start_date**: Start date for data (ISO format)
    - **end_date**: End date for data (ISO format)
    - **settlement_period_from**: Optional start settlement period (1-50)
    - **settlement_period_to**: Optional end settlement period (1-50)
    - **bm_units**: Optional list of BM Unit IDs to filter
    - **store_data**: Whether to store the fetched data in database
    """
    client = ElexonClient()

    try:
        df, metadata = await client.fetch_physical_data(
            start=request.start_date,
            end=request.end_date,
            settlement_period_from=request.settlement_period_from,
            settlement_period_to=request.settlement_period_to,
            bm_units=request.bm_units,
        )

        # Convert DataFrame to list of ElexonDataPoint
        data_points = []
        records_to_store = []
        
        if not df.empty:
            for _, row in df.iterrows():
                data_point = ElexonDataPoint(
                    timestamp=row["timestamp"],
                    bm_unit=row["bm_unit"],
                    value=row["value"],
                    unit=row["unit"],
                    settlement_period=row.get("settlement_period"),
                )
                data_points.append(data_point)
                
                if store_data:
                    records_to_store.append({
                        "timestamp": row["timestamp"],
                        "bm_unit": row["bm_unit"],
                        "settlement_period": row.get("settlement_period"),
                        "value": row["value"],
                        "unit": row["unit"],
                    })
        
        # Store data if requested
        stored_count = 0
        if store_data and records_to_store:
            storage_service = ElexonStorageService(db)
            stored_count = await storage_service.store_generation_data(records_to_store, current_user)

        response = ElexonGenerationDataResponse(data=data_points, metadata=metadata)
        
        # Add storage information to metadata
        if store_data:
            response.metadata["stored"] = True
            response.metadata["records_stored"] = stored_count
        
        return response

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/generation/windfarm/{windfarm_id}")
async def get_windfarm_generation_data(
    windfarm_id: int,
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    store_data: bool = Query(True, description="Store fetched data in database"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get generation data for a specific windfarm from Elexon API and store it.

    This endpoint:
    1. Fetches the windfarm and its generation units
    2. Uses generation unit codes as BM Unit IDs to query Elexon API
    3. Stores the data in database
    4. Returns generation data with matched generation units
    """
    # Get windfarm
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")

    # Get generation units for this windfarm with ELEXON source
    gen_units_stmt = select(GenerationUnit).where(
        GenerationUnit.windfarm_id == windfarm_id,
        GenerationUnit.is_active == True,
        GenerationUnit.source == "ELEXON",
    )
    gen_units_result = await db.execute(gen_units_stmt)
    generation_units = gen_units_result.scalars().all()

    if not generation_units:
        raise HTTPException(
            status_code=400,
            detail="No ELEXON generation units found for this windfarm. Please ensure generation units are properly configured with source='ELEXON'.",
        )

    # Fetch and store data using the storage service
    storage_service = ElexonStorageService(db)
    result = await storage_service.fetch_and_store_generation(
        start_date=start_date,
        end_date=end_date,
        bm_units=[unit.code for unit in generation_units],
        generation_units=generation_units,
        current_user=current_user,
        store_data=store_data
    )

    # Prepare response
    response = {
        "windfarm": {
            "id": windfarm.id,
            "code": windfarm.code,
            "name": windfarm.name,
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
            "bm_units_requested": [unit.code for unit in generation_units],
            "stored": result.get("stored", False),
            "records_stored": result.get("records_stored", 0),
            "records_fetched": result.get("records_fetched", 0),
        },
    }

    return response


@router.get("/generation/windfarm/{windfarm_id}/stored")
async def get_stored_windfarm_data(
    windfarm_id: int,
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    include_gaps: bool = Query(True, description="Include gap detection in response"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get stored historical generation data for a specific windfarm.

    This endpoint:
    1. Fetches the windfarm and its generation units
    2. Retrieves stored data from database
    3. Detects gaps in the data
    4. Returns data with gap analysis and statistics
    """
    # Get windfarm
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")

    # Get generation units for this windfarm with ELEXON source
    gen_units_stmt = select(GenerationUnit).where(
        GenerationUnit.windfarm_id == windfarm_id,
        GenerationUnit.is_active == True,
        GenerationUnit.source == "ELEXON",
    )
    gen_units_result = await db.execute(gen_units_stmt)
    generation_units = gen_units_result.scalars().all()

    if not generation_units:
        raise HTTPException(
            status_code=400,
            detail="No ELEXON generation units found for this windfarm.",
        )

    # Get stored data with gap detection
    storage_service = ElexonStorageService(db)
    stored_data = await storage_service.get_stored_data(
        bm_units=[unit.code for unit in generation_units],
        start_date=start_date,
        end_date=end_date,
        include_gaps=include_gaps
    )

    # Format data for response
    data_points = []
    for record in stored_data["data"]:
        data_points.append({
            "timestamp": record.timestamp.isoformat(),
            "bm_unit": record.bm_unit,
            "settlement_period": record.settlement_period,
            "value": float(record.value) if record.value else None,
            "unit": record.unit,
            "generation_unit_id": str(record.generation_unit_id) if record.generation_unit_id else None,
        })

    return {
        "windfarm": {
            "id": windfarm.id,
            "code": windfarm.code,
            "name": windfarm.name,
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
        "data": data_points,
        "gaps": stored_data.get("gaps", []),
        "statistics": stored_data.get("statistics", {}),
        "metadata": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "bm_units": [unit.code for unit in generation_units],
            "include_gaps": include_gaps,
        },
    }


@router.get("/generation/windfarm/{windfarm_id}/availability")
async def get_windfarm_data_availability(
    windfarm_id: int,
    year: int = Query(..., description="Year to check availability"),
    month: int = Query(..., description="Month to check availability (1-12)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get data availability calendar for a specific windfarm and month.

    Returns:
    - Dates with available data
    - Coverage statistics
    - Expected settlement periods per day
    """
    # Validate month
    if month < 1 or month > 12:
        raise HTTPException(status_code=400, detail="Month must be between 1 and 12")

    # Get windfarm
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")

    # Get generation units for this windfarm with ELEXON source
    gen_units_stmt = select(GenerationUnit).where(
        GenerationUnit.windfarm_id == windfarm_id,
        GenerationUnit.is_active == True,
        GenerationUnit.source == "ELEXON",
    )
    gen_units_result = await db.execute(gen_units_stmt)
    generation_units = gen_units_result.scalars().all()

    if not generation_units:
        raise HTTPException(
            status_code=400,
            detail="No ELEXON generation units found for this windfarm.",
        )

    # Get availability data
    storage_service = ElexonStorageService(db)
    availability = await storage_service.get_data_availability(
        bm_units=[unit.code for unit in generation_units],
        year=year,
        month=month
    )

    return {
        "windfarm": {
            "id": windfarm.id,
            "code": windfarm.code,
            "name": windfarm.name,
        },
        "generation_units": [
            {
                "id": unit.id,
                "code": unit.code,
                "name": unit.name,
            }
            for unit in generation_units
        ],
        **availability,
    }


@router.post("/generation/backfill")
async def backfill_generation_data(
    windfarm_id: int,
    start_date: datetime = Query(..., description="Start date for backfill"),
    end_date: datetime = Query(..., description="End date for backfill"),
    only_fill_gaps: bool = Query(True, description="Only fetch data for detected gaps"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Backfill historical generation data for a windfarm.
    
    This endpoint:
    1. Detects gaps in stored data
    2. Fetches missing data from Elexon API
    3. Stores the fetched data
    4. Returns summary of backfilled data
    """
    # Get windfarm and generation units
    windfarm = await WindfarmService.get_windfarm(db, windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")

    gen_units_stmt = select(GenerationUnit).where(
        GenerationUnit.windfarm_id == windfarm_id,
        GenerationUnit.is_active == True,
        GenerationUnit.source == "ELEXON",
    )
    gen_units_result = await db.execute(gen_units_stmt)
    generation_units = gen_units_result.scalars().all()

    if not generation_units:
        raise HTTPException(
            status_code=400,
            detail="No ELEXON generation units found for this windfarm.",
        )

    storage_service = ElexonStorageService(db)
    
    # If only filling gaps, first detect them
    if only_fill_gaps:
        stored_data = await storage_service.get_stored_data(
            bm_units=[unit.code for unit in generation_units],
            start_date=start_date,
            end_date=end_date,
            include_gaps=True
        )
        
        gaps = stored_data.get("gaps", [])
        if not gaps:
            return {
                "message": "No gaps detected in the specified date range",
                "windfarm_id": windfarm_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "gaps_found": 0,
                "records_backfilled": 0,
            }
        
        # TODO: Implement smart gap-filling logic that only fetches missing periods
        # For now, we'll fetch the entire range
    
    # Fetch and store data
    result = await storage_service.fetch_and_store_generation(
        start_date=start_date,
        end_date=end_date,
        bm_units=[unit.code for unit in generation_units],
        generation_units=generation_units,
        current_user=current_user,
        store_data=True
    )
    
    return {
        "message": "Backfill completed successfully",
        "windfarm_id": windfarm_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "records_fetched": result.get("records_fetched", 0),
        "records_stored": result.get("records_stored", 0),
        "metadata": result.get("metadata", {}),
    }