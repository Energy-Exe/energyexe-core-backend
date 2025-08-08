"""API endpoints for Taipower integration."""

from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.models.generation_unit import GenerationUnit
from app.models.power_generation_data import PowerGenerationData
from app.schemas.taipower import (
    TaipowerGenerationDataRequest,
    TaipowerGenerationDataResponse,
    TaipowerLiveDataResponse,
)
from app.services.taipower_client import TaipowerClient
from app.services.windfarm import WindfarmService

router = APIRouter()


@router.get("/live", response_model=TaipowerLiveDataResponse)
async def get_live_generation_data(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch live generation data from Taipower API.
    
    Returns current power generation across all Taiwan power plants.
    """
    client = TaipowerClient()
    
    try:
        # Fetch live data from Taipower
        data, metadata = await client.fetch_live_data()
        
        if not data:
            raise HTTPException(
                status_code=503,
                detail=f"Failed to fetch data from Taipower: {metadata.get('errors', [])}"
            )
        
        # Calculate summary statistics
        stats = client.calculate_summary_statistics(data)
        
        # Transform to data points
        data_points = client.transform_to_data_points(data)
        
        return TaipowerLiveDataResponse(
            success=True,
            timestamp=data.datetime,
            total_generation_mw=stats["total_generation_mw"],
            generation_by_type=stats["generation_by_type"],
            units=data_points,
            metadata={
                **metadata,
                **stats,
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching Taipower data: {str(e)}"
        )


@router.post("/generation/windfarm", response_model=TaipowerGenerationDataResponse)
async def fetch_windfarm_generation_data(
    request: TaipowerGenerationDataRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch generation data from Taipower API for a specific windfarm.
    
    This endpoint:
    1. Fetches the windfarm and its generation units
    2. Fetches live data from Taipower API
    3. Filters and returns data for matching generation units
    """
    # Get windfarm if specified
    windfarm = None
    if request.windfarm_id:
        windfarm = await WindfarmService.get_windfarm(db, request.windfarm_id)
        if not windfarm:
            raise HTTPException(status_code=404, detail="Windfarm not found")
    
    # Get generation units with TAIPOWER source
    generation_units_query = select(GenerationUnit).where(
        GenerationUnit.is_active == True,
        GenerationUnit.source == "TAIPOWER",
    )
    
    if request.windfarm_id:
        generation_units_query = generation_units_query.where(
            GenerationUnit.windfarm_id == request.windfarm_id
        )
    
    result = await db.execute(generation_units_query)
    generation_units = result.scalars().all()
    
    if not generation_units and request.windfarm_id:
        raise HTTPException(
            status_code=400,
            detail="No Taipower generation units found for this windfarm"
        )
    
    # Create map of generation units
    gen_units_map = {unit.name: unit for unit in generation_units}
    
    # Fetch live data from Taipower
    client = TaipowerClient()
    data, metadata = await client.fetch_live_data()
    
    if not data:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to fetch data from Taipower: {metadata.get('errors', [])}"
        )
    
    # No database storage - just return the live data
    
    # Transform to data points with generation unit mapping
    data_points = client.transform_to_data_points(data, gen_units_map)
    
    # Filter data points if windfarm is specified
    if request.windfarm_id:
        data_points = [dp for dp in data_points if dp.generation_unit_id is not None]
    
    return TaipowerGenerationDataResponse(
        success=True,
        data=data_points,
        metadata=metadata,
        windfarm_id=request.windfarm_id,
        windfarm_name=windfarm.name if windfarm else None,
    )


