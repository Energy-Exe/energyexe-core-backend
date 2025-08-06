"""API endpoints for Elexon integration."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.schemas.elexon import (
    ElexonGenerationDataRequest,
    ElexonGenerationDataResponse,
    ElexonDataPoint,
)
from app.services.elexon_client import ElexonClient

router = APIRouter()


@router.post("/generation/fetch", response_model=ElexonGenerationDataResponse)
async def fetch_generation_data(
    request: ElexonGenerationDataRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch real-time generation data from Elexon API.

    - **start_date**: Start date for data (ISO format)
    - **end_date**: End date for data (ISO format)
    - **settlement_period_from**: Optional start settlement period (1-50)
    - **settlement_period_to**: Optional end settlement period (1-50)
    - **bm_units**: Optional list of BM Unit IDs to filter
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

        return ElexonGenerationDataResponse(data=data_points, metadata=metadata)

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/generation/windfarm/{windfarm_id}")
async def get_windfarm_generation_data(
    windfarm_id: int,
    start_date: datetime = Query(..., description="Start date (ISO format)"),
    end_date: datetime = Query(..., description="End date (ISO format)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get generation data for a specific windfarm from Elexon API.

    This endpoint:
    1. Fetches the windfarm and its generation units
    2. Uses generation unit codes as BM Unit IDs to query Elexon API
    3. Returns generation data with matched generation units
    """
    from app.services.windfarm import WindfarmService
    from app.models.generation_unit import GenerationUnit
    from sqlalchemy import select

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

    # Extract generation unit codes to use as BM Unit IDs
    bm_units = [unit.code for unit in generation_units]

    # Fetch data from Elexon
    client = ElexonClient()

    try:
        df, metadata = await client.fetch_physical_data(
            start=start_date,
            end=end_date,
            bm_units=bm_units,
        )

        # Convert DataFrame to data points and match with generation units
        data_points = []
        generation_unit_map = {unit.code: unit for unit in generation_units}

        if not df.empty:
            for _, row in df.iterrows():
                # Check if this BM unit matches one of our generation units
                matched_unit_code = None
                if row["bm_unit"] in generation_unit_map:
                    matched_unit_code = row["bm_unit"]

                data_point = ElexonDataPoint(
                    timestamp=row["timestamp"],
                    bm_unit=row["bm_unit"],
                    value=row["value"],
                    unit=row["unit"],
                    settlement_period=row.get("settlement_period"),
                    generation_unit_id=matched_unit_code,
                )
                data_points.append(data_point)

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
            "generation_data": {
                "data": data_points,
                "metadata": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "bm_units": bm_units,
                    "record_count": len(data_points),
                    **metadata,
                },
            },
            "metadata": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "bm_units_requested": bm_units,
                "bm_units_found": list(metadata.get("bm_units_found", [])),
            },
        }

        return response

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
