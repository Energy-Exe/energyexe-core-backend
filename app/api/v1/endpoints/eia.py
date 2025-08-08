"""API endpoints for EIA integration."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.models.generation_unit import GenerationUnit
from app.schemas.eia import (
    EIAGenerationDataRequest,
    EIAGenerationDataResponse,
    EIAWindfarmGenerationResponse,
    EIADataPoint,
)
from app.services.eia_client import EIAClient
from app.services.windfarm import WindfarmService

router = APIRouter()


@router.post("/generation/windfarm", response_model=EIAWindfarmGenerationResponse)
async def fetch_windfarm_generation_data(
    request: EIAGenerationDataRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch monthly generation data from EIA API for a specific windfarm.

    This endpoint:
    1. Fetches the windfarm and its generation units
    2. Uses generation unit codes as plant codes to query EIA API
    3. Returns monthly generation data with matched generation units

    - **windfarm_id**: ID of the windfarm
    - **start_year**: Start year for data
    - **start_month**: Start month (1-12)
    - **end_year**: End year for data
    - **end_month**: End month (1-12)
    """
    # Get windfarm
    windfarm = await WindfarmService.get_windfarm(db, request.windfarm_id)
    if not windfarm:
        raise HTTPException(status_code=404, detail="Windfarm not found")

    # Get generation units for this windfarm with EIA source
    gen_units_stmt = select(GenerationUnit).where(
        GenerationUnit.windfarm_id == request.windfarm_id,
        GenerationUnit.is_active == True,
        GenerationUnit.source == "EIA",
    )
    gen_units_result = await db.execute(gen_units_stmt)
    generation_units = gen_units_result.scalars().all()

    if not generation_units:
        # If no EIA-specific units, try to get any generation units
        gen_units_stmt = select(GenerationUnit).where(
            GenerationUnit.windfarm_id == request.windfarm_id,
            GenerationUnit.is_active == True,
        )
        gen_units_result = await db.execute(gen_units_stmt)
        generation_units = gen_units_result.scalars().all()

        if not generation_units:
            raise HTTPException(
                status_code=400,
                detail="No generation units found for this windfarm. Please ensure generation units are properly configured.",
            )

    # Extract generation unit codes to use as plant codes
    plant_codes = [unit.code for unit in generation_units if unit.code]

    if not plant_codes:
        raise HTTPException(
            status_code=400,
            detail="Generation units do not have codes configured. Plant codes are required for EIA API.",
        )

    # Fetch data from EIA
    client = EIAClient()

    try:
        df, metadata = await client.fetch_monthly_generation_data(
            plant_codes=plant_codes,
            start_year=request.start_year,
            start_month=request.start_month,
            end_year=request.end_year,
            end_month=request.end_month,
        )

        # Convert DataFrame to data points and match with generation units
        data_points = []
        generation_unit_map = {unit.code: unit for unit in generation_units}

        if not df.empty:
            for _, row in df.iterrows():
                # Check if this plant code matches one of our generation units
                plant_code = str(row.get("plantCode", ""))
                matched_unit_code = None
                if plant_code in generation_unit_map:
                    matched_unit_code = plant_code

                data_point = EIADataPoint(
                    period=row.get("period", ""),
                    plant_code=plant_code,
                    plant_name=row.get("plantName", None),
                    generation=float(row.get("generation", 0)),
                    unit="MWh",
                    fuel_type=row.get("fuel2002", "WND"),
                    generation_unit_id=matched_unit_code,
                )
                data_points.append(data_point)

        # Prepare response
        response = EIAWindfarmGenerationResponse(
            windfarm={
                "id": windfarm.id,
                "code": windfarm.code,
                "name": windfarm.name,
            },
            generation_units=[
                {
                    "id": unit.id,
                    "code": unit.code,
                    "name": unit.name,
                    "capacity_mw": float(unit.capacity_mw) if unit.capacity_mw else None,
                    "source": unit.source,
                }
                for unit in generation_units
            ],
            generation_data=EIAGenerationDataResponse(
                data=data_points,
                metadata={
                    "start_year": request.start_year,
                    "start_month": request.start_month,
                    "end_year": request.end_year,
                    "end_month": request.end_month,
                    "plant_codes": plant_codes,
                    "record_count": len(data_points),
                    **metadata,
                },
            ),
            metadata={
                "windfarm_id": request.windfarm_id,
                "period": f"{request.start_year}-{request.start_month:02d} to {request.end_year}-{request.end_month:02d}",
                "plant_codes_requested": plant_codes,
                "plant_codes_found": list(metadata.get("plant_codes_found", [])),
            },
        )

        return response

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
