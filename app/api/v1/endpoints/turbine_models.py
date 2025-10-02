from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import DEFAULT_PAGINATION_LIMIT, MAX_PAGINATION_LIMIT, MIN_PAGINATION_LIMIT
from app.core.database import get_db
from app.schemas.turbine_model import TurbineModel, TurbineModelCreate, TurbineModelUpdate
from app.services.turbine_model import TurbineModelService

router = APIRouter()


@router.get("/", response_model=List[TurbineModel])
async def get_turbine_models(
    skip: int = Query(0, ge=0),
    limit: int = Query(DEFAULT_PAGINATION_LIMIT, ge=MIN_PAGINATION_LIMIT, le=MAX_PAGINATION_LIMIT),
    db: AsyncSession = Depends(get_db),
):
    """Get all turbine models with pagination"""
    return await TurbineModelService.get_turbine_models(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[TurbineModel])
async def search_turbine_models(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(DEFAULT_PAGINATION_LIMIT, ge=MIN_PAGINATION_LIMIT, le=MAX_PAGINATION_LIMIT),
    db: AsyncSession = Depends(get_db),
):
    """Search turbine models by model name"""
    return await TurbineModelService.search_turbine_models(db, query=q, skip=skip, limit=limit)


@router.get("/{turbine_model_id}", response_model=TurbineModel)
async def get_turbine_model(turbine_model_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific turbine model by ID"""
    turbine_model = await TurbineModelService.get_turbine_model(db, turbine_model_id)
    if not turbine_model:
        raise HTTPException(status_code=404, detail="Turbine model not found")
    return turbine_model


@router.get("/{turbine_model_id}/with-turbine-units")
async def get_turbine_model_with_units(turbine_model_id: int, db: AsyncSession = Depends(get_db)):
    """Get a turbine model with all its turbine units"""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.models.turbine_model import TurbineModel as TurbineModelModel
    from app.models.turbine_unit import TurbineUnit

    # Get turbine model with all turbine units and their windfarms
    from app.models.windfarm import Windfarm
    result = await db.execute(
        select(TurbineModelModel)
        .options(
            selectinload(TurbineModelModel.turbine_units)
            .selectinload(TurbineUnit.windfarm)
        )
        .where(TurbineModelModel.id == turbine_model_id)
    )
    turbine_model = result.scalar_one_or_none()

    if not turbine_model:
        raise HTTPException(status_code=404, detail="Turbine model not found")

    # For each turbine unit, fetch windfarm and generation units
    turbine_units_with_relations = []
    for unit in turbine_model.turbine_units:
        # Fetch windfarm
        windfarm_data = None
        if unit.windfarm:
            windfarm_data = {
                "id": unit.windfarm.id,
                "code": unit.windfarm.code,
                "name": unit.windfarm.name,
                "status": unit.windfarm.status,
            }

        # Fetch generation units for this windfarm
        generation_units = []
        if unit.windfarm_id:
            from app.models.generation_unit import GenerationUnit
            result = await db.execute(
                select(GenerationUnit)
                .where(GenerationUnit.windfarm_id == unit.windfarm_id)
            )
            gen_units = result.scalars().all()
            generation_units = [
                {
                    "id": gu.id,
                    "code": gu.code,
                    "name": gu.name,
                    "source": gu.source,
                }
                for gu in gen_units
            ]

        turbine_units_with_relations.append({
            "id": unit.id,
            "code": unit.code,
            "status": unit.status,
            "hub_height_m": float(unit.hub_height_m) if unit.hub_height_m else None,
            "start_date": unit.start_date.isoformat() if unit.start_date else None,
            "end_date": unit.end_date.isoformat() if unit.end_date else None,
            "windfarm": windfarm_data,
            "generation_units": generation_units,
        })

    return {
        "id": turbine_model.id,
        "model": turbine_model.model,
        "supplier": turbine_model.supplier,
        "original_supplier": turbine_model.original_supplier,
        "rated_power_kw": turbine_model.rated_power_kw,
        "rotor_diameter_m": float(turbine_model.rotor_diameter_m) if turbine_model.rotor_diameter_m else None,
        "cut_in_wind_speed_ms": float(turbine_model.cut_in_wind_speed_ms) if turbine_model.cut_in_wind_speed_ms else None,
        "cut_out_wind_speed_ms": float(turbine_model.cut_out_wind_speed_ms) if turbine_model.cut_out_wind_speed_ms else None,
        "rated_wind_speed_ms": float(turbine_model.rated_wind_speed_ms) if turbine_model.rated_wind_speed_ms else None,
        "blade_length_m": float(turbine_model.blade_length_m) if turbine_model.blade_length_m else None,
        "generator_type": turbine_model.generator_type,
        "created_at": turbine_model.created_at.isoformat(),
        "updated_at": turbine_model.updated_at.isoformat(),
        "turbine_units": turbine_units_with_relations,
        "turbine_units_count": len(turbine_units_with_relations),
    }


@router.get("/model/{model}", response_model=TurbineModel)
async def get_turbine_model_by_model(model: str, db: AsyncSession = Depends(get_db)):
    """Get a turbine model by its model name"""
    turbine_model = await TurbineModelService.get_turbine_model_by_model(db, model)
    if not turbine_model:
        raise HTTPException(status_code=404, detail="Turbine model not found")
    return turbine_model


@router.post("/", response_model=TurbineModel, status_code=201)
async def create_turbine_model(
    turbine_model: TurbineModelCreate, db: AsyncSession = Depends(get_db)
):
    """Create a new turbine model"""
    # Check if turbine model with same model name already exists
    existing_turbine_model = await TurbineModelService.get_turbine_model_by_model(
        db, turbine_model.model
    )
    if existing_turbine_model:
        raise HTTPException(
            status_code=400, detail="Turbine model with this model name already exists"
        )

    return await TurbineModelService.create_turbine_model(db, turbine_model)


@router.put("/{turbine_model_id}", response_model=TurbineModel)
async def update_turbine_model(
    turbine_model_id: int,
    turbine_model_update: TurbineModelUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a turbine model"""
    # Check if turbine model with same model name already exists (excluding current turbine model)
    if turbine_model_update.model:
        existing_turbine_model = await TurbineModelService.get_turbine_model_by_model(
            db, turbine_model_update.model
        )
        if existing_turbine_model and existing_turbine_model.id != turbine_model_id:
            raise HTTPException(
                status_code=400, detail="Turbine model with this model name already exists"
            )

    updated_turbine_model = await TurbineModelService.update_turbine_model(
        db, turbine_model_id, turbine_model_update
    )
    if not updated_turbine_model:
        raise HTTPException(status_code=404, detail="Turbine model not found")
    return updated_turbine_model


@router.delete("/{turbine_model_id}", response_model=TurbineModel)
async def delete_turbine_model(turbine_model_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a turbine model"""
    deleted_turbine_model = await TurbineModelService.delete_turbine_model(db, turbine_model_id)
    if not deleted_turbine_model:
        raise HTTPException(status_code=404, detail="Turbine model not found")
    return deleted_turbine_model
