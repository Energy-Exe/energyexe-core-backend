from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import DEFAULT_PAGINATION_LIMIT, MAX_PAGINATION_LIMIT, MIN_PAGINATION_LIMIT
from app.core.database import get_db
from app.schemas.turbine_unit import TurbineUnit, TurbineUnitCreate, TurbineUnitUpdate
from app.services.turbine_unit import TurbineUnitService

router = APIRouter()


@router.get("", response_model=List[TurbineUnit])
async def get_turbine_units(
    skip: int = Query(0, ge=0),
    limit: int = Query(DEFAULT_PAGINATION_LIMIT, ge=MIN_PAGINATION_LIMIT, le=MAX_PAGINATION_LIMIT),
    db: AsyncSession = Depends(get_db),
):
    """Get all turbine_units with pagination"""
    return await TurbineUnitService.get_turbine_units(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[TurbineUnit])
async def search_turbine_units(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(DEFAULT_PAGINATION_LIMIT, ge=MIN_PAGINATION_LIMIT, le=MAX_PAGINATION_LIMIT),
    db: AsyncSession = Depends(get_db),
):
    """Search turbine_units by name"""
    return await TurbineUnitService.search_turbine_units(db, query=q, skip=skip, limit=limit)


@router.get("/{turbine_unit_id}", response_model=TurbineUnit)
async def get_turbine_unit(turbine_unit_id: int, db: AsyncSession = Depends(get_db)):
    """Get a specific turbine_unit by ID"""
    turbine_unit = await TurbineUnitService.get_turbine_unit(db, turbine_unit_id)
    if not turbine_unit:
        raise HTTPException(status_code=404, detail="TurbineUnit not found")
    return turbine_unit


@router.get("/{turbine_unit_id}/with-relations")
async def get_turbine_unit_with_relations(turbine_unit_id: int, db: AsyncSession = Depends(get_db)):
    """Get a turbine unit with windfarm, turbine model, and generation units"""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload
    from app.models.turbine_unit import TurbineUnit as TurbineUnitModel
    from app.models.generation_unit import GenerationUnit

    # Get turbine unit with windfarm and turbine_model
    result = await db.execute(
        select(TurbineUnitModel)
        .options(
            selectinload(TurbineUnitModel.windfarm),
            selectinload(TurbineUnitModel.turbine_model)
        )
        .where(TurbineUnitModel.id == turbine_unit_id)
    )
    turbine_unit = result.scalar_one_or_none()

    if not turbine_unit:
        raise HTTPException(status_code=404, detail="Turbine unit not found")

    # Get generation units for the same windfarm
    generation_units = []
    if turbine_unit.windfarm_id:
        result = await db.execute(
            select(GenerationUnit)
            .where(GenerationUnit.windfarm_id == turbine_unit.windfarm_id)
        )
        generation_units = result.scalars().all()

    return {
        "id": turbine_unit.id,
        "code": turbine_unit.code,
        "lat": turbine_unit.lat,
        "lng": turbine_unit.lng,
        "status": turbine_unit.status,
        "hub_height_m": float(turbine_unit.hub_height_m) if turbine_unit.hub_height_m else None,
        "start_date": turbine_unit.start_date.isoformat() if turbine_unit.start_date else None,
        "end_date": turbine_unit.end_date.isoformat() if turbine_unit.end_date else None,
        "created_at": turbine_unit.created_at.isoformat(),
        "updated_at": turbine_unit.updated_at.isoformat(),
        "windfarm": {
            "id": turbine_unit.windfarm.id,
            "code": turbine_unit.windfarm.code,
            "name": turbine_unit.windfarm.name,
            "nameplate_capacity_mw": turbine_unit.windfarm.nameplate_capacity_mw,
            "status": turbine_unit.windfarm.status,
            "lat": turbine_unit.windfarm.lat,
            "lng": turbine_unit.windfarm.lng,
        } if turbine_unit.windfarm else None,
        "turbine_model": {
            "id": turbine_unit.turbine_model.id,
            "model": turbine_unit.turbine_model.model,
            "supplier": turbine_unit.turbine_model.supplier,
            "original_supplier": turbine_unit.turbine_model.original_supplier,
            "rated_power_kw": turbine_unit.turbine_model.rated_power_kw,
            "rotor_diameter_m": float(turbine_unit.turbine_model.rotor_diameter_m) if turbine_unit.turbine_model.rotor_diameter_m else None,
            "hub_height_m": float(turbine_unit.turbine_model.cut_in_wind_speed_ms) if turbine_unit.turbine_model.cut_in_wind_speed_ms else None,
            "blade_length_m": float(turbine_unit.turbine_model.blade_length_m) if turbine_unit.turbine_model.blade_length_m else None,
        } if turbine_unit.turbine_model else None,
        "generation_units": [
            {
                "id": unit.id,
                "code": unit.code,
                "name": unit.name,
                "fuel_type": unit.fuel_type,
                "capacity_mw": float(unit.capacity_mw) if unit.capacity_mw else None,
                "source": unit.source,
                "is_active": unit.is_active,
            }
            for unit in generation_units
        ]
    }


@router.get("/code/{code}", response_model=TurbineUnit)
async def get_turbine_unit_by_code(code: str, db: AsyncSession = Depends(get_db)):
    """Get a turbine_unit by its code"""
    turbine_unit = await TurbineUnitService.get_turbine_unit_by_code(db, code)
    if not turbine_unit:
        raise HTTPException(status_code=404, detail="TurbineUnit not found")
    return turbine_unit


@router.post("/", response_model=TurbineUnit, status_code=201)
async def create_turbine_unit(turbine_unit: TurbineUnitCreate, db: AsyncSession = Depends(get_db)):
    """Create a new turbine_unit"""
    # Check if turbine_unit with same code already exists
    existing_turbine_unit = await TurbineUnitService.get_turbine_unit_by_code(db, turbine_unit.code)
    if existing_turbine_unit:
        raise HTTPException(status_code=400, detail="TurbineUnit with this code already exists")

    return await TurbineUnitService.create_turbine_unit(db, turbine_unit)


@router.put("/{turbine_unit_id}", response_model=TurbineUnit)
async def update_turbine_unit(
    turbine_unit_id: int, turbine_unit_update: TurbineUnitUpdate, db: AsyncSession = Depends(get_db)
):
    """Update a turbine_unit"""
    # Check if turbine_unit with same code already exists (excluding current turbine_unit)
    if turbine_unit_update.code:
        existing_turbine_unit = await TurbineUnitService.get_turbine_unit_by_code(
            db, turbine_unit_update.code
        )
        if existing_turbine_unit and existing_turbine_unit.id != turbine_unit_id:
            raise HTTPException(status_code=400, detail="TurbineUnit with this code already exists")

    updated_turbine_unit = await TurbineUnitService.update_turbine_unit(
        db, turbine_unit_id, turbine_unit_update
    )
    if not updated_turbine_unit:
        raise HTTPException(status_code=404, detail="TurbineUnit not found")
    return updated_turbine_unit


@router.delete("/{turbine_unit_id}", response_model=TurbineUnit)
async def delete_turbine_unit(turbine_unit_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a turbine_unit"""
    deleted_turbine_unit = await TurbineUnitService.delete_turbine_unit(db, turbine_unit_id)
    if not deleted_turbine_unit:
        raise HTTPException(status_code=404, detail="TurbineUnit not found")
    return deleted_turbine_unit
