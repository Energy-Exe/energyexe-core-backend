from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.turbine_model import TurbineModel, TurbineModelCreate, TurbineModelUpdate
from app.services.turbine_model import TurbineModelService

router = APIRouter()


@router.get("/", response_model=List[TurbineModel])
async def get_turbine_models(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """Get all turbine models with pagination"""
    return await TurbineModelService.get_turbine_models(db, skip=skip, limit=limit)


@router.get("/search", response_model=List[TurbineModel])
async def search_turbine_models(
    q: str = Query(..., min_length=1),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
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
