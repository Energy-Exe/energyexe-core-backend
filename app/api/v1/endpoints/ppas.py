"""API endpoints for PPA (Power Purchase Agreement) management."""

from typing import List

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import DEFAULT_PAGINATION_LIMIT, MAX_PAGINATION_LIMIT, MIN_PAGINATION_LIMIT
from app.core.database import get_db
from app.schemas.ppa import (
    PPA,
    PPACreate,
    PPAImportResult,
    PPAListResponse,
    PPAUpdate,
    PPAWithWindfarm,
)
from app.services.ppa_service import PPAService

router = APIRouter()


@router.get("", response_model=PPAListResponse)
async def get_ppas(
    skip: int = Query(0, ge=0),
    limit: int = Query(DEFAULT_PAGINATION_LIMIT, ge=MIN_PAGINATION_LIMIT, le=MAX_PAGINATION_LIMIT),
    db: AsyncSession = Depends(get_db),
):
    """Get all PPAs with pagination."""
    service = PPAService(db)
    ppas, total = await service.get_ppas(skip=skip, limit=limit)
    return PPAListResponse(
        items=ppas,
        total=total,
        limit=limit,
        offset=skip,
        has_more=(skip + limit) < total,
    )


@router.get("/by-windfarm/{windfarm_id}", response_model=List[PPA])
async def get_ppas_by_windfarm(
    windfarm_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get all PPAs for a specific windfarm."""
    from app.models.windfarm import Windfarm
    from sqlalchemy import select

    # Verify windfarm exists
    result = await db.execute(select(Windfarm).where(Windfarm.id == windfarm_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Windfarm not found")

    service = PPAService(db)
    return await service.get_ppas_by_windfarm(windfarm_id)


@router.get("/{ppa_id}", response_model=PPAWithWindfarm)
async def get_ppa(
    ppa_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific PPA by ID."""
    service = PPAService(db)
    ppa = await service.get_ppa(ppa_id)
    if not ppa:
        raise HTTPException(status_code=404, detail="PPA not found")
    return ppa


@router.post("", response_model=PPA, status_code=201)
async def create_ppa(
    ppa: PPACreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new PPA."""
    from app.models.windfarm import Windfarm
    from sqlalchemy import select
    from sqlalchemy.exc import IntegrityError

    # Verify windfarm exists
    result = await db.execute(select(Windfarm).where(Windfarm.id == ppa.windfarm_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Windfarm not found")

    # Validate dates if both provided
    if ppa.ppa_start_date and ppa.ppa_end_date:
        if ppa.ppa_end_date <= ppa.ppa_start_date:
            raise HTTPException(status_code=400, detail="End date must be after start date")

    service = PPAService(db)
    try:
        return await service.create_ppa(ppa)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail="A PPA with the same buyer, start date, and end date already exists for this windfarm"
        )


@router.put("/{ppa_id}", response_model=PPA)
async def update_ppa(
    ppa_id: int,
    ppa_update: PPAUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update an existing PPA."""
    from app.models.windfarm import Windfarm
    from sqlalchemy import select
    from sqlalchemy.exc import IntegrityError

    # If windfarm_id is being updated, verify it exists
    if ppa_update.windfarm_id:
        result = await db.execute(select(Windfarm).where(Windfarm.id == ppa_update.windfarm_id))
        if not result.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Windfarm not found")

    service = PPAService(db)
    try:
        updated_ppa = await service.update_ppa(ppa_id, ppa_update)
        if not updated_ppa:
            raise HTTPException(status_code=404, detail="PPA not found")
        return updated_ppa
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail="A PPA with the same buyer, start date, and end date already exists for this windfarm"
        )


@router.delete("/{ppa_id}", response_model=PPA)
async def delete_ppa(
    ppa_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Delete a PPA."""
    service = PPAService(db)
    deleted_ppa = await service.delete_ppa(ppa_id)
    if not deleted_ppa:
        raise HTTPException(status_code=404, detail="PPA not found")
    return deleted_ppa


@router.post("/import", response_model=PPAImportResult)
async def import_ppas_from_excel(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Import PPAs from Excel file.

    **Expected columns:**
    - `windfarm_name` (required): Name of the windfarm (exact match to database)
    - `ppa_buyer` (required): Buyer company name
    - `ppa_size_mw` (required): PPA size in MW
    - `ppa_duration_years` (optional): Duration in years
    - `ppa_start_date` (optional): Start date
    - `ppa_end_date` (optional): End date
    - `ppa_notes` (optional): Notes (max 200 characters)

    **Upsert behavior:**
    - If a PPA with the same (windfarm, buyer, start_date, end_date) exists, it will be updated
    - Otherwise, a new PPA will be created

    **Returns:**
    - Summary of created, updated, and skipped records
    - List of errors for rows that failed
    - List of unmatched windfarm names
    """
    # Validate file type
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=400,
            detail="Only Excel files (.xlsx, .xls) are supported",
        )

    try:
        file_content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

    if len(file_content) == 0:
        raise HTTPException(status_code=400, detail="File is empty")

    service = PPAService(db)
    result = await service.import_from_excel(file_content, file.filename)

    return result
