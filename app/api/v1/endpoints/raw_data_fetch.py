"""API endpoints for fetching raw data from external APIs."""

import structlog
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.schemas.raw_data_fetch import (
    RawDataFetchRequest,
    RawDataFetchResponse,
    UnifiedRawDataFetchRequest,
    UnifiedRawDataFetchResponse,
)
from app.services.raw_data_storage_service import RawDataStorageService

logger = structlog.get_logger()
router = APIRouter()


@router.post("/fetch", response_model=UnifiedRawDataFetchResponse)
async def fetch_raw_data_unified(
    request: UnifiedRawDataFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch data from external APIs for all available sources.

    This endpoint supports two modes:
    1. Fetch by windfarms: Provide windfarm_ids, auto-detects sources
    2. Fetch by source: Provide source name, fetches all windfarms for that source

    Examples:
    - Fetch specific windfarms: {"windfarm_ids": [1,2,3], "start_date": "...", "end_date": "..."}
    - Fetch all ENTSOE data: {"source": "ENTSOE", "start_date": "...", "end_date": "..."}

    This will:
    1. Determine windfarms (from IDs or by source)
    2. Fetch data from each source's external API
    3. Transform the data to match generation_data_raw format
    4. Store or update records in the database (source_type='api')
    5. Return summary of what was stored/updated per source
    """
    # Validate input
    if not request.windfarm_ids and not request.source:
        raise HTTPException(
            status_code=400,
            detail="Must provide either 'windfarm_ids' or 'source' parameter"
        )

    service = RawDataStorageService(db)

    try:
        result = await service.fetch_and_store_all_sources(
            windfarm_ids=request.windfarm_ids,
            start_date=request.start_date,
            end_date=request.end_date,
            user_id=current_user.id,
            source_filter=request.source,
        )
        return result
    except Exception as e:
        logger.error(f"Error in unified raw data fetch: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/entsoe/fetch", response_model=RawDataFetchResponse)
async def fetch_entsoe_data(
    request: RawDataFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch ENTSOE data from external API and store in generation_data_raw.

    This will:
    1. Fetch data from ENTSOE API for the specified windfarms and date range
    2. Transform the data to match generation_data_raw format
    3. Store or update records in the database (source_type='api')
    4. Return summary of what was stored/updated
    """
    service = RawDataStorageService(db)

    try:
        result = await service.fetch_and_store_entsoe(request, current_user.id)
        return result
    except Exception as e:
        logger.error(f"Error fetching ENTSOE data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/elexon/fetch", response_model=RawDataFetchResponse)
async def fetch_elexon_data(
    request: RawDataFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch ELEXON data from external API and store in generation_data_raw.

    This will:
    1. Fetch data from ELEXON API for the specified windfarms and date range
    2. Transform the data to match generation_data_raw format
    3. Store or update records in the database (source_type='api')
    4. Return summary of what was stored/updated
    """
    service = RawDataStorageService(db)

    try:
        result = await service.fetch_and_store_elexon(request, current_user.id)
        return result
    except Exception as e:
        logger.error(f"Error fetching ELEXON data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/eia/fetch", response_model=RawDataFetchResponse)
async def fetch_eia_data(
    request: RawDataFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch EIA data from external API and store in generation_data_raw.

    Note: EIA API fetching is not yet implemented.
    """
    service = RawDataStorageService(db)

    try:
        result = await service.fetch_and_store_eia(request, current_user.id)
        return result
    except Exception as e:
        logger.error(f"Error fetching EIA data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/taipower/fetch", response_model=RawDataFetchResponse)
async def fetch_taipower_data(
    request: RawDataFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch TAIPOWER data from external API and store in generation_data_raw.

    Note: TAIPOWER API fetching is not yet implemented.
    """
    service = RawDataStorageService(db)

    try:
        result = await service.fetch_and_store_taipower(request, current_user.id)
        return result
    except Exception as e:
        logger.error(f"Error fetching TAIPOWER data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/nve/fetch", response_model=RawDataFetchResponse)
async def fetch_nve_data(
    request: RawDataFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch NVE data from external API and store in generation_data_raw.

    Note: NVE API fetching is not yet implemented.
    """
    service = RawDataStorageService(db)

    try:
        result = await service.fetch_and_store_nve(request, current_user.id)
        return result
    except Exception as e:
        logger.error(f"Error fetching NVE data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/energistyrelsen/fetch", response_model=RawDataFetchResponse)
async def fetch_energistyrelsen_data(
    request: RawDataFetchRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch ENERGISTYRELSEN data from external API and store in generation_data_raw.

    Note: ENERGISTYRELSEN API fetching is not yet implemented.
    """
    service = RawDataStorageService(db)

    try:
        result = await service.fetch_and_store_energistyrelsen(request, current_user.id)
        return result
    except Exception as e:
        logger.error(f"Error fetching ENERGISTYRELSEN data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
