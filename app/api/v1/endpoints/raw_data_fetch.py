"""API endpoints for fetching raw data from external APIs and file uploads."""

import asyncio
import json
from datetime import datetime
from typing import AsyncGenerator

import structlog
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_active_user, get_db
from app.models.user import User
from app.schemas.raw_data_fetch import (
    FileUploadProgressUpdate,
    FileUploadResponse,
    RawDataFetchRequest,
    RawDataFetchResponse,
    UnifiedRawDataFetchRequest,
    UnifiedRawDataFetchResponse,
)
from app.services.file_import_service import FileImportService
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
    - Fetch and aggregate: {"windfarm_ids": [1,2,3], "start_date": "...", "end_date": "...", "process_to_hourly": true}

    This will:
    1. Determine windfarms (from IDs or by source)
    2. Fetch data from each source's external API
    3. Transform the data to match generation_data_raw format
    4. Store or update records in the database (source_type='api')
    5. Optionally aggregate raw data to hourly resolution (if process_to_hourly=true)
    6. Return summary of what was stored/updated per source
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
            process_to_hourly=request.process_to_hourly,
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


# File Upload Endpoints with Server-Sent Events for progress


@router.post("/nve/upload")
async def upload_nve_file(
    file: UploadFile = File(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    clean_first: bool = Form(True),
    workers: int = Form(4),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Upload and process NVE Excel file with real-time progress updates.

    Returns Server-Sent Events stream with progress updates, followed by final result.

    Form Parameters:
    - file: Excel file (.xlsx)
    - start_date: Start date for filtering (ISO format)
    - end_date: End date for filtering (ISO format)
    - clean_first: Whether to clear existing NVE data first (default: true)
    - workers: Number of parallel workers (1-8, default: 4)
    """
    # Validate file type
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    # Parse dates
    try:
        start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")

    # Read file content
    try:
        file_content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

    # Create async generator for SSE
    async def event_generator() -> AsyncGenerator[str, None]:
        progress_queue = asyncio.Queue()
        result_holder = {"result": None, "error": None}

        # Progress callback that puts updates in queue
        async def progress_callback(update: FileUploadProgressUpdate):
            await progress_queue.put(update)

        # Background task that processes the file
        async def process_file():
            try:
                service = FileImportService(db)
                result = await service.import_nve_file(
                    file_content=file_content,
                    filename=file.filename,
                    start_date=start_dt,
                    end_date=end_dt,
                    clean_first=clean_first,
                    progress_callback=progress_callback,
                )
                result_holder["result"] = result
            except Exception as e:
                logger.error(f"Error processing NVE file: {str(e)}")
                result_holder["error"] = str(e)
                await progress_queue.put(
                    FileUploadProgressUpdate(
                        status="error", message=f"Error: {str(e)}", progress_percent=0
                    )
                )
            finally:
                await progress_queue.put(None)  # Signal completion

        # Start background processing
        task = asyncio.create_task(process_file())

        # Stream progress updates
        try:
            while True:
                update = await progress_queue.get()
                if update is None:  # Completion signal
                    break

                # Send SSE event
                event_data = update.model_dump_json()
                yield f"data: {event_data}\n\n"

            # Wait for task to complete
            await task

            # Send final result
            if result_holder["result"]:
                final_data = result_holder["result"].model_dump_json()
                yield f"event: complete\ndata: {final_data}\n\n"
            elif result_holder["error"]:
                error_data = json.dumps({"error": result_holder["error"]})
                yield f"event: error\ndata: {error_data}\n\n"

        except Exception as e:
            logger.error(f"Error in event generator: {str(e)}")
            error_data = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {error_data}\n\n"

    return StreamingResponse(
        event_generator(), media_type="text/event-stream"
    )


@router.post("/energistyrelsen/upload")
async def upload_energistyrelsen_file(
    file: UploadFile = File(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    clean_first: bool = Form(True),
    workers: int = Form(4),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Upload and process Energistyrelsen Excel file with real-time progress updates.

    Returns Server-Sent Events stream with progress updates, followed by final result.

    Form Parameters:
    - file: Excel file (.xlsx)
    - start_date: Start date for filtering (ISO format)
    - end_date: End date for filtering (ISO format)
    - clean_first: Whether to clear existing Energistyrelsen data first (default: true)
    - workers: Number of parallel workers (1-8, default: 4)
    """
    # Validate file type
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    # Parse dates
    try:
        start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")

    # Read file content
    try:
        file_content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

    # Create async generator for SSE
    async def event_generator() -> AsyncGenerator[str, None]:
        progress_queue = asyncio.Queue()
        result_holder = {"result": None, "error": None}

        # Progress callback that puts updates in queue
        async def progress_callback(update: FileUploadProgressUpdate):
            await progress_queue.put(update)

        # Background task that processes the file
        async def process_file():
            try:
                service = FileImportService(db)
                result = await service.import_energistyrelsen_file(
                    file_content=file_content,
                    filename=file.filename,
                    start_date=start_dt,
                    end_date=end_dt,
                    clean_first=clean_first,
                    progress_callback=progress_callback,
                )
                result_holder["result"] = result
            except Exception as e:
                logger.error(f"Error processing Energistyrelsen file: {str(e)}")
                result_holder["error"] = str(e)
                await progress_queue.put(
                    FileUploadProgressUpdate(
                        status="error", message=f"Error: {str(e)}", progress_percent=0
                    )
                )
            finally:
                await progress_queue.put(None)  # Signal completion

        # Start background processing
        task = asyncio.create_task(process_file())

        # Stream progress updates
        try:
            while True:
                update = await progress_queue.get()
                if update is None:  # Completion signal
                    break

                # Send SSE event
                event_data = update.model_dump_json()
                yield f"data: {event_data}\n\n"

            # Wait for task to complete
            await task

            # Send final result
            if result_holder["result"]:
                final_data = result_holder["result"].model_dump_json()
                yield f"event: complete\ndata: {final_data}\n\n"
            elif result_holder["error"]:
                error_data = json.dumps({"error": result_holder["error"]})
                yield f"event: error\ndata: {error_data}\n\n"

        except Exception as e:
            logger.error(f"Error in event generator: {str(e)}")
            error_data = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {error_data}\n\n"

    return StreamingResponse(
        event_generator(), media_type="text/event-stream"
    )

@router.post("/taipower/upload")
async def upload_taipower_file(
    file: UploadFile = File(...),
    unit_code: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    clean_first: bool = Form(True),
    workers: int = Form(4),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Upload and process Taipower Excel file with real-time progress updates.

    Returns Server-Sent Events stream with progress updates, followed by final result.

    Form Parameters:
    - file: Excel file (.xlsx)
    - unit_code: Generation unit code (Chinese code, e.g., '彰工')
    - start_date: Start date for filtering (ISO format)
    - end_date: End date for filtering (ISO format)
    - clean_first: Whether to clear existing data for this unit first (default: true)
    - workers: Number of parallel workers (1-8, default: 4)
    """
    # Validate file type
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    # Parse dates
    try:
        start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")

    # Read file content
    try:
        file_content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

    # Create async generator for SSE
    async def event_generator() -> AsyncGenerator[str, None]:
        progress_queue = asyncio.Queue()
        result_holder = {"result": None, "error": None}

        # Progress callback that puts updates in queue
        async def progress_callback(update: FileUploadProgressUpdate):
            await progress_queue.put(update)

        # Background task that processes the file
        async def process_file():
            try:
                service = FileImportService(db)
                result = await service.import_taipower_file(
                    file_content=file_content,
                    filename=file.filename,
                    unit_code=unit_code,
                    start_date=start_dt,
                    end_date=end_dt,
                    clean_first=clean_first,
                    progress_callback=progress_callback,
                )
                result_holder["result"] = result
            except Exception as e:
                logger.error(f"Error processing Taipower file: {str(e)}")
                result_holder["error"] = str(e)
                await progress_queue.put(
                    FileUploadProgressUpdate(
                        status="error", message=f"Error: {str(e)}", progress_percent=0
                    )
                )
            finally:
                await progress_queue.put(None)  # Signal completion

        # Start background processing
        task = asyncio.create_task(process_file())

        # Stream progress updates
        try:
            while True:
                update = await progress_queue.get()
                if update is None:  # Completion signal
                    break

                # Send SSE event
                event_data = update.model_dump_json()
                yield f"data: {event_data}\n\n"

            # Wait for task to complete
            await task

            # Send final result
            if result_holder["result"]:
                final_data = result_holder["result"].model_dump_json()
                yield f"event: complete\ndata: {final_data}\n\n"
            elif result_holder["error"]:
                error_data = json.dumps({"error": result_holder["error"]})
                yield f"event: error\ndata: {error_data}\n\n"

        except Exception as e:
            logger.error(f"Error in event generator: {str(e)}")
            error_data = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {error_data}\n\n"

    return StreamingResponse(
        event_generator(), media_type="text/event-stream"
    )
