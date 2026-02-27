"""API endpoints for Financial Data management."""

import io
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import DEFAULT_PAGINATION_LIMIT, MAX_PAGINATION_LIMIT, MIN_PAGINATION_LIMIT
from app.core.database import get_db
from app.schemas.financial_data import (
    FinancialData,
    FinancialDataCreate,
    FinancialDataImportResult,
    FinancialDataListResponse,
    FinancialDataSummary,
    FinancialDataUpdate,
    FinancialDataWithEntity,
    FinancialRatiosResponse,
)
from app.services.financial_data_service import FinancialDataService

router = APIRouter()


@router.get("", response_model=FinancialDataListResponse)
async def list_data(
    skip: int = Query(0, ge=0),
    limit: int = Query(DEFAULT_PAGINATION_LIMIT, ge=MIN_PAGINATION_LIMIT, le=MAX_PAGINATION_LIMIT),
    entity_id: Optional[int] = Query(None),
    year: Optional[int] = Query(None),
    currency: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Get financial data records with pagination and filters."""
    service = FinancialDataService(db)
    items, total = await service.get_list(
        skip=skip, limit=limit, entity_id=entity_id, year=year, currency=currency
    )
    return FinancialDataListResponse(
        items=items,
        total=total,
        limit=limit,
        offset=skip,
        has_more=(skip + limit) < total,
    )


@router.get("/template")
async def download_template():
    """Download an Excel template for financial data import."""
    columns = [
        "entity_code", "period_start", "period_end", "period_length_months",
        "currency", "is_synthetic",
        "reported_generation_gwh",
        "revenue", "other_revenue",
        "cost_of_goods", "grid_cost", "land_cost", "payroll_expenses",
        "service_agreements", "insurance", "other_operating_expenses",
        "depreciation", "net_interest", "net_other_financial", "tax",
        "comment",
    ]
    df = pd.DataFrame(columns=columns)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Financial Data")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=financial_data_template.xlsx"
        },
    )


@router.get("/by-windfarm/{windfarm_id}", response_model=List[FinancialDataWithEntity])
async def get_by_windfarm(
    windfarm_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get all financial data records for a windfarm."""
    service = FinancialDataService(db)
    return await service.get_by_windfarm(windfarm_id)


@router.get("/summary/{windfarm_id}", response_model=List[FinancialDataSummary])
async def get_summary(
    windfarm_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get financial summary for a windfarm (most recent period per entity)."""
    service = FinancialDataService(db)
    return await service.get_windfarm_financial_summary(windfarm_id)


@router.get("/ratios/{windfarm_id}", response_model=List[FinancialRatiosResponse])
async def get_financial_ratios(
    windfarm_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get computed financial ratios (revenue/MWh, opex/MWh, EBITDA margin) for a windfarm."""
    service = FinancialDataService(db)
    return await service.calculate_financial_ratios(windfarm_id)


@router.get("/{data_id}", response_model=FinancialDataWithEntity)
async def get_data(
    data_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific financial data record by ID."""
    service = FinancialDataService(db)
    record = await service.get_financial_data(data_id)
    if not record:
        raise HTTPException(status_code=404, detail="Financial data record not found")
    return record


@router.post("", response_model=FinancialData, status_code=201)
async def create_data(
    data: FinancialDataCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new financial data record."""
    service = FinancialDataService(db)
    try:
        return await service.create(data)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail="A financial data record already exists for this entity and period",
        )


@router.put("/{data_id}", response_model=FinancialData)
async def update_data(
    data_id: int,
    data: FinancialDataUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update an existing financial data record."""
    service = FinancialDataService(db)
    try:
        updated = await service.update(data_id, data)
        if not updated:
            raise HTTPException(status_code=404, detail="Financial data record not found")
        return updated
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=400,
            detail="A financial data record already exists for this entity and period",
        )


@router.delete("/{data_id}", response_model=FinancialData)
async def delete_data(
    data_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Delete a financial data record."""
    service = FinancialDataService(db)
    deleted = await service.delete(data_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Financial data record not found")
    return deleted


@router.post("/import", response_model=FinancialDataImportResult)
async def import_excel(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Import financial data from an Excel file."""
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

    service = FinancialDataService(db)
    return await service.import_from_excel(file_content, file.filename)
