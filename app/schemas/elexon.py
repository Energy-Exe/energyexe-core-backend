"""Pydantic schemas for Elexon API."""

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class ElexonGenerationDataRequest(BaseModel):
    """Request model for fetching Elexon generation data."""

    start_date: datetime = Field(..., description="Start date for data fetch")
    end_date: datetime = Field(..., description="End date for data fetch")
    settlement_period_from: Optional[int] = Field(
        None, ge=1, le=50, description="Start settlement period (1-50)"
    )
    settlement_period_to: Optional[int] = Field(
        None, ge=1, le=50, description="End settlement period (1-50)"
    )
    bm_units: Optional[List[str]] = Field(None, description="List of BM Unit IDs to filter")


class ElexonDataPoint(BaseModel):
    """Single data point from Elexon API."""

    timestamp: str = Field(..., description="ISO format timestamp")
    bm_unit: str = Field(..., description="BM Unit identifier")
    value: float = Field(..., description="Generation value in MW")
    unit: str = Field(default="MW", description="Unit of measurement")
    settlement_period: Optional[int] = Field(None, description="Settlement period (1-50)")
    generation_unit_id: Optional[str] = Field(
        None, description="Matched generation unit code from our system"
    )


class ElexonGenerationDataResponse(BaseModel):
    """Response model for Elexon generation data."""

    data: List[ElexonDataPoint] = Field(
        default_factory=list, description="List of generation data points"
    )
    metadata: Dict = Field(..., description="Response metadata")


class ElexonWindfarmGenerationResponse(BaseModel):
    """Response model for windfarm-specific Elexon generation data."""

    windfarm: Dict = Field(..., description="Windfarm information")
    generation_units: List[Dict] = Field(..., description="Generation units for the windfarm")
    generation_data: ElexonGenerationDataResponse = Field(
        ..., description="Generation data from Elexon"
    )
    metadata: Dict = Field(..., description="Additional metadata")
