"""Pydantic schemas for ENTSOE data."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class GenerationDataPoint(BaseModel):
    """Schema for a single generation data point."""

    timestamp: datetime
    value: float = Field(..., description="Generation value in MW")
    production_type: str = Field(..., description="Type of production (wind, solar)")
    area_code: str = Field(..., description="Area/bidding zone code")
    unit: str = Field(default="MW", description="Unit of measurement")


class GenerationDataRequest(BaseModel):
    """Schema for generation data request."""

    start_date: datetime = Field(..., description="Start date for data query")
    end_date: datetime = Field(..., description="End date for data query")
    area_codes: List[str] = Field(..., description="List of area codes to query")
    production_types: List[str] = Field(
        default=["wind", "solar"], description="List of production types to query"
    )
    store_data: bool = Field(default=True, description="Whether to store data in database")


class AreaMetadata(BaseModel):
    """Metadata for a single area's fetch results."""

    success: bool
    records: int
    errors: List[str]


class GenerationDataMetadata(BaseModel):
    """Metadata for generation data response."""

    areas: Dict[str, AreaMetadata]
    total_records: int
    errors: List[Dict[str, Any]]
    storage: Optional[Dict[str, Any]] = None


class GenerationDataResponse(BaseModel):
    """Schema for generation data response."""

    data: List[Dict[str, Any]] = Field(..., description="Generation data points")
    metadata: Dict[str, Any]


class AreaCodeResponse(BaseModel):
    """Schema for area code information."""

    code: str = Field(..., description="Area/bidding zone code")
    name: str = Field(..., description="Human-readable area name")