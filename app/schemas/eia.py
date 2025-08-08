"""Pydantic schemas for EIA API."""

from datetime import date
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class EIAGenerationDataRequest(BaseModel):
    """Request model for fetching EIA generation data."""

    windfarm_id: int = Field(..., description="Windfarm ID")
    start_year: int = Field(..., description="Start year for data fetch")
    start_month: int = Field(..., ge=1, le=12, description="Start month (1-12)")
    end_year: int = Field(..., description="End year for data fetch")
    end_month: int = Field(..., ge=1, le=12, description="End month (1-12)")


class EIADataPoint(BaseModel):
    """Single data point from EIA API."""

    period: str = Field(..., description="YYYY-MM format period")
    plant_code: str = Field(..., description="Plant code identifier")
    plant_name: Optional[str] = Field(None, description="Plant name")
    generation: float = Field(..., description="Generation value in MWh")
    unit: str = Field(default="MWh", description="Unit of measurement")
    fuel_type: str = Field(default="WND", description="Fuel type (WND for wind)")
    generation_unit_id: Optional[str] = Field(
        None, description="Matched generation unit code from our system"
    )


class EIAGenerationDataResponse(BaseModel):
    """Response model for EIA generation data."""

    data: List[EIADataPoint] = Field(
        default_factory=list, description="List of generation data points"
    )
    metadata: Dict = Field(..., description="Response metadata including request parameters")


class EIAWindfarmGenerationResponse(BaseModel):
    """Response model for windfarm-specific EIA generation data."""

    windfarm: Dict = Field(..., description="Windfarm information")
    generation_units: List[Dict] = Field(..., description="Generation units for the windfarm")
    generation_data: EIAGenerationDataResponse = Field(..., description="Generation data from EIA")
    metadata: Dict = Field(..., description="Additional metadata")
