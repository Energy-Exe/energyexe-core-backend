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


# BOAV (Bid-Offer Acceptance Volumes) Schemas


class BOAVPairVolume(BaseModel):
    """Volume for a specific acceptance pair (from/to level)."""

    from_level: Optional[float] = Field(
        None, alias="fromLevel", description="Starting MW level"
    )
    to_level: Optional[float] = Field(
        None, alias="toLevel", description="Ending MW level"
    )
    volume: Optional[float] = Field(None, description="Volume in MWh")

    class Config:
        populate_by_name = True


class BOAVDataPoint(BaseModel):
    """Single data point from BOAV API."""

    timestamp: str = Field(..., description="ISO format UTC timestamp")
    settlement_date: str = Field(..., description="UK settlement date (YYYY-MM-DD)")
    settlement_period: int = Field(..., ge=1, le=50, description="Settlement period (1-50)")
    bm_unit: str = Field(..., description="BM Unit identifier")
    acceptance_id: int = Field(..., description="Unique acceptance ID")
    total_volume_accepted: float = Field(
        ..., description="Total volume in MWh (negative for bids/curtailment)"
    )
    acceptance_duration: Optional[str] = Field(
        None, description="'S' (short) or 'L' (long) duration"
    )
    pair_volumes: Optional[List[BOAVPairVolume]] = Field(
        None, description="Detailed volume breakdown by from/to levels"
    )


class BOAVRequest(BaseModel):
    """Request model for fetching BOAV data."""

    settlement_date: str = Field(..., description="Settlement date (YYYY-MM-DD)")
    bid_offer: str = Field(
        ...,
        description="'bid' for curtailment or 'offer' for increase",
        pattern="^(bid|offer)$",
    )
    bm_units: Optional[List[str]] = Field(
        None, description="Optional list of BM Unit IDs to filter"
    )


class BOAVResponse(BaseModel):
    """Response model for BOAV data."""

    data: List[BOAVDataPoint] = Field(
        default_factory=list, description="List of acceptance volume data points"
    )
    metadata: Dict = Field(..., description="Response metadata")


class BOAVAggregatedData(BaseModel):
    """Aggregated BOAV data for a specific hour and BM unit."""

    hour: str = Field(..., description="Hour in ISO format (UTC)")
    bm_unit: str = Field(..., description="BM Unit identifier")
    total_bid_volume_mwh: float = Field(
        default=0.0, description="Sum of bid volumes (curtailment) in MWh"
    )
    total_offer_volume_mwh: float = Field(
        default=0.0, description="Sum of offer volumes in MWh"
    )
    bid_count: int = Field(default=0, description="Number of bid acceptances")
    offer_count: int = Field(default=0, description="Number of offer acceptances")
    acceptance_ids: List[int] = Field(
        default_factory=list, description="List of acceptance IDs included"
    )
