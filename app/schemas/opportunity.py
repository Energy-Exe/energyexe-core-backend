"""Pydantic schemas for opportunity detection."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class OpportunityResponse(BaseModel):
    """Full opportunity response."""

    id: int
    windfarm_id: int
    windfarm_name: Optional[str] = None
    schema_code: str
    severity: str
    branch: Optional[str] = None
    status: str
    data_slots: Dict[str, Any] = Field(default_factory=dict)
    missing_slots: List[str] = Field(default_factory=list)
    triggered_by_id: Optional[int] = None
    detection_period_start: datetime
    detection_period_end: datetime
    detection_run_id: Optional[int] = None
    suppression_reason: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class OpportunityListResponse(BaseModel):
    """Paginated opportunity list with summary counts."""

    items: List[OpportunityResponse]
    total: int
    summary: Dict[str, int] = Field(
        default_factory=dict,
        description="Counts by severity: {CONFIRMED: n, INDICATIVE: n, WATCH: n}",
    )


class OpportunityStatusUpdate(BaseModel):
    """Update opportunity status (acknowledge/resolve)."""

    status: str = Field(..., description="New status: ACKNOWLEDGED or RESOLVED")


class OpportunityDetectRequest(BaseModel):
    """Manual trigger for opportunity detection."""

    windfarm_ids: Optional[List[int]] = Field(
        None, description="Specific windfarm IDs to scan. If empty, scans all operational."
    )
    period_months: int = Field(default=24, description="Lookback period in months")
