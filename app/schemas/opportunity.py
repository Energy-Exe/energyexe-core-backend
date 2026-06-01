"""Pydantic schemas for opportunity detection."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class TriggeredBySummary(BaseModel):
    """Compact summary of the parent opportunity that triggered a dependent one.

    Dependent schemas (e.g. OPS-03 → OPS-01, MKT-02 → MKT-01) only fire when a
    parent finding exists. Surfacing the parent's name/severity here lets the UI
    render a meaningful link ("Volatile Disruption Periods (OPS-01), Confirmed")
    instead of a bare numeric ``triggered_by_id``.
    """

    id: int
    schema_code: str
    schema_name: Optional[str] = None
    severity: str
    status: str


class OpportunityResponse(BaseModel):
    """Full opportunity response."""

    id: int
    windfarm_id: int
    windfarm_name: Optional[str] = None
    schema_code: str
    schema_name: Optional[str] = Field(
        default=None,
        description=(
            "Human-readable schema name (e.g. 'Volatile Disruption Periods'), "
            "resolved from SCHEMA_NAMES. Null for unknown/legacy codes — clients "
            "should fall back to schema_code in that case."
        ),
    )
    schema_one_liner: Optional[str] = Field(
        default=None,
        description=(
            "One-line analyst-facing meaning of the schema (from SCHEMA_ONE_LINERS), "
            "for surfacing as a 'what this means' subtitle. Null for unknown codes."
        ),
    )
    severity: str
    branch: Optional[str] = None
    status: str
    data_slots: Dict[str, Any] = Field(default_factory=dict)
    missing_slots: List[str] = Field(default_factory=list)
    triggered_by_id: Optional[int] = None
    triggered_by: Optional[TriggeredBySummary] = Field(
        default=None,
        description=(
            "Resolved parent-opportunity summary when this finding is dependent "
            "(triggered_by_id is set). Null for standalone findings."
        ),
    )
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
        None,
        description=(
            "Specific windfarm IDs to scan. If provided, the run is executed "
            "SYNCHRONOUSLY and results are returned inline (scoped, fast — for "
            "single-asset debugging). If empty/null, a fleet-wide run is scheduled "
            "as a BACKGROUND job and a job_id is returned immediately."
        ),
    )
    period_months: int = Field(default=24, description="Lookback period in months")
    schema_codes: Optional[List[str]] = Field(
        None,
        description=(
            "Optional whitelist of schema codes to run (e.g. ['OPS_01', 'MKT_01']). "
            "If null/empty, every registered schema runs."
        ),
    )


class DetectionTriggerResponse(BaseModel):
    """Response for a backgrounded fleet-wide POST /detect run.

    The detection job is scheduled as a FastAPI background task and this returns
    immediately; poll progress/results via GET /opportunities and the
    ``import_job_executions`` row identified by ``job_id``.
    """

    job_id: int = Field(..., description="ImportJobExecution id tracking this run")
    status: str = Field("scheduled", description="Job status at trigger time (e.g. 'scheduled')")
    mode: str = Field(
        "background",
        description="Execution mode: 'background' (fleet-wide) or 'sync' (scoped)",
    )
    message: Optional[str] = Field(None, description="Human-readable status detail")
