"""Schemas for the SCADA opportunities (Revenue-at-Risk register) endpoints.

Unlike the SCADA chart endpoints (untyped dicts), this surface is a structured list contract worth
typing — it mirrors the core /opportunities envelope ({items, total, summary}) but ranks by £
(rank_gbp), which the core feature has no column for.
"""

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict


class ScadaOpportunity(BaseModel):
    """One register line: a priced FLEET trigger or a per-turbine condition/alert."""

    model_config = ConfigDict(extra="ignore")

    farm: str
    id: int
    run_id: str
    trigger: Optional[str] = None
    trigger_name: Optional[str] = None
    domain: Optional[str] = None
    action_type: Optional[str] = None
    persona_primary: Optional[str] = None
    commercial_upside: Optional[str] = None
    lead_time: Optional[str] = None
    scope: str
    scope_kind: str
    item: Optional[str] = None
    status: Optional[str] = None
    cls: str
    basis: Optional[str] = None
    gbp_year: Optional[float] = None
    cond_mean_lo: Optional[float] = None
    cond_mean_hi: Optional[float] = None
    cond_worst_hi: Optional[float] = None
    cond_worst_month: Optional[int] = None
    value_of_acting_early: Optional[float] = None
    additive: bool
    confidence: Optional[str] = None
    note: Optional[str] = None
    now_costing_gbp: Optional[float] = None
    now_floor_gbp: Optional[float] = None
    now_basis: Optional[str] = None
    now_available: Optional[bool] = None
    now_confounded: Optional[bool] = None
    rank_gbp: float

    # Phase 7b finding lifecycle — LEFT-joined from scada_finding_action by the stable natural key
    # (farm, trigger, scope, cls). Null on an unworked finding (implicit status NEW).
    lifecycle_status: Optional[str] = None
    lifecycle_notes: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    lifecycle_updated_at: Optional[datetime] = None
    lifecycle_actor: Optional[str] = None


class ScadaClassSummary(BaseModel):
    count: int
    gbp_year: float


class ScadaOpportunitySummary(BaseModel):
    """Farm-level register snapshot — run provenance + per-class rollup (filter-independent)."""

    farm: str
    run_id: str
    site_name: Optional[str] = None
    generated_at: datetime
    window_start_year: Optional[int] = None
    window_end_year: Optional[int] = None
    ann_years: Optional[int] = None
    realized_gbp_year: Optional[float] = None
    recoverable_gbp_year: Optional[float] = None
    curtailment_gbp_year: Optional[float] = None
    headline_gbp_year: Optional[float] = None
    n_rows: int
    register_version: Optional[str] = None
    by_class: Dict[str, ScadaClassSummary]


class ScadaOpportunityListResponse(BaseModel):
    items: List[ScadaOpportunity]
    total: int
    summary: Optional[ScadaOpportunitySummary] = None


class ScadaTrigger(BaseModel):
    code: str
    name: Optional[str] = None
    domain: Optional[str] = None
    sub_domain: Optional[str] = None
    layer: Optional[str] = None
    action_type: Optional[str] = None
    persona_primary: Optional[str] = None
    persona_secondary: Optional[str] = None
    commercial_upside: Optional[str] = None
    confidence: Optional[str] = None
    lead_time: Optional[str] = None
    schema_status: Optional[str] = None


class ScadaFindingActionUpdate(BaseModel):
    """Request to set a finding's lifecycle state, keyed by its stable natural key.

    The client passes the finding's natural key (already on every register row) rather than the
    volatile register ``id`` — the pipeline reassigns ``id`` on every persist. ``status`` must be one
    of ACKNOWLEDGED / CONFIRMED / DISMISSED / RESOLVED (validated in the endpoint).
    """

    farm: str
    trigger: Optional[str] = None
    scope: str
    cls: str
    status: str
    notes: Optional[str] = None


class ScadaFindingActionResult(BaseModel):
    """The persisted lifecycle state returned after a write."""

    farm: str
    trigger: str
    scope: str
    cls: str
    status: str
    notes: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    actor: Optional[str] = None
