"""Response models for /scada/ledger: the value-ledger document, version 2.

They follow energyexe-scada-pipeline ``docs/reference/value-ledger-format/document.schema.json``. Money is in
the run's ``currency``; fields ending ``_year`` are per year over the years covered, the others calendar-year
totals. Labels come from ``causes`` and ``groups`` only.
"""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel

LedgerReadStatus = Literal["ok", "stale"]


class LedgerStatusKey(BaseModel):
    trigger: str
    scope: str
    cls: str


class LedgerCause(BaseModel):
    code: str
    label: str
    group: str
    cls: str
    recoverability: str
    summed: bool
    status_key: LedgerStatusKey


class LedgerGroup(BaseModel):
    key: str
    label: str


class LedgerYearDetail(BaseModel):
    year: int
    days_covered: int
    partial_year: bool


class LedgerBridgeDetail(BaseModel):
    key: str
    label: str
    change: float


class LedgerBridgeRow(BaseModel):
    key: str
    label: str
    change: Optional[float]
    headline_after: float
    detail: List[LedgerBridgeDetail]


class LedgerBridgeBasis(BaseModel):
    build: str
    window_from: str
    window_to: str
    headline_year: float


class LedgerRange(BaseModel):
    key: str
    basis_build: str
    basis_headline_year: float
    default_change: float
    low_change: float
    high_change: float
    rebuild: Optional[str]


class LedgerCoverage(BaseModel):
    year: int
    share_valued: Optional[float]
    share_valued_unclaimed_stops_not_counted: Optional[float]
    share_data_gap: Optional[float]
    unexplained_share_of_expected: Optional[float]
    whole_remainder_share_of_expected: Optional[float]
    degradation: Optional[float]
    degradation_lo: Optional[float]
    degradation_hi: Optional[float]


class LedgerCheck(BaseModel):
    n: int
    name: str
    result: str
    window: str
    values: Dict[str, Any]


class LedgerLine(BaseModel):
    line: str
    register_year: Optional[float]
    today_year: float
    ledger_year: float


class LedgerRealised(BaseModel):
    year: int
    value: float
    mwh: float
    unpriced_periods: int


class LedgerRun(BaseModel):
    farm: str
    asset_code: str
    run_id: str
    register_run_id: str
    method_version: str
    format: str
    currency: str
    window_from: str
    window_to: str
    years: float
    years_basis: str
    years_detail: List[LedgerYearDetail]
    headline_year: float
    today_same_years_year: float
    register_year: float
    realised_year: float
    turbines: List[str]
    bridge: List[LedgerBridgeRow]
    bridge_start_basis: str
    bridge_basis: LedgerBridgeBasis
    ranges: List[LedgerRange]
    coverage: List[LedgerCoverage]
    checks: List[LedgerCheck]
    lines: List[LedgerLine]
    realised: List[LedgerRealised]


class LedgerByCause(BaseModel):
    cause: str
    value_year: float
    claimed_mwh_year: Optional[float]


class LedgerByYear(BaseModel):
    year: int
    cause: str
    value: float
    claimed_mwh: Optional[float]
    claims: Optional[int]


class LedgerByTurbine(BaseModel):
    turbine: str
    cause: str
    value_year: float


class LedgerDocument(BaseModel):
    format: str
    run: LedgerRun
    groups: List[LedgerGroup]
    causes: List[LedgerCause]
    by_cause: List[LedgerByCause]
    by_year: List[LedgerByYear]
    by_turbine: List[LedgerByTurbine]


class LedgerStatus(BaseModel):
    """A finding status on one of the run's causes (scada_finding_action, joined on read)."""

    trigger: str
    scope: str
    cls: str
    status: str
    notes: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    actor: Optional[str] = None


class LedgerAsset(BaseModel):
    asset_code: str
    currency: str
    window_from: str
    window_to: str
    status: LedgerReadStatus


class LedgerRegisterBasis(BaseModel):
    """The register's current run, as the portal's first bridge row names it (rollout Stage 8)."""

    window_start_year: int
    window_end_year: int
    ann_years: int


class LedgerResponse(BaseModel):
    """One asset's ledger. ``document`` is null when stale: the register run the ledger was built beside is no
    longer the register's current run (or its rows are missing)."""

    status: LedgerReadStatus
    register_run_id: str
    current_register_run_id: Optional[str]
    register: Optional[LedgerRegisterBasis] = None
    document: Optional[LedgerDocument]
    statuses: List[LedgerStatus]
