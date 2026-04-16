"""Pydantic schemas for the performance pipeline API."""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class PipelineRunRequest(BaseModel):
    windfarm_ids: Optional[List[int]] = Field(None, description="Specific windfarms. If empty, scans all operational.")
    start_year: Optional[int] = None
    end_year: Optional[int] = None


class PipelineRunResponse(BaseModel):
    job_id: int
    windfarms_processed: int
    succeeded: int
    failed: int


class PowerCurveBinResponse(BaseModel):
    wind_bin: float
    q50_pu: Optional[float] = None
    q90_pu: Optional[float] = None
    mean_pu: Optional[float] = None
    mad_pu: Optional[float] = None
    sample_count: int


class PowerCurveResponse(BaseModel):
    windfarm_id: int
    curve_type: str
    year: Optional[int] = None
    bins: List[PowerCurveBinResponse]


class ODIMetricsResponse(BaseModel):
    period_type: str
    year: int
    month: Optional[int] = None
    odi_pct_underperf: Optional[float] = None
    odi_pct_loss_mwh: Optional[float] = None
    odi_pct_loss_eur: Optional[float] = None
    lost_mwh: Optional[float] = None
    lost_eur: Optional[float] = None
    total_hours: Optional[int] = None
    underperf_hours: Optional[int] = None
    long_run_count: Optional[int] = None
    max_run_hours: Optional[int] = None


class NormalisationResponse(BaseModel):
    period_type: str
    year: int
    month: Optional[int] = None
    norm_ratio_p50: Optional[float] = None
    norm_index_p50: Optional[float] = None
    norm_ratio_p10: Optional[float] = None
    norm_index_p10: Optional[float] = None


class DegradationResponse(BaseModel):
    windfarm_id: int
    reference_curve: str
    slope_pct_per_year: Optional[float] = None
    slope_pu_per_year: Optional[float] = None
    r_squared: Optional[float] = None
    p_value: Optional[float] = None
    ci_lower_95: Optional[float] = None
    ci_upper_95: Optional[float] = None
    data_points: int
    analysis_start: date
    analysis_end: date

    class Config:
        from_attributes = True


class PerformanceAnomalyResponse(BaseModel):
    hour: datetime
    anomaly_type: str
    actual_p_pu: Optional[float] = None
    expected_p_pu: Optional[float] = None
    wind_speed: Optional[float] = None
    lost_mwh: Optional[float] = None
    lost_eur: Optional[float] = None
    run_id: Optional[int] = None


class PerformanceSummaryResponse(BaseModel):
    period_type: str
    year: int
    month: Optional[int] = None
    # ODI
    odi_pct_underperf: Optional[float] = None
    lost_mwh: Optional[float] = None
    lost_eur: Optional[float] = None
    # Normalisation
    norm_index_p50: Optional[float] = None
    norm_index_p10: Optional[float] = None
    # Commercial
    constraint_proxy_mwh: Optional[float] = None
    lost_value_eur: Optional[float] = None

    class Config:
        from_attributes = True


class PPAScenarioRequest(BaseModel):
    year: int
    price_scenarios: List[float] = Field(default=[20.0, 25.0, 30.0, 35.0, 40.0])


class PPAScenarioResponse(BaseModel):
    ppa_eur_per_mwh: float
    actual_mwh: float
    revenue_eur: float
    revenue_vs_p50_eur: float
    value_of_1pct_eur_per_year: float
