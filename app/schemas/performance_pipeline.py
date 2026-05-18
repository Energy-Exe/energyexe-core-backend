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
    # Spec items 2/4/5/6: zone-average comparison per bin
    zone_avg_q50_pu: Optional[float] = None
    zone_avg_q90_pu: Optional[float] = None
    zone_windfarm_count: Optional[int] = None


class PowerCurveResponse(BaseModel):
    windfarm_id: int
    curve_type: str
    year: Optional[int] = None
    bins: List[PowerCurveBinResponse]
    bidzone_id: Optional[int] = None
    bidzone_name: Optional[str] = None


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
    # Spec item 5: vs bidzone average
    zone_avg_odi_pct_underperf: Optional[float] = None
    zone_avg_odi_pct_loss_mwh: Optional[float] = None
    zone_avg_odi_pct_loss_eur: Optional[float] = None
    zone_windfarm_count: Optional[int] = None
    vs_zone_diff_underperf: Optional[float] = None
    vs_zone_diff_loss_mwh: Optional[float] = None
    vs_zone_diff_loss_eur: Optional[float] = None


class NormalisationResponse(BaseModel):
    period_type: str
    year: int
    month: Optional[int] = None
    norm_ratio_p50: Optional[float] = None
    norm_index_p50: Optional[float] = None
    norm_ratio_p10: Optional[float] = None
    norm_index_p10: Optional[float] = None


class WindNormalisedHourPoint(BaseModel):
    """Per-hour wind-normalised generation point for the client scatter chart.

    Unblocks faisal-energyexe/energyexe-client-ui#25 — the Actual /
    Wind-normalised toggle. `actual_mwh` plots as the current series;
    `wind_normalised_mwh` plots as the new series. Hours without a power-curve
    value or with wind_speed below the normalisation floor (4 m/s) are
    omitted entirely so the chart only shows qualifying hours.
    """
    hour: datetime
    actual_mwh: float
    expected_mwh: float
    wind_normalised_mwh: float
    norm_ratio: float
    wind_speed: float


class WindNormalisedHourlyResponse(BaseModel):
    windfarm_id: int
    reference_curve: str  # 'q50' or 'q90'
    reference_wind_speed_mps: float
    long_run_avg_norm_ratio: float
    qualifying_hours: int
    points: List[WindNormalisedHourPoint]


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
    # Spec item 4: vs bidzone average
    zone_avg_slope_pct_per_year: Optional[float] = None
    zone_windfarm_count: Optional[int] = None
    vs_zone_diff_pct: Optional[float] = None

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


# ─── Spec item 3: Generation Concentration ────────────────────


class GenerationConcentrationResponse(BaseModel):
    """One persisted concentration summary row (yearly or monthly)."""

    windfarm_id: int
    period_type: str
    year: int
    month: Optional[int] = None
    total_mwh: Optional[float] = None
    total_hours: Optional[int] = None
    weighted_avg_capture_price_eur: Optional[float] = None
    time_weighted_avg_price_eur: Optional[float] = None
    capture_ratio: Optional[float] = None
    top_decile_share_pct: Optional[float] = None
    top_quartile_share_pct: Optional[float] = None
    bottom_decile_share_pct: Optional[float] = None
    bottom_quartile_share_pct: Optional[float] = None
    decile_shares: Optional[Dict[str, float]] = None
    vs_zone_capture_ratio_diff: Optional[float] = None
    vs_zone_top_decile_diff: Optional[float] = None
    computed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


# ─── Cross-cutting: Peer aggregate / vs-zone comparison ───────


class PeerAggregateResponse(BaseModel):
    """Peer-group aggregate of a single metric for one period."""

    group_type: str  # 'bidzone' | 'country' | 'owner' | 'turbine_model'
    group_id: int
    metric_key: str
    period_type: str
    year: int
    month: Optional[int] = None
    windfarm_count: int
    avg_value: Optional[float] = None
    p10_value: Optional[float] = None
    p50_value: Optional[float] = None
    p90_value: Optional[float] = None
    computed_at: Optional[datetime] = None
