"""v2 measured-delivery contract, mirrored from the pipeline's
scada_pipeline/measured/summary_contract_v2.py — keep the two byte-identical below
this docstring (tests/test_scada_preview_schemas.py compares them when the pipeline
checkout is present).

v1 (schemas.py IngestionSummary) keeps describing the Lutelandet run. v2 serves a
multi-turbine 10-minute measured farm (Raggovidda, pipeline D-053/D-054): farm and
source identity checked against FARMS, the interval label's evidence source,
longer bounded charts, turbines[] and meters[].
"""

from typing import Annotated, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, field_validator, model_validator

Count = Annotated[int, Field(ge=0)]
MAX_SUMMARY_BYTES = 1_500_000

# slug -> fixed identity; a v2 payload for any other farm is refused
FARMS = {
    "raggovidda": {"name": "Raggovidda", "windfarm_id": 7206, "installed_turbine_count": 27},
}


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Farm(Payload):
    slug: str = Field(pattern=r"^[a-z0-9_]+$")
    name: str
    windfarm_id: int
    supplied_turbines: list[str] = Field(min_length=1, max_length=40)
    installed_turbine_count: int = Field(ge=1, le=200)
    identity_status: Literal["provisional", "confirmed"]

    @model_validator(mode="after")
    def known_farm(self):
        fixed = FARMS.get(self.slug)
        if fixed is None:
            raise ValueError(f"unknown v2 farm {self.slug!r}")
        for key, value in fixed.items():
            if getattr(self, key) != value:
                raise ValueError(f"farm {self.slug}: {key} must be {value!r}")
        if len(self.supplied_turbines) > self.installed_turbine_count:
            raise ValueError("more supplied turbines than installed")
        return self


class SourceFile(Payload):
    name: str
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    size_bytes: Count
    s3_key: str
    s3_version_id: str


class Source(Payload):
    reporting_label: str = Field(min_length=1, max_length=80)
    timezone: str
    interval_seconds: Literal[300, 600]
    interval_label: Literal["unconfirmed", "start", "end"]
    interval_evidence_source: Literal["supplier", "inferred"]
    interval_evidence: str = Field(max_length=2000)
    first_label_utc: str
    last_label_utc: str
    window_start_utc: str | None
    window_end_utc: str | None
    files: list[SourceFile] = Field(min_length=1, max_length=60)

    @field_validator("timezone")
    @classmethod
    def real_timezone(cls, tz: str) -> str:
        ZoneInfo(tz)
        return tz


class Ingestion(Payload):
    processed_at: str
    pipeline_version: str
    mapping_version: str
    status: Literal["validated"]


class Signal(Payload):
    tag: str
    label: str
    unit: str
    canonical: str | None
    observed: Count
    missing: Count
    nonfinite: Count


class Coverage(Payload):
    expected_rows: Count
    observed_rows: Count
    duplicate_rows: Count
    signals: list[Signal] = Field(max_length=100)
    power_valid_rows: Count
    wind_valid_rows: Count


class Energy(Payload):
    estimated_kwh: FiniteFloat | None
    method: Literal["mean_power_integral"]
    status: Literal["estimated"]
    assumption: str
    valid_intervals: Count
    missing_intervals: Count
    negative_intervals: Count
    dated_status: Literal["available", "blocked"]
    dated_reason: str | None


class Capability(Payload):
    id: str
    label: str
    supported: bool
    reason: str | None


class DailyTrend(Payload):
    date_utc: str
    power_mean_kw: FiniteFloat | None
    wind_mean_ms: FiniteFloat | None
    power_count: Count
    wind_count: Count


class PowerWind(Payload):
    wind_bin_ms: FiniteFloat
    power_mean_kw: FiniteFloat
    power_p10_kw: FiniteFloat
    power_p90_kw: FiniteFloat
    count: Count


class MonthlyEnergy(Payload):
    month_utc: str
    energy_kwh: FiniteFloat | None
    expected_intervals: Count
    valid_intervals: Count
    partial: bool


class Charts(Payload):
    daily_trends: list[DailyTrend] = Field(max_length=600)
    power_wind: list[PowerWind] = Field(max_length=500)
    monthly_energy: list[MonthlyEnergy] = Field(max_length=24)


class StatusValue(Payload):
    value: str
    count: Count


class Status(Payload):
    tag: str
    label: str
    records: Count
    values: list[StatusValue] = Field(max_length=1000)


class Turbine(Payload):
    station_id: int
    label: str
    group: str = Field(max_length=40)
    model: str | None
    rated_kw: FiniteFloat | None
    expected_rows: Count
    observed_rows: Count
    power_valid_rows: Count
    wind_valid_rows: Count
    missing_days: Count
    energy_kwh: FiniteFloat | None
    monthly_energy: list[MonthlyEnergy] = Field(max_length=24)
    power_wind: list[PowerWind] = Field(max_length=150)


class MeterMonth(Payload):
    month_utc: str
    meter_mwh: FiniteFloat | None
    turbine_sum_mwh: FiniteFloat | None
    reference_mwh: FiniteFloat | None
    hours_compared: Count
    meter_vs_reference: FiniteFloat | None
    turbines_vs_reference: FiniteFloat | None


class Meter(Payload):
    station_id: int
    label: str
    group: str = Field(max_length=40)
    unit: Literal["MW"]
    reference: str = Field(max_length=200)
    expected_rows: Count
    observed_rows: Count
    counter_check: str = Field(max_length=500)
    months: list[MeterMonth] = Field(max_length=24)


class IngestionSummaryV2(Payload):
    schema_version: Literal[2]
    run_id: str = Field(min_length=1, max_length=128)
    farm: Farm
    source: Source
    ingestion: Ingestion
    coverage: Coverage
    energy: Energy
    capabilities: list[Capability] = Field(max_length=50)
    charts: Charts
    statuses: list[Status] = Field(max_length=6)
    limitations: list[str] = Field(max_length=100)
    turbines: list[Turbine] = Field(min_length=1, max_length=40)
    meters: list[Meter] = Field(max_length=4)

    @model_validator(mode="after")
    def respect_unconfirmed_intervals(self):
        if self.source.interval_label == "unconfirmed" and (
            self.source.window_start_utc is not None
            or self.source.window_end_utc is not None
            or self.charts.monthly_energy
            or self.energy.dated_status != "blocked"
            or any(t.monthly_energy for t in self.turbines)
            or any(m.months for m in self.meters)
        ):
            raise ValueError(
                "Unconfirmed timestamps cannot publish dated energy or interval windows"
            )
        return self

    @model_validator(mode="after")
    def turbines_match_farm(self):
        labels = [t.label for t in self.turbines]
        if labels != self.farm.supplied_turbines:
            raise ValueError("turbines[] must list exactly farm.supplied_turbines, in order")
        return self
