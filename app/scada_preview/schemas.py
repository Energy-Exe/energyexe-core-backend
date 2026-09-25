"""Versioned, bounded measured-ingestion payload shared with the pipeline."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, model_validator

Count = Annotated[int, Field(ge=0)]


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Farm(Payload):
    slug: Literal["lutelandet"]
    name: Literal["Lutelandet"]
    windfarm_id: Literal[7197]
    supplied_turbines: list[Literal["T09"]] = Field(min_length=1, max_length=1)
    installed_turbine_count: Literal[9]


class SourceFile(Payload):
    name: str
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    size_bytes: Count
    s3_key: str
    s3_version_id: str


class Source(Payload):
    reporting_label: Literal["2025 delivery"]
    timezone: Literal["Europe/Oslo"]
    interval_seconds: Literal[300]
    interval_label: Literal["unconfirmed", "start", "end"]
    first_label_utc: str
    last_label_utc: str
    window_start_utc: str | None
    window_end_utc: str | None
    files: list[SourceFile] = Field(min_length=3, max_length=3)


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
    daily_trends: list[DailyTrend] = Field(max_length=367)
    power_wind: list[PowerWind] = Field(max_length=500)
    monthly_energy: list[MonthlyEnergy] = Field(max_length=13)


class StatusValue(Payload):
    value: str
    count: Count


class Status(Payload):
    tag: str
    label: str
    records: Count
    values: list[StatusValue] = Field(max_length=1000)


class IngestionSummary(Payload):
    schema_version: Literal[1]
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

    @model_validator(mode="after")
    def respect_unconfirmed_intervals(self):
        if self.source.interval_label == "unconfirmed" and (
            self.source.window_start_utc is not None
            or self.source.window_end_utc is not None
            or self.charts.monthly_energy
            or self.energy.dated_status != "blocked"
        ):
            raise ValueError(
                "Unconfirmed timestamps cannot publish dated energy or interval windows"
            )
        return self


def farm_metadata(summary: IngestionSummary) -> dict:
    return {
        "profile": "measured_native",
        "run_id": summary.run_id,
        "installed_turbine_count": summary.farm.installed_turbine_count,
        "supplied_turbine_count": len(summary.farm.supplied_turbines),
        "interval_seconds": summary.source.interval_seconds,
        "reporting_label": summary.source.reporting_label,
        "interval_label": summary.source.interval_label,
        "processed_at": summary.ingestion.processed_at,
    }
