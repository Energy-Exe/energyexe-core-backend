"""v2 measured-delivery contract, mirrored from the pipeline's
scada_pipeline/measured/summary_contract_v2.py — keep the two byte-identical below
this docstring (tests/test_scada_preview_schemas.py compares them when the pipeline
checkout is present).

v1 (schemas.py IngestionSummary) keeps describing the Lutelandet run. v2 serves a
multi-turbine 10-minute measured farm (Raggovidda, pipeline D-053/D-054): farm and
source identity checked against FARMS, the interval label's evidence source,
longer bounded charts, turbines[] and meters[]. Lutelandet's full fleet (pipeline
D-092) is a second v2 farm: turbines keyed by label, a row breakdown that sums to
the expected rows, yearly energy and the data-system outage events.
"""

from typing import Annotated, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, field_validator, model_validator

Count = Annotated[int, Field(ge=0)]
MAX_SUMMARY_BYTES = 1_500_000

# slug -> fixed identity; a v2 payload for any other farm is refused
FARMS = {
    "raggovidda": {"name": "Raggovidda", "windfarm_id": 7206, "installed_turbine_count": 27},
    "lutelandet": {"name": "Lutelandet", "windfarm_id": 7197, "installed_turbine_count": 9},
}
# farms whose turbines are keyed by the vendor's station id (Siemens WPS)
STATION_ID_FARMS = {"raggovidda"}
# farms that must publish the D-092 outage evidence (row breakdown + outage events)
OUTAGE_FARMS = {"lutelandet"}


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
    files: list[SourceFile] = Field(min_length=1, max_length=120)

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


class Breakdown(Payload):
    """Every expected turbine-interval in exactly one class (D-092)."""
    valid: Count
    held: Count
    blend: Count
    placeholder: Count
    other_invalid: Count
    absent: Count

    def total(self) -> int:
        return self.valid + self.held + self.blend + self.placeholder + self.other_invalid + self.absent


class Coverage(Payload):
    expected_rows: Count
    observed_rows: Count
    duplicate_rows: Count
    signals: list[Signal] = Field(max_length=100)
    power_valid_rows: Count
    wind_valid_rows: Count
    breakdown: Breakdown | None = None


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
    held_intervals: Count | None = None


class YearlyEnergy(Payload):
    """One local calendar year (the delivery's timezone)."""
    year: int = Field(ge=2000, le=2100)
    energy_kwh: FiniteFloat | None
    expected_intervals: Count
    valid_intervals: Count
    held_intervals: Count
    partial: bool


class Outage(Payload):
    start_utc: str
    end_utc: str
    turbines: list[str] = Field(min_length=1, max_length=40)
    held_rows: Count


class Outages(Payload):
    rule: str = Field(max_length=500)
    events_total: Count
    held_rows_total: Count
    events: list[Outage] = Field(max_length=1000)

    @model_validator(mode="after")
    def complete(self):
        if len(self.events) != self.events_total or sum(e.held_rows for e in self.events) != self.held_rows_total:
            raise ValueError("outages: events must list every event and sum to held_rows_total")
        return self


class Charts(Payload):
    daily_trends: list[DailyTrend] = Field(max_length=2500)
    power_wind: list[PowerWind] = Field(max_length=500)
    monthly_energy: list[MonthlyEnergy] = Field(max_length=72)
    yearly_energy: list[YearlyEnergy] = Field(default_factory=list, max_length=10)


class StatusValue(Payload):
    value: str
    count: Count


class Status(Payload):
    tag: str
    label: str
    records: Count
    values: list[StatusValue] = Field(max_length=1000)


class Turbine(Payload):
    station_id: int | None = None
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
    monthly_energy: list[MonthlyEnergy] = Field(max_length=72)
    power_wind: list[PowerWind] = Field(max_length=150)
    breakdown: Breakdown | None = None
    yearly_energy: list[YearlyEnergy] = Field(default_factory=list, max_length=10)


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
    months: list[MeterMonth] = Field(max_length=72)


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
    outages: Outages | None = None

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

    @model_validator(mode="after")
    def station_ids_per_farm(self):
        keyed = self.farm.slug in STATION_ID_FARMS
        if any((t.station_id is None) == keyed for t in self.turbines):
            raise ValueError(f"farm {self.farm.slug}: station ids are "
                             + ("required" if keyed else "not used; key turbines by label"))
        return self

    @model_validator(mode="after")
    def breakdowns_add_up(self):
        if self.farm.slug in OUTAGE_FARMS and (
            self.outages is None or self.coverage.breakdown is None
            or any(t.breakdown is None for t in self.turbines)
        ):
            raise ValueError(f"farm {self.farm.slug} must publish the row breakdown and the outage events")
        if self.farm.slug in OUTAGE_FARMS and (
            not self.charts.yearly_energy or any(not t.yearly_energy for t in self.turbines)
        ):
            raise ValueError(f"farm {self.farm.slug} must publish yearly energy")

        def held_parts_agree(who, b, monthly, yearly):
            held = [m.held_intervals for m in monthly]
            if monthly and all(h is not None for h in held) and sum(held) != b.held:
                raise ValueError(f"{who}: monthly held_intervals do not sum to breakdown.held")
            if yearly and sum(y.held_intervals for y in yearly) != b.held:
                raise ValueError(f"{who}: yearly held_intervals do not sum to breakdown.held")

        for t in self.turbines:
            if t.breakdown is None:
                continue
            if t.breakdown.total() != t.expected_rows:
                raise ValueError(f"turbine {t.label}: breakdown does not sum to expected_rows")
            if t.breakdown.valid != t.power_valid_rows:
                raise ValueError(f"turbine {t.label}: breakdown.valid must equal power_valid_rows")
            if t.observed_rows != t.expected_rows - t.breakdown.absent:
                raise ValueError(f"turbine {t.label}: observed_rows must equal expected_rows - absent")
            held_parts_agree(f"turbine {t.label}", t.breakdown, t.monthly_energy, t.yearly_energy)
        b = self.coverage.breakdown
        if b is not None:
            if b.total() != self.coverage.expected_rows:
                raise ValueError("coverage breakdown does not sum to expected_rows")
            if self.coverage.observed_rows != self.coverage.expected_rows - b.absent:
                raise ValueError("coverage observed_rows must equal expected_rows - absent")
            held_parts_agree("farm", b, self.charts.monthly_energy, self.charts.yearly_energy)
            parts = [t.breakdown for t in self.turbines]
            if all(parts):
                for f in ("valid", "held", "blend", "placeholder", "other_invalid", "absent"):
                    if getattr(b, f) != sum(getattr(p, f) for p in parts):
                        raise ValueError(f"coverage breakdown.{f} is not the sum of the turbines'")
            if b.valid != self.coverage.power_valid_rows or b.valid != self.energy.valid_intervals:
                raise ValueError("breakdown.valid must equal power_valid_rows and energy.valid_intervals")
            if self.outages is not None and self.outages.held_rows_total != b.held:
                raise ValueError("outage held rows must equal breakdown.held")
        if self.outages is not None:
            supplied = set(self.farm.supplied_turbines)
            if any(not set(e.turbines) <= supplied for e in self.outages.events):
                raise ValueError("outage events may only name supplied turbines")
        return self
