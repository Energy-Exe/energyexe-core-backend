"""Measured-delivery contracts: v1 (Lutelandet) and v2 (Raggovidda) side by side.

A stored run is parsed by its schema_version. v1 keeps its pinned Lutelandet
identity; v2 checks the farm against its own registry, so neither version can
carry the other's farm. The v2 module must stay identical to the pipeline's.
"""

import asyncio
import copy
import json
from pathlib import Path

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.scada_preview import schemas_v2
from app.scada_preview.schemas import SUMMARY_ADAPTER, IngestionSummary, farm_metadata
from app.scada_preview.schemas_v2 import IngestionSummaryV2
from app.scada_preview.service import MAX_SUMMARY_BYTES, IngestionSummaryService

PIPELINE_V2 = (
    Path(__file__).resolve().parents[2]
    / "energyexe-scada-pipeline/scada_pipeline/measured/summary_contract_v2.py"
)
SHA = "a" * 64


def _source(**over):
    base = {
        "first_label_utc": "2025-01-01T00:10:00Z",
        "last_label_utc": "2025-02-01T00:00:00Z",
        "window_start_utc": "2025-01-01T00:00:00Z",
        "window_end_utc": "2025-02-01T00:00:00Z",
        "interval_label": "end",
    }
    return {**base, **over}


def _common():
    return {
        "ingestion": {
            "processed_at": "2026-10-02T00:00:00+00:00",
            "pipeline_version": "0.9.0",
            "mapping_version": "x",
            "status": "validated",
        },
        "coverage": {
            "expected_rows": 10,
            "observed_rows": 9,
            "duplicate_rows": 0,
            "signals": [],
            "power_valid_rows": 9,
            "wind_valid_rows": 9,
        },
        "energy": {
            "estimated_kwh": 1500.0,
            "method": "mean_power_integral",
            "status": "estimated",
            "assumption": "x",
            "valid_intervals": 9,
            "missing_intervals": 1,
            "negative_intervals": 0,
            "dated_status": "available",
            "dated_reason": None,
        },
        "capabilities": [],
        "charts": {
            "daily_trends": [],
            "power_wind": [],
            "monthly_energy": [
                {
                    "month_utc": "2025-01",
                    "energy_kwh": 1500.0,
                    "expected_intervals": 10,
                    "valid_intervals": 9,
                    "partial": True,
                }
            ],
        },
        "statuses": [],
        "limitations": ["x"],
    }


def v1_payload():
    return {
        "schema_version": 1,
        "run_id": "sfe-1",
        "farm": {
            "slug": "lutelandet",
            "name": "Lutelandet",
            "windfarm_id": 7197,
            "supplied_turbines": ["T09"],
            "installed_turbine_count": 9,
        },
        "source": _source(
            reporting_label="2025 delivery",
            timezone="Europe/Oslo",
            interval_seconds=300,
            interval_label="start",
            files=[
                {
                    "name": f"f{i}",
                    "sha256": SHA,
                    "size_bytes": 1,
                    "s3_key": "k",
                    "s3_version_id": "v",
                }
                for i in range(3)
            ],
        ),
        **_common(),
    }


def v2_payload():
    turbine = {
        "station_id": 3000585,
        "label": "3000585",
        "group": "phase_1",
        "model": "SWT-3.0-101",
        "rated_kw": 3000.0,
        "expected_rows": 10,
        "observed_rows": 9,
        "power_valid_rows": 9,
        "wind_valid_rows": 9,
        "missing_days": 0,
        "energy_kwh": 1500.0,
        "monthly_energy": _common()["charts"]["monthly_energy"],
        "power_wind": [],
    }
    return {
        "schema_version": 2,
        "run_id": "raggovidda-1",
        "farm": {
            "slug": "raggovidda",
            "name": "Raggovidda",
            "windfarm_id": 7206,
            "supplied_turbines": ["3000585"],
            "installed_turbine_count": 27,
            "identity_status": "provisional",
        },
        "source": _source(
            reporting_label="2025-01 – 2026-07 delivery",
            timezone="UTC",
            interval_seconds=600,
            interval_evidence_source="inferred",
            interval_evidence="NVE comparison",
            files=[
                {"name": "z", "sha256": SHA, "size_bytes": 1, "s3_key": "k", "s3_version_id": "v"}
            ],
        ),
        **_common(),
        "turbines": [turbine],
        "meters": [
            {
                "station_id": 91,
                "label": "Park meter 91",
                "group": "phase_1",
                "unit": "MW",
                "reference": "NVE",
                "expected_rows": 10,
                "observed_rows": 10,
                "counter_check": "ok",
                "months": [],
            }
        ],
    }


def test_versions_dispatch_on_schema_version():
    assert isinstance(SUMMARY_ADAPTER.validate_python(v1_payload()), IngestionSummary)
    assert isinstance(SUMMARY_ADAPTER.validate_python(v2_payload()), IngestionSummaryV2)
    with pytest.raises(ValidationError):
        SUMMARY_ADAPTER.validate_python({**v2_payload(), "schema_version": 3})


def test_neither_version_carries_the_other_farm():
    crossed = v1_payload()
    crossed["farm"]["slug"] = "raggovidda"
    with pytest.raises(ValidationError):
        SUMMARY_ADAPTER.validate_python(crossed)
    crossed = v2_payload()
    crossed["farm"].update(slug="kelmarsh", name="Kelmarsh", windfarm_id=1)
    with pytest.raises(ValidationError, match="unknown v2 farm"):
        SUMMARY_ADAPTER.validate_python(crossed)
    # Lutelandet is a v2 farm too (D-092), but only with its own rules: a Siemens-shaped
    # payload (station ids, no row breakdown) relabelled as Lutelandet is refused.
    crossed = v2_payload()
    crossed["farm"].update(
        slug="lutelandet", name="Lutelandet", windfarm_id=7197, installed_turbine_count=9
    )
    with pytest.raises(ValidationError, match="station ids are not used"):
        SUMMARY_ADAPTER.validate_python(crossed)
    for t in crossed["turbines"]:
        t.pop("station_id")
    with pytest.raises(ValidationError, match="must publish the row breakdown"):
        SUMMARY_ADAPTER.validate_python(crossed)


def test_v2_identity_and_unconfirmed_rules():
    bad = v2_payload()
    bad["farm"]["windfarm_id"] = 1
    with pytest.raises(ValidationError, match="windfarm_id"):
        SUMMARY_ADAPTER.validate_python(bad)
    bad = v2_payload()
    bad["source"]["interval_label"] = "unconfirmed"
    with pytest.raises(ValidationError, match="Unconfirmed"):
        SUMMARY_ADAPTER.validate_python(bad)


def test_farm_metadata_reads_both_versions():
    for payload in (v1_payload(), v2_payload()):
        meta = farm_metadata(SUMMARY_ADAPTER.validate_python(payload))
        assert meta["profile"] == "measured_native"
        assert meta["run_id"] == payload["run_id"]


def lut_v2_payload():
    """Lutelandet's fleet shape (D-092): label-keyed turbines, a row breakdown,
    yearly energy, held intervals per month and the outage events."""
    p = v2_payload()
    p["run_id"] = "lutelandet-1"
    p["farm"].update(
        slug="lutelandet",
        name="Lutelandet",
        windfarm_id=7197,
        supplied_turbines=["T09"],
        installed_turbine_count=9,
        identity_status="confirmed",
    )
    p["source"].update(timezone="Europe/Oslo", interval_seconds=300, interval_label="start")
    b = {"valid": 9, "held": 1, "blend": 0, "placeholder": 0, "other_invalid": 0, "absent": 0}
    year = {
        "year": 2025,
        "energy_kwh": 1500.0,
        "expected_intervals": 10,
        "valid_intervals": 9,
        "held_intervals": 1,
        "partial": True,
    }
    p["coverage"]["breakdown"] = dict(b)
    p["charts"]["yearly_energy"] = [year]
    p["charts"]["monthly_energy"][0]["held_intervals"] = 1
    t = p["turbines"][0]
    t.pop("station_id")
    t.update(label="T09", group="Lutelandet", model="Nordex N149/5700", rated_kw=5700.0)
    t.update(breakdown=dict(b), yearly_energy=[year])
    p["meters"] = []
    p["outages"] = {
        "rule": "held",
        "events_total": 1,
        "held_rows_total": 1,
        "events": [
            {
                "start_utc": "2025-01-01T00:00:00Z",
                "end_utc": "2025-01-01T00:05:00Z",
                "turbines": ["T09"],
                "held_rows": 1,
            }
        ],
    }
    return p


def test_lutelandet_fleet_v2_validates_and_adds_up():
    s = SUMMARY_ADAPTER.validate_python(lut_v2_payload())
    assert isinstance(s, IngestionSummaryV2) and s.turbines[0].station_id is None
    assert s.outages.held_rows_total == s.coverage.breakdown.held == 1
    bad = lut_v2_payload()
    bad["turbines"][0]["breakdown"]["valid"] = 8
    with pytest.raises(ValidationError, match="does not sum"):
        SUMMARY_ADAPTER.validate_python(bad)
    bad = lut_v2_payload()
    bad["outages"]["held_rows_total"] = 2
    with pytest.raises(ValidationError):
        SUMMARY_ADAPTER.validate_python(bad)
    bad = lut_v2_payload()
    bad["coverage"]["breakdown"].update(valid=8, held=2)
    bad["turbines"][0]["breakdown"].update(valid=8, held=2)
    with pytest.raises(ValidationError, match="breakdown.valid must equal"):
        SUMMARY_ADAPTER.validate_python(bad)


def test_raggovidda_still_needs_station_ids():
    p = v2_payload()
    p["turbines"][0].pop("station_id")
    with pytest.raises(ValidationError, match="station ids are required"):
        SUMMARY_ADAPTER.validate_python(p)


def test_service_serves_lutelandet_v2_and_falls_back_to_v1_on_rollback():
    for payload, kind in ((lut_v2_payload(), IngestionSummaryV2), (v1_payload(), IngestionSummary)):
        db = _Session({"lutelandet": payload, "raggovidda": v2_payload()})
        svc = IngestionSummaryService(db)
        assert isinstance(asyncio.run(svc.get("lutelandet")), kind)
        farms = {f["farm"]: f for f in asyncio.run(svc.farms())["farms"]}
        assert farms["lutelandet"]["windfarm_id"] == 7197


def test_worst_case_lutelandet_fleet_payload_fits_the_store_cap():
    p = lut_v2_payload()
    month = p["charts"]["monthly_energy"][0]
    year = p["charts"]["yearly_energy"][0]
    t = p["turbines"][0]
    t["monthly_energy"] = [month] * 60
    t["yearly_energy"] = [year] * 10
    t["power_wind"] = [
        {
            "wind_bin_ms": 1.25,
            "power_mean_kw": 1.0,
            "power_p10_kw": 1.0,
            "power_p90_kw": 1.0,
            "count": 9,
        }
    ] * 150
    p["turbines"] = [dict(copy.deepcopy(t), label=f"T{i:02d}") for i in range(1, 10)]
    p["farm"]["supplied_turbines"] = [f"T{i:02d}" for i in range(1, 10)]
    p["coverage"]["breakdown"] = {k: v * 9 for k, v in p["coverage"]["breakdown"].items()}
    p["coverage"]["expected_rows"] = 90
    p["coverage"]["power_valid_rows"] = p["energy"]["valid_intervals"] = 81
    p["outages"]["held_rows_total"] = p["outages"]["events_total"] = 9
    p["outages"]["events"] = [
        dict(p["outages"]["events"][0], turbines=p["farm"]["supplied_turbines"], held_rows=1)
    ] * 9
    SUMMARY_ADAPTER.validate_python(p)
    p["outages"]["events"] = p["outages"]["events"] * 55  # 495 events: size only
    p["charts"]["monthly_energy"] = [month] * 60
    p["charts"]["daily_trends"] = [
        {
            "date_utc": "2025-01-01",
            "power_mean_kw": 1.0,
            "wind_mean_ms": 1.0,
            "power_count": 1,
            "wind_count": 1,
        }
    ] * 1500
    p["source"]["files"] = p["source"]["files"] * 120
    assert len(json.dumps(p).encode()) < MAX_SUMMARY_BYTES


@pytest.mark.skipif(not PIPELINE_V2.exists(), reason="pipeline checkout not alongside")
def test_v2_contract_mirrors_the_pipeline():
    body = lambda text: text[text.index("from typing") :]  # noqa: E731
    assert body(Path(schemas_v2.__file__).read_text()) == body(PIPELINE_V2.read_text())


# --- the service, driven end to end with a fake session ---------------------


class _Result:
    def __init__(self, scalar=None, row=None, rows=()):
        self._scalar, self._row, self._rows = scalar, row, rows

    def scalar_one_or_none(self):
        return self._scalar

    def mappings(self):
        return self

    def one_or_none(self):
        return self._row

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _Nested:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Session:
    def __init__(self, runs: dict[str, dict]):
        self.runs = runs

    def begin_nested(self):
        return _Nested()

    async def execute(self, query, params=None):
        sql = str(query)
        if "to_regclass" in sql:
            return _Result(scalar="scada_ingestion.ingestion_run")
        if "SELECT farm FROM" in sql:
            return _Result(rows=sorted(self.runs))
        summary = self.runs.get(params["farm"])
        return _Result(
            row=None
            if summary is None
            else {"run_id": summary["run_id"], "summary": json.dumps(summary)}
        )


def test_service_lists_and_reads_both_measured_farms():
    db = _Session({"lutelandet": v1_payload(), "raggovidda": v2_payload()})
    svc = IngestionSummaryService(db)
    farms = {f["farm"]: f for f in asyncio.run(svc.farms())["farms"]}
    assert set(farms) == {"lutelandet", "raggovidda"}
    assert farms["raggovidda"]["n_turbines"] == 1
    assert farms["raggovidda"]["ingestion"]["interval_seconds"] == 600
    assert isinstance(asyncio.run(svc.get("raggovidda")), IngestionSummaryV2)
    assert isinstance(asyncio.run(svc.get("lutelandet")), IngestionSummary)


def test_service_refuses_a_run_stored_under_the_wrong_farm():
    db = _Session({"raggovidda": v1_payload()})
    with pytest.raises(HTTPException) as err:
        asyncio.run(IngestionSummaryService(db).get("raggovidda"))
    assert err.value.status_code == 503


def test_worst_case_v2_payload_fits_the_store_cap():
    p = v2_payload()
    month = p["charts"]["monthly_energy"][0]
    t = p["turbines"][0]
    t["monthly_energy"] = [month] * 24
    t["power_wind"] = [
        {
            "wind_bin_ms": 1.25,
            "power_mean_kw": 1.0,
            "power_p10_kw": 1.0,
            "power_p90_kw": 1.0,
            "count": 9,
        }
    ] * 150
    p["turbines"] = [dict(copy.deepcopy(t), label=str(i), station_id=i) for i in range(27)]
    p["farm"]["supplied_turbines"] = [str(i) for i in range(27)]
    p["charts"]["monthly_energy"] = [month] * 24
    p["charts"]["daily_trends"] = [
        {
            "date_utc": "2025-01-01",
            "power_mean_kw": 1.0,
            "wind_mean_ms": 1.0,
            "power_count": 1,
            "wind_count": 1,
        }
    ] * 600
    SUMMARY_ADAPTER.validate_python(p)
    assert len(json.dumps(p).encode()) < MAX_SUMMARY_BYTES
