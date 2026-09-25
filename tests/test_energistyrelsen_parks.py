from datetime import date
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from openpyxl import Workbook

from app.services.energistyrelsen_import import plan_turbine_changes, reconcile_vinddata
from app.services.energistyrelsen_parks import parse_parkproduktion, split_park_values
from scripts.seeds.aggregate_generation_data.process_generation_data_monthly import MonthlyGenerationProcessor
from tests.test_energistyrelsen_workbook import Result, fake_db, workbook

PARK = "9FF2FEBA-CEFC-4570-9052-E430239F5A03"


def park_workbook(rows, months=("2025-01", "2025-02")):
    book = Workbook()
    sheet = book.active
    sheet.title = "Vindmølleparkers produktion"
    sheet.append(["Anlægsdata", "Produktionsdata"])
    sheet.append(["Parknummer (relation)", *months])
    for row in rows:
        sheet.append(row)
    output = BytesIO()
    book.save(output)
    return output.getvalue()


def attrs(park=PARK, connected=date(2020, 1, 1), decommissioned=None):
    return {"park": park, "connected": connected, "decommissioned": decommissioned,
            "capacity_kw": None, "hub_height_m": None, "make": None, "model": None}


def test_parkproduktion_parses_guid_and_danish_comma():
    values = parse_parkproduktion(park_workbook([
        [PARK.lower(), "70321,36", None],
        ["ABC", "1.234.567,5", "0"],
    ]), "2025-01", "2025-02")
    assert values == {(PARK, "2025-01"): Decimal("70321.36"),
                      ("ABC", "2025-01"): Decimal("1234567.5"), ("ABC", "2025-02"): Decimal("0")}
    # A park re-registered mid-month has two partial rows that add up.
    assert parse_parkproduktion(park_workbook([["P", "1", None], ["P", "2", "3"]]), "2025-01", "2025-02") == {
        ("P", "2025-01"): Decimal("3"), ("P", "2025-02"): Decimal("3")}


def test_park_split_uses_active_turbine_denominator():
    attributes = {"1": attrs(), "2": attrs(), "3": attrs(connected=date(2025, 2, 10)),
                  "4": attrs(park="OTHER")}
    values = {}
    provenance, report = split_park_values(
        {(PARK, "2025-01"): Decimal("3000"), (PARK, "2025-02"): Decimal("3000")},
        attributes, matched={"1", "3"}, values=values)
    # January: 2 active turbines; February: 3 (turbine 3 connected mid-month).
    assert values == {("1", "2025-01"): Decimal("1.500"), ("1", "2025-02"): Decimal("1.000"),
                      ("3", "2025-02"): Decimal("1.000")}
    assert provenance[("1", "2025-01")] == {"park": PARK, "split_n": 2, "park_kwh": "3000"}
    assert report["mapped"][PARK]["n_turbines_in_file"] == 3
    assert report["mapped"][PARK]["values_written"] == 3


def test_park_blank_month_and_no_active_turbines_are_reported():
    attributes = {"1": attrs(decommissioned=date(2024, 12, 31))}
    values = {}
    _, report = split_park_values({(PARK, "2025-02"): Decimal("10")}, attributes, {"1"}, values)
    assert values == {}
    assert report["no_active_turbines"] == [[PARK, "2025-02"]]


def test_direct_value_wins_over_park_split():
    values = {("1", "2025-01"): Decimal("9.999")}
    _, report = split_park_values({(PARK, "2025-01"): Decimal("1000")}, {"1": attrs()}, {"1"}, values)
    assert values == {("1", "2025-01"): Decimal("9.999")}
    assert report["direct_wins"] == 1


def test_unmapped_park_with_production_is_reported():
    _, report = split_park_values({(PARK, "2025-01"): Decimal("1000"), (PARK, "2025-03"): Decimal("5")},
                                  {"1": attrs()}, matched=set(), values={})
    assert report["unmapped_with_production"] == {
        PARK: {"n_turbines_in_file": 1, "first_month": "2025-01", "last_month": "2025-03", "n_months": 2}}


def test_plan_turbine_changes_create_and_idempotent():
    windfarm = SimpleNamespace(id=8801, lat=56.3, lng=7.6)
    attributes = {
        "new1": dict(attrs(connected=date(2026, 2, 23)), model="SG DD-222", hub_height_m=Decimal("150")),
        "new2": dict(attrs(connected=date(2026, 3, 1)), model="Unknown-X"),
        "old": dict(attrs(), decommissioned=date(2024, 11, 15)),
        "untracked": dict(attrs(park="OTHER"), model="SG DD-222"),
        "existing": dict(attrs(), decommissioned=date(2025, 6, 1)),
    }
    turbines = {"existing": SimpleNamespace(id=5, code="existing", windfarm_id=8801,
                                            end_date=None, status="operational")}
    changes = plan_turbine_changes(attributes, turbines, {PARK: 8801}, {"sg dd-222": 727},
                                   {8801: windfarm}, "2025-01")
    assert [c["code"] for c in changes["create"]] == ["new1"]
    assert changes["create"][0] == {"code": "new1", "windfarm_id": 8801, "turbine_model_id": 727,
                                    "lat": 56.3, "lng": 7.6, "status": "operational",
                                    "hub_height_m": Decimal("150"), "start_date": date(2026, 2, 23),
                                    "end_date": None}
    assert changes["unresolved"] == [{"gsrn": "new2", "park": PARK, "reasons": ["model_unknown:Unknown-X"]}]
    assert changes["decommission"] == [{"gsrn": "existing", "turbine_unit_id": 5, "windfarm_id": 8801,
                                        "end_date": "2025-06-01", "already_applied": False}]
    # Once created, a second plan yields nothing new.
    turbines["new1"] = SimpleNamespace(id=6, code="new1", windfarm_id=8801, end_date=None, status="operational")
    again = plan_turbine_changes(attributes, turbines, {PARK: 8801}, {"sg dd-222": 727}, {8801: windfarm}, "2025-01")
    assert again["create"] == []


@pytest.mark.asyncio
async def test_reconcile_applies_turbine_changes_before_months(monkeypatch):
    existing = SimpleNamespace(id=5, code="123", windfarm_id=8801, end_date=None, status="operational")
    created = SimpleNamespace(id=6, code="456", windfarm_id=8801, end_date=None, status="operational")
    windfarm = SimpleNamespace(id=8801, lat=56.3, lng=7.6)
    model = SimpleNamespace(id=727, model="SG-14.0-222")
    db = fake_db(
        Result([existing]),            # turbines
        Result([model]),               # turbine models
        Result([windfarm]),            # windfarms for mapped parks
        Result([existing, created]),   # reload after commit
        Result([]),                    # advisory lock
        Result([]),                    # existing raw rows for 2025-01
    )
    monkeypatch.setattr(MonthlyGenerationProcessor, "load_turbine_units", AsyncMock())
    monkeypatch.setattr(MonthlyGenerationProcessor, "process_source_for_month",
                        AsyncMock(return_value={"saved": 2}))
    content = workbook([
        ["123", PARK, "2020-01-01", "2025-06-01", "660", "47", "45", "Vestas", "V47", None, None],
        ["456", PARK, "2024-12-01", "", "14700", "222", "150", "Siemens", "SG DD-222", None, None],
    ], months=("2025-01",), full=True)
    park = park_workbook([[PARK, "2000"]], months=("2025-01",))
    report = await reconcile_vinddata(db, content, "2025-01", "2025-01", apply=True,
                                      park_content=park, model_map={"SG DD-222": 727},
                                      add_turbines=True, update_decommissions=True)
    assert report["turbines"]["applied"] is True
    assert [c["code"] for c in report["turbines"]["create"]] == ["456"]
    assert existing.status == "decommissioned" and existing.end_date == date(2025, 6, 1)
    added_units = [call.args[0] for call in db.add.call_args_list if call.args[0].__class__.__name__ == "TurbineUnit"]
    assert len(added_units) == 1 and added_units[0].code == "456"
    # Park 2000 kWh split over the two active turbines → 1.000 MWh each, both written.
    assert report["months"]["2025-01"] == {"added": 2, "revised": 0, "unchanged": 0, "preserved_absent": 0,
                                           "duplicate_legacy_rows": 0, "from_park": 2, "revisions": [],
                                           "processed": 2}
    raws = [call.args[0] for call in db.add.call_args_list if call.args[0].__class__.__name__ == "GenerationDataRaw"]
    assert {r.identifier: r.value_extracted for r in raws} == {"123": Decimal("1.000"), "456": Decimal("1.000")}
    assert raws[0].data["park"] == PARK and raws[0].data["split_n"] == 2
    assert db.commit.await_count == 2
