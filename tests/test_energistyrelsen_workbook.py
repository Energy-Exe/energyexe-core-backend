from datetime import date, datetime, timezone
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from openpyxl import Workbook

from app.services.energistyrelsen_workbook import parse_vinddata, turbine_attributes
from app.services.energistyrelsen_import import reconcile_vinddata
from scripts.seeds.aggregate_generation_data.process_generation_data_monthly import MonthlyGenerationProcessor

STAMDATA = ["Møllenummer (GSRN)", "Parknummer (relation)", "Dato for oprindelig nettilslutning",
            "Dato for afmeldning", "Kapacitet (kW)", "Rotor-diameter (m)", "Navhøjde (m)",
            "Fabrikat", "Typebetegnelse"]


def workbook(rows, months=("2025-01", "2025-02"), full=False):
    """Rows are [gsrn, park, *month values] or, with full=True, [*STAMDATA, *months]."""
    book = Workbook()
    sheet = book.active
    sheet.title = "Vinddatasæt"
    sheet.append(["Stamdata", None, "Produktionsdata"])
    sheet.append(["Identifikation", None, None])
    headers = STAMDATA if full else STAMDATA[:2]
    sheet.append([*headers, *months])
    for row in rows:
        sheet.append(row)
    output = BytesIO()
    book.save(output)
    return output.getvalue()


def test_workbook_merges_blanks_and_keeps_zero():
    values, metadata, blanks = parse_vinddata(
        workbook([["000123", "P7", "1.234,5", None],
                  ["000123", "P7", None, "0"]]), "2025-01", "2025-02"
    )
    assert values == {("000123", "2025-01"): Decimal("1.235"),
                      ("000123", "2025-02"): Decimal("0.000")}
    assert metadata["000123"]["Parknummer (relation)"] == "P7"
    assert blanks == {"2025-01": 0, "2025-02": 0}


def test_workbook_conflicting_duplicate_fails():
    with pytest.raises(ValueError, match="Conflicting GSRN"):
        parse_vinddata(workbook([["123", None, 1000, None],
                                 ["123", None, 2000, None]]), "2025-01", "2025-02")


def test_month_header_oct_nov_dec():
    months = ("2023-09", "2023-10", "2023-11", "2023-12", "2024-1")
    values, _, _ = parse_vinddata(
        workbook([["123", None, "100", "200", "300", "400", "500"]], months=months),
        "2023-01", "2024-12",
    )
    assert {m: v for (_, m), v in values.items()} == {
        "2023-09": Decimal("0.100"), "2023-10": Decimal("0.200"), "2023-11": Decimal("0.300"),
        "2023-12": Decimal("0.400"), "2024-01": Decimal("0.500"),
    }


def test_turbine_attributes_from_stamdata():
    _, metadata, _ = parse_vinddata(workbook([
        ["123", "9ff2feba-1", "2026-02-23", "", "14700", "222", "150", "SIEMENS", "SG DD-222", "1", "2"],
        ["124", "", "2015-06-15 00:00:00", "2024-11-15", "660", "47", "45", "Vestas", "V 47-660", "", ""],
    ], full=True), "2025-01", "2025-02")
    a = turbine_attributes(metadata["123"])
    assert a["park"] == "9FF2FEBA-1" and a["connected"] == date(2026, 2, 23)
    assert a["decommissioned"] is None and a["capacity_kw"] == Decimal("14700")
    assert a["hub_height_m"] == Decimal("150") and a["model"] == "SG DD-222"
    b = turbine_attributes(metadata["124"])
    assert b["park"] is None and b["connected"] == date(2015, 6, 15)
    assert b["decommissioned"] == date(2024, 11, 15)


def test_monthly_transform_canonicalizes_legacy_six_hour_timestamp():
    processor = MonthlyGenerationProcessor(None)
    processor.turbine_units_cache = {"123": {"id": 1, "windfarm_id": 2, "capacity_mw": 2}}
    raw = SimpleNamespace(id=10, identifier="123", period_start=datetime(2025, 1, 31, 18, tzinfo=timezone.utc),
                          value_extracted=Decimal("0"), data={"month": "2025-02"})
    output = processor.transform_energistyrelsen([raw, raw])
    assert len(output) == 1
    assert output[0].month == datetime(2025, 2, 1, tzinfo=timezone.utc)
    assert output[0].generation_mwh == 0


def test_monthly_transform_keeps_identical_park_shares():
    """Equal park shares are legitimate readings, never held back."""
    processor = MonthlyGenerationProcessor(None)
    processor.turbine_units_cache = {code: {"id": i, "windfarm_id": 7, "capacity_mw": 2}
                                     for i, code in enumerate(("1", "2", "3"))}
    raws = [SimpleNamespace(id=i, identifier=code, period_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                            value_extracted=Decimal("12.345"), data={"month": "2025-01"})
            for i, code in enumerate(("1", "2", "3"))]
    assert len(processor.transform_energistyrelsen(raws)) == 3


class Result:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return self.rows

    def __iter__(self):
        return iter(self.rows)


def fake_db(*results):
    return SimpleNamespace(
        execute=AsyncMock(side_effect=list(results)),
        flush=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock(),
        add=MagicMock(), delete=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_month_rebuild_rolls_back_on_failure(monkeypatch):
    turbine = SimpleNamespace(id=1, code="123", windfarm_id=7)
    db = fake_db(Result([turbine]), Result([]), Result([]))
    monkeypatch.setattr(MonthlyGenerationProcessor, "load_turbine_units", AsyncMock())
    monkeypatch.setattr(MonthlyGenerationProcessor, "process_source_for_month",
                        AsyncMock(side_effect=RuntimeError("rebuild failed")))
    with pytest.raises(RuntimeError, match="rebuild failed"):
        await reconcile_vinddata(db, workbook([["123", None, 1000, None]]),
                                 "2025-01", "2025-01", apply=True)
    db.rollback.assert_awaited_once()
    db.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_tolerance_treats_rounding_as_unchanged():
    turbine = SimpleNamespace(id=1, code="123", windfarm_id=7)
    old = SimpleNamespace(identifier="123", value_extracted=Decimal("1.234"),
                          period_start=None, period_end=None)
    db = fake_db(Result([turbine]), Result([old]), Result([old]))
    report = await reconcile_vinddata(db, workbook([["123", None, "1234,5", "1240"]]),
                                      "2025-01", "2025-02")
    assert report["months"]["2025-01"]["unchanged"] == 1
    assert report["months"]["2025-02"]["revised"] == 1
    assert report["months"]["2025-02"]["revisions"][0]["new"] == "1.240"


@pytest.mark.asyncio
async def test_entsoe_wins_same_farm_month(monkeypatch):
    raw = SimpleNamespace(id=1, identifier="123", period_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                          value_extracted=Decimal("1"), data={"month": "2025-01"})
    db = SimpleNamespace(execute=AsyncMock(side_effect=[Result([raw]), Result([7])]))
    processor = MonthlyGenerationProcessor(db)
    processor.turbine_units_cache = {"123": {"id": 1, "windfarm_id": 7, "capacity_mw": 2}}
    processor.clear_existing_data = AsyncMock()
    processor.save_monthly_records = AsyncMock(return_value=0)
    result = await processor.process_source_for_month(
        "ENERGISTYRELSEN", datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2025, 2, 1, tzinfo=timezone.utc),
    )
    assert result["saved"] == 0
    assert processor.save_monthly_records.await_args.args[0] == []
