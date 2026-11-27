"""D-095: a register row is served with the wording of its own run.

The name comes from the run's line record (``scada.opportunity_record``, ``record_kind = 'line'``) and falls back to
the catalogue name; a row that is CONTEXT and not additive carries no catalogue action type or commercial upside.
The catalogue table itself is never written. The SQL is Postgres-only, so the query text and the row rule are checked
against a fake session (as in ``test_scada_opportunity_farm_discovery``).
"""

import asyncio

import pytest

from app.services import scada_opportunity_service as module
from app.services.scada_opportunity_service import ScadaOpportunityService, run_wording


def _row(**kw):
    base = {
        "farm": "hill_of_towie",
        "asset_code": "HOT",
        "id": 7,
        "run_id": "run-1",
        "trigger": "OPS_03",
        "scope": "FLEET",
        "cls": "CONTEXT",
        "additive": False,
        "trigger_name": "Residual Power-Curve Underperformance (Priced)",
        "domain": "OPS",
        "action_type": "Maintenance queue",
        "persona_primary": "Asset manager",
        "commercial_upside": "Cheap fixes, direct value recovery",
        "lead_time": "n/a",
        "line_name": "Power-curve comparison with the fleet",
    }
    base.update(kw)
    return base


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return [type("Row", (), {"_mapping": r})() for r in self._rows]

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else None

    def scalar(self):
        return len(self._rows)


class _FakeDb:
    def __init__(self, rows):
        self.rows, self.sql = rows, []

    async def execute(self, statement, params=None):
        self.sql.append(str(statement))
        return _Result(self.rows)


@pytest.fixture
def records(monkeypatch):
    """Sets whether the interpretation tables exist; the summary and condition labels are not under test."""

    def _set(present: bool):
        async def _present(db):
            return present

        async def _summary(self, farm, asset):
            return {"currency": "GBP"}

        async def _labels(self, farm, asset):
            return []

        monkeypatch.setattr(module, "scada_records_present", _present)
        monkeypatch.setattr(ScadaOpportunityService, "summary", _summary)
        monkeypatch.setattr(ScadaOpportunityService, "condition_labels", _labels)

    return _set


# --- the row rule ---------------------------------------------------------------------------------------------------


def test_the_runs_own_line_name_is_preferred():
    out = run_wording(_row())
    assert out["trigger_name"] == "Power-curve comparison with the fleet"
    assert "line_name" not in out


@pytest.mark.parametrize("line_name", [None, "", "   "])
def test_the_catalogue_name_is_the_fallback(line_name):
    out = run_wording(_row(line_name=line_name))
    assert out["trigger_name"] == "Residual Power-Curve Underperformance (Priced)"
    assert "line_name" not in out


def test_a_row_without_the_joined_column_keeps_the_catalogue_name():
    row = _row()
    del row["line_name"]
    assert run_wording(row)["trigger_name"] == "Residual Power-Curve Underperformance (Priced)"


@pytest.mark.parametrize("additive", [False, None])
def test_a_descriptive_context_row_carries_no_action_or_upside(additive):
    out = run_wording(_row(additive=additive))
    assert out["action_type"] is None and out["commercial_upside"] is None
    # the rest of the catalogue join is untouched
    assert out["domain"] == "OPS" and out["persona_primary"] == "Asset manager"
    assert out["lead_time"] == "n/a"


@pytest.mark.parametrize(
    "cls, additive",
    [
        ("REALIZED", True),
        ("RECOVERABLE", True),
        ("RECOVERABLE", False),  # shown-not-summed but still a loss class: the catalogue copy stays
        ("CURTAILMENT", False),
        ("CONDITIONAL", False),
        ("ALERT", False),
        ("CONTEXT", True),
    ],
)
def test_every_other_row_keeps_the_catalogue_action_and_upside(cls, additive):
    out = run_wording(_row(cls=cls, additive=additive))
    assert out["action_type"] == "Maintenance queue"
    assert out["commercial_upside"] == "Cheap fixes, direct value recovery"


def test_the_rule_names_no_trigger_code():
    """Data-driven: the same class and additivity give the same answer whatever the trigger."""
    for trigger in ("OPS_03", "OPS_15", "FIN_04", "DET_aux", None):
        out = run_wording(_row(trigger=trigger))
        assert out["action_type"] is None and out["commercial_upside"] is None
        kept = run_wording(_row(trigger=trigger, cls="REALIZED", additive=True))
        assert kept["action_type"] == "Maintenance queue"


def test_the_input_row_is_not_mutated():
    row = _row()
    run_wording(row)
    assert row["line_name"] and row["action_type"] == "Maintenance queue"


# --- the query ------------------------------------------------------------------------------------------------------


def _line_join(sql: str) -> bool:
    return "LEFT JOIN scada.opportunity_record ln" in sql


def test_list_reads_the_line_name_of_the_same_run(records):
    records(True)
    db = _FakeDb([_row()])
    out = asyncio.run(ScadaOpportunityService(db).list_opportunities("hill_of_towie", asset="HOT"))
    select = db.sql[0]
    assert "ln.name AS line_name" in select and _line_join(select)
    for clause in (
        "ln.farm = r.farm",
        "ln.asset_code = r.asset_code",
        "ln.run_id = r.run_id",
        "ln.record_kind = 'line'",
        "ln.record_key = r.trigger",
    ):
        assert clause in select
    # the catalogue is still the fallback source and the count stays on the base join
    assert "t.name AS trigger_name" in select
    assert not _line_join(db.sql[-1]) and "COUNT(*)" in db.sql[-1]
    item = out["items"][0]
    assert item["trigger_name"] == "Power-curve comparison with the fleet"
    assert item["action_type"] is None and item["commercial_upside"] is None
    assert item["currency"] == "GBP" and "line_name" not in item


def test_get_reads_the_line_name_of_the_same_run(records):
    records(True)
    db = _FakeDb([_row(cls="REALIZED", additive=True)])
    out = asyncio.run(ScadaOpportunityService(db).get("hill_of_towie", 7, "HOT"))
    assert _line_join(db.sql[0]) and "ln.run_id = r.run_id" in db.sql[0]
    assert out["trigger_name"] == "Power-curve comparison with the fleet"
    assert out["action_type"] == "Maintenance queue" and "line_name" not in out


def test_without_the_record_table_the_query_does_not_touch_it(records):
    """An environment that has the register but not the interpretation tables keeps the catalogue name."""
    records(False)
    row = _row()
    del row["line_name"]
    db = _FakeDb([row])
    service = ScadaOpportunityService(db)
    listed = asyncio.run(service.list_opportunities("hill_of_towie", asset="HOT"))
    got = asyncio.run(service.get("hill_of_towie", 7, "HOT"))
    assert not any("opportunity_record" in sql for sql in db.sql)
    assert listed["items"][0]["trigger_name"] == "Residual Power-Curve Underperformance (Priced)"
    assert got["trigger_name"] == "Residual Power-Curve Underperformance (Priced)"
    # the class rule does not depend on the record table
    assert listed["items"][0]["action_type"] is None and got["commercial_upside"] is None


def test_no_query_writes_the_catalogue(records):
    records(True)
    db = _FakeDb([_row()])
    asyncio.run(ScadaOpportunityService(db).list_opportunities("hill_of_towie", asset="HOT"))
    for sql in db.sql:
        assert sql.lstrip().upper().startswith("SELECT")
