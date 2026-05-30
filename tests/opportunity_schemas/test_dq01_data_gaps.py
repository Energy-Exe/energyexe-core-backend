"""DQ-01 detector tests (issue #109) — generation-gap detection + anomaly plumbing.

DQ-01 finds consecutive missing-generation-hour runs over the window, logs each
gap >= 24h as a ``missing_generation_data`` ``DataAnomaly`` (carrying a typed
``gap_hours``), and fires a DQ-01 ``DetectorResult`` at the severity of the
largest gap. Severity ladder: ``>= 72h → CONFIRMED``, ``>= 48h → INDICATIVE``,
``>= 24h → WATCH``, else None.

All tests are DB-free:
  * the pure helpers (:func:`find_generation_gaps`, :func:`classify_gap_severity`)
    are exercised directly;
  * the detector reads gaps injected via
    ``DetectionContext(prefetched={"generation_gaps": [...]})``;
  * the idempotent anomaly upsert is exercised against a mock db that records the
    ``delete`` + ``add`` operations.

Also includes a SQLite-safe migration test (assert importable + callable +
correct ``down_revision``) per the M6 test-DB constraint.
"""

import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from app.models.data_anomaly import AnomalyType, DataAnomaly
from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.dq01_data_gaps import (
    MISSING_GENERATION_DATA,
    classify_gap_severity,
    detect,
    detect_generation_gaps,
    find_generation_gaps,
)

START = datetime(2024, 1, 1)
END = datetime(2024, 2, 1)  # one month window
WF_ID = 100


def _present_hours_except(missing_runs):
    """Build a complete month of hourly timestamps minus the given missing runs.

    ``missing_runs`` is a list of ``(start_offset_hours, length_hours)`` describing
    contiguous runs to remove from the otherwise-complete coverage. Returns the
    list of *present* hours, suitable for ``find_generation_gaps``.
    """
    total_hours = int((END - START).total_seconds() // 3600)
    missing = set()
    for start_off, length in missing_runs:
        for i in range(length):
            missing.add(start_off + i)
    return [START + timedelta(hours=h) for h in range(total_hours) if h not in missing]


def _ctx(generation_gaps):
    return DetectionContext(
        db=None,
        windfarm=WF_ID,
        period_start=START,
        period_end=END,
        prefetched={"generation_gaps": generation_gaps},
    )


# ─── classify_gap_severity (pure) ─────────────────────────────────────────────


def test_severity_boundaries():
    """24→WATCH, 23→None, 48→INDICATIVE, 72→CONFIRMED."""
    assert classify_gap_severity(24) == Severity.WATCH
    assert classify_gap_severity(23) is None
    assert classify_gap_severity(48) == Severity.INDICATIVE
    assert classify_gap_severity(72) == Severity.CONFIRMED


def test_severity_extra_boundaries():
    """47→WATCH, 71→INDICATIVE, 100→CONFIRMED; None/0 → None."""
    assert classify_gap_severity(47) == Severity.WATCH
    assert classify_gap_severity(71) == Severity.INDICATIVE
    assert classify_gap_severity(100) == Severity.CONFIRMED
    assert classify_gap_severity(None) is None
    assert classify_gap_severity(0) is None


# ─── find_generation_gaps (pure) ──────────────────────────────────────────────


def test_complete_month_no_gap():
    """All hours present → no gap."""
    present = _present_hours_except([])
    assert find_generation_gaps(present, START, END) == []


def test_single_25h_gap_found():
    """A single 25h missing run between present data → one gap of 25h."""
    # Remove hours 100..124 inclusive (25 hours). Hour 99 and hour 125 bracket it.
    present = _present_hours_except([(100, 25)])
    gaps = find_generation_gaps(present, START, END)
    assert len(gaps) == 1
    gap_start, gap_end, gap_hours = gaps[0]
    assert gap_hours == 25
    assert gap_start == START + timedelta(hours=100)
    assert gap_end == START + timedelta(hours=125)


def test_two_nonconsecutive_gaps_found_separately():
    """Two separate 20h missing runs → two distinct gaps of 20h each."""
    present = _present_hours_except([(100, 20), (300, 20)])
    gaps = find_generation_gaps(present, START, END)
    assert sorted(g[2] for g in gaps) == [20, 20]


def test_no_present_hours_yields_no_gap():
    """Empty present-hours → no gap (NOT one giant window-sized gap)."""
    assert find_generation_gaps([], START, END) == []


def test_single_present_hour_yields_no_gap():
    """A single present hour cannot bracket a gap → no gap."""
    assert find_generation_gaps([START + timedelta(hours=5)], START, END) == []


# ─── detect() ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_complete_month_no_anomaly():
    """Full coverage → no gaps injected → detect returns None."""
    present = _present_hours_except([])
    gaps = find_generation_gaps(present, START, END)
    ctx = _ctx(gaps)
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_25h_gap_fires_watch():
    """A 25h gap → DQ-01 WATCH finding with max_gap_hours=25."""
    present = _present_hours_except([(100, 25)])
    gaps = find_generation_gaps(present, START, END)
    ctx = _ctx(gaps)
    result = await detect(ctx)
    assert result is not None
    assert result.schema_code is SchemaCode.DQ_01
    assert result.severity is Severity.WATCH
    assert result.data_slots["max_gap_hours"] == 25
    assert result.data_slots["gap_count"] == 1


@pytest.mark.asyncio
async def test_two_nonconsecutive_20h_gaps_do_not_fire():
    """Two separate 20h gaps → neither reaches 24h → detect returns None."""
    present = _present_hours_except([(100, 20), (300, 20)])
    gaps = find_generation_gaps(present, START, END)
    ctx = _ctx(gaps)
    assert await detect(ctx) is None


@pytest.mark.asyncio
async def test_largest_gap_drives_severity():
    """Mixed gaps (25h WATCH + 80h CONFIRMED) → fires CONFIRMED on the largest."""
    present = _present_hours_except([(100, 25), (300, 80)])
    gaps = find_generation_gaps(present, START, END)
    ctx = _ctx(gaps)
    result = await detect(ctx)
    assert result is not None
    assert result.severity is Severity.CONFIRMED
    assert result.data_slots["max_gap_hours"] == 80
    # Both gaps are loggable (>=24h) so both are counted/aggregated.
    assert result.data_slots["gap_count"] == 2
    assert result.data_slots["total_gap_hours"] == 105


@pytest.mark.asyncio
async def test_empty_gaps_returns_none():
    """No gaps at all → None (legacy-scenario / snapshot-safety contract)."""
    assert await detect(_ctx([])) is None
    assert await detect(_ctx(None)) is None


# ─── detect_generation_gaps: idempotent anomaly upsert ────────────────────────


class _CapturingSession:
    """Mock async session that records delete statements and added anomalies.

    ``execute`` distinguishes the present-hours SELECT (returns a result whose
    ``fetchall`` yields the configured present-hour rows) from the reconcile
    DELETE (recorded; returns an empty result). ``add`` collects the inserted
    ``DataAnomaly`` rows so a test can assert the reconciled set.
    """

    def __init__(self, present_hours):
        self._present_hours = present_hours
        self.added: list = []
        self.delete_calls = 0

    async def execute(self, statement, params=None):
        # A text() SELECT carries bind params (the present-hours query); a
        # delete() construct does not. Use that to route.
        if params is not None:
            rows = [type("Row", (), {"present_hour": h})() for h in self._present_hours]
            return type("Res", (), {"fetchall": lambda self: rows})()
        # Otherwise it's the reconcile DELETE.
        self.delete_calls += 1
        return type("Res", (), {})()

    def add(self, obj):
        self.added.append(obj)


@pytest.mark.asyncio
async def test_25h_gap_logs_watch_anomaly():
    """A 25h gap → one missing_generation_data anomaly, gap_hours=25, WATCH-tier."""
    present = _present_hours_except([(100, 25)])
    db = _CapturingSession(present)

    gaps = await detect_generation_gaps(db, WF_ID, START, END)

    assert len(gaps) == 1
    assert gaps[0][2] == 25
    # Exactly one anomaly row was logged.
    assert len(db.added) == 1
    anomaly = db.added[0]
    assert isinstance(anomaly, DataAnomaly)
    assert anomaly.anomaly_type == AnomalyType.MISSING_GENERATION_DATA
    assert anomaly.anomaly_type == "missing_generation_data"
    assert anomaly.anomaly_type == MISSING_GENERATION_DATA
    assert anomaly.gap_hours == 25
    assert anomaly.windfarm_id == WF_ID
    # WATCH-tier gap → "medium" anomaly severity.
    assert anomaly.severity == "medium"
    # The reconcile DELETE always runs first (idempotency).
    assert db.delete_calls == 1


@pytest.mark.asyncio
async def test_sub_threshold_gap_logs_no_anomaly():
    """A 20h gap (< 24h WATCH floor) → no anomaly logged, but DELETE still runs."""
    present = _present_hours_except([(100, 20)])
    db = _CapturingSession(present)

    gaps = await detect_generation_gaps(db, WF_ID, START, END)

    assert gaps == []
    assert db.added == []
    assert db.delete_calls == 1  # reconcile still clears any prior rows


@pytest.mark.asyncio
async def test_upsert_idempotent_on_rerun():
    """After a backfill (gap gone) a re-run removes the row and inserts nothing.

    Run 1: a 25h gap → one anomaly logged.
    Run 2 (backfilled, complete coverage): the reconcile DELETE clears the prior
    anomaly and nothing new is inserted → idempotent (final state has no row).
    """
    # Run 1 — gap present.
    present_with_gap = _present_hours_except([(100, 25)])
    db1 = _CapturingSession(present_with_gap)
    gaps1 = await detect_generation_gaps(db1, WF_ID, START, END)
    assert len(gaps1) == 1
    assert len(db1.added) == 1

    # Run 2 — backfilled (complete coverage), simulating the same reconcile run.
    present_backfilled = _present_hours_except([])
    db2 = _CapturingSession(present_backfilled)
    gaps2 = await detect_generation_gaps(db2, WF_ID, START, END)
    assert gaps2 == []
    # The stale row is removed (DELETE ran) and nothing new is inserted.
    assert db2.delete_calls == 1
    assert db2.added == []


# ─── Migration test (SQLite-safe) ─────────────────────────────────────────────


def _load_migration():
    """Load the gap_hours migration module by path (alembic/versions is not a pkg)."""
    path = (
        Path(__file__).resolve().parents[2]
        / "alembic"
        / "versions"
        / "d4e5f6a7b8c9_add_gap_hours_and_missing_generation_anomaly.py"
    )
    spec = importlib.util.spec_from_file_location("_add_gap_hours_migration", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_migration_chains_onto_enum_expansion_head():
    """down_revision must be the current head c1f2a3b4d5e6 (single linear chain)."""
    mod = _load_migration()
    assert mod.revision == "d4e5f6a7b8c9"
    assert mod.down_revision == "c1f2a3b4d5e6"


def test_migration_upgrade_and_downgrade_are_callable():
    """upgrade/downgrade are importable + callable.

    Per the M6 test-DB constraint we cannot run alembic against the JSONB tables on
    SQLite, so we assert the migration is importable and its add_column/drop_column
    functions are callable (they are exercised for real against Postgres in CI).
    """
    mod = _load_migration()
    assert callable(mod.upgrade)
    assert callable(mod.downgrade)
