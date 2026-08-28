"""Reliability tests for OpportunityDetectionService run/commit/failure handling.

DB-free: a ``_FakeSession`` records commit/rollback/add and stubs ``execute`` /
``get``. ``_detect_windfarm`` / ``detect_all`` are monkeypatched so each test
drives a specific success/failure shape without a real Postgres.

Covers:
  * B2 — per-windfarm atomicity: each windfarm commits; a failing one rolls back
    and is counted, the rest still commit (no all-or-nothing loss).
  * B3 — job_id reuse: an existing job row is reused (not duplicated).
  * B4 — no SUCCESS-on-total-failure: all windfarms erroring → job FAILED.
  * EPR-126 — per-windfarm window clip: the end is the last instant of the
    farm's last metered day (never the start); no data / current data / a
    failed probe all leave the window whole; stats reach ``job_metadata``.
"""

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import select, text

from app.models.import_job_execution import ImportJobExecution, ImportJobStatus
from app.services import opportunity_detection_service as svc_module
from app.services.generation_coverage import GenerationCoverage
from app.services.opportunity_detection_service import OpportunityDetectionService


class _EmptyResult:
    def fetchall(self):
        return []

    def scalar_one_or_none(self):
        return None

    def first(self):
        return None  # EPR-126 coverage probe → "no data in window"


class _FakeSession:
    """Minimal AsyncSession stand-in tracking transaction calls."""

    def __init__(self, job=None):
        self.committed = 0
        self.rolled_back = 0
        self.refreshed = 0
        self.added: list = []
        self._job = job

    async def refresh(self, obj, *a, **k):
        self.refreshed += 1

    async def execute(self, *a, **k):
        return _EmptyResult()

    async def commit(self):
        self.committed += 1

    async def rollback(self):
        self.rolled_back += 1

    def add(self, obj):
        if getattr(obj, "id", None) is None:
            obj.id = 1
        self.added.append(obj)

    async def flush(self):
        pass

    async def get(self, model, pk):
        return self._job


def _service(job=None):
    svc = OpportunityDetectionService.__new__(OpportunityDetectionService)
    svc.db = _FakeSession(job=job)
    svc.price_analytics = None
    svc._last_succeeded = 0
    svc._last_failed = 0
    svc._last_truncated = 0
    svc._last_window_stats = {}
    return svc


@pytest.mark.asyncio
async def test_detect_all_commits_each_windfarm_and_isolates_failures():
    """B2: windfarm #2 errors → rolled back + counted; #1 and #3 still commit."""
    svc = _service()

    async def fake_detect_windfarm(wf_id, *a, **k):
        if wf_id == 2:
            raise RuntimeError("boom")
        return [object()]  # one "opportunity"

    svc._detect_windfarm = fake_detect_windfarm

    opps = await svc.detect_all([1, 2, 3], period_months=24)

    assert len(opps) == 2  # only #1 and #3 produced rows
    assert svc._last_succeeded == 2
    assert svc._last_failed == 1
    assert svc.db.committed == 2  # one commit per successful windfarm
    assert svc.db.rolled_back == 1  # the failing windfarm rolled back


@pytest.mark.asyncio
async def test_run_detection_job_marks_failed_when_all_windfarms_error():
    """B4: succeeded==0 and failed>0 → the job row is FAILED, not SUCCESS."""
    svc = _service()

    async def fake_detect_all(windfarm_ids, *a, **k):
        svc._last_succeeded = 0
        svc._last_failed = len(windfarm_ids)
        return []

    svc.detect_all = fake_detect_all

    result = await svc.run_detection_job(windfarm_ids=[1, 2, 3])

    job = svc.db.added[0]
    assert isinstance(job, ImportJobExecution)
    assert job.status == ImportJobStatus.FAILED
    assert job.job_metadata["failed"] == 3
    assert result["windfarms_failed"] == 3


@pytest.mark.asyncio
async def test_run_detection_job_marks_success_with_counts_on_partial():
    """B4: at least one success → SUCCESS, with succeeded/failed in job_metadata."""
    svc = _service()

    async def fake_detect_all(windfarm_ids, *a, **k):
        svc._last_succeeded = 2
        svc._last_failed = 1
        return [object(), object()]

    svc.detect_all = fake_detect_all

    await svc.run_detection_job(windfarm_ids=[1, 2, 3])

    job = svc.db.added[0]
    assert job.status == ImportJobStatus.SUCCESS
    assert job.job_metadata == {**job.job_metadata, "succeeded": 2, "failed": 1}


@pytest.mark.asyncio
async def test_run_detection_job_reuses_existing_job_row():
    """B3: a supplied job_id reuses that row (no duplicate ImportJobExecution)."""
    existing = ImportJobExecution(
        job_name="opportunity-detection",
        source="SYSTEM",
        job_type="manual",
        status=ImportJobStatus.PENDING,
    )
    existing.id = 4242
    svc = _service(job=existing)

    async def fake_detect_all(windfarm_ids, *a, **k):
        svc._last_succeeded = len(windfarm_ids)
        svc._last_failed = 0
        return []

    svc.detect_all = fake_detect_all

    result = await svc.run_detection_job(windfarm_ids=[1], job_id=4242)

    assert result["job_id"] == 4242
    assert svc.db.added == []  # reused, NOT a new row
    assert existing.status == ImportJobStatus.SUCCESS  # driven to a terminal state


# ── EPR-126: per-windfarm window truncation ──────────────────────────────

_NOW_UTC = datetime(2026, 8, 27, 2, 0, tzinfo=timezone.utc)
_NOW = _NOW_UTC.replace(tzinfo=None)


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return _NOW_UTC if tz is not None else _NOW


def _freeze_now(monkeypatch):
    monkeypatch.setattr(svc_module, "datetime", _FrozenDatetime)


def _capture_windows(svc):
    """Record the (start, end, kwargs) each windfarm is detected with."""
    calls = []

    async def fake_detect_windfarm(wf_id, period_start, period_end, *a, **k):
        calls.append({"wf_id": wf_id, "start": period_start, "end": period_end, **k})
        return []

    svc._detect_windfarm = fake_detect_windfarm
    return calls


def _coverage(last_day: date, source: str = "NVE") -> GenerationCoverage:
    return GenerationCoverage(
        last_hour=datetime.combine(last_day, datetime.min.time(), tzinfo=timezone.utc).replace(
            hour=23
        ),
        last_day=last_day,
        source=source,
    )


@pytest.mark.asyncio
async def test_detect_all_clips_window_end_to_last_metered_day(monkeypatch):
    """NVE farm, generation ends 2025-12-31 while the run is 2026-08-27: the
    end becomes the last instant of 31 Dec, the start does not move, and the
    point-in-time schemas still get the run date as ``as_of``."""
    _freeze_now(monkeypatch)
    svc = _service()
    calls = _capture_windows(svc)

    async def fake_probe(db, wf_id, start, end):
        assert (start, end) == (_NOW - timedelta(days=720), _NOW)
        return _coverage(date(2025, 12, 31))

    monkeypatch.setattr(svc_module, "generation_data_through", fake_probe)

    await svc.detect_all([7213], period_months=24)

    call = calls[0]
    assert call["end"] == datetime(2025, 12, 31, 23, 59, 59, 999999)
    assert call["end"].tzinfo is None  # naive UTC, like the rest of the nightly
    assert call["start"] == _NOW - timedelta(days=720)
    assert call["as_of"] == date(2026, 8, 27)
    assert svc._last_truncated == 1
    stats = svc._last_window_stats
    assert stats["truncated"] == 1 and stats["current"] == 0 and stats["no_data"] == 0
    assert stats["requested_end"] == "2026-08-27"
    assert stats["lag_days_max"] == (date(2026, 8, 27) - date(2025, 12, 31)).days
    assert stats["lag_buckets"][">90d"] == 1
    assert svc.db.committed == 1 and svc.db.rolled_back == 0


@pytest.mark.asyncio
async def test_detect_all_keeps_full_window_without_data_or_when_current(monkeypatch):
    _freeze_now(monkeypatch)
    svc = _service()
    calls = _capture_windows(svc)
    coverage_by_farm = {1: None, 2: _coverage(date(2026, 8, 27))}  # no data / current

    async def fake_probe(db, wf_id, start, end):
        return coverage_by_farm[wf_id]

    monkeypatch.setattr(svc_module, "generation_data_through", fake_probe)

    await svc.detect_all([1, 2], period_months=24)

    assert [c["end"] for c in calls] == [_NOW, _NOW]
    assert svc._last_truncated == 0
    assert svc._last_window_stats["no_data"] == 1
    assert svc._last_window_stats["current"] == 1
    assert svc._last_window_stats["lag_days_max"] == 0


@pytest.mark.asyncio
async def test_detect_all_probe_failure_keeps_window_and_the_windfarm(monkeypatch):
    """A failing coverage probe must not fail the windfarm: it is rolled back
    (it runs BEFORE the supersede UPDATE, so nothing is lost), logged, and
    detection proceeds over the un-clipped window."""
    _freeze_now(monkeypatch)
    svc = _service()
    calls = _capture_windows(svc)

    async def fake_probe(db, wf_id, start, end):
        raise RuntimeError("statement timeout")

    monkeypatch.setattr(svc_module, "generation_data_through", fake_probe)

    await svc.detect_all([1], period_months=24)

    assert calls[0]["end"] == _NOW
    assert svc._last_succeeded == 1 and svc._last_failed == 0
    assert svc.db.rolled_back == 1  # the probe's own rollback
    assert svc.db.committed == 1
    assert svc._last_window_stats["probe_failed"] == 1


@pytest.mark.asyncio
async def test_run_detection_job_records_truncation_in_job_metadata():
    svc = _service()

    async def fake_detect_all(windfarm_ids, *a, **k):
        svc._last_succeeded = len(windfarm_ids)
        svc._last_failed = 0
        svc._last_truncated = 2
        svc._last_window_stats = {"truncated": 2, "current": 1, "lag_days_max": 239}
        return []

    svc.detect_all = fake_detect_all

    result = await svc.run_detection_job(windfarm_ids=[1, 2, 3])

    job = svc.db.added[0]
    assert job.status == ImportJobStatus.SUCCESS
    assert job.job_metadata["truncated"] == 2
    assert job.job_metadata["lag_days_max"] == 239
    assert result["windfarms_truncated"] == 2


def _sqlite_job_engine():
    """A real async engine with just ``import_job_executions`` — the rollback
    expiry these tests exercise only happens on a real ``AsyncSession``."""
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import StaticPool

    return create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )


@pytest.mark.asyncio
async def test_run_detection_job_finalizes_after_a_windfarm_rollback():
    """Regression: a failed windfarm rolls the shared session back, which expires
    the ORM ``job`` row; finalising it then lazy-loaded ``started_at`` from async
    code → ``MissingGreenlet`` → the row stayed RUNNING forever (77 rows on
    staging, 47 on prod). Real session, so the expiry is real."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    engine = _sqlite_job_engine()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(ImportJobExecution.__table__.create)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with factory() as db:
            svc = OpportunityDetectionService(db)

            async def fake_detect_all(windfarm_ids, *a, **k):
                # A statement first: SQLAlchemy 2.0 autobegins on it, and only a
                # rollback of a BEGUN transaction expires the session's instances
                # (a rollback with nothing begun is a no-op). The real run always
                # has the coverage probe / supersede UPDATE before the rollback.
                await svc.db.execute(text("SELECT 1"))
                await svc.db.rollback()  # exactly what a failed windfarm does
                svc._last_succeeded, svc._last_failed = 1, 1
                return []

            svc.detect_all = fake_detect_all
            result = await svc.run_detection_job(windfarm_ids=[1, 2])

        async with factory() as db:
            job = await db.get(ImportJobExecution, result["job_id"])
            assert job.status == ImportJobStatus.SUCCESS
            assert job.job_metadata["failed"] == 1
            assert job.completed_at is not None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_run_detection_job_marks_failed_after_rollback_then_raise():
    """Same expiry on the ``except`` path: a run that rolls back and then raises
    must still land the row on FAILED with the error, not strand it RUNNING."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    engine = _sqlite_job_engine()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(ImportJobExecution.__table__.create)
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with factory() as db:
            svc = OpportunityDetectionService(db)

            async def fake_detect_all(windfarm_ids, *a, **k):
                await svc.db.execute(text("SELECT 1"))  # autobegin, see above
                await svc.db.rollback()
                raise RuntimeError("zone scan timed out")

            svc.detect_all = fake_detect_all
            with pytest.raises(RuntimeError):
                await svc.run_detection_job(windfarm_ids=[1])
            job_id = svc.db.added[0].id if hasattr(svc.db, "added") else None

        async with factory() as db:
            rows = (await db.execute(select(ImportJobExecution))).scalars().all()
            assert len(rows) == 1 and (job_id is None or rows[0].id == job_id)
            assert rows[0].status == ImportJobStatus.FAILED
            assert "zone scan timed out" in rows[0].error_message
    finally:
        await engine.dispose()
