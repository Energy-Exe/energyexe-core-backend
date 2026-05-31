"""Reliability tests for OpportunityDetectionService run/commit/failure handling.

DB-free: a ``_FakeSession`` records commit/rollback/add and stubs ``execute`` /
``get``. ``_detect_windfarm`` / ``detect_all`` are monkeypatched so each test
drives a specific success/failure shape without a real Postgres.

Covers:
  * B2 — per-windfarm atomicity: each windfarm commits; a failing one rolls back
    and is counted, the rest still commit (no all-or-nothing loss).
  * B3 — job_id reuse: an existing job row is reused (not duplicated).
  * B4 — no SUCCESS-on-total-failure: all windfarms erroring → job FAILED.
"""

import pytest

from app.models.import_job_execution import ImportJobExecution, ImportJobStatus
from app.services.opportunity_detection_service import OpportunityDetectionService


class _EmptyResult:
    def fetchall(self):
        return []

    def scalar_one_or_none(self):
        return None


class _FakeSession:
    """Minimal AsyncSession stand-in tracking transaction calls."""

    def __init__(self, job=None):
        self.committed = 0
        self.rolled_back = 0
        self.added: list = []
        self._job = job

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
