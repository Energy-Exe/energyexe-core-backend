# Re-run-on-confirm trigger — design

**Status:** designed 2026-05-29, NOT yet implemented. **Gated on the 7404
transaction-rollback fix** (see "Dependencies"). Decision: auto-debounced
queue, ship after 7404.

## Why

Confirmed-only masking (issue #79) means an analyst confirming a structural-
constraint flag in the dashboard changes *nothing* until the pipeline re-runs
for that windfarm. `set_review_status` only flips a DB column; the masking is
applied by `load_active_periods` (confirmed-only) on the *next*
`run_pipeline`. Today that re-run is a manual/scheduled batch, so confirmation
and recomputation are decoupled. This trigger closes the gap automatically.

## Why not run inline in the PATCH handler

1. **Latency** — a single long-history windfarm (EAO 7371 / Hornsea 1 7384,
   exactly the ones that get confirmed) takes minutes; the Confirm click would
   hang or time out.
2. **Burst amplification** — triaging one windfarm = several confirms in a
   row. Inline = N runs for one windfarm; we want one.
3. **7404 hazard** — these long runs are the ones that roll back when the
   peer-aggregate refresh exceeds ~6 min. Running them unserialized inside a
   web worker is worse.

→ Decoupled, debounced, per-windfarm queue.

## Components

### 1. Transition gate (`set_review_status`)
Only enqueue when the **confirmed set actually changes**:

```python
affects_masking = (old_status == "confirmed") != (new_status == "confirmed")
```

- `pending→confirmed`, `confirmed→pending`, `confirmed→dismissed`,
  `dismissed→confirmed` → enqueue.
- `pending→dismissed`, `dismissed→pending` → confirmed set unchanged, output
  identical → **no re-run**. (Dismissing false positives — the bulk of the 609
  pending flags — never triggers a run.)

### 2. Queue table `pipeline_rerun_requests`
One live row per windfarm; confirmations coalesce by UPSERT.

| col | purpose |
|---|---|
| `windfarm_id` UNIQUE | coalesce key |
| `status` | `queued` / `running` / `done` / `failed` |
| `requested_at` | bumped every confirm → drives debounce |
| `requested_by` | analyst user id (audit) |
| `attempts` | retry cap |
| `pipeline_run_id` FK→import_job_executions | the run that applied it |
| `started_at` / `finished_at` / `error` | visibility |

Enqueue = `INSERT … ON CONFLICT (windfarm_id) DO UPDATE SET requested_at=now(),
status='queued', requested_by=…`. 5 confirms on EAO → 1 row.

### 3. Drain job (reuse existing `AsyncIOScheduler`, single-instance)
`pipeline-rerun-drain`, every ~60–90s. Same wiring pattern as
`app/cron/pipeline_daily.py`. Gate behind an env flag
(`PIPELINE_RERUN_ENABLED`) like the daily job.

```sql
SELECT windfarm_id FROM pipeline_rerun_requests
WHERE status='queued'
  AND requested_at < now() - interval '<DEBOUNCE>s'   -- e.g. 120s analyst-idle
ORDER BY requested_at
FOR UPDATE SKIP LOCKED                                 -- no double-grab
LIMIT <BATCH>;
```

Per windfarm, **serially**: mark `running` → fresh session via
`get_session_factory()` → `PerformancePipelineService(db).run_pipeline_batch(
windfarm_ids=[wf])` (gets the tracked `import_job_executions` row + commit for
free) → mark `done` + `pipeline_run_id`, or `failed`/`attempts++` (cap, then
stop retrying and surface). Serial = no concurrent runs, no DB overload,
7404-fragile long runs stay one-at-a-time.

DEBOUNCE turns a triage burst into one run.

### 4. Endpoints
- PATCH handler: auto-enqueue when `affects_masking`.
- `GET /performance-pipeline/rerun-status?windfarm_id=` — dashboard polls:
  *queued → running → updated 2m ago*.

### 5. Admin-UI
- After Confirm: toast "Re-run queued — numbers refresh in a few minutes".
- Per-windfarm status badge driven by the status endpoint.

## Dependencies
- **7404 transaction-rollback fix MUST land first.** Each re-run is a single-WF
  `run_pipeline`; the WFs most likely to be confirmed (EAO/Hornsea) are the
  long-history ones that roll back on the peer-aggregate refresh. Without the
  fix, a confirmed re-run can mask hours, compute, then roll back — analyst
  sees "done" with stale numbers.

## Edge cases
- Coalescing: bursts within DEBOUNCE → one run.
- Concurrent drain ticks: `FOR UPDATE SKIP LOCKED` + `status='running'`.
- Multi-worker: APScheduler must run in ONE process (same constraint the daily
  job already has).
- Retry/backoff: keep `failed` with `attempts`, cap, then surface — don't lose
  the dirty signal silently.
