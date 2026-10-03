# Lessons from retired session logs

Session logs are not kept in the repo (see the conventions in
[`docs/README.md`](../README.md)). When one is deleted, the durable lesson is
recorded here with the date it was learned. The user-facing change goes to the
workspace `UPDATES.md`.

Each entry names the log it came from so `git log --all -- <path>` can recover
the full text if ever needed.

## 2026-02-12 — ENTSOE data gap (FR consumption rows, GB `entsoe-py` crash)

From `docs/sessions/2026-02-12-entsoe-data-gap-fix.md` (deleted 2026-10-04).

- When the ENTSOE client learns to parse a new column (here `data_direction`:
  generation vs consumption), **every consumer** must change in the same PR —
  `app/services/entsoe_client.py`, `raw_data_storage_service.py` and
  `scripts/seeds/raw_generation_data/entsoe/import_from_api.py`. The import
  script was missed and FR rows collided on the unique key.
- `INSERT ... ON CONFLICT DO UPDATE` rejects two rows with the same key **inside
  one batch** ("cannot affect row a second time"). Split or de-duplicate before
  inserting; the fix stored consumption with `source_type='api_consumption'`.
- `entsoe-py` does not survive an empty `Acknowledgement_MarketDocument`
  response (`'RangeIndex' object has no attribute 'set_levels'`). Wrap
  per-plant queries and treat that error as "no data", so one control area
  cannot block the others.
- An import that stores 0 records must **fail loudly**. The job was marked
  `success` with `records_imported=0` for three weeks; the gap was only noticed
  when a downstream aggregation crashed.

## 2026-04-16 — Performance pipeline backfill (1,468 windfarms)

From `docs/pipeline_backfill_investigation_2026-04-16.md` and the run output in
`docs/backfill_2026_04_16/` (deleted 2026-10-04).

- 80% of failures were **NaN wind data**, i.e. an upstream ERA5 import problem,
  not a pipeline bug. Check input coverage before debugging module code.
- "Phantom OK": the log said `OK` but nothing was committed. One failed INSERT
  left the SQLAlchemy session in a "rolled back due to a previous exception
  during flush" state and the next query's **autoflush** aborted the whole
  transaction. Fix was in `_load_hourly_data` / `performance_anomaly_service.py`
  (flush explicitly, or `with db.no_autoflush`), then re-run the 131 farms.
- A long backfill needs a per-windfarm **retry list** derived from the DB
  state, not from the log file — logs lie when commits are rolled back.

## 2026-05-25/26 — Pipeline correctness backfill (246 hourly-source farms)

From `docs/pipeline/SESSION_2026_05_25_PROGRESS.md` and
`SESSION_2026_05_26_SUMMARY.md` (deleted 2026-10-04). The engineering
outcome is frozen in [`HANDOFF-2026-05-25.md`](HANDOFF-2026-05-25.md).

- Production was **missing five Alembic migrations**; Module 5/6 persistence had
  been failing silently for weeks. Always compare `alembic current` on prod
  with `alembic heads` before trusting nightly results.
- **RDS pool cascade**: RDS drops an idle connection mid-transaction, asyncpg
  raises `ConnectionDoesNotExistError`, SQLAlchemy cannot roll back the invalid
  SAVEPOINT and every later operation on that session fails. `pool_pre_ping`
  only protects the checkout boundary. Mitigations that worked: one fresh
  session per windfarm, lower parallelism (4 → 2 cut the error rate from ~30%
  to ~6%), and restart-to-reset. `scripts/jobs/run_detection_shard.py` encodes
  the session-per-windfarm pattern.
- **Shared transaction hazard** (Ormonde 7404): Modules 1–6 and the peer
  aggregate refresh ran in one transaction, so a refresh failure after ~7 min
  of CPU work rolled back correct module results. Commit module output first;
  treat follow-on refreshes as best-effort.
- The spec script's `pd.read_csv(dayfirst=True)` silently corrupted ISO
  timestamps (month/day swapped for day ≤ 12, NaT otherwise) — a "sign-flip"
  headline was an artefact. Pin reference-pipeline patches
  (`tests/reference/spec_patches.py`) and re-derive any headline from patched
  output before publishing it.
- Module 1b run-grouping per hour fragments real multi-month constraints
  (EAO 2024: 320 sub-runs). Month-level grouping with a 25% flagged-share
  threshold was the fix.
- Keep laptops awake (`caffeinate`) and `tee` the log for multi-hour runs; two
  of eight runs were lost to sleep/restart.
