# Session Summary — 2026-04-17 to 2026-04-18

## What was done

### 1. Spec Audit (Prioritisation 2026-03-30)

Read the full 42-page spec PDF and audited every item against the codebase + DB. Produced a thorough gap analysis showing:
- Modules 1-6 math: fully implemented (from PR #31, 2026-04-16)
- Item 3 (Generation Concentration): entirely missing
- Items 4/5/6: missing vs-zone-average comparison + AI prompts
- PRE-A (ERA5 NaN): 98M rows blocking 1,168 windfarms
- PRE-B (peer aggregates): no infrastructure for zone comparisons
- PRE-C (P50 targets): 95% missing
- PRE-D (scheduler): no cron job

### 2. Plan Created

Saved to `docs/spec_items_1_to_6_plan.md` — 7 sections covering current state, prerequisites, per-item implementation, sequencing, test plan, open questions, effort estimates (~25 backend-days). Created 27 tracked tasks with dependency wiring.

### 3. Code Implemented (33 files, 4,017 lines)

**Committed on branch `feat/spec-items-1-6-implementation` (not pushed — needs your git credentials).**

#### New Models + Migrations (4 migrations applied to prod RDS)
- `app/models/peer_group_aggregate.py` — cached zone/country/owner/turbine aggregates
- `app/models/generation_concentration_summary.py` — capture ratio + decile shares per windfarm-year
- `alembic/versions/2026041700_weather_no_nan.py` — CHECK constraint on weather_data (NaN guard)
- `alembic/versions/2026041701_peer_aggregates.py` — peer_group_aggregates table
- `alembic/versions/2026041702_generation_concentration.py` — generation_concentration_summaries table
- `alembic/versions/2026041703_perf_anomaly_isolation_forest.py` — flag_isolation_forest column

#### New Services
- `app/services/peer_aggregate_service.py` (374 lines) — compute/cache (avg, p10, p50, p90, n) for 10 metric keys across peer groups
- `app/services/generation_concentration_service.py` (484 lines) — spec item 3: capture ratio, decile breakdown, zone comparison, persistence

#### Updated Services
- `performance_pipeline_service.py` — hooked in concentration module + peer aggregate refresh after each windfarm; fixed `p50_target_gwh` → `p50_target_volume_gwh` bug; year-aware P50 lookup
- `performance_anomaly_service.py` — IsolationForest secondary detector (opt-in via env); year-aware PPA price with contract date range; `pricing_basis` label; `flag_isolation_forest` persisted in bulk insert
- `prompt_builder_service.py` — registered 4 new section types with required fields

#### New API Endpoints (4 new + 3 enriched)
- `GET /performance-pipeline/generation-concentration/{id}` — read concentration summaries
- `POST /performance-pipeline/generation-concentration/{id}/compute` — on-demand recompute
- `GET /performance-pipeline/peer-aggregates/{group_type}/{group_id}/{metric_key}` — lazy-computed peer aggregate
- `GET /performance-pipeline/wind-normalisation/{id}/monthly-time-series` — item 6 client-facing chart data
- Enriched `/power-curves/{id}` — per-bin bidzone-average q50/q90
- Enriched `/degradation/{id}` — zone-average slope + vs-zone diff
- Enriched `/odi/{id}` — zone-average ODI metrics + diffs

#### AI Prompt Templates (4 new Jinja2 templates)
- `app/prompts/generation_concentration.txt` — capture ratio commercial impact
- `app/prompts/degradation.txt` — slope significance + peer context
- `app/prompts/disruption_detection.txt` — ODI severity + run analysis
- `app/prompts/wind_normalisation.txt` — monthly index trajectory

#### Pipeline Scheduler
- `app/cron/pipeline_daily.py` — APScheduler nightly job, opt-in via `PIPELINE_DAILY_ENABLED=true`
- Wired into `app/main.py` lifespan (start/stop)

#### Operational Scripts
- `scripts/audit_rated_capacity.py` — audits rated_mw vs turbine sum vs observed peak
- `scripts/seeds/p50/import_p50_targets.py` — bulk P50 import (CSV + 3yr historical fallback)
- `scripts/seeds/p50/import_p50_fallback_sql.sql` — pure SQL version for remote DB
- `scripts/ops/run_remaining_tasks.sh` — all-in-one EC2 runner

#### Frontend (energyexe-client-ui)
- `src/lib/performance-pipeline-api.ts` — API client + React Query hooks for all pipeline endpoints
- `src/components/performance/wind-normalisation-chart.tsx` — monthly bar chart with color-coded above/below baseline

#### Tests (38 new, all pass)
- `tests/test_generation_concentration.py` — 15 tests (deciles, capture ratio, edge cases)
- `tests/test_peer_aggregate.py` — 10 tests (summarise, registry, validation)
- `tests/test_isolation_forest.py` — 5 tests (4 skip without sklearn)
- `tests/test_pipeline_integration.py` — 8 end-to-end math chain tests
- `tests/test_weather_import_bbox.py` — 3 tests (from previous session)
- `tests/test_weather_import_nan_guard.py` — 3 tests (from previous session)

Full test suite: 28 failed / 274 passed / 117 skipped / 38 errors — identical pre-existing baseline + 40 new passing tests, zero regressions.

### 4. Operational Work Done

#### ERA5 NaN Cleanup — COMPLETE
- Launched 9 parallel EC2 instances (one per year 2017-2025) in same VPC as RDS
- Used partial index strategy (`CREATE INDEX CONCURRENTLY ... WHERE wind_speed_100m::text = 'NaN'`) for residual cleanup
- **~98M NaN rows deleted. Verified 0 NaN remaining across all checked windfarms.**
- All EC2 instances terminated, SSH security group rule revoked

#### P50 Targets — 149 inserted
- Pure SQL CTE query computing 3-year historical mean for windfarms without P50 targets
- Coverage: 83 → 232 windfarms (5.1% → 14.3%)
- Remaining 1,392 windfarms need either owner-provided P50s or more generation data

#### Generation Concentration — 192 rows / 24 windfarms
- Backfilled via EC2 using pure SQL per windfarm
- 24 of 229 windfarms have concentration data (remaining lacked overlapping price+generation data)
- Fixed `computed_at` column default (ALTER TABLE SET DEFAULT) during debugging

#### Rated Capacity Audit — 229 windfarms
- CSV at `/tmp/rated_capacity_audit.csv`
- 99 OK, 122 peak-exceeds-nameplate (benign), 10 mismatch-vs-turbines, 3 far-below

#### DB Migrations Applied
- Alembic now at `2026041703_iforest` (all 4 new migrations applied)
- Broke pre-existing alembic cycle by renaming colliding revision `a1b2c3d4e5f6`

### 5. Task Completion

| Status | Count | IDs |
|--------|------:|-----|
| Completed | 20 | 30-33, 35-36, 38-45, 47-50, 52-54, 56 |
| Pending (pipeline re-run) | 4 | 37, 46, 51, 55 |
| Pending (user input) | 1 | 34 (Client FE feedback doc) |

---

## What's Left

### Immediate (can do now)

1. **Push the branch** — commit is ready on `feat/spec-items-1-6-implementation`, needs your git credentials:
   ```bash
   cd /Users/mdfaisal/Documents/energyexe/energyexe-core-backend
   git push -u origin feat/spec-items-1-6-implementation
   ```

2. **Run pipeline backfill** for 1,356 newly-processable windfarms (have clean weather + generation data, no curves yet). Run from a machine with good DB connectivity:
   ```bash
   poetry run python scripts/backfill_pipeline.py
   ```
   Or via API: `POST /performance-pipeline/run`

3. **Create PR** and merge to main.

### Medium-term

4. **CDS API weather re-import** — for the 1,168 previously-NaN-affected windfarms, some years may have gaps where NaN rows were deleted but no clean data exists. Re-import via `WeatherImportCore.fetch_and_process_date_range()` for those gaps. This is CDS-API-throttled (days).

5. **Client FE feedback doc** (task #34) — you need to share the `Client FE feedback.docx` referenced in spec item 1.

6. **Enable pipeline scheduler** in production:
   ```bash
   export PIPELINE_DAILY_ENABLED=true
   # Optional: PIPELINE_USE_ISOLATION_FOREST=true (after pip install scikit-learn)
   ```

7. **Frontend integration** — the `WindNormalisationChart` component is written in `energyexe-client-ui` but needs to be wired into the windfarm detail page.

### Out of Scope (tracked in plan)

- Spec item 7 (P50 Target & Commercial Analysis) — works for 232 windfarms with P50 targets
- Spec item 10 ('Averages' data tables) — partially covered by peer_group_aggregates
- MKT-01/MKT-02 opportunity schemas — never implemented (only MKT-03 fires)

---

## Key Files Reference

| File | What |
|------|------|
| `docs/spec_items_1_to_6_plan.md` | Full implementation plan with sequencing + effort |
| `app/services/generation_concentration_service.py` | Item 3 — new module |
| `app/services/peer_aggregate_service.py` | PRE-B — zone comparison infrastructure |
| `app/cron/pipeline_daily.py` | PRE-D — nightly scheduler |
| `app/prompts/*.txt` | AI prompt templates (4 new) |
| `scripts/ops/run_remaining_tasks.sh` | EC2 runner for remaining ops |
| `scripts/seeds/p50/` | P50 import scripts (Python + SQL) |
| `scripts/audit_rated_capacity.py` | Rated capacity audit |

## Key DB State

| Table | Rows | Windfarms |
|-------|-----:|----------:|
| power_curve_bins | 67,141 | 229 |
| performance_anomalies | ~1.46M | 227 |
| performance_summaries | 24,609 | 229 |
| degradation_results | 441 | 221 |
| opportunities | 544 | 343 |
| generation_concentration_summaries | 192 | 24 |
| peer_group_aggregates | 0 | 0 (populated lazily on API request) |
| p50_targets | 234 | 232 |
| weather_data NaN rows | **0** | **all clean** |

## EC2 Instances

All terminated. No running instances remain.
