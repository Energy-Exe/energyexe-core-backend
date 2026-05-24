# Pipeline correctness work — handoff (2026-05-25)

Read this first when picking up the pipeline correctness work after a fresh start. It points to everything else.

## What this effort is

Wind-farm performance pipeline (Modules 1-6 in `app/services/`) publishes the headline KPIs — degradation %/year, ODI loss %, wind-normalised indices, commercial revenue — that drive the platform's published numbers. May 2026, a consultant delivered a reference Python implementation (`tests/reference/energyexe_pipeline_full.py`). Line-by-line comparison found 3 statistical bugs in Module 5, a missing Module 1b detector, and smaller issues in Modules 3/4/6. This effort fixed all of them.

## Status — 2026-05-25

**All 10 PRs merged to master.** Master alembic head: `f3a4b5c6d7e8`. Full pipeline test suite: **92/92 passing**.

| PR | What | Merged |
|---|---|---|
| #62 | Documentation + reference comparison + verification harness + per-windfarm baselines | ✅ |
| #63 | Per-module structured logging (`module_2_complete` ... `module_6_complete` events) | ✅ |
| #64 | Module 5 — hourly OLS + seasonal decomposition (Bug A + B) | ✅ |
| #70 | Module 5 — per-windfarm `baseline_cap_pu` + CI95 in % | ✅ |
| #66 | Module 4 — `yearly = mean(monthly means)` + separate yearly historical_mean (C1) + W1 | ✅ |
| #67 | Module 6 — `contract_revenue_eur` + `contract_revenue_vs_p50_target_eur` + PPA uplift (D1+D2) | ✅ |
| #68 | Module 1b — structural constraint detector with B1.5 Q50-ratio extension | ✅ |
| #69 | Module 3 — `anomaly_nan_price_heavy` warning (W2) | ✅ |
| #71 | FX1 — orchestrator routes `df_no_over` through Modules 3/4/5 (was sending raw `df_all`) | ✅ |
| #72 | FX2 — Modules 3/4/5 mask out active structural-constraint flags | ✅ |

## What's left

**Everything code-wise is done.** Only operational work remains:

### 1. Production backfill (needs human authorization)

The pipeline now produces different (correct) numbers for every windfarm. To make those land in production we have to run the corrected pipeline across all ~360 windfarms.

- **Authorization required:** explicit "yes, write to production RDS" from the user. The memory has a strict "be strictly read-only during validation work" rule for the prod RDS instance.
- **How to run:** `POST /api/v1/performance-pipeline/run` (admin JWT required — only the user can produce that). Or via a script that bypasses the API and uses superuser credentials from `.env`.
- **Pre-flight:** snapshot diffs against `tests/fixtures/baselines/*.json` (captured pre-change in PR #62) so we can show before/after per windfarm.
- **Estimated time:** ~5-15 min per windfarm × 360 = several hours. Run off-peak; monitor RDS metrics.

### 2. Release note (drafted from backfill output)

Stakeholders need a one-pager explaining what numbers changed and why. Should include:

- 3-4 example windfarms grouped by class (offshore high-CF / onshore mid-CF / etc.) with explicit "was X%, now Y%" numbers.
- The Hornsea 1 sign-flip explicitly — that's the loudest single change.
- Brief explanation of WHY (per-windfarm baseline, hourly OLS, constraint exclusion).
- Note that R² will look lower across the board — this is correct (hourly data is noisier than monthly), not a regression. Use CI95 for significance.
- Distribute to `KG / OS / ASR / stakeholders` (user knows the exact list and channel).

### 3. Analyst review UI (admin-ui repo, separate effort)

- Queue page listing `structural_constraint_flags WHERE review_status='pending_review'`.
- Confirm/dismiss actions hitting (TODO) `POST /api/v1/structural-constraints/{id}/review`.
- Email notification when new flags appear.

**This is the only part of the spec's design we deliberately didn't ship in backend** — flags currently apply downstream automatically when `pending_review` (so EAO/Hornsea fixes don't wait on analyst review), with `dismissed` opting back out. UI work is needed before analysts can actually dismiss false positives via a click.

## Where to read for context

| File | What it tells you |
|---|---|
| `docs/pipeline/HANDOFF.md` | (this file) overview + status |
| `docs/pipeline/README.md` | Pipeline architecture diagram + module index |
| `docs/pipeline/spec-vs-implementation.md` | Original bug list + improvement plan; status section at top |
| `docs/pipeline/module-{1,1b,2,3,4,5,6}-*.md` | Per-module reference docs. Modules 5 + 1b were rewritten in this effort. |
| `tests/reference/p-1-validation-notes.md` | Real-data validation findings on Lutelandet, EAO, Hornsea 1. Captures the baselines and bug magnitudes. |
| `tests/reference/energyexe_pipeline_full.py` | The consultant's reference pipeline. Pinned by SHA-256 in `tests/reference/VERSION.md`. |
| `tests/fixtures/reference_windfarms.yaml` | 5 windfarms chosen for ongoing spec equivalence checks |
| `tests/fixtures/baselines/*_pre.json` | Pre-change snapshots. Diff against `*_post.json` after backfill. |
| `~/.claude/plans/fluttering-gliding-treehouse.md` | The locked plan from session 1. Mostly historical now. |

## Key decisions that were locked

| Decision | Choice |
|---|---|
| Module 1b detection scope | B1 (Q90-ratio per spec) + **B1.5 extension** (parallel Q50-ratio) — needed for multi-BMU UK offshore farms where Q50 collapses but Q90 looks normal |
| Verification rigour | All 3 layers — synthetic golden + spec side-by-side + permanent CI |
| Module 5 rollout | Backfill + release note (no dual-publish toggle, no opt-in flag) |
| Constraint flag gate for Modules 2/3/5 | Both `pending_review` AND `confirmed` count as active. `dismissed` opts hours back in. (Option 1 from the FX2 decision in session 2.) |

## How to restart cleanly

Drop this prompt into a new conversation:

> Continuing the EnergyExe wind performance pipeline correctness work. All 10 code PRs merged to master (`f3a4b5c6d7e8` is the alembic head). Read `docs/pipeline/HANDOFF.md` first — it has the status summary, what's left, and pointers to everything else. Next operational step is the production backfill + release note, both of which need explicit user authorization for the write-to-prod step.

## Real-data expectations (what stakeholders will see)

When backfill runs, **every windfarm's published degradation number will change**. Expected patterns from the P-1 validation:

| Class | Direction | Magnitude |
|---|---|---|
| High-CF offshore (Hornsea 1, EAO, London Array) | Larger shifts; some may flip sign | ±0.5-2 %/year |
| Mid-CF onshore (Nordic farms) | Small shifts | ±0.1-0.3 %/year |
| Low-CF onshore | Opposite direction from offshore | ±0.2-0.5 %/year |

**The loudest single change**: Hornsea 1 currently publishes `+0.68 %/yr` ("improving"). True value per the spec: `-0.60 %/yr` (significantly degrading, CI excludes zero). After backfill + automatic constraint-flag exclusion of the 2024 cable hours, our number should land at or near the true value.

R² values will look noticeably lower across the board. This is **correct, not a regression** — hourly data is noisier than monthly aggregates. Stakeholders should use CI95 for significance assessment, not R².

## Test count summary

The effort added these test files:

| File | Tests | Covers |
|---|---|---|
| `tests/test_degradation.py` | 23 | Module 5 hourly OLS + seasonal + per-WF baseline + CI% |
| `tests/test_wind_normalisation.py` | 4 | Module 4 yearly-aggregation fix |
| `tests/test_commercial_module6.py` | 7 | Contract revenue + PPA uplift formulas |
| `tests/test_structural_constraint_detection.py` | 11 | Module 1b detector (Q90 + B1.5 Q50) |
| `tests/test_fx2_constraint_consumption.py` | 7 | Active-flag mask building |
| `tests/test_w2_nan_price_warning.py` | 6 | NaN-price warning helper |
| `tests/test_snapshot_diff.py` | 5 | Snapshot-diff utility |
| **Total new** | **63** | |
| Pre-existing pipeline tests | 29 | Power curve + anomaly + concentration + integration |
| **Total pipeline-relevant** | **92** | All passing on master |

## Two structural choices to revisit later (NOT today)

These work fine today but are technical debt:

1. **`_load_hourly_data` is a private cross-service import.** `DegradationService`, `WindNormalisationService`, and `PerformanceAnomalyService` all reach into `PowerCurveService._load_hourly_data`. Should be promoted to a public helper or extracted to a `HourlyDataLoader`.
2. **`pipeline_run_audit` row counts not persisted.** Spec's `data_quality_report.csv` / `cleaning_exclusion_summary.csv` are useful for debugging pipeline drift. Cheap to add; deferred.

---

## File index for the pipeline correctness work

**Services touched:**
- `app/services/degradation_service.py` (rewritten — hourly OLS + seasonal + per-WF baseline + CI% + constraint-hour count)
- `app/services/wind_normalisation_service.py` (yearly aggregation fix + dead-code drop)
- `app/services/structural_constraint_detection_service.py` (NEW — Module 1b)
- `app/services/performance_pipeline_service.py` (orchestrator + Module 6 contract revenue + structural constraint hook)
- `app/services/performance_anomaly_service.py` (W2 NaN-price warning)
- `app/services/power_curve_service.py` (`return_df_no_over` option)

**Models touched:**
- `app/models/degradation_result.py` (+ `ci_lower_95_pct`, `ci_upper_95_pct`, `n_constraint_hours_excluded`)
- `app/models/performance_summary.py` (+ `contract_revenue_eur`, `contract_revenue_vs_p50_target_eur`)
- `app/models/structural_constraint_flag.py` (NEW)

**API touched:**
- `app/api/v1/endpoints/structural_constraints.py` (NEW — GET list endpoint)
- `app/api/v1/router.py` (wire the new endpoint)
- `app/schemas/performance_pipeline.py` (new fields on Degradation/Summary/PPA responses)

**Migrations added:**
- `c8d9e0f1a2b3` — `degradation_results.ci_lower_95_pct, ci_upper_95_pct`
- `d1f2a3b4c5e6` — `performance_summaries.contract_revenue_eur, contract_revenue_vs_p50_target_eur`
- `e2f3a4b5c6d7` — `structural_constraint_flags` table
- `f3a4b5c6d7e8` — `degradation_results.n_constraint_hours_excluded` (current head)

**Dependencies added:**
- `statsmodels = "^0.14"` (for `seasonal_decompose`)
