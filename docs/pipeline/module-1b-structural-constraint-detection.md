# Module 1b — Structural constraint detection

Auto-detect sustained periods (≥ 2 weeks, default 336 h) where windfarm output is systematically truncated relative to wind conditions — the signature of partial export-infrastructure failure (single cable on a multi-cable farm, half-BMU offline, sustained curtailment campaign). These periods quietly contaminate Modules 2, 3 and 5 if not excluded.

**Implementation: PR #68 (detector) + PR #72 (downstream wiring). Live as of 2026-05-25.**

## Concepts

### What a "structural constraint" looks like

On a two-cable offshore farm, if one cable fails, generation continues but is capped at ~50 % of nameplate. The turbines never go dark — they keep producing — but a hard ceiling appears in the upper wind bins:

```
Pre-failure, mid-to-high wind bins:        Post-failure (cable down):
  10–25 m/s → p_pu ≈ 0.90–1.00              10–25 m/s → p_pu ≈ 0.50 (flat)
```

### Why hour-by-hour anomaly detection (Module 3) misses this

Module 3 uses MAD-based thresholds **per wind bin per year**. During a 7-month cable outage:
- The yearly capability curve for that bin gets *built from constrained hours*, so the curve itself shifts down.
- Each individual constrained hour looks "near the median" against that shifted-down curve.
- Module 3 reports **zero anomalies** even though half the energy is missing.

The constraint is a **distributional shift across many hours and bins simultaneously** — invisible to point-anomaly detectors.

### Why leave-one-year-out reference

You can't fix this by comparing 2024 against a baseline that *includes* 2024 — the baseline absorbs the contamination. The detector builds a reference percentile-per-wind-bin from **all years except Y**, then compares year Y's observed percentile against that clean reference. A depressed year stands out.

### Wind-banded ratio thresholds

The detector flags a (bin, month) as constrained when either `observed_Q90 / reference_Q90` or `observed_Q50 / reference_Q50` falls below a band-specific threshold:

| Wind band | Q90 threshold | Q50 threshold (B1.5) | Why |
|---|---|---|---|
| < 7 m/s | no check | no check | output is naturally noisy near cut-in; ratio is unreliable |
| 7 – 10 m/s | 0.70 | 0.70 | ramp region — natural variance is higher |
| 10 – 25 m/s | 0.80 | 0.85 | rated region — turbines should be near full output; truncation is unambiguous |

A run of flagged bin-months gets grouped into a candidate "run". Runs shorter than 336 hours (2 weeks) are dropped — short events belong to Module 3, not 1b.

### B1 vs B1.5 — why two paths

The reference spec only checks the Q90 ratio. P-1 validation against three real windfarms (Lutelandet / EAO / Hornsea 1) found that **two-of-two multi-BMU UK offshore farms in 2024** show a different signature: the *median* hour is constrained (~50 % output, signature of one-of-N BMUs offline most of the time) while the *upper-decile* hour stays near normal (the BMUs that ARE running can still hit full capacity). A Q90-only detector misses this entirely.

**B1.5** runs the same LOYO machinery on the Q50 (median) ratio in parallel. A bin-month is flagged constrained if EITHER ratio drops below its band threshold. The `flag_trigger` column on each detected run records which path fired: `'q90_ratio'`, `'q50_ratio'`, or `'both'`. This is a deliberate extension beyond the reference spec.

### Mean Q90/Q50 ratio is a fault-type fingerprint

The persisted `mean_q90_ratio` and `mean_q50_ratio` columns are the 90th/50th percentile of `p_pu` within the run (not the mean of ratios — naming is historical, matches spec verbatim).

- `mean_q50 ≈ 0.5, mean_q90 ≈ 0.5` → full half-output ceiling (e.g. single cable down)
- `mean_q50 ≈ 0.5, mean_q90 ≈ 0.9` → half-BMU pattern (typical hour capped, peak hour OK)
- `mean_q50 ≈ 0.0, mean_q90 ≈ 0.0` → full outage
- `mean_q50` and `mean_q90` both in 0.5-0.8 → partial curtailment or noise restriction (analyst review needed)

### Downstream contamination this prevents

Without the constraint mask, a 7-month cable constraint in a 4-year dataset poisons every downstream module:

| Module | Effect of contamination |
|---|---|
| 2 — power curve | Yearly Q50/Q90 in mid-to-high wind bins are depressed → capability curve understates true capability. **Module 2 still uses the unmasked sample so its curves are unaffected by the constraint** — this is intentional and matches spec. |
| 3 — losses | Constraint hours are dropped from the loss calculation before classification. Lost MWh attributable to known infrastructure is reported separately under Module 6's `constraint_proxy_mwh`. |
| 4 — wind norm | Index for the constrained period reads accurately once those hours are dropped from the sample. |
| 5 — degradation | OLS slope is no longer dragged by the step-change at the constraint boundary. `n_constraint_hours_excluded` reports how many hours were dropped. |
| 6 — commercial | Picks up cleaner capability stats automatically; `lost_value_eur` separates constraint-attributable lost value. |

### The review workflow

Detection is borderline (real constraint vs prolonged maintenance vs noise curtailment), so flags are staged for analyst review:

1. **Auto-detector** runs during the daily pipeline, writes candidate runs with `review_status='pending_review'`.
2. **Modules 3/4/5 mask out all active flags (`pending_review` OR `confirmed`) automatically.** This is the locked design (Option 1 from the FX2 decision) — fixes EAO/Hornsea 1 immediately, without waiting on analyst review.
3. **Analyst reviews** each candidate. Marking a flag `dismissed` opts those hours back into the calculation on the next pipeline run.
4. **Read API**: `GET /api/v1/structural-constraints?windfarm_id=&review_status=` lists flags. Confirm/dismiss actions are a future admin-ui ticket (out of backend scope).

## Implementation walkthrough

**File:** `app/services/structural_constraint_detection_service.py`

### Pure helpers (testable, no DB)

| Function | What it does |
|---|---|
| `compute_loyo_reference(df_curve, percentile)` | Leave-one-year-out percentile per wind bin. Returns `(wind_bin, _year, ref_value)`. Empty if `<2` years. |
| `compute_observed_percentile(df_curve, percentile)` | Observed percentile per `(wind_bin, _month, _year)`. |
| `flag_bin_months(observed, reference, bands)` | Joins observed vs reference, computes ratio, applies band thresholds, returns flagged `(wind_bin, _month)` pairs. |
| `group_into_runs(df_curve, flagged_q90, flagged_q50, min_hours)` | Maps each hour to "constrained or not" via `(wind_bin, _month)` lookup, groups consecutive flagged hours into runs, filters by `min_hours`, labels `flag_trigger`. |
| `detect_constraints_df(df_curve, ...)` | Full pipeline — orchestrates the above into a runs DataFrame. |
| `build_constraint_mask(df, periods, time_col='hour')` | Builds a boolean Series aligned to `df.index`, True where `df[time_col]` falls inside any period (closed-closed). Handles tz-naive and tz-aware inputs. |

### Service class

`StructuralConstraintDetectionService(db)`

| Method | Purpose |
|---|---|
| `detect_constraints(windfarm_id, df_curve, *, pipeline_run_id=None, replace_existing=True)` | Runs `detect_constraints_df` and persists each run as a `pending_review` row. With `replace_existing=True` (default), drops prior auto-detected `pending_review` rows for the windfarm first — analyst-curated rows (status != pending_review) are preserved. |
| `load_active_periods(windfarm_id)` | Returns all flag periods with `review_status IN ('pending_review', 'confirmed')`. Used by the orchestrator to build the downstream mask. |

### Orchestrator integration

In `PerformancePipelineService.run_pipeline`:

```python
# Module 1b runs between Module 2 (curves built from raw df_no_over)
# and Modules 3/4/5 (which receive the constraint-masked df_no_over).
detector = StructuralConstraintDetectionService(self.db)
detect_out = await detector.detect_constraints(windfarm_id, df_for_detect, ...)

active_periods = await detector.load_active_periods(windfarm_id)
if active_periods:
    mask = build_constraint_mask(df_no_over, active_periods)
    n_constraint_hours_excluded = int(mask.sum())
    df_no_over = df_no_over[~mask].reset_index(drop=True)

# Modules 3, 4, 5 now consume the masked df_no_over.
```

The `module_1b_complete` structlog event records `runs_detected`, `total_constrained_hours`, `active_periods`, `hours_masked_from_downstream`.

## Configuration constants

In `app/services/structural_constraint_detection_service.py`:

```python
Q90_RATIO_BANDS = [
    {"wind_min": 7.0,  "wind_max": 10.0, "threshold": 0.70},
    {"wind_min": 10.0, "wind_max": 25.0, "threshold": 0.80},
]
Q50_RATIO_BANDS = [
    {"wind_min": 7.0,  "wind_max": 10.0, "threshold": 0.70},
    {"wind_min": 10.0, "wind_max": 25.0, "threshold": 0.85},
]
CONSTRAINT_MIN_HOURS = 336  # ~14 days; ignore short blips
```

## DB schema

**File:** `app/models/structural_constraint_flag.py`
**Table:** `structural_constraint_flags`
**Migration:** `e2f3a4b5c6d7_add_structural_constraint_flags.py`

| Column | Type | Notes |
|---|---|---|
| `id` | SERIAL PK | |
| `windfarm_id` | Int FK | CASCADE delete on windfarm removal |
| `period_start` | TIMESTAMPTZ | first constrained hour |
| `period_end` | TIMESTAMPTZ | last constrained hour (inclusive) |
| `duration_hours` | Int | total flagged hours in the run |
| `wind_bins_affected` | Int | distinct wind bins involved |
| `mean_q90_ratio` | Numeric(6,3) | 90th-pct p_pu within the run |
| `mean_q50_ratio` | Numeric(6,3) | 50th-pct p_pu within the run (B1.5) |
| `flag_trigger` | VARCHAR(20) | `'q90_ratio'` / `'q50_ratio'` / `'both'` |
| `flag_source` | VARCHAR(40) | default `'auto_constraint_detector'` |
| `review_status` | VARCHAR(20) | `'pending_review'` / `'confirmed'` / `'dismissed'` |
| `analyst_notes` | TEXT | free-form analyst commentary |
| `reviewed_by` | Int FK users | SET NULL on user delete |
| `reviewed_at` | TIMESTAMPTZ | |
| `pipeline_run_id` | Int FK | SET NULL on job delete |
| `created_at` | TIMESTAMPTZ | |

**Unique:** `(windfarm_id, period_start, period_end)`.
**Indexes:** `review_status`, `windfarm_id`.

## Known limitations

- Needs **≥ 2 years of data** to build the leave-one-year-out reference. Single-year windfarms produce no flags (a warning is logged).
- **Per-hour run grouping breaks across non-flagged wind bins** (typically wind < 7 m/s). Real sustained constraints coincide with sustained mid/high wind, so this isn't a problem in practice — and it matches the reference pipeline's behaviour (`tests/reference/energyexe_pipeline_full.py:477-494`).
- ERA5 wind-speed bias (0.5 m/s at 10–12 m/s) translates to 5–8 % expected-output error per bin, which can push borderline periods over or under the threshold. Analyst review is the safety net.
- Cannot distinguish a real cable failure from prolonged noise curtailment or multi-turbine derating campaigns — the `pending_review` queue is intentional for this.

## File reference

- Service: `app/services/structural_constraint_detection_service.py`
- Model: `app/models/structural_constraint_flag.py`
- Migration: `alembic/versions/e2f3a4b5c6d7_add_structural_constraint_flags.py`
- Read API: `app/api/v1/endpoints/structural_constraints.py`
- Orchestrator integration: `app/services/performance_pipeline_service.py:run_pipeline` (Module 1b block)
- Tests: `tests/test_structural_constraint_detection.py` (11 unit tests including B1.5 half-BMU synthetic), `tests/test_fx2_constraint_consumption.py` (7 mask-builder tests)
- P-1 validation findings: `tests/reference/p-1-validation-notes.md` (EAO + Hornsea 1 Q50-suppression evidence)
