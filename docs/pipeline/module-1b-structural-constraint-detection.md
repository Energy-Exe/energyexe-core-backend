# Module 1b — Structural constraint detection

> **Status: NOT IMPLEMENTED.** This module appears in the May 2026 spec ("post-Niord update") but has no corresponding service, model, or migration in our codebase as of 2026-05-20. This doc records the concept, why it matters, and where it would slot in — useful when we build it.

## Purpose

Detect sustained periods (≥ 2 weeks, default) where windfarm output is systematically truncated relative to wind conditions — the signature of a partial export-infrastructure failure (typically a single cable on a multi-cable offshore farm). These periods quietly contaminate Modules 2, 3 and 5 if not excluded.

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
You can't fix this by comparing 2024 against a baseline that *includes* 2024 — the baseline absorbs the contamination. The detector builds a reference Q90-per-wind-bin from **all years except Y**, then compares year Y's observed Q90 against that clean reference. Now a depressed year stands out.

### Wind-banded ratio thresholds
The detector flags a (bin, month) as constrained when `observed_Q90 / reference_Q90` falls below a band-specific threshold:

| Wind band | Default threshold | Why this threshold |
|---|---|---|
| < 7 m/s | no check | output is naturally noisy near cut-in; ratio is unreliable |
| 7 – 10 m/s | 0.70 | ramp region — natural variance is higher |
| 10 – 25 m/s | 0.80 | rated region — turbines should be near full output; truncation is unambiguous |

A run of flagged bin-months gets grouped into a candidate "run". Runs shorter than 336 hours (2 weeks) are dropped — short events belong to Module 3, not 1b.

### Mean Q90 ratio is a fault-type fingerprint
- Ratio ≈ 0.5 → single cable failure on a two-cable farm
- Ratio ≈ 0.0 → full export outage
- Ratio in 0.5–0.8 → partial curtailment or noise restriction (analyst review needed)

### Downstream contamination if not implemented
Without Module 1b, a 7-month cable constraint in a 4-year dataset:

| Module | Effect of contamination |
|---|---|
| 2 — power curve | Yearly Q50/Q90 in mid-to-high wind bins are depressed → capability curve understates true capability |
| 3 — losses | Anomalies computed against an *already-degraded* baseline → losses are reported as ~zero even though half the energy is missing |
| 5 — degradation | OLS sees the constraint as a step-down → fits a steep negative slope → reports fake degradation that can flip the sign of the real estimate |
| 4 — wind norm | Reference curve (`overall_clean`) is built from contaminated hours → index for the constrained period reads "normal" (~100), masking the problem |

### The pending_review flow
This is a borderline detection (real constraint vs prolonged maintenance campaign vs noise curtailment). The spec defines a two-stage flow:

1. **Auto-detector** runs as part of the pipeline, writes candidate runs with `review_status='pending_review'`.
2. **Analyst** reviews each candidate, confirms it's a real constraint (`confirmed`) or rejects it (`dismissed`).
3. **Backend emails the analyst** when new `pending_review` rows appear (notification logic lives outside the Python pipeline).
4. Downstream modules **exclude only confirmed runs** from their reference data.

## What exists in the codebase today

`grep` across `app/`, `scripts/`, `alembic/` for `structural_constraint`, `constraint_run`, `constraint_detection`, `leave_one_year`, `cable_fail`, `export_constraint`, `q90_ratio`, `pending_review` — **zero matches**.

Closest existing patterns:

| Component | What it does | Why it's NOT Module 1b |
|---|---|---|
| `DataAnomalyService` (`app/services/data_anomaly_service.py`) | Detects raw-data quality issues — capacity factor > 120 %, data gaps, generation spikes vs unit capacity. | Operates on raw input data, not on distributional shifts of a power curve. |
| `PerformanceAnomalyService` (`app/services/performance_anomaly_service.py`) | Per-hour MAD-based under/overperformance flags (Module 3). | Treats hours independently — exactly the blindspot Module 1b is designed to cover. |
| `PerformanceSummary` (`app/models/performance_summary.py`) | Stores monthly/yearly ODI metrics. | No constraint-related columns. |

## Proposed integration points (when we build it)

### Where it sits in the pipeline
Between Module 1 (data load) and Module 2 (power curve fitting). It needs `df_clean` to spot constraints, and it needs to produce `df_curve_clean` (= `df_curve` minus confirmed-constraint hours) before Module 2 builds the capability curves.

In `PerformancePipelineService.run_pipeline` (`app/services/performance_pipeline_service.py:92–230`), this would go between the data-load (~line 117) and the `pcs.build_power_curves(...)` call (~line 123):

```python
# Module 1b: structural constraint detection
constraint_svc = StructuralConstraintDetectionService(self.db)
constraint_result = await constraint_svc.detect(
    windfarm_id, df_clean, df_curve, years
)
df_curve_clean = constraint_svc.apply_exclusions(df_curve, constraint_result.confirmed_runs)
result["structural_constraints"] = constraint_result.summary
```

### Proposed DB schema

```sql
CREATE TABLE structural_constraint_flags (
    id              SERIAL PRIMARY KEY,
    windfarm_id     INT NOT NULL REFERENCES windfarms(id) ON DELETE CASCADE,
    period_start    TIMESTAMPTZ NOT NULL,
    period_end      TIMESTAMPTZ NOT NULL,
    duration_hours  INT NOT NULL,
    wind_bins_affected INT,
    mean_q90_ratio  NUMERIC(6,3),       -- ~0.5 = single cable, ~0.0 = full outage
    flag_source     VARCHAR(40) DEFAULT 'auto_constraint_detector',
    review_status   VARCHAR(20) DEFAULT 'pending_review',  -- pending_review | confirmed | dismissed
    analyst_notes   TEXT,
    reviewed_by     INT REFERENCES users(id),
    reviewed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (windfarm_id, period_start, period_end)
);
CREATE INDEX ix_structural_constraint_flags_status ON structural_constraint_flags(review_status);
```

A second table can hold the leave-one-year-out reference if we want to expose it (otherwise it's recomputed each run from `power_curve_bins`).

### Downstream wiring once it exists

| Module | Required change |
|---|---|
| 2 | Build `overall_clean` and `yearly_capability_stats` from `df_curve_clean` (excludes confirmed runs). |
| 3 | For constrained hours, use `overall_clean.q50` (not `yearly_capability_stats.q50`) as the loss reference. Add `flag_structural_constraint` column on `performance_anomalies`. New `constraint_loss_summary` output (per-run lost MWh / EUR). |
| 4 | `overall_clean` is now genuinely clean → no service change required, output naturally becomes more accurate. |
| 5 | Exclude confirmed-constraint hours from the OLS fit. Add `n_constraint_hours_excluded` column on `degradation_results`. |
| 6 | No change — picks up cleaner capability stats automatically. |

### Notification

Per spec, the backend (not the Python pipeline) should email the responsible analyst when new `pending_review` rows appear. Candidate hooks:

- A cron sweep (e.g. `pipeline_daily.py` post-step) that queries `structural_constraint_flags WHERE review_status='pending_review' AND created_at > last_sweep`.
- Or a Postgres NOTIFY / trigger feeding into the existing alerting infrastructure (`app/services/alert_service.py`).

## Limitations (also from spec)

- Needs **≥ 2 years of data** to build the leave-one-year-out reference. Single-year windfarms should pass `df_curve` through unchanged with a warning.
- Cannot distinguish a real cable failure from other sustained upper-curve truncations (prolonged noise curtailment, multi-turbine campaigns, derating). The `pending_review` flow exists precisely for this reason.
- ERA5 wind-speed bias (0.5 m/s at 10–12 m/s) translates to 5–8 % expected-output error per bin, which can push borderline periods over or under the threshold.

## File reference (for when we build it)

- Where to add the service: `app/services/structural_constraint_detection_service.py` (new)
- Where to add the model: `app/models/structural_constraint_flag.py` (new)
- Where to add the migration: `alembic/versions/{hash}_add_structural_constraint_flags.py` (new)
- Where to call it from: `app/services/performance_pipeline_service.py:~120` (between data load and power-curve build)
- Where to expose review UI: `app/api/v1/endpoints/` (new endpoint group) + admin-ui review page
