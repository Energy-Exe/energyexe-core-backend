# Module 5 — Degradation analysis

## Purpose

Estimate the long-run trend in operational performance — is the windfarm degrading (or recovering) over time, after stripping out wind variability? Fits a linear regression to monthly residuals and reports a slope in **% per year** with a 95 % confidence interval. Run twice — once with the Q50 (P50) reference, once with the Q90 (P10) reference.

## Concepts

### What "degradation" means physically
Wind turbines slow down over time:
- Blade leading-edge erosion (rain, ice, dust) → lower aerodynamic efficiency.
- Gearbox / bearing wear → more friction losses.
- Sensor drift on the pitch / yaw control → suboptimal alignment.
- Increasing downtime (more frequent unplanned maintenance).

Typical industry numbers: **0.1 – 0.5 % per year**. Anything ≥ 0.5 %/yr is a concern (maintenance plan; possibly an early-life manufacturing issue). Anything ≥ 1 %/yr is unusual without a known fault.

### What a "residual" is
For each hour:
```
residual_pu = actual_p_pu - reference_bin_p_pu
```

Where `reference_bin_p_pu` is the yearly capability curve value for this wind bin. A residual of 0 means "the turbine produced exactly what its curve predicts at this wind speed". Negative residuals = underperformance. The residual *removes* the wind signal — what's left is the operational signal.

### Why seasonal decomposition matters
Residuals have an annual cycle that's not degradation:
- Cold months — denser air → small positive boost to power.
- Summer months — heat derates, control mode changes, blade icing in cold-climate sites.

If you fit a line through residuals without removing the seasonal cycle, the slope is biased by *where* your dataset starts/ends in the year. A dataset running Jan 2020 – Dec 2022 (3 full years) is fine; a dataset running Aug 2020 – May 2023 will show artifactual trend from the seasonal mismatch.

**Spec calls for additive seasonal decomposition (period 8760 h = 1 year) before the OLS fit.** Our implementation does NOT do this — see [Gaps vs spec](#gaps-vs-spec).

### OLS, slope, intercept, R², CI95 in plain words
- **OLS** — ordinary least squares: fit the line that minimises sum of squared residuals.
- **Slope** — average change in residual per year. Negative = degrading.
- **Intercept** — fitted residual at year 0 (a fitted extrapolation; almost never physically meaningful at year 0).
- **R²** — fraction of variance the line explains. 0.05 = very weak (line barely better than the mean); 0.5+ = strong (clear trend).
- **CI95** — 95 % confidence interval. **More important than the point estimate** because:
  - `-0.3 %/yr ± 1.0 %` → CI includes zero → not statistically significant
  - `-0.3 %/yr ± 0.05 %` → CI excludes zero → real degradation

### Why ≥ 2 years
OLS with `n = 2 monthly aggregates` is a line through two points — meaningless CI. The service falls back to `ci95 = None` if `n < 3`. Anything below ~24 months is unreliable; trustworthy degradation estimates want 3+ years.

### Why structural constraints contaminate this fit
A 7-month export outage creates a step-down in the residual time series. OLS sees the step as accelerated downtrend and reports a much steeper slope than the true degradation rate. For long enough outages the bias can flip the sign of the answer. **Module 1b is what fixes this** — by excluding constrained hours from the OLS fit (`n_constraint_hours_excluded` column in the spec). Today we don't have Module 1b → degradation results during/after a major outage are unreliable.

### Why Q50 and Q90 references
- **Q50 (P50) slope** — degradation of the *median* hour. The headline number.
- **Q90 (P10) slope** — degradation of the *upper-decile* hours. If your best hours are slipping faster than your median hours, you're losing top-capacity faster than overall — typically signals derate creep or partial constraint creep.

Both are computed; the UI shows both.

## Implementation walkthrough

**File:** `app/services/degradation_service.py:1–323`

| Spec section | What it does | Lines |
|---|---|---|
| 5a operational subset | wind 4–14 m/s, drop bins where `q50_bin < 0.10 pu` | constants `OP_WIND_MIN = 4.0`, `OP_WIND_MAX = 14.0`, `MIN_MEDIAN_PU_FOR_OPERATIONAL = 0.10` at `:28–30`; applied at `:161` |
| 5b residual computation | `residual_pu = actual - reference_bin` from yearly `capability` curves | `:146–199` |
| 5c seasonal decomposition | **NOT IMPLEMENTED** — see Gaps | comment at `:8` |
| 5d OLS trend fit | `linregress(year_fraction, mean_residual_pu)` | `:201–251` |

### Step-by-step

1. **Load capability curves** for every year in the dataset (`PowerCurveBin` with `curve_type='capability'`) into a `{year: {wind_bin: ref_pu}}` lookup.
2. **Filter** hourly data to wind 4–14 m/s. Drop bins where reference Q50 < 0.10 (too close to cut-in, too noisy to learn from).
3. **Compute hourly residuals.** `actual_p_pu - reference_bin_p_pu` for each hour.
4. **Aggregate to monthly.** Mean and median of `residual_pu` per `(year, month)`; also `n_hours`. Build `year_fraction = year + (month - 0.5) / 12` (`:197`).
5. **OLS fit.** `scipy.stats.linregress(year_fraction, mean_residual_pu)` → slope, intercept, R², p-value, stderr.
6. **CI95.** `t.ppf(0.975, n-2) × stderr` if scipy available, else `1.96 × stderr`.
7. **Express as %/yr.** `slope_pct = (slope_pu / baseline_cap_pu) × 100`. Baseline is hardcoded `0.35 pu` default (`:237`) — overridden only if `pipeline_run_id` is provided and a peer aggregate exists. This is a real gap (see below).
8. **Persist.** One row per reference (q50, q90) in `degradation_results`, after deleting any prior row for the same `(windfarm_id, reference_curve, pipeline_run_id)`.

## DB model

**File:** `app/models/degradation_result.py:21–72`
**Table:** `degradation_results`

| Column | Type | Notes |
|---|---|---|
| `id` | BigInt PK | |
| `windfarm_id` | Int FK | |
| `reference_curve` | VARCHAR | `'q50'` or `'q90'` |
| `analysis_start` | Date | first month included |
| `analysis_end` | Date | last month included |
| `data_points` | Int | number of monthly aggregates |
| `slope_pu_per_year` | Numeric(12,8) | e.g. -0.00152543 |
| `slope_pct_per_year` | Numeric(10,3) | e.g. -0.385 |
| `intercept` | Numeric(14,6) | large values (~-1500 to +500 typical for `year_fraction ≈ 2020`) |
| `r_squared` | Numeric(6,5) | [0, 1] |
| `p_value` | Numeric(8,6) | statistical significance |
| `ci_lower_95` | Numeric(12,8) | lower CI bound on slope_pu |
| `ci_upper_95` | Numeric(12,8) | upper CI bound on slope_pu |
| `baseline_cap_pu` | Numeric(6,5) | reference baseline used to compute slope_pct |
| `pipeline_run_id` | Int FK, nullable | enables result vs peer aggregate |

**Unique:** `(windfarm_id, reference_curve, pipeline_run_id)`. So **two rows per windfarm per run** — one each for Q50 and Q90, not two columns.

### Why the precision was widened

Migration `alembic/versions/d7f91a2b3c4e_widen_degradation_precision.py:1–36`.

The OLS fits `residual_pu ~ year_fraction`. Year fraction is around 2020–2024. The fitted intercept is approximately `-slope × 2020 + residual_at_year_2020`. With slopes of `~-0.001 pu/yr`, the intercept lands at `~+2` to `~-2000` depending on era and sign. The original `Numeric(8,6)` column allowed only `|value| < 100` → silent insert failures for anything realistic.

Migration widened:
- `intercept` 8 → **14**
- `slope_pu_per_year` 8 → **12**
- `ci_lower_95`, `ci_upper_95` 8 → **12**
- `slope_pct_per_year` 6 → **10**

If you ever extend the analysis window or change `year_fraction` to a centered variable (e.g. `year - 2020`), these widths would no longer be required — but for now keep them.

## Inputs / outputs

**Inputs:**
- `df_clean` (from Module 1, pre-loaded via the orchestrator).
- `power_curve_bins` with `curve_type='capability'` (per-year Q50 / Q90 from Module 2).
- `windfarms.nameplate_capacity_mw` for any p_pu/MW conversions.

**Outputs:**
- Rows in `degradation_results` (one per `(windfarm, reference, pipeline_run_id)`).
- Returned dict from `compute_degradation`:

```python
{
  "slope_pu_per_year": -0.00152,
  "slope_pct_per_year": -0.43,
  "intercept": -3.08,
  "r2": 0.124,
  "p_value": 0.018,
  "ci_lower_95": -0.0025,
  "ci_upper_95": -0.00055,
  "baseline_cap_pu": 0.353,
  "data_points": 36,
  "analysis_start": "2021-01-01",
  "analysis_end": "2023-12-01",
  "n": 36
}
```

## LLM commentary integration

**Prompt:** `app/prompts/degradation.txt:1–57`

Jinja2 template that takes `slope_pct, ci_lower_95, ci_upper_95, r2, baseline_cap_pu, analysis_start, analysis_end, vs_zone_avg_slope, zone_windfarm_count` and writes 2–3 paragraphs:

1. **Headline** — is the windfarm degrading, by how much, and is it statistically significant?
2. **Operational implication** — what does this mean in MWh / EUR over the next 5 years, and does it warrant intervention?
3. **Peer context** — how does the slope compare to the bidzone average?

Rendered via the broader LLM commentary infrastructure (not directly called from `degradation_service.py`).

## Caller graph

### Orchestrator
- `PerformancePipelineService.run_pipeline` calls `compute_degradation` per reference inside SAVEPOINT blocks (`app/services/performance_pipeline_service.py:168–179`).

### Cron
- Daily 03:00 UTC via `app/cron/pipeline_daily.py`.

### HTTP read API
- `GET /api/v1/performance-pipeline/degradation/{windfarm_id}` (`app/api/v1/endpoints/performance_pipeline.py:269–336`) — returns both Q50 and Q90 rows. Optionally enriched with bidzone peer average (`PeerAggregateService`, metric keys `degradation_slope_pct_per_year_q50`, `_q90`).

### Frontend
- `energyexe-client-ui/src/lib/performance-pipeline-api.ts:60–73` — `DegradationRow` TS interface, `useDegradation(windfarmId)` query hook.
- Client UI windfarm page renders the slope, CI band, and a "vs zone" comparison.

## Gaps vs spec

| Spec ask | Status | Notes |
|---|---|---|
| Wind range 4–14 m/s, drop bins where `q50 < 0.10` | ✓ | constants and filter at `:28–30`, `:161` |
| Residual = actual - capability reference | ✓ | per-year capability curve |
| **Seasonal decomposition (additive, period 8760 h)** | ✗ | Comment at `:8` mentions it; no statsmodels call. Residuals are not deseasonalised before fit. Potential slope bias when dataset doesn't span whole calendar years cleanly. |
| OLS slope + CI95 | ✓ | scipy.stats.linregress + t-distribution |
| Express as %/yr vs baseline cap | ✓ but flawed | `baseline_cap_pu` defaults to hardcoded `0.35` (`:237`) unless a peer aggregate exists. Should compute per-windfarm Q50 mean in the operational range. Wrong baseline → wrong %/yr. |
| **`n_constraint_hours_excluded` column** | ✗ | Tied to Module 1b. |
| **Exclude structurally constrained hours from OLS fit** | ✗ | Tied to Module 1b. A multi-month outage in the dataset will produce a spurious slope. |
| `≥2 years` requirement | ✓ (soft) | Service computes `ci95 = None` for `n < 3` monthly points, surfacing the limitation indirectly. |
| Surface `p_value` to API | ✗ | Computed and stored, but the response schema (`DegradationResponse`) omits it. Users can't directly see significance — they have to inspect whether the CI straddles zero. |
| CSV outputs | ✗ | persisted to Postgres |

## File reference

- Service: `app/services/degradation_service.py:1–323`
- Model: `app/models/degradation_result.py:21–72`
- Migration (precision fix): `alembic/versions/d7f91a2b3c4e_widen_degradation_precision.py`
- LLM prompt: `app/prompts/degradation.txt:1–57`
- Tests: `tests/test_degradation.py`
- Orchestrator call site: `app/services/performance_pipeline_service.py:168–179`
- Read API: `app/api/v1/endpoints/performance_pipeline.py:269–336`
- Frontend: `energyexe-client-ui/src/lib/performance-pipeline-api.ts:60–73`
