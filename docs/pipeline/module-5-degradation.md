# Module 5 — Degradation analysis

## Purpose

Estimate the long-run trend in operational performance — is the windfarm degrading (or recovering) over time, after stripping out wind variability and seasonality? Fits a linear regression on per-hour deseasonalised residuals and reports a slope in **% per year** with a 95 % confidence interval. Run twice per windfarm — once with the Q50 (P50) reference, once with the Q90 (P10) reference.

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

`reference_bin_p_pu` is the yearly capability curve value for the hour's wind bin. A residual of 0 means "the turbine produced exactly what its curve predicts at this wind speed". Negative residuals = underperformance. The residual *removes* the wind signal — what's left is the operational signal.

### Why seasonal decomposition matters

Residuals have an annual cycle that's not degradation:
- Cold months — denser air → small positive boost to power.
- Summer months — heat derates, control mode changes, blade icing in cold-climate sites.

If you fit a line through residuals without removing the seasonal cycle, the slope is biased by *where* your dataset starts/ends in the year. A dataset running Jan 2020 – Dec 2022 (3 full years) is fine; a dataset running Aug 2020 – May 2023 will show artifactual trend from the seasonal mismatch.

We use `statsmodels.tsa.seasonal_decompose(model="additive", period=8760)` on the per-hour residual series before the OLS fit. When the series is shorter than `2 × period`, the decomposition is skipped and a warning is logged (the dataset is too short to learn a stable seasonal cycle).

### OLS, slope, intercept, R², CI95 in plain words

- **OLS** — ordinary least squares: fit the line that minimises sum of squared residuals.
- **Slope** — average change in residual per year. Negative = degrading.
- **Intercept** — fitted residual at year 0 (a fitted extrapolation; almost never physically meaningful).
- **R²** — fraction of variance the line explains. **Hourly data is noisier than monthly aggregates**, so R² in this module looks lower (typically 0.001-0.05) than you might expect from a monthly fit. That's correct, not a regression — significance is in the CI, not the R².
- **CI95** — 95 % confidence interval. **More important than the point estimate** because:
  - `-0.3 %/yr ± 1.0 %` → CI includes zero → not statistically significant
  - `-0.3 %/yr ± 0.05 %` → CI excludes zero → real degradation

Exposed on the API in both p.u./yr and %/yr units (`ci_lower_95`, `ci_upper_95` and `ci_lower_95_pct`, `ci_upper_95_pct`).

### Why ≥ 2 years

OLS with `n = 2 monthly aggregates` is a line through two points — meaningless CI. We now run on hourly data so `n` is typically 10k-40k, but the same idea applies: anything below ~24 months of coverage gives unreliable seasonal-decomposition output. The service falls back to returning `None` (no degradation row written) if there are fewer than 100 qualifying hours after filtering.

### Why structural constraints contaminate this fit

A 7-month export outage creates a step-down in the residual time series. OLS sees the step as accelerated downtrend and reports a much steeper slope than the true degradation rate. For long enough outages the bias can flip the sign of the answer.

**Module 1b is what handles this.** Active constraint flags (`review_status IN ('pending_review', 'confirmed')`) are loaded by the orchestrator and used to mask out constrained hours from the dataset before this module fits. The number of hours excluded is persisted on `degradation_results.n_constraint_hours_excluded` so reports can show "X hours of Y excluded as known constraints" alongside the slope.

### Why Q50 and Q90 references

- **Q50 (P50) slope** — degradation of the *median* hour. The headline number.
- **Q90 (P10) slope** — degradation of the *upper-decile* hours. If your best hours are slipping faster than your median hours, you're losing top-capacity faster than overall — typically signals derate creep or partial constraint creep.

Both are computed; the UI shows both.

## Implementation walkthrough

**File:** `app/services/degradation_service.py`

| Spec section | What it does | Where |
|---|---|---|
| 5a operational subset | wind 4–14 m/s, drop bins where `q50_bin < 0.10 pu` | constants `OP_WIND_MIN`, `OP_WIND_MAX`, `MIN_MEDIAN_PU_FOR_OPERATIONAL`; applied in `compute_residuals` |
| 5b residual computation | per-hour `residual_pu = p_pu - ref_pu` from yearly capability curves | `compute_residuals` |
| 5c seasonal decomposition | `statsmodels.tsa.seasonal_decompose(period=8760)`, subtract seasonal | `remove_seasonal_component` (module-level helper) |
| 5d OLS trend fit | `scipy.stats.linregress(year_fraction, residual_deseasonalised)` | `fit_degradation_trend` |
| 5e baseline | hours-weighted median of `ref_pu` in first year of `df_fit` | inside `fit_degradation_trend` |
| 5f constraint masking | active flags masked out of input `df` before this module runs | orchestrator (`run_pipeline`), not here |

### Step-by-step

1. **Load capability curves** for every year in the dataset (`PowerCurveBin` with `curve_type='capability'`) into a `{year: {wind_bin: ref_pu}}` lookup.
2. **Filter** hourly data to wind 4–14 m/s. Drop bins where the reference is below 0.10 (too close to cut-in, too noisy to learn from).
3. **Compute hourly residuals.** `actual_p_pu - reference_bin_p_pu` per hour. `year_fraction = year + (dayofyear - 1) / 365.25` (matches the reference at `tests/reference/energyexe_pipeline_full.py:1001`).
4. **Deseasonalise.** Set the residual series to a `hour`-indexed Series; pass through `remove_seasonal_component(period=8760)`. Subtracts the additive seasonal component. For series shorter than `2 × period`, returns unchanged.
5. **OLS fit.** `scipy.stats.linregress(year_fraction, residual_deseasonalised)` → slope, intercept, r_value, p_value, std_err. R² = r_value².
6. **CI95.** `t.ppf(0.975, n-2) × std_err` for `n ≥ 3` and non-zero std_err.
7. **Baseline.** Median of `ref_pu` within the first year of `df_fit` (rows where `year_fraction.between(first_year, first_year + 1)`). Hours-weighted because each row contributes one ref_pu value, so a wind bin with many hours contributes that many identical values. Falls back to `0.35` and logs a warning if the first-year window is empty or yields NaN.
8. **Express as %/yr.** `slope_pct = slope / baseline_cap_pu × 100`. Same conversion for `ci95_pct`.
9. **Persist.** One row per reference (`q50`, `q90`) in `degradation_results`, after deleting any prior row for the same `(windfarm_id, reference_curve, pipeline_run_id)`.

## DB model

**File:** `app/models/degradation_result.py`
**Table:** `degradation_results`

| Column | Type | Notes |
|---|---|---|
| `id` | BigInt PK | |
| `windfarm_id` | Int FK | |
| `reference_curve` | VARCHAR | `'q50'` or `'q90'` |
| `analysis_start` | Date | first day included |
| `analysis_end` | Date | last day included |
| `data_points` | Int | number of hourly residuals used in the fit |
| `slope_pu_per_year` | Numeric(12,8) | e.g. -0.00152543 |
| `slope_pct_per_year` | Numeric(10,3) | e.g. -0.385 |
| `intercept` | Numeric(14,6) | (large values typical given `year_fraction ≈ 2020`) |
| `r_squared` | Numeric(6,5) | [0, 1] — low for hourly fits is normal |
| `p_value` | Numeric(8,6) | statistical significance |
| `ci_lower_95` | Numeric(12,8) | lower CI bound on slope, p.u./yr |
| `ci_upper_95` | Numeric(12,8) | upper CI bound on slope, p.u./yr |
| `ci_lower_95_pct` | Numeric(10,3) | lower CI bound on slope, %/yr |
| `ci_upper_95_pct` | Numeric(10,3) | upper CI bound on slope, %/yr |
| `baseline_cap_pu` | Numeric(6,5) | per-windfarm first-year median used to compute slope_pct |
| `n_constraint_hours_excluded` | Int | hours masked out by active Module 1b flags |
| `pipeline_run_id` | Int FK, nullable | enables join with `import_job_executions` |

**Unique:** `(windfarm_id, reference_curve, pipeline_run_id)`. **Two rows per windfarm per run** — one each for Q50 and Q90.

### Why the precision was widened (historical)

Migration `d7f91a2b3c4e_widen_degradation_precision.py`. OLS fits `residual ~ year_fraction`; with `year_fraction ≈ 2020`, the intercept lands at `~+2` to `~-2000`. The original `Numeric(8,6)` allowed only `|value| < 100`. Widened columns: `intercept`, `slope_pu_per_year`, `slope_pct_per_year`, `ci_lower_95`, `ci_upper_95`.

## Inputs / outputs

**Inputs:**
- Hourly DataFrame (pre-loaded by the orchestrator; constraint hours already masked out — see "Why structural constraints contaminate this fit" above).
- `power_curve_bins` with `curve_type='capability'` per year (from Module 2).
- `windfarms.nameplate_capacity_mw` for p_pu / MW conversions.

**Outputs:**
- Rows in `degradation_results`.
- Returned dict from `analyze_degradation_from_df`:

```python
{
  "reference": "q50",
  "slope_pu_per_year": -0.00152,
  "slope_pct_per_year": -0.43,
  "r_squared": 0.005,
  "p_value": 0.018,
  "ci_95": (-0.0025, -0.00055),
  "ci_95_pct": (-0.71, -0.16),
  "baseline_cap_pu": 0.353,
  "n_constraint_hours_excluded": 1428,
  "data_points": 18203,
  "analysis_range": "2021-01-01 to 2023-12-31",
}
```

## LLM commentary integration

**Prompt:** `app/prompts/degradation.txt`

Jinja2 template that takes `slope_pct, ci_lower_95, ci_upper_95, r2, baseline_cap_pu, analysis_start, analysis_end, vs_zone_avg_slope, zone_windfarm_count, n_constraint_hours_excluded` and writes 2–3 paragraphs:

1. **Headline** — is the windfarm degrading, by how much, and is it statistically significant?
2. **Operational implication** — what does this mean in MWh / EUR over the next 5 years, and does it warrant intervention?
3. **Peer context** — how does the slope compare to the bidzone average?

## Caller graph

- **Orchestrator:** `PerformancePipelineService.run_pipeline` calls `analyze_degradation_from_df` per reference inside SAVEPOINT blocks, passing the constraint-masked `df_no_over`.
- **Cron:** Daily 03:00 UTC via `app/cron/pipeline_daily.py`.
- **HTTP read API:** `GET /api/v1/performance-pipeline/degradation/{windfarm_id}` returns both Q50 and Q90 rows, optionally enriched with bidzone peer averages.
- **Frontend:** `energyexe-client-ui/src/lib/performance-pipeline-api.ts`, the windfarm-detail page renders slope, CI band, and zone comparison.

## Status vs spec (post 2026-05-25)

| Spec ask | Status |
|---|---|
| Wind range 4–14 m/s, drop bins where `q50 < 0.10` | ✓ |
| Residual = actual - capability reference (per-year) | ✓ |
| Seasonal decomposition (additive, period 8760 h) | ✓ (PR #64) |
| OLS on per-hour deseasonalised residuals | ✓ (PR #64) |
| `year_fraction = year + (dayofyear - 1) / 365.25` | ✓ |
| OLS slope + CI95 + t-distribution | ✓ |
| Baseline = median of cap_bin in first year | ✓ (PR #70) |
| `slope_pct = slope / baseline × 100` | ✓ |
| `ci_95_pct` exposed alongside `ci_95` | ✓ (PR #70) |
| `p_value` in API | ✓ |
| Exclude constrained hours from OLS fit | ✓ (PR #72 — via orchestrator-level mask, not in this service) |
| `n_constraint_hours_excluded` reported | ✓ (PR #72) |

## File reference

- Service: `app/services/degradation_service.py`
- Model: `app/models/degradation_result.py`
- Migrations: `d7f91a2b3c4e` (precision), `c8d9e0f1a2b3` (ci_pct), `f3a4b5c6d7e8` (n_constraint_hours_excluded)
- LLM prompt: `app/prompts/degradation.txt`
- Tests: `tests/test_degradation.py` (23 tests covering A1-A3 golden + integration)
- Orchestrator call site: `app/services/performance_pipeline_service.py:run_pipeline` (Module 5 block)
- Read API: `app/api/v1/endpoints/performance_pipeline.py` (`GET /degradation/{windfarm_id}`)
- Frontend: `energyexe-client-ui/src/lib/performance-pipeline-api.ts`
