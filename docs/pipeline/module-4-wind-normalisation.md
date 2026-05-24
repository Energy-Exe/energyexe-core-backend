# Module 4 — Wind normalisation

## Purpose

Strip the wind-resource variability out of the performance signal so we can answer "how did the *asset* perform?" independently of "how windy was it?". Outputs an index where **100 = the windfarm's own long-run typical performance** and deviations reflect operational state (better/worse than usual at the same wind conditions).

Two variants are produced — once with the Q50 (P50) reference curve, once with the Q90 (P10) reference curve.

## Concepts

### The problem this solves
Raw MWh output mixes two signals: operational performance and weather. A "low MWh year" might just be a low-wind year. Comparing 2023 (8 m/s average) to 2024 (10 m/s average) raw is meaningless. Normalising against the empirical power curve removes the weather signal.

### `norm_ratio = actual / expected`
For each hour:
- Look up `expected_mw = q50_bin × rated_mw` from the `overall_clean` curve at this hour's wind bin.
- Compute `norm_ratio = actual_mw / expected_mw`.

Reads as: "given the wind we actually had, did the turbine deliver what the curve predicts?"
- 1.00 = exactly at typical capability
- 1.05 = 5 % above typical (could be excellent operations or imperfect curve)
- 0.95 = 5 % below typical

### Why 4 m/s minimum
Below cut-in (~3.5 m/s) the turbine is barely producing, and measurement noise dominates the ratio. A hour with `expected_mw = 0.01` and `actual_mw = 0.02` gives `norm_ratio = 2.0` — pure noise. Excluding `wind_speed < 4 m/s` makes the index meaningful.

### Index vs historical mean — the anchoring trick
A bare ratio of 0.95 is hard to interpret. So we anchor:

```
historical_mean = mean(all monthly norm_ratios across the entire dataset)
index_vs_base   = monthly_norm_ratio / historical_mean × 100
```

An index of 100 = "you're operating at your typical long-run level". An index of 92 = "this month was 8 % below your typical". This accounts for site-specific quirks (wake losses, control strategy, persistent wind sector preferences) — the windfarm is compared to itself, not to nameplate.

### Q50 vs Q10 (P50 vs P10) — two different questions

| Reference | Index reads as |
|---|---|
| Q50 (P50) — median expectation | "performance vs typical" — the everyday operational health metric |
| Q90 (P10) — upper-capability bound | "performance vs upper-capability potential" — exposes whether the best hours are slipping (sign of creeping derate or partial constraint) |

Both are computed and stored. The client UI shows both depending on the question.

### Contrast with Module 5 (degradation)
- Module 4 (wind-norm) reports **level** of performance period-by-period (100 = baseline).
- Module 5 (degradation) reports **trend** in residuals across years.

Both build on the same residual concept. Module 4 reports the residual averaged over a month or year as an index; Module 5 fits a line through the time series of residuals and reports the slope.

## Implementation walkthrough

**File:** `app/services/wind_normalisation_service.py`

Three tiers of aggregation:

| Spec section | Method (or section) | Lines |
|---|---|---|
| Hourly ratio computation | `compute_normalisation_from_df` (hour loop) | `117–152` |
| Monthly aggregation + index | (monthly groupby) | `155–192` |
| Yearly aggregation + index | (yearly groupby) | inside same method |
| Curve lookup cache | `_load_curve_lookup` | `196–215` |
| Reference name → DB column mapping | (q50 → `p50`, q90 → `p10`) | `232` |

### Step-by-step

1. **Build the curve lookup** once — fetch `power_curve_bins` rows with `curve_type='overall_clean'` and map `wind_bin → q50_pu` (or `q90_pu` for the Q10 run). Avoids querying Postgres per hour.
2. **Per-hour ratio.** Wind-bin every hour (1 m/s). Look up `expected_pu` from the cache. Compute `expected_mw = expected_pu × rated_mw` and `norm_ratio = actual_mw / expected_mw`. Drop hours where `wind_speed < 4 m/s`, no curve value, or `expected_mw ≤ 0`.
3. **Monthly average ratio.** Group qualifying hours by `(year, month)`, mean of `norm_ratio` → `avg_norm_ratio` per month.
4. **Historical mean.** Mean of all monthly avg_norm_ratios across the dataset.
5. **Monthly index.** `index_vs_base = avg_norm_ratio / historical_mean × 100`.
6. **Yearly aggregation.** Mean of monthly ratios per year → `avg_norm_ratio_yearly`. `index = avg_norm_ratio_yearly / historical_mean × 100`.
7. **Persistence.** Upsert into `performance_summaries` — monthly rows (period_type='month') and yearly rows (period_type='year', month NULL).

### Reference naming convention

The service uses `reference='q50'` and `reference='q90'` internally but stores them on `performance_summaries` as `_p50` and `_p10`:

| Code | DB column | Industry term |
|---|---|---|
| `q50` | `norm_ratio_p50`, `norm_index_p50` | P50 (median) |
| `q90` | `norm_ratio_p10`, `norm_index_p10` | P10 (top decile) |

(`q90` = 90th percentile = "only 10 % of hours exceed this" = **P10** in industry notation.)

### Upsert mechanics

Monthly rows use `INSERT ... ON CONFLICT DO UPDATE` (cheap, ON CONFLICT matches the `(windfarm, period_type, year, month)` unique key).

Yearly rows have `month=NULL`, which breaks naive ON CONFLICT. The service does UPDATE-then-INSERT (fast path: row already exists from Module 3; slow path: first time, insert it). See `app/services/wind_normalisation_service.py:219–306`.

## DB persistence

**Table:** `performance_summaries` (shared with Modules 3 and 6).

Module 4 columns (`app/models/performance_summary.py:49–52`):

| Column | Notes |
|---|---|
| `norm_ratio_p50` | average norm_ratio for the period (Q50 reference) |
| `norm_index_p50` | `ratio / historical_mean × 100` |
| `norm_ratio_p10` | average norm_ratio (Q90 / P10 reference) |
| `norm_index_p10` | index for the P10 reference |

Grain: one row per `(windfarm, period_type='month'|'year', year, month-or-null)`.

## LLM commentary integration

**Prompt:** `app/prompts/wind_normalisation.txt`

A Jinja2 template that takes the yearly and monthly indices and asks an LLM to write 2–3 paragraphs of board-facing commentary:

- Did the year read strong / normal / weak *after wind correction*?
- Are there seasonal patterns (e.g. icing in Q1, summer-heat derate)?
- How does it compare to the bidzone peer average?

Variables wired into the prompt:
- `yearly_index`, `yearly_avg_norm_ratio`, `yearly_hours_used`, `reference_curve_label` (`app/prompts/wind_normalisation.txt:9–15`)
- `monthly_breakdown[]` with month_name, index, hours used (`:19–23`)
- Bidzone peer aggregate context for vs-zone framing (`:25–32`)

The prompt requires the LLM to make it clear that the index is *wind-corrected* — non-technical readers should not assume 105 means a windy year.

## Caller graph

### Orchestrator
- `PerformancePipelineService.run_pipeline` calls `compute_normalisation_from_df` twice (once per reference) inside SAVEPOINT blocks (`app/services/performance_pipeline_service.py:153–165`). A failure in one reference does not abort the other.

### Cron
- Daily 03:00 UTC via `app/cron/pipeline_daily.py`.

### HTTP read APIs
- `GET /api/v1/performance-pipeline/normalisation/{windfarm_id}` — stored monthly/yearly indices (`app/api/v1/endpoints/performance_pipeline.py:238`).
- `GET /api/v1/performance-pipeline/wind-normalisation/{windfarm_id}/monthly-time-series` — formatted for the client-ui bar chart (`:596`).
- `GET /api/v1/performance-pipeline/wind-normalisation/{windfarm_id}/hourly` — on-demand per-hour computation feeding the scatter-plot toggle (`:647–769`). Note: this recomputes every request; for multi-year farms (~28k qualifying hours) latency is 1–2 s. No caching today.

### Frontend
- `energyexe-client-ui/src/components/.../WindNormalisationChart` — monthly index bar chart with 100 reference line.
- `energyexe-client-ui/src/components/.../GenerationScatterChart` — toggles between actual and wind-normalised generation, calling the on-demand hourly endpoint.
- LLM commentary: rendered via the broader commentary infrastructure (`app/services/llm_commentary_service.py` + `windfarm_reports`).

## Status vs spec (post 2026-05-25)

| Spec ask | Status | Notes |
|---|---|---|
| Hourly `norm_ratio = actual / expected` from `overall_clean` curve | ✓ | |
| `wind_speed >= 4 m/s` floor | ✓ | constant `NORM_WIND_MIN_MPS = 4.0` |
| Monthly avg + monthly historical_mean → monthly index | ✓ | |
| **Yearly avg = mean of monthly means** | ✓ (PR #66) | matches spec at `tests/reference/energyexe_pipeline_full.py:910-917` byte-identically on Lutelandet |
| **Yearly historical_mean separate from monthly** | ✓ (PR #66) | |
| Q50 and Q10 references (separate runs) | ✓ | naming reconciled at storage time (q90 → p10) |
| **Constraint hours masked out before this module runs** | ✓ (PR #72) | orchestrator masks active `structural_constraint_flags` from `df_no_over` before Module 4 receives it |
| CSV outputs (`wind_norm_*.csv`, `*.png`) | ✗ | persisted to Postgres; charts rendered client-side |
| Quality/coverage flag when curve has gaps in wind bins | ✗ | hours in gap bins are silently dropped — no warning logged |
| Caching of on-demand hourly endpoint | ✗ | every request recomputes (1–2 s for multi-year farms) |

## File reference

- Service: `app/services/wind_normalisation_service.py`
- LLM prompt: `app/prompts/wind_normalisation.txt`
- Model: `app/models/performance_summary.py:49–52` (Module 4 columns)
- Orchestrator call site: `app/services/performance_pipeline_service.py:153–165`
- Read APIs: `app/api/v1/endpoints/performance_pipeline.py:238`, `:596`, `:647–769`
- Frontend: `energyexe-client-ui/src/components/.../WindNormalisationChart`, `GenerationScatterChart`
