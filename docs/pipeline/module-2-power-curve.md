# Module 2 — Power curve analysis

## Purpose

Build empirical power curves — per-wind-bin statistics (q50, q90, MAD, sample count) — from the cleaned hourly data. These curves are the **single source of truth that Modules 3, 4, 5 and 6 all reference**. They define what "expected output at this wind speed" means for every downstream calculation.

Three curve flavours are produced per windfarm:

| Curve type | Year column | Built from | Used by |
|---|---|---|---|
| `raw` | each year | `df_curve` (all curve-range hours, including outliers) | diagnostic only |
| `capability` | each year | `df_no_over` (overperformance removed) | Module 3 (yearly anomaly reference), Module 5 (yearly residual reference) |
| `overall_clean` | NULL | all-years `df_no_over` pooled | Module 3 (constrained-hour reference once 1b lands), Module 4 (wind-norm baseline), Module 6 (constraint-proxy) |

## Concepts

### Empirical power curve
A turbine ships with a manufacturer's power curve (lab conditions, perfect inflow). In the real world there are wakes, control losses, downtime, sensor calibration drift, blade soiling. The **empirical** power curve is what the turbine actually does in this site, learned from years of data. For each 1 m/s wind bin we ask "what does generation typically look like?" and use the median.

### Why median (q50) not mean
One bad week of telemetry — a stuck inverter reporting 1.5× — can shift a mean by several %. The median sits at the 50th percentile and shrugs off outliers. We want a *typical* hour, not a noise-weighted average.

### Why MAD not standard deviation
MAD (median absolute deviation) is the median of `|x - median(x)|`. Stddev squares every deviation, so a single extreme value blows it up by an order of magnitude. MAD is **robust** — one bad week doesn't poison the spread estimate, which means it doesn't accidentally validate other glitchy weeks.

We use MAD to flag outliers (`> q90 + 1.5 × MAD` is "overperforming"). Using stddev would let one outlier inflate the threshold and admit more outliers — a feedback loop. MAD breaks the loop.

### Why we remove overperformers before computing the reference
Suppose three sensor glitches in 2023 report p_pu ≈ 1.15 in the 12 m/s bin. If we naïvely compute q50/q90 over all hours, those three hours shift the percentiles up. Every downstream "expected output" becomes inflated, and real underperformance gets masked.

Module 2 deals with this in two passes:
1. Build the **raw** curve including outliers.
2. Flag hours where `p_pu > q90_bin + 1.5 × MAD_bin` OR `p_pu > 1.02` (absolute ceiling).
3. Drop them → `df_no_over`.
4. Rebuild the curve from `df_no_over` → that's the **capability** curve, the reference used downstream.

### q50 vs q90 (P50 vs P10)
Industry notation is the inverse of statistical percentile notation:

| Industry term | Meaning | Statistical equivalent | Code column |
|---|---|---|---|
| P50 | median — 50 % of hours exceed this | 50th percentile | `q50` |
| P10 | top decile — only 10 % of hours exceed this | 90th percentile | `q90` |

- **q50 / P50** = the typical output at this wind speed. Used wherever we ask "how did we perform vs typical?"
- **q90 / P10** = the upper-capability bound. Used wherever we ask "how much headroom is there?" (Module 6 commercial proxy) or "are the *best* hours slipping?" (Module 5 Q10 reference).

### Why 30 hours per bin minimum
A 90th-percentile estimate from 10 samples is essentially noise. Asymptotically, percentile estimates from `n=30` samples are stable enough to use as a reference. Bins with `n < 30` are excluded entirely.

## Implementation walkthrough

**File:** `app/services/power_curve_service.py:37–421`

The `PowerCurveService` class has six core methods. Spec sections 2a/2b/2c/2d/2e map onto them as follows:

| Spec section | Method | Lines |
|---|---|---|
| (Module 1) data load | `_load_hourly_data` | `94–181` |
| (Module 1) hard filters | `apply_hard_filters` | `185–204` |
| 2a bin aggregation | `compute_bin_stats` | `208–247` |
| 2b yearly raw | `_build_and_store_curves` (per-year loop) | `294–353` |
| 2c overperformance flag | `flag_overperformance` | `251–290` |
| 2d yearly capability | `_build_and_store_curves` (rerun after dropping flagged rows) | `320–340` |
| 2e overall_clean | `_build_and_store_curves` (all-years aggregation) | `335–353` |
| Storage | `_store_bins` | `355–381` |
| Read API | `get_power_curve` | `391–420` |

### `compute_bin_stats` — the workhorse

```python
df['wind_bin'] = pd.cut(df.wind_speed, bins=np.arange(2, 26), right=False)
stats = df.groupby('wind_bin')['p_pu'].agg(
    q50='median', q90=lambda s: s.quantile(0.9),
    mad=lambda s: (s - s.median()).abs().median(),
    n='count'
)
stats = stats[stats.n >= 30]
```

Pure pandas; static method; fully testable (`tests/test_power_curve_service.py`).

### `flag_overperformance`

```python
overperf = (df.p_pu > df.q90_bin + 1.5 * df.mad_bin) | (df.p_pu > 1.02)
```

Two thresholds:
- Statistical: per-bin (catches bin-specific glitches).
- Absolute: `1.02` (catches anything physically impossible regardless of bin).

Typically removes 1–3 % of curve-range hours.

### Idempotence

`build_power_curves` always calls `_delete_existing_curves(windfarm_id)` first (`app/services/power_curve_service.py:383`). Re-running the pipeline produces the same rows; there's no append behaviour and no versioning.

## DB model

**File:** `app/models/power_curve_bin.py`
**Table:** `power_curve_bins`

| Column | Type | Notes |
|---|---|---|
| `id` | BigInt PK | |
| `windfarm_id` | Int FK → `windfarms.id`, CASCADE delete | |
| `year` | Int, **nullable** | `NULL` is the sentinel for `overall_clean` (all-years pooled) |
| `curve_type` | VARCHAR(20) | `'raw'` \| `'capability'` \| `'overall_clean'` |
| `wind_bin` | Numeric(4,1) | bin left edge: 2.0, 3.0, …, 25.0 |
| `q50_pu` | Numeric(6,5) | median p_pu in this bin |
| `q90_pu` | Numeric(6,5) | 90th-percentile p_pu (P10) |
| `mean_pu` | Numeric(6,5) | arithmetic mean (informational; unused downstream) |
| `mad_pu` | Numeric(6,5) | median absolute deviation |
| `sample_count` | Int | hours in this bin (≥ 30) |
| `created_at`, `updated_at` | TimestampTZ | |

**Unique:** `(windfarm_id, year, curve_type, wind_bin)` — NULL year is treated as distinct.
**Index:** `(windfarm_id, year)` for lookups.

### Typical row counts per windfarm

For a 3-year windfarm with the default 2–25 m/s bin range (23 bins):

| Curve | Years | Bins | Rows |
|---|---|---|---|
| raw | 3 | 23 | ≤ 69 |
| capability | 3 | 23 | ≤ 69 |
| overall_clean | 1 (NULL) | 23 | ≤ 23 |
| **Total** | | | ~115 |

(Sparse bins with `n < 30` are dropped, so actual counts are usually slightly lower.)

## Inputs / outputs

**Inputs:** `df_curve` from Module 1 (one DataFrame, hourly, columns `hour, year, generation_mwh, wind_speed, market_price, p_pu`).

**Outputs:**

1. Rows in `power_curve_bins` (deleted-then-rewritten).
2. Return dict on the orchestrator path:

```python
{
  "years": [2021, 2022, 2023],
  "overperformance_removed_pct": 2.34,
  "bins_stored": 115,
  "raw_rows": 26280,
  "clean_rows": 25890,
  "curve_rows": 23650
}
```

## Caller graph

### Orchestrator
- `PerformancePipelineService.run_pipeline` calls `pcs.build_power_curves(...)` at `app/services/performance_pipeline_service.py:123`. If this fails the whole pipeline fails for that windfarm (everything downstream depends on the curves).

### Cron
- `app/cron/pipeline_daily.py:42–88` — daily at 03:00 UTC.

### HTTP read API
- `POST /api/v1/performance-pipeline/run` — manual full-pipeline trigger (`app/api/v1/endpoints/performance_pipeline.py:39–50`).
- `GET /api/v1/performance-pipeline/power-curves/{windfarm_id}` — reads `power_curve_bins`, optional `year` and `curve_type` query params (default `overall_clean`). Optionally enriches with bidzone peer-average curve for comparison (`app/api/v1/endpoints/performance_pipeline.py:53–156`).

### Frontend consumers
- **Admin UI** — `energyexe-admin-ui/src/components/windfarms/windfarm-detail-page.tsx` renders the power-curve chart and exposes the "Run Pipeline" button.
- **Client UI** — `energyexe-client-ui/src/lib/performance-pipeline-api.ts` fetches the same endpoint for the windfarm detail page; the scatter chart uses the overall_clean q50 curve as its overlay reference.

## Gaps vs spec

| Spec ask | Status | Notes |
|---|---|---|
| Bin aggregation (q50, q90, MAD, n) | ✓ | `compute_bin_stats` |
| `min_samples_per_bin = 30` default | ✓ | `MIN_SAMPLES_PER_BIN` constant |
| Yearly raw + yearly capability + overall_clean | ✓ | All three written to `power_curve_bins` |
| Overperformance threshold `q90 + 1.5×MAD` OR `p_pu > 1.02` | ✓ | `flag_overperformance` |
| `df_curve_clean` (excludes structurally constrained hours) replaces `df_curve` | ✗ | Module 1b not implemented — `overall_clean` is currently built from all non-overperforming hours, including hours that *would* be structurally constrained. |
| CSV outputs (`yearly_power_curves*.csv`, `power_curve_unclean_vs_clean_q50.png`) | ✗ | We store in Postgres and render charts client-side; no CSV/PNG artefacts. |
| Per-bin "quality flag" | ✗ | Bins with `n < 30` are simply dropped, not flagged with a confidence score. |

## File reference

- Service: `app/services/power_curve_service.py:37–421`
- Model: `app/models/power_curve_bin.py`
- Tests: `tests/test_power_curve_service.py`
- Orchestrator call site: `app/services/performance_pipeline_service.py:123`
- Read API: `app/api/v1/endpoints/performance_pipeline.py:53–156`
