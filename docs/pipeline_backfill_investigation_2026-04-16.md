# Performance Pipeline Backfill — Investigation

**Date:** 2026-04-16
**Backfill run:** 1,468 windfarms processed in ~8.6 hours
**Log file:** `/tmp/backfill_pipeline.log` (process PID 16818)

> **Status update (2026-04-16, later same day):** Root cause for the 131 phantom-OK /
> autoflush / unknown-error failures identified as a single bug in `_load_hourly_data`
> (multi-unit / multi-source SQL join producing duplicate hours → `UniqueViolationError`
> → silently-aborted Postgres transaction). Fixed, all 131 windfarms re-processed
> successfully. DB now has 229 + 131 = **360 windfarms with full pipeline data**.
> Remaining failures (1,168 ERA5 NaN, 71 insufficient-data) are data-quality issues
> outside the pipeline's control. See `Recovery Plan` at bottom for the actual changes
> landed.

---

## Executive Summary

| Status | Count | % | Action Required |
|--------|------:|--:|-----------------|
| ✅ Successfully processed (data in DB) | **98** | 6.7% | None — usable now |
| ⚠️ "Phantom OK" (logged OK but no data in DB) | **121** | 8.2% | **Retry** — likely commit silently failed |
| ❌ NaN wind data | **1,168** | 79.6% | Fix ERA5 weather import (data engineering) |
| ❌ Insufficient data for curves | **71** | 4.8% | Wait — needs more time/data |
| ❌ Autoflush bug | **9** | 0.6% | Fix code, then retry |
| ❌ Other error | **1** | 0.1% | Investigate windfarm 7190 |
| **Total** | **1,468** | 100% | |

**Actually queryable now:** 98 windfarms. **Recoverable with quick fixes:** 130 more (121 retry + 9 autoflush). **Blocked on data quality:** 1,239 (mostly ERA5 NaN).

---

## DB State After Backfill

| Table | Row Count |
|-------|----------:|
| `power_curve_bins` | 27,113 |
| `performance_anomalies` | 505,694 |
| `performance_summaries` | 9,570 |
| `degradation_results` | 188 |
| `opportunities` | 404 |

**Unique windfarms with curves:** 98 (Norway: 61, UK: 18, Denmark: 7, Taiwan: 6, Belgium: 5, Netherlands: 1)

**Opportunities by severity:** CONFIRMED: 7, INDICATIVE: 85, WATCH: 312
**Opportunities by schema:** OPS-01: 67, OPS-02: 224, OPS-03: 67, MKT-03: 46

---

## Failure Category 1: NaN Wind Data — 1,168 windfarms (79.6%)

### Root Cause

`weather_data.wind_speed_100m` column contains **98,364,288 rows** with NaN values (not NULL — actual NaN floats). The SQL filter `wind_speed_100m IS NOT NULL` doesn't catch NaN, so they pass through to pandas which then filters them out, leaving zero usable hours.

### Pattern

NaN rows by year (showing systemic ERA5 import problem):

| Period | NaN rows/year | Status |
|--------|--------------:|--------|
| 2000-2012 | 30K - 600K | Normal (small) |
| 2013-2016 | 1.5M - 3M | Transition (deteriorating) |
| **2017-2025** | **~9.5M per year** | **Broken (most modern data)** |

Most operational windfarms generate data 2017+, so they hit the NaN window directly.

### Country Breakdown

| Country | Affected windfarms |
|---------|-------------------:|
| United States of America | 1,090 |
| Denmark | 74 |
| Taiwan | 4 |

**Action:** Investigate the ERA5 weather import pipeline. Why does it write NaN instead of skipping/erroring? Re-import affected years from Copernicus.

### Sample Affected Windfarms

| ID | Name |
|----|------|
| 7234 | Changfang & Xidao 1 |
| 7237 | Formosa 2 |
| 7361 | Block Island |
| 7415 | South Fork |
| 7435 | Hsinyuan |

Full list: see `/tmp/nan_wind_ids.txt` (in temp; regenerate from log if needed)

---

## Failure Category 2: "Phantom OK" — 121 windfarms (8.2%)

### Root Cause

These windfarms reported `OK` in the backfill log with full results (e.g., "[7224] OK (75.2s): 20 years, 739 bins, 1715 underperf hrs") but **none of the data was committed to the DB**. All 4 tables show 0 rows for these windfarm_ids.

### Pattern

All 121 affected IDs are in the range **7224-7424** — the same window where the autoflush errors occurred (9 errors on IDs 7236, 7264, 7269, 7281, 7314, 7326, 7328, 7331, 7430).

### Hypothesis

When autoflush errors occurred mid-batch, the connection pool may have returned poisoned connections to subsequent windfarms. The `db.commit()` call appeared to succeed but was actually rolled back at the asyncpg level. Each windfarm got its own SQLAlchemy session (`async with factory() as db`), but underlying asyncpg connections may have been reused from the pool with corrupted state.

### Country Breakdown

| Country | Affected |
|---------|---------:|
| United Kingdom | 112 |
| Belgium | 3 |
| France | 3 |
| Norway | 1 |
| Germany | 1 |
| Denmark | 1 |

### Action

**Retry these 121 windfarms.** They have valid data and the pipeline ran successfully — just need their results to actually persist.

### Sample Affected Windfarms

| ID | Name | Country |
|----|------|---------|
| 7224 | Valsneset | Norway |
| 7243 | A'Chruach 1 | United Kingdom |
| 7244 | Afton | United Kingdom |
| ... | (118 more in UK/EU range 7245-7424) | ... |

Full list: see `/tmp/missing_data_ids.txt` and `/tmp/missing_data_details.txt`.

---

## Failure Category 3: No Yearly Curves — 71 windfarms (4.8%)

### Root Cause

Data passes plausibility filters (wind 0-40 m/s, p_pu -0.05 to 1.20) but no wind speed bin gets the minimum 30 samples needed to produce stable percentile statistics.

### Causes

- **Very new windfarms** — only weeks/months of data
- **Sparse data** — many gaps in generation hours
- **Narrow wind range** — wind speeds clustered (e.g., always 6-7 m/s)

### Country Breakdown

| Country | Affected |
|---------|---------:|
| Denmark | 39 |
| United States of America | 26 |
| United Kingdom | 6 |

### Action

**No code fix needed.** As more data accumulates over time, these windfarms will eventually qualify. Re-run the pipeline periodically.

Could also consider lowering `MIN_SAMPLES_PER_BIN` from 30 → 10-15 for early-stage windfarms (trade-off: less stable curves).

---

## Failure Category 4: Autoflush Bug — 9 windfarms (0.6%)

### Root Cause

SQLAlchemy session error: when an insert fails (likely a unique constraint violation on `performance_anomalies`), the next SELECT triggers an autoflush which finds a pending error and aborts. The session becomes unusable.

```
This Session's transaction has been rolled back due to a previous exception
during flush. Original exception was: (raised as a result of Query-invoked
autoflush; consider using a session.no_autoflush block if this flush is
occurring prematurely)
```

### Affected Windfarms

| ID | Name | Country |
|----|------|---------|
| 7236 | Formosa 1 | Taiwan |
| 7264 | Broken Cross | United Kingdom |
| 7269 | Causeymire | United Kingdom |
| 7281 | Cumberhead | United Kingdom |
| 7314 | Kirk Hill | United Kingdom |
| 7326 | Paul's Hill | United Kingdom |
| 7328 | Pines Burn | United Kingdom |
| 7331 | Rothes 1 | United Kingdom |
| 7430 | Sihu | Taiwan |

### Action

1. **Fix code** in `performance_anomaly_service.py` — wrap problematic SELECT inside `with self.db.no_autoflush:` block, OR use explicit `await self.db.flush()` before SELECT to surface errors cleanly.
2. **Retry** these 9 windfarms.

### Code location

Likely in `_store_anomalies_bulk()` or `_store_summaries()` — when inserting summaries while previous anomaly inserts may have pending issues.

---

## Failure Category 5: Other — 1 windfarm (0.1%)

| ID | Name | Error |
|----|------|-------|
| 7190 | (unknown) | Empty error message after 62.8s — needs investigation |

### Action

Run pipeline manually for windfarm 7190 with verbose logging to capture actual error.

---

## Recovery Plan (Quick Wins)

Total recoverable with code fixes + retries: **130 windfarms** (121 + 9)

### Step 1 — Fix autoflush bug (in code)
```python
# In performance_anomaly_service.py, wrap problematic queries:
async with self.db.no_autoflush:
    result = await self.db.execute(...)
```

### Step 2 — Retry script
```bash
# Combine missing-data IDs + autoflush IDs
cat /tmp/missing_data_ids.txt /tmp/autoflush_ids.txt | sort -u > /tmp/retry_ids.txt
poetry run python scripts/backfill_pipeline.py --windfarm-ids $(paste -sd' ' /tmp/retry_ids.txt)
```

Estimated time: 130 windfarms × 100s = **~3.6 hours**

### Step 3 — Then re-run opportunity detection on retried farms (already covered by retry script).

---

## Data Quality Issues (Outside Pipeline Scope)

These need separate investigation by the data team:

1. **ERA5 NaN values** — 98M rows of `wind_speed_100m = NaN` in `weather_data`. The ERA5 import process is silently writing NaN instead of failing or skipping. Affects 1,168 windfarms (79% of failures).

2. **Aggregated data in hourly column** — Some windfarms have monthly/daily totals stored in `generation_data` (with hourly `hour` column). Examples:
   - **Block Island (7361)**: 30 MW capacity, single record per day showing 13,764 MWh = 458× rated capacity
   - **25 Mile Creek (7436)**: 250 MW capacity, single record per day showing 117,044 MWh = 468× rated capacity
   - **South Fork (7415)**: 132 MW, only 15 records, all >24,000 MWh
   These get correctly filtered by plausibility checks but indicate broken upstream data.

---

## Files for Reference

| File | Description |
|------|-------------|
| `/tmp/backfill_pipeline.log` | Full backfill log (very large with warnings) |
| `/tmp/succeeded_full.csv` | 98 successful windfarms (id, name, country) |
| `/tmp/missing_data_ids.txt` | 121 phantom-OK windfarm IDs to retry |
| `/tmp/missing_data_details.txt` | Same with names + countries |
| `/tmp/nan_wind_ids.txt` | 1,168 NaN-affected windfarm IDs |
| `/tmp/no_curves_ids.txt` | 71 windfarms without enough data |
| `/tmp/autoflush_ids.txt` | 9 windfarms hit by autoflush bug |
| `/tmp/autoflush_details.txt` | Same with names + countries |
| `/tmp/other_error_ids.txt` | 1 unknown error (windfarm 7190) |

Note: `/tmp` files are ephemeral — regenerate by re-parsing the log if needed.

---

## Summary of Next Steps

| Priority | Action | Recovers | Effort |
|----------|--------|---------:|--------|
| **P1** | Fix autoflush bug + retry 130 windfarms | 130 | ~4h |
| **P1** | Investigate/fix ERA5 NaN write bug | 1,168 | Days (data eng) |
| **P2** | Investigate aggregated data in hourly table | ~5-10 | Hours |
| **P3** | Investigate windfarm 7190 specifically | 1 | 30 min |
| **P3** | Lower min_samples_per_bin for new windfarms | ~30 of 71 | 1h |
