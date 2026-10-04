# ELEXON BST Fix Log

Summary of all fixes applied to ELEXON BMU metered volume data to resolve BST timezone issues and data gaps.

**Date range of affected data:** January 2019 - December 2025
**Last updated:** February 6, 2026

---

## Fix 1: Settlement Date Missing from ELEXON API Imports

**File:** `app/services/elexon_client.py` (line ~175)

**Problem:** `settlement_date` was not included in `columns_to_keep`, so it was silently dropped during API imports. The ELEXON API returns `settlementDate` which gets renamed to `settlement_date`, but was then discarded before storage.

**Fix:** Added `settlement_date` to the `columns_to_keep` list:
```python
columns_to_keep = ["timestamp", "bm_unit", "value", "unit", "settlement_period", "settlement_date"]
```

**Impact:** Without `settlement_date` stored in raw records, the aggregation pipeline could not correctly derive UTC hours during BST. The `period_start` field alone is insufficient because it reflects a pre-computed UTC conversion that doesn't account for the settlement date + period relationship correctly during BST.

---

## Fix 2: BST Timezone Handling in Daily Aggregation

**File:** `scripts/seeds/aggregate_generation_data/process_generation_data_daily.py`

**Problem:** `transform_elexon()` used `period_start` directly for the UTC hour, which was wrong during BST months (1-hour offset). Settlement period 1 starts at 00:00 UK local time, which is 23:00 UTC the previous day during BST — but `period_start` was stored as if UK local time equaled UTC.

**Fix:**
1. Changed `transform_elexon()` to use `settlement_date` + `settlement_period` to derive the correct UTC hour via `_calculate_correct_elexon_hour()`
2. Extended the query window by 1 hour for ELEXON data to capture records that straddle the UTC day boundary during BST

**Impact:** All hourly aggregated data for BST months (late March through late October) was shifted by 1 hour. This affected ~6 months of data per year across all ELEXON BMUs.

---

## Fix 3: Historical BST Data Re-import (2020-2025)

**Scripts used:** `fix_elexon_bst.py`, `fix_2022/` through `fix_2025/`

**Problem:** Historical CSV-imported raw data had incorrect timezone handling. Settlement periods were converted to UTC without accounting for BST (UTC+1), causing data to be shifted by 1 hour during summer months.

**Fix applied per year:**
1. Deleted old CSV-imported records for the BST-affected date ranges
2. Re-imported with correct timezone conversion using `ZoneInfo('Europe/London')`
3. Deduplicated records on DST transition days (spring forward: 46 periods, fall back: 50 periods)

**Scope:**
- 2020-2021: Bulk fix from CSV re-import (5,156,760 raw records for 147 BMUs, 1,176 DST duplicates removed)
- 2022-2025: Year-by-year fix scripts

**Impact:** All raw ELEXON data for BST months had incorrect UTC timestamps. After fix, monthly totals match official ELEXON figures with only expected DST-day deviations (~16-18 MWh per transition day).

---

## Fix 4: Backfill Missing Settlement Dates

**Script used:** `fix_settlement_dates.py`

**Problem:** Existing raw records lacked `settlement_date` in their JSONB `data` column because of the bug described in Fix 1. These records were imported before the `columns_to_keep` fix.

**Fix:** Derived `settlement_date` from `period_start` and `settlement_period` using UK timezone math:
1. Converted `period_start` UTC timestamp to UK local time (`Europe/London`)
2. Calculated the settlement date accounting for settlement periods that span midnight
3. Stored the derived `settlement_date` back into the JSONB `data` column

**Impact:** Without this backfill, the aggregation pipeline (Fix 2) would fall back to using `period_start` for these records, negating the BST correction.

---

## Fix 5: Fill Small Data Gaps in 2025

**Script used:** `fill_small_gaps_2025.py`

**Problem:** Some BMUs had small gaps (fewer than 100 missing settlement periods) in 2025 data, likely caused by API fetch failures or transient issues.

**Fix:**
1. Identified BMUs with small gaps using settlement period analysis
2. Fetched missing data from the ELEXON B1610 API
3. Handled DST transitions correctly (March 30: 46 periods, October 26: 50 periods)

**Impact:** Filled data gaps to ensure continuous hourly aggregated data for 2025.

---

## Fix 6: 2019 BST Data Fix (Extending Fix 3 to 2019)

**Script used:** `scripts/seeds/raw_generation_data/elexon/fix_2019_bst.py`

**Problem:** 2019 raw CSV-imported data had the same BST offset bug as 2020-2025 (Fix 3), but was never corrected. Raw `period_start` timestamps were stored as if UK local time equaled UTC, causing a 1-hour shift during BST months (March 31 - October 27, 2019). Additionally, `metered_mwh` was NULL for 97% of aggregated records.

**Fix applied:**
1. Recalculated `period_start` and `period_end` for all 2,545,440 raw records using PostgreSQL `make_timestamptz()` with `'Europe/London'` timezone, deriving correct UTC timestamps from `settlement_date` + `settlement_period` in JSONB
2. Re-aggregated all 12 months using `reprocess_year_parallel.py --year 2019 --workers 4`

**Scope:**
- All 12 months processed (GMT months had no effective change, BST months shifted by -1 hour)
- 2,545,440 raw records updated
- 1,294,198 aggregated records regenerated

**Validation Results (2019):**
| Month | Raw CSV MWh | Agg MWh | Deviation | Status |
|-------|------------|---------|-----------|--------|
| Jan | 3,735,163 | 3,719,068 | 0.4% | OK |
| Feb | 3,835,910 | 3,855,950 | 0.5% | OK |
| Mar | 4,487,887 | 4,509,025 | 0.5% | OK |
| Apr | 3,178,985 | 3,184,936 | 0.2% | OK |
| May | 2,134,487 | 2,120,441 | 0.7% | OK |
| Jun | 2,805,906 | 2,811,229 | 0.2% | OK |
| Jul | 2,391,157 | 2,384,685 | 0.3% | OK |
| Aug | 3,533,548 | 3,538,776 | 0.1% | OK |
| Sep | 3,710,317 | 3,719,466 | 0.2% | OK |
| Oct | 4,390,726 | 4,420,471 | 0.7% | OK |
| Nov | 3,776,577 | 3,789,562 | 0.3% | OK |
| Dec | 5,282,899 | 5,318,615 | 0.7% | OK |

**Year total:** 43,372,224 MWh (metered), 1,294,198 records, 100% metered_mwh coverage.

---

## Known Remaining Issues

### 1. T_AFTOW-1 Missing Settlement Periods on Oct 26, 2025
- Missing SP 49-50 on October 26, 2025 (DST fall-back day)
- Estimated impact: ~53 MWh
- Likely cause: API did not return data for the extra settlement periods on the 25-hour day

### 2. BOAV Curtailment Data Without Aggregated Records
- 2,902 hour/unit combinations have bid-offer acceptance volume (BOAV) curtailment data but no corresponding aggregated `generation_data` record
- 65% of these are at 23:00 UTC, which is the day boundary in UK time
- Root cause: aggregation query window may not fully capture records at the UTC day boundary during BST

### 3. Offshore Wind Farm start_date Constraints
- Some offshore BMUs (T_HOWAO, T_EAAO, T_TKNWW, T_MOWEO) have raw data from before their `generation_units.start_date`
- Aggregation skips data before `start_date`, leaving gaps in early months
- Fix: update `start_date` in `generation_units` table to match actual raw data availability

---

## Validation Results

### Monthly Totals (2020, after all fixes)
| Month | Status | Notes |
|-------|--------|-------|
| Jan 2020 | EXACT | Matches expected values |
| Feb 2020 | EXACT | Leap year, 696 hours |
| Mar 2020 | ~DST | -16 MWh deviation (BST starts Mar 29, 743 hours) |
| Apr-Sep 2020 | EXACT | All match expected values |
| Oct 2020 | ~DST | -18 MWh deviation (BST ends Oct 25, 743 hours) |
| Nov-Dec 2020 | EXACT | All match expected values |

DST deviations are expected: clock-change days have 23 or 25 hours, but deduplication of the repeated UTC hour means 743 effective hours in the affected month.

### Key Validation Points
- Use `metered_mwh` (not `generation_mwh`) when comparing against official ELEXON figures
- `generation_mwh` is capacity-factor-based and shows ~2-5% inflation
- Monthly validation against Excel reference data: 85.3% exact match, 7.4% within 5-15%

---

## Operational Scripts (Retained)

| Script | Purpose |
|--------|---------|
| `scripts/seeds/elexon_processor.py` | Unified ELEXON processor (import, aggregate, verify) |
| `scripts/seeds/aggregate_generation_data/reprocess_year_parallel.py` | Parallel re-aggregation by month |
| `scripts/seeds/raw_generation_data/elexon/reaggregate_windfarm.py` | Per-windfarm re-aggregation utility |
| `scripts/seeds/raw_generation_data/elexon/verify_fixes.py` | Data validation/verification tool |
| `scripts/seeds/raw_generation_data/elexon/generate_single_bmu_report.py` | BMU validation reporting |
| `scripts/seeds/raw_generation_data/elexon/fix_2019_bst.py` | 2019 BST period_start fix (Fix 6) |
| `tests/test_elexon_curtailment_data_integrity.py` | Curtailment data integrity test |

---

## Database Reference

### Key Tables
- `generation_data_raw` — Raw 30-minute ELEXON BMU metered volume data (JSONB `data` column includes `settlement_date`, `settlement_period`)
- `generation_data` — Hourly aggregated generation data (`metered_mwh` for actual, `generation_mwh` for capacity-based)
- `generation_units` — BMU metadata (check `start_date` constraints)
- `import_job_executions` — Job tracking and logging

### Useful Queries
```sql
-- Monthly totals for validation
SELECT
    date_trunc('month', hour) as month,
    SUM(metered_mwh) as total_mwh,
    COUNT(*) as records
FROM generation_data
WHERE source = 'ELEXON'
  AND hour >= '2020-01-01' AND hour < '2026-01-01'
GROUP BY date_trunc('month', hour)
ORDER BY month;

-- Check for missing settlement_date in raw data
SELECT COUNT(*)
FROM generation_data_raw
WHERE source = 'ELEXON'
  AND (data->>'settlement_date' IS NULL OR data->>'settlement_date' = 'None');
```
