# ELEXON Aggregation Logic Update

## Date: 2025-09-26

## Change Summary
Updated ELEXON data aggregation from **averaging** to **summing** half-hourly settlement periods.

## Background
ELEXON provides generation data in 30-minute settlement periods. Each value represents the actual MWh generated during that 30-minute period.

## Previous Logic (Incorrect)
```python
# Averaging approach - INCORRECT
generation_mw = np.mean([float(r.value_extracted) for r in records])
# This incorrectly assumed values were MW readings, not MWh totals
```

## New Logic (Correct)
```python
# Summing approach - CORRECT
generation_mwh = sum([float(r.value_extracted) for r in records])
# Correctly sums the MWh from each 30-min period to get hourly total
```

## Example
For an hour with two 30-minute periods:
- Period 1 (00:00-00:30): 10 MWh
- Period 2 (00:30-01:00): 12 MWh

**Old (averaging)**: (10 + 12) / 2 = 11 MWh for the hour ❌
**New (summing)**: 10 + 12 = 22 MWh for the hour ✅

## Impact
This change affects all ELEXON data aggregation. Historical data needs to be reprocessed to correct the values.

## Reprocessing Commands

### Full ELEXON data reprocessing:
```bash
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --start 2013-04-01 \
  --end 2024-02-14 \
  --source ELEXON
```

### Process in batches (recommended for large datasets):
```bash
# 2013-2015
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --start 2013-04-01 --end 2015-12-31 --source ELEXON

# 2016-2019
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --start 2016-01-01 --end 2019-12-31 --source ELEXON

# 2020-2024
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --start 2020-01-01 --end 2024-02-14 --source ELEXON
```

## Verification
After reprocessing, generation values for ELEXON units should approximately double, as we're now correctly summing instead of averaging.

## Files Modified
1. `/scripts/seeds/aggregate_generation_data/process_generation_data_daily.py` - Updated `transform_elexon()` method
2. `/scripts/seeds/aggregate_generation_data/README.md` - Updated documentation

## Notes
- This change only affects ELEXON data
- Other sources (ENTSOE, NVE, TAIPOWER) remain unchanged
- The change is backward compatible - the script structure remains the same