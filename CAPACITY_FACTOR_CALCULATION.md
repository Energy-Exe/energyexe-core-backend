# Capacity Factor Calculation Process

## Overview
The capacity factor is calculated during the data aggregation process in `process_generation_data_daily.py`. It represents the ratio of actual generation to maximum possible generation.

## Formula
```
Capacity Factor = Generation (MWh) / Capacity (MW)
```

Since we're working with hourly data:
- Generation in MWh for 1 hour
- Capacity in MW
- Therefore: CF = MWh / MW (dimensionally correct as 1 hour cancels out)

## Implementation (Updated)

### Standard Calculation
```python
# capacity_mw always comes from generation_units table
if record.capacity_mw and record.capacity_mw > 0:
    calculated_cf = record.generation_mwh / record.capacity_mw
    capacity_factor = min(calculated_cf, 9.9999)  # Cap at 9.9999 for database NUMERIC(5,4)
```

### Raw Values Storage
```python
# Raw values from source data stored separately
raw_capacity_mw = record.metadata.get('raw_capacity_mw')
raw_capacity_factor = record.metadata.get('capacity_factor')  # Only TAIPOWER provides this
```

## Source-Specific Capacity Data

### 1. ENTSOE
- **Capacity Source for calculation**: `generation_units` table ONLY
- **Raw capacity available**: YES - stored in `raw_capacity_mw`
- **Data Resolution**: 15-min or hourly
- **Process**:
  - Calculation uses: `generation_units_cache[unit_key]`
  - Raw value stored: `installed_capacity_mw` from raw data

### 2. ELEXON
- **Capacity Source for calculation**: `generation_units` table ONLY
- **Raw capacity available**: NO
- **Data Resolution**: 30-min periods
- **Process**:
  - Calculation uses: `generation_units_cache[unit_key]`
  - Raw value: None

### 3. TAIPOWER
- **Capacity Source for calculation**: `generation_units` table ONLY
- **Raw capacity available**: YES - stored in `raw_capacity_mw`
- **Raw CF available**: YES - stored in `raw_capacity_factor`
- **Data Resolution**: Hourly
- **Process**:
  - Calculation uses: `generation_units_cache[unit_key]`
  - Raw capacity stored: `installed_capacity_mw` from raw data
  - Raw CF stored: `capacity_factor` from raw data

### 4. NVE
- **Capacity Source**: `generation_units` table via cache
- **Data Resolution**: Hourly (already in MWh)
- **Process**:
  - Gets capacity from `generation_units_cache[unit_key]`
  - No capacity in raw data
  - Lines 401-402

### 5. ENERGISTYRELSEN
- **Capacity Source**: Not processed in daily aggregation
- **Data Resolution**: Monthly totals
- **Note**: Skipped in daily processing (line 424)

### 6. EIA
- **Capacity Source**: `generation_units` table
- **Note**: Not shown in the daily processor, likely processed separately

### 7. EEX
- **Capacity Source**: `generation_units` table
- **Note**: Not shown in the daily processor, likely processed separately

## Data Quality Considerations

### Capacity Factor Capping
- Maximum value capped at 9.9999 to fit database constraint
- This allows for capacity factors > 1.0 (when actual generation exceeds nameplate capacity)
- Common reasons for CF > 1.0:
  - Wind turbines performing above rated capacity in optimal conditions
  - Measurement at different points (e.g., before/after transformer losses)
  - Capacity upratings not reflected in database

### Missing Capacity Data
- If `capacity_mw` is NULL, no capacity factor is calculated
- Capacity factor field remains NULL in database
- Does not prevent data storage, just missing CF metric

## Summary Table (Updated)

| Source | Capacity for CF Calc | Raw Capacity Available | Raw CF Available | Special Features |
|--------|---------------------|------------------------|------------------|------------------|
| ENTSOE | generation_units | ✅ YES | ❌ NO | 5M records with raw capacity |
| ELEXON | generation_units | ❌ NO | ❌ NO | No raw data |
| TAIPOWER | generation_units | ✅ YES | ✅ YES | Pre-calculated CF available |
| NVE | generation_units | ❌ NO | ❌ NO | Data already in MWh |
| ENERGISTYRELSEN | generation_units | ❌ NO | ❌ NO | Monthly aggregated |
| EIA | generation_units | ❌ NO | ❌ NO | Not in daily processor |
| EEX | generation_units | ❌ NO | ❌ NO | Not in daily processor |

## Key Insights

1. **Separation of concerns**:
   - `capacity_mw` and `capacity_factor` fields ALWAYS use `generation_units` data for consistency
   - `raw_capacity_mw` and `raw_capacity_factor` store source-provided values for comparison

2. **ENTSOE discovery**: Provides capacity data in 100% of records (5M+ records)

3. **TAIPOWER unique**: Only source providing pre-calculated capacity factors

4. **Data validation**: Can now compare raw vs calculated values for quality checks

5. **Database fields usage**:
   - `capacity_mw`: Always from `generation_units` table
   - `capacity_factor`: Always calculated from `generation_units` capacity
   - `raw_capacity_mw`: Source-provided capacity (ENTSOE, TAIPOWER)
   - `raw_capacity_factor`: Source-provided CF (TAIPOWER only)