# Raw Capacity Data Sources

## Summary of Raw Capacity Data Availability

Based on analysis of the `generation_data_raw` table, here's what each source actually provides:

## 📊 Sources with Raw Capacity Data

### ✅ **ENTSOE** - Has Raw Capacity
- **Field**: `installed_capacity_mw`
- **Coverage**: 100% of records (5,069,770 records)
- **Data Type**: Integer (MW)
- **Example**: 252 MW, 245 MW
- **Also provides**:
  - `actual_generation_output_mw`
  - `actual_consumption_mw`

### ✅ **TAIPOWER** - Has Raw Capacity AND Capacity Factor
- **Capacity Field**: `installed_capacity_mw`
- **Capacity Factor Field**: `capacity_factor`
- **Coverage**: 100% of records (639,056 records)
- **Data Type**: Float (MW for capacity, decimal for CF)
- **Example**: 76.0 MW capacity
- **Note**: Some records have null values for capacity or CF

## ❌ Sources WITHOUT Raw Capacity Data

### **ELEXON**
- **Raw Capacity**: Not provided
- **Total Records**: 22,874,762
- **Must use**: `generation_units` table for capacity

### **NVE** (Norway)
- **Raw Capacity**: Not provided
- **Total Records**: 4,543,020
- **Provides**: Only `generation_mwh` values
- **Must use**: `generation_units` table for capacity

### **ENERGISTYRELSEN** (Denmark)
- **Raw Capacity**: Not provided
- **Data Type**: Monthly aggregated data
- **Must use**: `generation_units` table for capacity

### **EIA** (USA)
- **Raw Capacity**: Not analyzed (not in daily processor)
- **Must use**: `generation_units` table for capacity

### **EEX** (Europe)
- **Raw Capacity**: Not analyzed (not in daily processor)
- **Must use**: `generation_units` table for capacity

## 🔄 Updated Aggregation Logic

The aggregation script has been updated to:

1. **ENTSOE**: Now captures `installed_capacity_mw` from raw data
   - Uses raw capacity as primary source
   - Falls back to `generation_units` table if not available

2. **TAIPOWER**: Already captures both capacity and CF from raw data
   - Uses `installed_capacity_mw` from raw data
   - Uses pre-calculated `capacity_factor` if available

3. **Others**: Continue using `generation_units` table only

## 📈 Impact

- **ENTSOE**: ~5 million records now have access to raw capacity data
- **TAIPOWER**: ~640K records have both raw capacity and CF
- **Total**: ~5.7 million records with raw capacity data available

## Database Fields Usage

| Field | ENTSOE | TAIPOWER | ELEXON | NVE | Others |
|-------|---------|----------|---------|-----|--------|
| `raw_capacity_mw` | ✅ Populated | ✅ Populated | NULL | NULL | NULL |
| `raw_capacity_factor` | NULL | ✅ Populated | NULL | NULL | NULL |
| `capacity_mw` | Raw or Cache | Raw or Cache | Cache | Cache | Cache |
| `capacity_factor` | Calculated | Raw or Calc | Calculated | Calculated | Calculated |

## Key Insights

1. **ENTSOE surprise**: We discovered ENTSOE provides capacity data in 100% of records
2. **Two sources with raw capacity**: ENTSOE and TAIPOWER (5.7M records total)
3. **TAIPOWER unique**: Only source with pre-calculated capacity factors
4. **ELEXON gap**: Despite 22M records, provides no capacity data
5. **Fallback strategy**: All sources can fall back to `generation_units` table