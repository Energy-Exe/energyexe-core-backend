# Taipower Data Processing

## Source Files
- **Location**: `data/*.xlsx` (22 Excel files)
- **Format**: Taiwan wind farm hourly generation data
- **Period**: 2020-2025

## Data Mapping

### File Name → Unit Code Mapping
```python
'Chang Kong.xlsx'        → '彰工' (database code)
'Changfang-Xidao.xlsx'   → '芳二風'
'Hai Neng.xlsx'          → '海能'
# ... 22 total mappings
```

### Excel Columns → Database Fields
```
Timestamp                → period_start
Installed capacity(MW)   → stored in data JSONB
Power generation(MWh)    → value_extracted
Capacity factor(%)       → stored in data JSONB
```

### Processing Logic
1. **Map filename** to Chinese unit code (e.g., 'Chang Kong' → '彰工')
2. **Check unit exists** in `generation_units` table with `source='Taipower'`
3. **Parse timestamps** and generation values
4. **Store** in `generation_data_raw` table:
   - `period_start`: Timestamp from Excel
   - `period_end`: period_start + 1 hour
   - `identifier`: Chinese unit code
   - `value_extracted`: Generation in MWh
   - `data`: JSONB with capacity, capacity factor, unit details

## Key Features
- English filename → Chinese code mapping
- Auto-cleanup of existing data before import
- Handles missing/invalid values gracefully
- Only processes files with configured units