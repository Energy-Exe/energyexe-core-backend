# NVE Data Processing

## Source Files
- **Location**: `data/vindprod2002-2024_kraftverk.xlsx`
- **Format**: Norwegian wind farm hourly data (pivoted)
- **Period**: 2002-2024

## Data Structure - Pivoted Format
```
             | Wind Farm 1 | Wind Farm 2 | Wind Farm 3 | ...
Row 1 (codes)| 1234       | 5678        | 9012        | ...
Row 2 (names)| Hitra      | Smola       | Fakken      | ...
2002-01-01   | 12.5       | 8.3         | 15.2        | ...
2002-01-02   | 14.2       | 9.1         | 16.8        | ...
```

## Data Mapping

### Processing Logic
1. **Read column headers** (row 1) to get wind farm codes
2. **Match codes** against `generation_units` table with `source='NVE'`
3. **Unpivot data** - transform from wide to long format:
   - Each column becomes multiple rows
   - Timestamp + Wind Farm = one record
4. **Store** in `generation_data_raw` table:
   - `period_start`: Timestamp from first column
   - `period_end`: period_start + 1 hour
   - `identifier`: Wind farm code (e.g., '1234')
   - `value_extracted`: Generation in MWh
   - `data`: JSONB with unit name, code, timestamp

## Key Features
- Handles pivoted/wide format data
- Maps 71 wind farm columns to individual records
- Filters to only configured units (63 of 71)
- Processes ~200K timestamps × configured units = millions of records