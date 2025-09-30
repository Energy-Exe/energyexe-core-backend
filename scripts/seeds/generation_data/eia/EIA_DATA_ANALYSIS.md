# EIA Data Analysis & Import Strategy

## Data Structure Overview

### Format Characteristics
- **Source**: EIA-923 Monthly Generation and Fuel Consumption Reports
- **File Format**: Excel (.xlsx), one file per year (2001-2025)
- **Data Layout**: Wide/pivoted format (months as columns)
- **Coverage**: 25 years of data (2001-2025 July)
- **Granularity**: Monthly generation data per plant and fuel type

### File Structure

```
Header Rows (skip first 5):
- Row 1: Title
- Row 2: Source info
- Row 3-4: Empty
- Row 5: Column headers

Columns (17 total):
0. Plant Id             - Unique plant identifier
1. Plant Name           - Human-readable plant name
2. Reported Fuel Type   - Fuel type code (WND=Wind, NG=Natural Gas, etc.)
3-14. Netgen [Month]    - Monthly generation in MWh (12 columns)
15. Net Generation      - Annual total in MWh
16. YEAR                - Year of the data
```

### Sample Data

```
Plant Id | Plant Name | Fuel Type | Netgen Jan | Netgen Feb | ... | Netgen Dec | Net Total | YEAR
---------|------------|-----------|------------|------------|-----|------------|-----------|-----
1        | Sand Point | DFO       | 317.488    | 285.899    | ... | 282.341    | 3616.0    | 2024
1        | Sand Point | WND       | 0.000      | 0.000      | ... | 0.000      | 0.0       | 2024
2        | Bankhead   | WAT       | 17936.000  | 23758.000  | ... | 9701.000   | 107847.0  | 2024
```

## Key Differences from Other Sources

### 1. **ELEXON** (UK)
- **Format**: CSV, long format
- **Granularity**: Half-hourly (30-min periods)
- **Identifier**: BMU ID (Balancing Mechanism Unit)
- **Time**: Settlement periods (1-48 per day)
- **Size**: ~100M+ records

### 2. **ENTSOE** (Europe)
- **Format**: Excel, long format
- **Granularity**: Hourly/15-minute
- **Identifier**: Generation Unit Code
- **Time**: DateTime UTC with resolution code
- **Size**: ~13M records (129 monthly files)

### 3. **Taipower** (Taiwan)
- **Format**: Excel, long format
- **Granularity**: Hourly
- **Identifier**: Chinese wind farm names
- **Time**: Timestamp per hour
- **Size**: ~500K records (22 files)

### 4. **NVE** (Norway)
- **Format**: Excel, **wide/pivoted** format ✓ Similar to EIA
- **Granularity**: Hourly
- **Identifier**: Numeric wind farm codes
- **Time**: Timestamp rows × wind farm columns
- **Size**: ~200K rows × 71 columns = 14M+ data points

### 5. **Energistyrelsen** (Denmark)
- **Format**: Excel, **wide/pivoted** format ✓ Similar to EIA
- **Granularity**: **Monthly** ✓ Same as EIA
- **Identifier**: GSRN codes
- **Time**: Turbine rows × month columns
- **Size**: ~10K rows × 276 months = 2.7M+ data points

## Similarity Analysis

### Most Similar: **Energistyrelsen**
Both EIA and Energistyrelsen share:
1. ✅ Wide/pivoted format (months as columns)
2. ✅ Monthly granularity (not hourly)
3. ✅ Multiple rows per entity (EIA: plant+fuel, Energistyrelsen: turbines)
4. ✅ MWh/kWh units
5. ✅ Multi-year files

### Also Similar: **NVE**
1. ✅ Wide/pivoted format
2. ❌ Hourly (not monthly)
3. ✅ Multiple files covering years
4. ✅ MWh units

## Import Strategy

### Recommended Approach: **Adapt Energistyrelsen Pattern**

The Energistyrelsen import script is the best template because:

1. **Pivoted Format Handling**: Already handles month columns
2. **Monthly Granularity**: Works with monthly data
3. **Data Transformation**: Melts wide format into long format
4. **Date Parsing**: Converts month columns to timestamps
5. **Batch Processing**: Handles large pivoted datasets efficiently

### Key Adaptations Needed

#### 1. **Header Handling**
```python
# Energistyrelsen: skiprows=3
# EIA: skiprows=5 (more header rows)
df = pd.read_excel(file_path, skiprows=5)
```

#### 2. **Month Column Pattern**
```python
# Energistyrelsen: Month columns like "2002-01", "2002-02"
# EIA: Month columns like "Netgen\nJanuary", "Netgen\nFebruary"

# Extract month columns
month_columns = [col for col in df.columns if 'Netgen\n' in str(col)]
```

#### 3. **Identifier Strategy**
```python
# EIA Plant ID maps directly to generation_unit.code
# We only care about wind data (fuel_type = 'WND')
# Filter for wind data only
df = df[df['Reported\nFuel Type Code'] == 'WND']

# Identifier is simply the Plant ID (as string)
df['identifier'] = df['Plant Id'].astype(str)
```

#### 4. **Data Unpivoting**
```python
# Melt from wide to long format
melted = df.melt(
    id_vars=['Plant Id', 'Plant Name', 'Reported\nFuel Type Code', 'YEAR'],
    value_vars=month_columns,
    var_name='month_col',
    value_name='generation_mwh'
)

# Parse month from column name
melted['month'] = melted['month_col'].str.replace('Netgen\n', '')
```

#### 5. **Period Calculation**
```python
# Create period_start and period_end for each month
# Month 1 = 2024-01-01 00:00:00 to 2024-02-01 00:00:00

melted['period_start'] = pd.to_datetime(
    melted['YEAR'].astype(str) + '-' + melted['month'],
    format='%Y-%B'
)
melted['period_end'] = melted['period_start'] + pd.DateOffset(months=1)
```

#### 6. **Unit Configuration**
```python
# Filter for generation units with source='EIA'
# Match by Plant Id (as string)
# generation_unit.code = Plant Id
relevant_units = await get_relevant_unit_codes('EIA')  # Returns set of plant IDs

# Additionally filter for Wind data only at file read level
df = df[df['Reported\nFuel Type Code'] == 'WND']
```

### Performance Optimizations

1. **Parallel Processing**: Process multiple year files in parallel (4-8 workers)
2. **Batch Accumulation**: Accumulate melted data before COPY
3. **PostgreSQL COPY**: Use asyncpg COPY for bulk insert
4. **Memory Management**: Process files in chunks if needed
5. **Unit Filtering**: Only import configured generation units

### Data Model Mapping

```python
GenerationDataRaw(
    source='EIA',
    source_type='excel',
    identifier='1',  # Plant ID as string (maps to generation_unit.code)
    period_type='month',
    period_start=datetime(2024, 1, 1),  # First of month
    period_end=datetime(2024, 2, 1),    # First of next month
    value_extracted=3616.0,              # MWh (wind generation only)
    unit='MWh',
    data={
        'plant_id': 1,
        'plant_name': 'Sand Point',
        'fuel_type': 'WND',  # Always 'WND' since we filter
        'month': 'January',
        'year': 2024,
        'generation_mwh': 3616.0
    }
)
```

## Expected Performance

### Data Volume Estimate
- **Files**: 25 years (2001-2025)
- **Rows per file**: ~15,000-30,000 (varies by year)
- **Wind rows per file**: ~500-1,500 (only fuel_type='WND')
- **Total wind rows**: ~12,500-37,500
- **After melting**: 25K × 12 months = ~300K records
- **After filtering**: Depends on configured units (likely 50K-150K records)

### Processing Time
- **With optimizations**: 5-10 minutes
- **Per file**: 20-30 seconds
- **Throughput**: ~50K-100K records/second

## Implementation Files

### Required Scripts

1. **`import_parallel_optimized.py`** (main import script)
   - Multi-worker parallel processing
   - Wide-to-long transformation
   - PostgreSQL COPY optimization
   - Progress tracking

2. **`check_import_status.py`** (verification script)
   - Count records by year
   - Show date ranges
   - Validate data integrity

3. **Update `README.md`** (documentation)
   - Add EIA section
   - Usage examples
   - Troubleshooting

## Fuel Type Codes (for reference)

Common EIA fuel type codes:
- **WND**: Wind
- **NG**: Natural Gas
- **SUN**: Solar
- **WAT**: Hydroelectric
- **NUC**: Nuclear
- **COL**: Coal
- **DFO**: Distillate Fuel Oil
- **GEO**: Geothermal
- **BIO**: Biomass

## Data Quality Considerations

1. **Zero Values**: Many months may have 0 generation (normal for seasonal/maintenance)
2. **Multiple Fuel Types**: Same plant may have multiple fuel sources
3. **Missing Data**: Some months may be blank/null
4. **Year Coverage**: Partial year for 2025 (only through July)
5. **Plant Changes**: Plants may be added/retired over years

## Next Steps

1. ✅ Create import script based on Energistyrelsen pattern
2. ✅ Add EIA-specific column handling
3. ✅ Implement wide-to-long transformation
4. ✅ Test with sample data (2024 file)
5. ✅ Full import all years
6. ✅ Verify data quality
