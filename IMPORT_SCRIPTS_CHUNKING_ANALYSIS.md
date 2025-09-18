# Import Scripts Chunking Analysis

## Summary
Analyzed all data import scripts for potential chunking issues similar to the NVE bug where incorrect column-to-unit mappings occurred during parallel processing.

## NVE Import Script ❌ (FIXED)

**File:** `/scripts/seeds/generation_data/nve/import_parallel_optimized.py`

### Issue Found
- **Problem:** The script chunked a single Excel file into multiple pieces for parallel processing
- **Bug:** Each chunk tried to recreate column-to-unit mapping from its first row
- **Impact:** Only the first chunk (rows 0-9999) had access to the header row with unit codes. Subsequent chunks used data rows instead, causing incorrect unit assignments
- **Example:** Values for units 6 and 39 were incorrectly assigned to unit 2

### Fix Applied
1. Column-to-unit mapping is now created once before chunking
2. The mapping is passed to all chunks
3. Each chunk uses the pre-computed mapping instead of recreating it

## Taipower Import Script ✅ (SAFE)

**File:** `/scripts/seeds/generation_data/taipower/import_parallel_optimized.py`

### Analysis
- **Processing Method:** Each worker processes a complete Excel file (not chunks of one file)
- **Unit Mapping:** Units are mapped based on filename, not column headers
- **Parallel Processing:** Multiple files processed in parallel, not chunks of single file
- **Conclusion:** No chunking issue possible - safe architecture

### Key Code Pattern
```python
def process_excel_file(args):
    file_path, configured_units, file_idx = args
    # Extract unit from filename
    unit_code = extract_unit_code_from_filename(file_path.name)
    # Process entire file with this unit code
```

## ENTSOE Import Script ✅ (SAFE)

**File:** `/scripts/seeds/generation_data/entsoe/import_parallel_optimized.py`

### Analysis
- **Processing Method:** Each worker processes a complete Excel file
- **Data Handling:** Converts Excel to CSV, then reads CSV in chunks for memory efficiency
- **Unit Filtering:** Unit codes are fetched once from database and shared via pickle file
- **Parallel Processing:** Multiple files processed in parallel, not chunks of single file
- **Conclusion:** No chunking issue - the chunking is only for reading CSV data, not for mapping

### Key Code Pattern
```python
# Unit codes fetched once and shared
relevant_unit_codes = await get_relevant_unit_codes()

# Each worker filters using the same set
filtered_df = chunk_df[chunk_df['GenerationUnitCode'].isin(relevant_unit_codes)]
```

## Elexon Import Script ✅ (SAFE)

**File:** `/scripts/seeds/generation_data/elexon/import_parallel_optimized.py`

### Analysis
- **Processing Method:** Each worker processes a complete CSV file
- **Data Handling:** Uses Polars or pandas to read and filter data
- **BMU Filtering:** BMU IDs are fetched once from database and shared across workers
- **Parallel Processing:** Multiple files processed in parallel, not chunks of single file
- **Conclusion:** No chunking issue - safe architecture

### Key Code Pattern
```python
# BMU IDs fetched once
relevant_bmu_ids = await get_relevant_bmu_ids()

# Filtering happens on data, not mapping
mask = np.isin(chunk_df['bmu_id'].values, bmu_ids_array)
filtered_df = chunk_df[mask]
```

## Recommendations

### 1. Best Practices for Future Import Scripts

When implementing parallel processing for data imports:

1. **Avoid chunking single files for column mapping**
   - If you must chunk a single file, create mappings before chunking
   - Pass complete mappings to all workers

2. **Prefer file-level parallelism**
   - Process multiple complete files in parallel
   - This avoids column mapping issues entirely

3. **Centralize configuration fetching**
   - Fetch unit/identifier mappings once
   - Share via serialization (pickle) or pass as arguments

### 2. Testing Recommendations

For any import script that uses chunking:

1. Test with data that spans multiple chunks
2. Verify that data in later chunks is correctly mapped
3. Add assertions to check mapping consistency across chunks

### 3. Code Review Checklist

When reviewing import scripts, check:

- [ ] Are column mappings created per-chunk or globally?
- [ ] Do all chunks have access to header/mapping information?
- [ ] Is the mapping logic dependent on chunk position?
- [ ] Are there tests for multi-chunk scenarios?

## Conclusion

Only the NVE import script had the chunking issue, which has been fixed. The other import scripts (Taipower, ENTSOE, Elexon) use a safer architecture where complete files are processed by each worker, avoiding the column mapping issue entirely.