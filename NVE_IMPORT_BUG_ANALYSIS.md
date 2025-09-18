# NVE Import Bug Analysis

## Problem Description
Three records with different generation values that should belong to different wind farms are all assigned to the same generation unit code "2" (Kjøllefjord):

- ID=30662742: 16.08 MWh → Should be Kjøllefjord (code 2) ✓
- ID=30662747: 0.2 MWh → Should be Nygårdsfjellet (code 6) ✗
- ID=30662761: 37.248 MWh → Should be Ytre Vikna (code 39) ✗

All three records were created at the same time: 2025-09-13 14:24:33

## Root Cause Identified

The bug occurs when the DataFrame is chunked for parallel processing. Each chunk tries to recreate the column-to-unit mapping from its first row, but chunks that don't start at row 0 don't have access to the header row containing unit codes.

### The Bug in Detail

1. **Excel Structure**: Row 0 contains unit codes, Row 1 is empty, Row 2+ contains actual data
2. **Chunking Issue**: When creating chunks for parallel processing:
   - Chunk 1: rows 0-9999 (has the header row with codes)
   - Chunk 2: rows 10000-19999 (NO header row)
   - Chunk 3: rows 20000-29999 (NO header row)
   - etc.

3. **Mapping Failure**: Each chunk's `process_nve_chunk` function tries to map columns using `chunk_df.iloc[0]`:
   - For Chunk 1: `iloc[0]` = header row with codes ✓
   - For Chunk 2+: `iloc[0]` = data row with generation values ✗

4. **Result**: Chunks without the header row create incorrect mappings, potentially assigning all values to wrong units

## The Fixed Code

The solution implemented:

1. **Create mapping once** in the main function before chunking:
```python
# In import_nve_data function, before chunking:
column_to_unit = {}
first_row = df.iloc[0]  # Get the actual header row with codes

for col in df.columns[1:]:
    code_value = first_row[col]
    if pd.notna(code_value):
        code_str = str(int(code_value))
        if code_str in units_by_code:
            column_to_unit[col] = units_by_code[code_str]
```

2. **Pass mapping to all chunks**:
```python
chunks.append((chunk, unit_mapping, i, chunk_size, column_to_unit))
```

3. **Use pre-computed mapping** in `process_nve_chunk`:
```python
def process_nve_chunk(args):
    chunk_df, unit_mapping, chunk_start, chunk_size, column_to_unit = args
    # Now use the passed column_to_unit instead of recreating it
```

4. **Adjust row processing** based on chunk position:
```python
# Skip header rows only for the first chunk
start_idx = 2 if chunk_start == 0 else 0
for idx in range(start_idx, len(chunk_df)):
    # Process data rows
```

## Verification

The fix ensures:
- Column-to-unit mapping is consistent across all chunks
- Each wind farm's data is correctly assigned to its unit code
- Parallel processing doesn't corrupt the mapping

## Impact

This bug would have caused misassignment of generation data whenever:
- The import used parallel processing (workers > 1)
- Chunks didn't include the header row
- Multiple wind farms had data at the same timestamp

The fix prevents these misassignments by centralizing the column mapping logic.