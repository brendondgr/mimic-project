# Refactor Filter Architecture to Filterer Base Class

## Overview

Refactor `utils/analysis/filtering.py` to create a base `Filterer` class with two specialized subclasses: `File_Filter` (current Filter functionality) and `Subject_Filter` (cross-file subject data retrieval). This enables both file-specific filtering and subject-centric data gathering.

## Architecture

### Base Class: `Filterer` (in `filtering.py`)

- Contains shared functionality:
  - `IDs` dictionary (moved to module level, already exists)
  - `_load_lookup_table()` - loads lookup CSV
  - `generate_byte_index()` - generates byte offsets for a file
  - Common utilities for indexed_gzip operations
  - Lookup table path resolution

### `File_Filter` (in `filters/file_filter.py`)

- Inherits from `Filterer`
- Handles single-file operations:
  - `__init__(file_id, file_path=None, debug=False)` - same as current Filter
  - `search_subject(subject_id)` - returns DataFrame for subject from this file (current implementation)
  - `filter_by_column(column_name, value, subject_id=None)` - NEW: filters data by column/value
    - If `subject_id` provided: loads subject data first, then filters
    - If `subject_id` is None: filters entire file (may be slow for large files)
  - `_get_value_at_index(index)` - utility method
  - All current Filter functionality preserved

### `Subject_Filter` (in `filters/subject_filter.py`)

- Inherits from `Filterer`
- Handles cross-file subject operations:
  - `__init__(debug=False)` - no file_id needed
  - `get_all_subject_data(subject_id)` - returns dict of DataFrames
    - Keys: file_ids (e.g., "chartevents", "inputevents", etc.)
    - Values: DataFrames with subject's data from that file
    - Uses lookup table to find byte offsets for each file
    - Iterates through all files in `IDs` dictionary

### `filtering.py` Updates

- Move `Filter` class to `filters/file_filter.py` as `File_Filter`
- Create base `Filterer` class with shared methods
- Keep `IDs` dictionary at module level
- Export: `Filterer`, `File_Filter`, `Subject_Filter`, `IDs`
- For backward compatibility: `Filter = File_Filter` (deprecated alias)

## Implementation Details

### File Structure

```
utils/analysis/
  filtering.py          # Base Filterer class + exports
  filters/
    __init__.py        # Package init
    file_filter.py     # File_Filter class
    subject_filter.py  # Subject_Filter class
```

### Key Methods to Refactor

1. **Filterer base class** (`filtering.py`):

   - `_load_lookup_table()` - abstract or shared implementation
   - `generate_byte_index(file_id, lookup_csv_path=None)` - needs file_id parameter
   - Shared lookup path resolution

2. **File_Filter** (`filters/file_filter.py`):

   - Move entire current `Filter` class
   - Rename to `File_Filter`
   - Add `filter_by_column()` method
   - Keep `generate_byte_index()` but call base method

3. **Subject_Filter** (`filters/subject_filter.py`):

   - `get_all_subject_data(subject_id)` - main method
   - Uses `File_Filter` instances internally or reuses byte offset logic
   - Returns `{file_id: DataFrame}` dictionary

### Backward Compatibility

1. **`create_lookup_index.py`**:

   - Update to use `File_Filter` instead of `Filter`
   - Fix parameter order bug: `Filter(file_id, file_path)` not `Filter(file_path, file_id)`

2. **`verify_optimization.py`**:

   - Update to use `File_Filter`
   - Fix parameter order bug

3. **`main.py`**:

   - No changes needed (uses `create_index` function)

4. **Module exports**:

   - `from utils.analysis.filtering import Filter` still works (via alias)
   - New: `from utils.analysis.filtering import File_Filter, Subject_Filter`

## Testing Considerations

- Ensure `python main.py --optimize-index` still works
- Ensure `python main.py --optimize-index chartevents` still works
- Test `File_Filter.filter_by_column(column_name, value, subject_id)` requires subject_id
- Test `Subject_Filter.get_all_subject_data(subject_id)` returns dict with file_id keys
- Test `Filterer("file", file_id="chartevents")` factory pattern works
- Test `Filterer("subject")` factory pattern works

## Migration Path

1. Create `Filterer` base class in `filtering.py`
2. Move `Filter` → `File_Filter` in `filters/file_filter.py`
3. Create `Subject_Filter` in `filters/subject_filter.py`
4. Update imports in `create_lookup_index.py` and `verify_optimization.py`
5. Add backward compatibility alias `Filter = File_Filter`
6. Fix parameter order bugs in existing code