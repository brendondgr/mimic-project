# MIMIC Project

A utility for managing MIMIC-IV dataset downloads and system hardware monitoring.

## Usage

```bash
python main.py [OPTIONS]
```

## Commands

- `--pcspecs`: Display PC hardware specifications
- `--download`: Download MIMIC-IV dataset from PhysioNet
- `--optimize-index`: Generate byte-offset index for chartevents.csv.gz to enable near-instantaneous subject lookups

## Examples

```bash
# Get PC specifications
python main.py --pcspecs

# Download dataset
python main.py --download

# Generate optimized index for fast lookups (one-time process)
# Default (all files):
python main.py --optimize-index

# Specific file only:
python main.py --optimize-index chartevents
```

## Optimization Index

The `--optimize-index` command creates a byte-offset index for the large CSV.gz files (like `chartevents`, `datetimeevents`, etc.), enabling extremely fast subject lookups. This is a one-time process that:

1. Scans the specified file(s)
2. Builds a gzip index for random access
3. Updates `data/icu_unique_subject_ids.csv` with byte offsets (e.g., `chartevents_byteidx_start`)
4. **Adds new subject IDs** to the lookup table if they are found in the data files but missing from the index
5. Verifies the optimization by performing a test lookup

**Note**: This process may take several minutes per file but will enable subsequent lookups to complete in <0.1 seconds.