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
python main.py --optimize-index
```

## Optimization Index

The `--optimize-index` command creates a byte-offset index for the `chartevents.csv.gz` file, enabling extremely fast subject lookups. This is a one-time process that:

1. Scans the entire chartevents.csv.gz file (3.3GB)
2. Builds a gzip index for random access
3. Updates `data/icu_unique_subject_ids.csv` with byte offsets (`chartevents_byteidx_start`, `chartevents_byteidx_end`)
4. Verifies the optimization by performing a test lookup

**Note**: This process may take several minutes on first run but will enable subsequent lookups to complete in <0.1 seconds.