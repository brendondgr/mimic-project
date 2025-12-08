IDs = {
    "chartevents": {"rows": 313645063, "ordered_by": "subject_id", "location": "physionet.org/files/mimiciv/3.1/icu/chartevents.csv.gz"},
    "datetimeevents": {"rows": 7112999, "ordered_by": "subject_id", "location": "physionet.org/files/mimiciv/3.1/icu/datetimeevents.csv.gz"},
    "ingredientevents": {"rows": 12229408, "ordered_by": "subject_id", "location": "physionet.org/files/mimiciv/3.1/icu/ingredientevents.csv.gz"},
    "inputevents": {"rows": 8978893, "ordered_by": "subject_id", "location": "physionet.org/files/mimiciv/3.1/icu/inputevents.csv.gz"},
    "outputevents": {"rows": 4234967, "ordered_by": "subject_id", "location": "physionet.org/files/mimiciv/3.1/icu/outputevents.csv.gz"},
    "procedureevents": {"rows": 696092, "ordered_by": "subject_id", "location": "physionet.org/files/mimiciv/3.1/icu/procedureevents.csv.gz"},
}

import pandas as pd
import time
import os
from io import BytesIO
import sys

try:
    import indexed_gzip
    HAS_INDEXED_GZIP = True
except ImportError:
    HAS_INDEXED_GZIP = False

# Import ROOT_URL from base config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from config.base_config import Config
ROOT_URL = Config.ROOT_URL

class Filterer:
    def __init__(self, debug=False):
        self.debug = debug
        # Try to find the lookup file in a standard location if not provided
        # For now, we default to the one in data/
        self.lookup_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'icu_unique_subject_ids.csv')
        self.lookup_df = None
        self._load_lookup_table()

    def _load_lookup_table(self):
        """Loads the lookup table if it exists."""
        if os.path.exists(self.lookup_path):
            try:
                self.lookup_df = pd.read_csv(self.lookup_path)
                if 'subject_id' in self.lookup_df.columns:
                    self.lookup_df = self.lookup_df.set_index('subject_id')
                    print(f"[Filterer] Loaded lookup table from {self.lookup_path}")
            except Exception as e:
                print(f"[Filterer] Failed to load lookup table: {e}")

    def _resolve_file_path(self, file_id, file_path=None):
        """Resolves the absolute path for a file_id."""
        if file_path:
            return file_path
        
        metadata = IDs.get(file_id)
        if not metadata:
            raise ValueError(f"ID {file_id} not found in IDs dictionary.")
            
        return os.path.join(ROOT_URL, metadata["location"])

    def generate_byte_index(self, file_id, file_path=None, lookup_csv_path=None):
        """
        Scans the file to generate byte offsets for each subject and updates the lookup CSV.
        
        Args:
            file_id (str): The ID of the file to index (e.g. "chartevents")
            file_path (str): Optional override for file path
            lookup_csv_path (str): Path to the CSV file to update. If None, uses self.lookup_path.
        """
        if not HAS_INDEXED_GZIP:
            print(f"[{file_id}] Error: indexed_gzip is required for generating byte index.")
            return

        target_csv_path = lookup_csv_path if lookup_csv_path else self.lookup_path
        print(f"[{file_id}] Starting index generation...")
        
        resolved_file_path = self._resolve_file_path(file_id, file_path)
        metadata = IDs.get(file_id)
        if not metadata:
             print(f"[{file_id}] Error: Metadata not found.")
             return
             
        sort_col = metadata["ordered_by"]
        
        if not os.path.exists(target_csv_path):
             print(f"[{file_id}] Error: Target CSV {target_csv_path} does not exist. Please provide a base CSV with subject_ids.")
             return

        # Load existing subjects
        subjects_df = pd.read_csv(target_csv_path)
        if 'subject_id' not in subjects_df.columns:
             print(f"[{file_id}] Error: 'subject_id' column missing in target CSV.")
             return
        
        # Check if columns already exist with valid data
        start_col = f"{file_id}_byteidx_start"
        end_col = f"{file_id}_byteidx_end"
        
        if start_col in subjects_df.columns and end_col in subjects_df.columns:
            # Check if there's any valid data (non -1 values)
            valid_count = (subjects_df[start_col] != -1).sum()
            if valid_count > 0:
                print(f"[{file_id}] Columns {start_col} and {end_col} already exist with {valid_count} valid entries. Skipping index generation.")
                print(f"[{file_id}] To regenerate, delete these columns from the CSV and run again.")
                return
        
        offsets = {}
        start_time = time.time()
        
        index_file_path = resolved_file_path + ".idx"
        
        with indexed_gzip.IndexedGzipFile(resolved_file_path, spacing=2**22) as f:
            if os.path.exists(index_file_path):
                print(f"[{file_id}] Loading existing gzip index from {index_file_path}...")
                f.import_index(filename=index_file_path)
            else:
                print(f"[{file_id}] Building gzip index (this may take a while)...")
                f.build_full_index()
                print(f"[{file_id}] Saving gzip index to {index_file_path}...")
                f.export_index(filename=index_file_path)
            
            print(f"[{file_id}] Scanning file for subject byte offsets...")
            
            # Skip header
            header_line = f.readline()
            current_offset = f.tell()
            
            # Identify subject_id column index
            header_str = header_line.decode('utf-8')
            cols = header_str.strip().split(',')
            try:
                subject_col_idx = cols.index(sort_col) # usually 'subject_id'
            except ValueError:
                print(f"[{file_id}] Error: Sort column '{sort_col}' not found in header")
                return

            current_subject = None
            subject_start_offset = current_offset
            
            for line in f:
                line_len = len(line)
                parts = line.split(b',', subject_col_idx + 1)
                if len(parts) <= subject_col_idx:
                    current_offset += line_len
                    continue
                
                sid_bytes = parts[subject_col_idx]
                try:
                    sid = int(sid_bytes)
                except ValueError:
                    sid = int(sid_bytes.decode('utf-8').strip('"'))
                
                if sid != current_subject:
                    if current_subject is not None:
                        # End of previous subject
                        offsets[current_subject] = (subject_start_offset, current_offset)
                    
                    current_subject = sid
                    subject_start_offset = current_offset
                    
                    if len(offsets) % 1000 == 0:
                        print(f"[{file_id}] Found {len(offsets)} subjects...", end='\r')
                
                current_offset += line_len
            
            # Last subject
            if current_subject is not None:
                offsets[current_subject] = (subject_start_offset, current_offset)
                
        print(f"\n[{file_id}] Scanning complete. Found {len(offsets)} subjects in {time.time() - start_time:.2f}s")
        
        # Identify new subjects
        existing_sids = set(subjects_df['subject_id'])
        found_sids = set(offsets.keys())
        new_sids = found_sids - existing_sids
        
        if new_sids:
            print(f"[{file_id}] Found {len(new_sids)} new subjects not in lookup table. Adding them...")
            new_subjects_df = pd.DataFrame({'subject_id': list(new_sids)})
            subjects_df = pd.concat([subjects_df, new_subjects_df], ignore_index=True)
            subjects_df = subjects_df.sort_values('subject_id').reset_index(drop=True)
            
        # Update DataFrame
        # Initialize columns if they don't exist
        if start_col not in subjects_df.columns:
            subjects_df[start_col] = -1
        if end_col not in subjects_df.columns:
            subjects_df[end_col] = -1
            
        # Update values using map for efficiency
        start_map = {k: v[0] for k, v in offsets.items()}
        end_map = {k: v[1] for k, v in offsets.items()}
        
        subjects_df[start_col] = subjects_df['subject_id'].map(start_map).fillna(subjects_df.get(start_col, -1)).astype(int)
        subjects_df[end_col] = subjects_df['subject_id'].map(end_map).fillna(subjects_df.get(end_col, -1)).astype(int)
        
        # Fill NaN with -1 if any (from map)
        subjects_df[start_col] = subjects_df[start_col].fillna(-1).astype(int)
        subjects_df[end_col] = subjects_df[end_col].fillna(-1).astype(int)
        
        subjects_df.to_csv(target_csv_path, index=False)
        print(f"[{file_id}] Updated {target_csv_path} with columns {start_col}, {end_col}")
        print(f"[{file_id}] Total subjects in lookup: {len(subjects_df)}")
        
        # Reload lookup table
        self._load_lookup_table()

# Lazy imports for export and backward compatibility to avoid circular imports
# These will be imported at the end of module initialization
_File_Filter = None
_Subject_Filter = None

def __getattr__(name):
    """Lazy load File_Filter and Subject_Filter on first access."""
    global _File_Filter, _Subject_Filter
    
    if name == 'File_Filter':
        if _File_Filter is None:
            try:
                from .filters.file_filter import File_Filter as FF
                _File_Filter = FF
            except ImportError:
                try:
                    from utils.analysis.filters.file_filter import File_Filter as FF
                    _File_Filter = FF
                except ImportError as e:
                    raise ImportError(f"Could not import File_Filter: {e}")
        return _File_Filter
    
    elif name == 'Subject_Filter':
        if _Subject_Filter is None:
            try:
                from .filters.subject_filter import Subject_Filter as SF
                _Subject_Filter = SF
            except ImportError:
                try:
                    from utils.analysis.filters.subject_filter import Subject_Filter as SF
                    _Subject_Filter = SF
                except ImportError as e:
                    raise ImportError(f"Could not import Subject_Filter: {e}")
        return _Subject_Filter
    
    elif name == 'Filter':
        # Backward compatibility alias
        return __getattr__('File_Filter')
    
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
