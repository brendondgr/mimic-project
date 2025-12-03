IDs = {
    "chartevents": {"rows": 313645063, "ordered_by": "subject_id"}
}

import pandas as pd
import time
import os
from io import BytesIO

try:
    import indexed_gzip
    HAS_INDEXED_GZIP = True
except ImportError:
    HAS_INDEXED_GZIP = False

class Filter:
    def __init__(self, file_path, file_id, debug=False):
        self.file_path = file_path
        self.file_id = file_id
        self.debug = debug
        self.metadata = IDs.get(file_id)
        if not self.metadata:
            raise ValueError(f"ID {file_id} not found in IDs dictionary.")
        
        self.total_rows = self.metadata["rows"]
        self.sort_col = self.metadata["ordered_by"]
        print(f"[Filter] Initialized for {file_id} with {self.total_rows} rows, sorted by {self.sort_col}")
        
        # Read header to get column names and index of sort column
        # We read just the header line
        self.header = pd.read_csv(self.file_path, nrows=0).columns.tolist()
        try:
            self.sort_col_idx = self.header.index(self.sort_col)
        except ValueError:
            raise ValueError(f"Column {self.sort_col} not found in CSV header.")

        # Optimization: Load lookup table if available
        self.lookup_df = None
        # Try to find the lookup file in a standard location if not provided
        # For now, we default to the one in data/
        self.lookup_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'icu_unique_subject_ids.csv')
        
        self._load_lookup_table()

    def _load_lookup_table(self):
        """Loads the lookup table if it exists."""
        if os.path.exists(self.lookup_path):
            try:
                df = pd.read_csv(self.lookup_path)
                # Check if we have the specific columns for this file_id
                start_col = f"{self.file_id}_byteidx_start"
                end_col = f"{self.file_id}_byteidx_end"
                
                if 'subject_id' in df.columns:
                    self.lookup_df = df.set_index('subject_id')
                    if start_col in self.lookup_df.columns and end_col in self.lookup_df.columns:
                         print(f"[Filter] Loaded optimized lookup table from {self.lookup_path} with columns for {self.file_id}")
                    else:
                        print(f"[Filter] Lookup table loaded, but columns for {self.file_id} not found.")
            except Exception as e:
                print(f"[Filter] Failed to load lookup table: {e}")

    def generate_byte_index(self, lookup_csv_path=None):
        """
        Scans the file to generate byte offsets for each subject and updates the lookup CSV.
        
        Args:
            lookup_csv_path (str): Path to the CSV file to update. If None, uses self.lookup_path.
        """
        if not HAS_INDEXED_GZIP:
            print("[Error] indexed_gzip is required for generating byte index.")
            return

        target_csv_path = lookup_csv_path if lookup_csv_path else self.lookup_path
        print(f"[generate_byte_index] Starting index generation for {self.file_id}...")
        
        if not os.path.exists(target_csv_path):
             print(f"[Error] Target CSV {target_csv_path} does not exist. Please provide a base CSV with subject_ids.")
             return

        # Load existing subjects
        subjects_df = pd.read_csv(target_csv_path)
        if 'subject_id' not in subjects_df.columns:
             print("[Error] 'subject_id' column missing in target CSV.")
             return
        
        # Ensure sorted for efficient matching if needed, but we'll use a dict for updates
        # actually, the file scan gives us subjects in order.
        
        offsets = {}
        start_time = time.time()
        
        index_file_path = self.file_path + ".idx"
        
        with indexed_gzip.IndexedGzipFile(self.file_path, spacing=2**22) as f:
            if os.path.exists(index_file_path):
                print(f"Loading existing index from {index_file_path}...")
                f.import_index(filename=index_file_path)
            else:
                print("Building gzip index (this may take a while)...")
                f.build_full_index()
                print(f"Saving index to {index_file_path}...")
                f.export_index(filename=index_file_path)
            
            print("Scanning file for offsets...")
            
            # Skip header
            header_line = f.readline()
            current_offset = f.tell()
            
            # Identify subject_id column index
            header_str = header_line.decode('utf-8')
            cols = header_str.strip().split(',')
            try:
                subject_col_idx = cols.index(self.sort_col) # usually 'subject_id'
            except ValueError:
                print(f"Error: Sort column '{self.sort_col}' not found in header")
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
                        print(f"Found {len(offsets)} subjects...", end='\r')
                
                current_offset += line_len
            
            # Last subject
            if current_subject is not None:
                offsets[current_subject] = (subject_start_offset, current_offset)
                
        print(f"\nScanning complete. Found {len(offsets)} subjects in {time.time() - start_time:.2f}s")
        
        # Update DataFrame
        start_col = f"{self.file_id}_byteidx_start"
        end_col = f"{self.file_id}_byteidx_end"
        
        start_vals = []
        end_vals = []
        
        found_count = 0
        for sid in subjects_df['subject_id']:
            if sid in offsets:
                start, end = offsets[sid]
                start_vals.append(start)
                end_vals.append(end)
                found_count += 1
            else:
                start_vals.append(-1)
                end_vals.append(-1)
        
        subjects_df[start_col] = start_vals
        subjects_df[end_col] = end_vals
        
        subjects_df.to_csv(target_csv_path, index=False)
        print(f"Updated {target_csv_path} with columns {start_col}, {end_col}")
        print(f"Matched {found_count}/{len(subjects_df)} subjects.")
        
        # Reload lookup table
        self._load_lookup_table()

    def _get_value_at_index(self, index):
        """
        Reads the value of the sort column at the specified 0-based data index.
        """
        if index < 0 or index >= self.total_rows:
            return None
            
        # Skip header (1 row) + index rows
        skip = 1 + index
        try:
            # Read only the sort column
            # usecols expects indices or names. We use index for robustness if names are tricky, 
            # but names are usually safer if we have them. 
            # However, we have self.sort_col_idx.
            df = pd.read_csv(
                self.file_path, 
                skiprows=skip, 
                nrows=1, 
                header=None, 
                usecols=[self.sort_col_idx]
            )
            if df.empty:
                return None
            return df.iloc[0, 0]
        except Exception as e:
            print(f"Error reading index {index}: {e}")
            return None

    def search_subject(self, subject_id):
        """
        Searches for a subject_id and returns all their records using byte-offset indexing.
        
        This method requires a pre-generated byte-offset index. Run `python main.py --optimize-index`
        to generate the index if it doesn't exist.
        
        Args:
            subject_id (int): The subject_id to search for
            
        Returns:
            pd.DataFrame: DataFrame containing all records for the subject, or empty DataFrame if not found
        """
        start_time = time.time()
        print(f"[search_subject] Searching for subject_id: {subject_id}")
        
        # Define column names based on file_id
        start_col = f"{self.file_id}_byteidx_start"
        end_col = f"{self.file_id}_byteidx_end"
        
        # Check if indexed_gzip is available
        if not HAS_INDEXED_GZIP:
            error_msg = (
                "[ERROR] indexed_gzip is required for search_subject. "
                "Please install it: pip install indexed_gzip"
            )
            print(error_msg)
            raise ImportError(error_msg)
        
        # Check if lookup table is loaded
        if self.lookup_df is None:
            error_msg = (
                f"[ERROR] Lookup table not found at {self.lookup_path}. "
                f"Please run 'python main.py --optimize-index' to generate the byte-offset index."
            )
            print(error_msg)
            raise FileNotFoundError(error_msg)
        
        # Check if the required columns exist
        if start_col not in self.lookup_df.columns or end_col not in self.lookup_df.columns:
            error_msg = (
                f"[ERROR] Byte-offset index columns ({start_col}, {end_col}) not found in lookup table. "
                f"Please run 'python main.py --optimize-index' to generate the index for {self.file_id}."
            )
            print(error_msg)
            raise ValueError(error_msg)
        
        # Check if subject_id exists in the lookup table
        if subject_id not in self.lookup_df.index:
            print(f"[search_subject] Subject {subject_id} not found in lookup table")
            return pd.DataFrame(columns=self.header)
        
        # Get byte offsets for this subject
        info = self.lookup_df.loc[subject_id]
        start_byte = int(info[start_col])
        end_byte = int(info[end_col])
        
        # Check if the subject has valid byte offsets
        if start_byte == -1 or end_byte == -1:
            print(f"[search_subject] Subject {subject_id} has no data in {self.file_id}")
            return pd.DataFrame(columns=self.header)
        
        length_bytes = end_byte - start_byte
        print(f"[search_subject] Using byte-offset lookup: offset={start_byte}, length={length_bytes} bytes")
        
        # Open the gzip file with indexed_gzip for random access
        index_file_path = self.file_path + ".idx"
        
        try:
            with indexed_gzip.IndexedGzipFile(self.file_path) as f:
                # Import pre-built index if available
                if os.path.exists(index_file_path):
                    f.import_index(filename=index_file_path)
                else:
                    print(f"[WARNING] No .idx file found at {index_file_path}. Building index on-the-fly (this may be slow)...")
                    # The index will be built automatically when seeking
                
                # Seek to the start byte and read the exact number of bytes
                f.seek(start_byte)
                data = f.read(length_bytes)
            
            # Parse the bytes into a DataFrame
            result_df = pd.read_csv(BytesIO(data), names=self.header, header=None)
            
            end_time = time.time()
            duration = end_time - start_time
            
            print(f"[search_subject] Successfully loaded {len(result_df)} rows for subject {subject_id}")
            print(f"[TIMER] Total search time: {duration:.6f}s")
            
            return result_df
            
        except Exception as e:
            error_msg = f"[ERROR] Failed to read data for subject {subject_id}: {str(e)}"
            print(error_msg)
            raise RuntimeError(error_msg) from e


