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
        Finds the subject_id and returns the DataFrame for that subject.
        Uses optimized byte-offset lookup if available, otherwise falls back to binary search.
        """
        start_time = time.time()
        print(f"[search_subject] START - Searching for subject_id: {subject_id} at {start_time:.6f}")

        # --- Optimized Path ---
        start_col = f"{self.file_id}_byteidx_start"
        end_col = f"{self.file_id}_byteidx_end"

        if HAS_INDEXED_GZIP and self.lookup_df is not None and subject_id in self.lookup_df.index:
            try:
                # Check if we have the columns for this file
                if start_col in self.lookup_df.columns and end_col in self.lookup_df.columns:
                    info = self.lookup_df.loc[subject_id]
                    start_byte = int(info[start_col])
                    end_byte = int(info[end_col])
                    
                    if start_byte != -1:
                        length_bytes = end_byte - start_byte
                        print(f"[search_subject] Using optimized lookup: offset={start_byte}, length={length_bytes}")
                        
                        index_file = self.file_path + ".idx"
                        
                        with indexed_gzip.IndexedGzipFile(self.file_path) as f:
                            if os.path.exists(index_file):
                                f.import_index(filename=index_file)
                            
                            f.seek(start_byte)
                            data = f.read(length_bytes)
                            
                        result_df = pd.read_csv(BytesIO(data), names=self.header, header=None)
                        
                        end_time = time.time()
                        print(f"[search_subject] Successfully loaded {len(result_df)} rows for subject {subject_id} (Optimized)")
                        print(f"[TIMER] TOTAL search_subject time: {end_time - start_time:.6f}s")
                        return result_df
            except Exception as e:
                print(f"[search_subject] Optimized lookup failed: {e}. Falling back to binary search.")

        # --- Fallback: Binary Search ---
        print("[search_subject] Using binary search fallback...")
        low = 0
        high = self.total_rows - 1
        found_index = -1

        # 1. Binary Search to find ANY instance
        binary_search_start = time.time()
        while low <= high:
            mid = (low + high) // 2
            mid_val = self._get_value_at_index(mid)
            
            if mid_val is None:
                # Fallback or error handling
                break

            if mid_val == subject_id:
                found_index = mid
                if self.debug:
                    print(f"[DEBUG] Found exact match at index {mid}: value={mid_val}")
                break
            elif mid_val < subject_id:
                if self.debug:
                    print(f"[DEBUG] {mid_val} too low at index {mid}, searching right. Indices: low={low}, high={high}")
                low = mid + 1
            else:
                if self.debug:
                    print(f"[DEBUG] {mid_val} too high at index {mid}, searching left. Indices: low={low}, high={high}")
                high = mid - 1
        
        binary_search_end = time.time()
        if found_index == -1:
            print(f"[search_subject] Subject {subject_id} not found")
            return pd.DataFrame(columns=self.header)
        
        print(f"[search_subject] Found subject at index {found_index}, expanding boundaries...")
        print(f"[TIMER] Binary search completed in {binary_search_end - binary_search_start:.6f}s")

        # 2. Expand search to find boundaries
        expansion_start = time.time()
        
        # --- Find Start Index ---
        start_index = found_index
        step = 1
        # Exponential search to the left
        while start_index - step >= 0:
            val = self._get_value_at_index(start_index - step)
            if val == subject_id:
                start_index -= step
                step *= 2
            else:
                break
        
        # Binary search in the range [max(0, start_index - step), start_index]
        search_low = max(0, start_index - step)
        search_high = start_index
        final_start_index = start_index
        
        while search_low <= search_high:
            mid = (search_low + search_high) // 2
            mid_val = self._get_value_at_index(mid)
            if mid_val == subject_id:
                final_start_index = mid
                search_high = mid - 1
            else:
                search_low = mid + 1
        
        start_index = final_start_index

        # --- Find End Index ---
        end_index = found_index
        step = 1
        # Exponential search to the right
        while end_index + step < self.total_rows:
            val = self._get_value_at_index(end_index + step)
            if val == subject_id:
                end_index += step
                step *= 2
            else:
                break
                
        # Binary search in the range [end_index, min(total_rows-1, end_index + step)]
        search_low = end_index
        search_high = min(self.total_rows - 1, end_index + step)
        final_end_index = end_index
        
        while search_low <= search_high:
            mid = (search_low + search_high) // 2
            mid_val = self._get_value_at_index(mid)
            if mid_val == subject_id:
                final_end_index = mid
                search_low = mid + 1
            else:
                search_high = mid - 1
        
        end_index = final_end_index
        
        expansion_end = time.time()
        print(f"[search_subject] Boundaries: start_index={start_index}, end_index={end_index}")
        print(f"[TIMER] Boundary expansion completed in {expansion_end - expansion_start:.6f}s")

        # 3. Load the chunk
        load_start = time.time()
        skip = 1 + start_index
        nrows = end_index - start_index + 1
        print(f"[search_subject] Loading {nrows} rows starting at index {start_index}")
        
        result_df = pd.read_csv(
            self.file_path, 
            skiprows=skip, 
            nrows=nrows, 
            header=None, 
            names=self.header
        )
        load_end = time.time()
        end_time = time.time()
        print(f"[search_subject] Successfully loaded {len(result_df)} rows for subject {subject_id}")
        print(f"[TIMER] CSV load completed in {load_end - load_start:.6f}s")
        print(f"[TIMER] TOTAL search_subject time: {end_time - start_time:.6f}s")
        return result_df

