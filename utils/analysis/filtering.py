IDs = {
    "chartevents": {"rows": 313645063, "ordered_by": "subject_id"}
}

import pandas as pd

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
        Binary search to find the subject_id, then expand to find range, 
        and return the DataFrame for that subject.
        """
        print(f"[search_subject] Searching for subject_id: {subject_id}")
        low = 0
        high = self.total_rows - 1
        found_index = -1

        # 1. Binary Search to find ANY instance
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
        
        if found_index == -1:
            print(f"[search_subject] Subject {subject_id} not found")
            return pd.DataFrame(columns=self.header)
        
        print(f"[search_subject] Found subject at index {found_index}, expanding boundaries...")

        # 2. Expand search to find boundaries
        
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
        
        print(f"[search_subject] Boundaries: start_index={start_index}, end_index={end_index}")

        # 3. Load the chunk
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
        print(f"[search_subject] Successfully loaded {len(result_df)} rows for subject {subject_id}")
        return result_df

