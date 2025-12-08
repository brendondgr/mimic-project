import os
import pandas as pd
import time
from io import BytesIO
import sys

# Import from parent module
# Note: internal imports might be tricky depending on how this is run. 
# Attempting relative import assuming this is used as a package.
try:
    from ..filtering import Filterer, IDs, ROOT_URL, HAS_INDEXED_GZIP
except ImportError:
    # If run directly or path issues, try absolute import
    from utils.analysis.filtering import Filterer, IDs, ROOT_URL, HAS_INDEXED_GZIP

try:
    import indexed_gzip
except ImportError:
    pass

class File_Filter(Filterer):
    def __init__(self, file_id, file_path=None, debug=False):
        super().__init__(debug=debug)
        self.file_id = file_id
        
        self.metadata = IDs.get(file_id)
        if not self.metadata:
            raise ValueError(f"ID {file_id} not found in IDs dictionary.")
        
        # Use provided file_path or fall back to metadata location
        if file_path:
            self.file_path = file_path
        else:
            self.file_path = self._resolve_file_path(file_id)
        
        self.total_rows = self.metadata["rows"]
        self.sort_col = self.metadata["ordered_by"]
        if self.debug:
            print(f"[File_Filter] Initialized for {file_id} with {self.total_rows} rows, sorted by {self.sort_col}")
        else:
            print(f"[File_Filter] Initialized for {file_id}")
        
        # Read header to get column names and index of sort column
        if os.path.exists(self.file_path):
            self.header = pd.read_csv(self.file_path, nrows=0).columns.tolist()
            try:
                self.sort_col_idx = self.header.index(self.sort_col)
            except ValueError:
                raise ValueError(f"Column {self.sort_col} not found in CSV header.")
        else:
            print(f"[File_Filter] Warning: File {self.file_path} not found.")
            self.header = []
            self.sort_col_idx = -1

    def generate_byte_index(self, lookup_csv_path=None):
        """
        Scans the file to generate byte offsets for each subject and updates the lookup CSV.
        Calls the base class implementation.
        """
        super().generate_byte_index(self.file_id, self.file_path, lookup_csv_path)

    def _get_value_at_index(self, index):
        """
        Reads the value of the sort column at the specified 0-based data index.
        """
        if index < 0 or index >= self.total_rows:
            return None
            
        # Skip header (1 row) + index rows
        skip = 1 + index
        try:
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
        """
        start_time = time.time()
        if self.debug:
            print(f"[search_subject] Searching for subject_id: {subject_id}")
        
        start_col = f"{self.file_id}_byteidx_start"
        end_col = f"{self.file_id}_byteidx_end"
        
        if not HAS_INDEXED_GZIP:
            error_msg = "[ERROR] indexed_gzip is required for search_subject."
            print(error_msg)
            raise ImportError(error_msg)
        
        if self.lookup_df is None:
            error_msg = f"[ERROR] Lookup table not found at {self.lookup_path}."
            print(error_msg)
            raise FileNotFoundError(error_msg)
        
        if start_col not in self.lookup_df.columns or end_col not in self.lookup_df.columns:
            error_msg = f"[ERROR] Byte-offset index columns ({start_col}, {end_col}) not found in lookup table."
            print(error_msg)
            raise ValueError(error_msg)
        
        if subject_id not in self.lookup_df.index:
            if self.debug:
                print(f"[search_subject] Subject {subject_id} not found in lookup table")
            return pd.DataFrame(columns=self.header)
        
        info = self.lookup_df.loc[subject_id]
        start_byte = int(info[start_col])
        end_byte = int(info[end_col])
        
        if start_byte == -1 or end_byte == -1:
            if self.debug:
                print(f"[search_subject] Subject {subject_id} has no data in {self.file_id}")
            return pd.DataFrame(columns=self.header)
        
        length_bytes = end_byte - start_byte
        if self.debug:
            print(f"[search_subject] Using byte-offset lookup: offset={start_byte}, length={length_bytes} bytes")
        
        index_file_path = self.file_path + ".idx"
        
        try:
            with indexed_gzip.IndexedGzipFile(self.file_path) as f:
                if os.path.exists(index_file_path):
                    f.import_index(filename=index_file_path)
                else:
                    print(f"[WARNING] No .idx file found at {index_file_path}. Building index on-the-fly...")
                
                f.seek(start_byte)
                data = f.read(length_bytes)
            
            result_df = pd.read_csv(BytesIO(data), names=self.header, header=None)
            
            if not result_df.empty:
                actual_subject = result_df.iloc[0, self.sort_col_idx]
                # Handle types if needed (str vs int)
                # Assuming int for subject_id as per usual MIMIC
                try:
                    actual_subject = int(actual_subject)
                    subject_id = int(subject_id)
                except ValueError:
                    pass 

                if actual_subject != subject_id:
                    print(f"[search_subject] ERROR: Loaded data for subject {actual_subject}, but expected {subject_id}.")
                    return pd.DataFrame(columns=self.header)
            
            if self.debug:
                end_time = time.time()
                duration = end_time - start_time
                print(f"[search_subject] Successfully loaded {len(result_df)} rows for subject {subject_id} in {duration:.4f}s")
            
            return result_df
            
        except Exception as e:
            error_msg = f"[ERROR] Failed to read data for subject {subject_id}: {str(e)}"
            print(error_msg)
            raise RuntimeError(error_msg) from e

    def filter_by_column(self, column_name, value, subject_id=None):
        """
        Filters data by column/value.
        If subject_id is provided, loads subject data first then filters.
        If subject_id is None, filters entire file (chunks).
        """
        if subject_id is not None:
            df = self.search_subject(subject_id)
            if df.empty:
                return df
            
            if column_name in df.columns:
                # Handle numeric vs string comparison roughly
                # Though pandas usually handles it if types match.
                # Just doing direct comparison.
                return df[df[column_name] == value]
            else:
                print(f"[filter_by_column] Column {column_name} not found.")
                return pd.DataFrame(columns=self.header)
        else:
            if self.debug:
                print(f"[File_Filter] Filtering entire file {self.file_id} for {column_name} == {value}...")
            
            if column_name not in self.header:
                 print(f"[filter_by_column] Column {column_name} not found.")
                 return pd.DataFrame(columns=self.header)

            filtered_chunks = []
            try:
                # Use chunksize to handle large files
                chunk_size = 100000
                for chunk in pd.read_csv(self.file_path, chunksize=chunk_size):
                    if column_name in chunk.columns:
                        match = chunk[chunk[column_name] == value]
                        if not match.empty:
                            filtered_chunks.append(match)
                
                if filtered_chunks:
                    return pd.concat(filtered_chunks)
                else:
                    return pd.DataFrame(columns=self.header)
            except Exception as e:
                print(f"[File_Filter] Error filtering file: {e}")
                return pd.DataFrame(columns=self.header)
