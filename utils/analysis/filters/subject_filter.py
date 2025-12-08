from ..filtering import Filterer, IDs
from .file_filter import File_Filter
import pandas as pd
import time

class Subject_Filter(Filterer):
    def __init__(self, debug=False):
        """
        Initializes the Subject_Filter.
        Pre-initializes File_Filter instances for all available files.
        """
        super().__init__(debug=debug)
        self.filters = {}
        if self.debug:
            print("[Subject_Filter] Initializing child filters for all files...")
        
        for file_id in IDs.keys():
            try:
                self.filters[file_id] = File_Filter(file_id, debug=debug)
            except Exception as e:
                print(f"[Subject_Filter] Warning: Failed to initialize filter for {file_id}: {e}")
                self.filters[file_id] = None

    def get_all_subject_data(self, subject_id):
        """
        Retrieves all data for a specific subject across all files.
        
        Args:
            subject_id (int): The subject ID to retrieve data for.
            
        Returns:
            dict: A dictionary where keys are file_ids and values are DataFrames.
        """
        results = {}
        
        if self.debug:
            start_time = time.time()
            print(f"[Subject_Filter] Starting data retrieval for subject {subject_id}")

        for file_id, filter_instance in self.filters.items():
            if filter_instance:
                try:
                    if self.debug:
                        print(f"[Subject_Filter] Fetching {file_id} for subject {subject_id}...")
                    df = filter_instance.search_subject(subject_id)
                    results[file_id] = df
                except Exception as e:
                    print(f"[Subject_Filter] Error fetching data from {file_id} for subject {subject_id}: {e}")
                    results[file_id] = pd.DataFrame()
            else:
                if self.debug:
                   print(f"[Subject_Filter] Skipping {file_id} (not initialized).")
                results[file_id] = None
        
        if self.debug:
            end_time = time.time()
            duration = end_time - start_time
            print(f"[Subject_Filter] Finished data retrieval for subject {subject_id} in {duration:.4f}s")
                
        return results
