import os
from pathlib import Path
from utils.analysis.filtering import Filter, IDs

# Paths
BASE_DIR = Path("/home/bdg20b/mimic-project")
SUBJECT_IDS_PATH = BASE_DIR / "data/icu_unique_subject_ids.csv"

def create_index(target_file_id=None):
    """
    Generates byte-offset indices for specified file(s).
    
    Args:
        target_file_id (str, optional): The file_id to index (e.g., 'chartevents'). 
                                      If 'all' or None, indexes all files in IDs.
    """
    
    # Determine which files to process
    if target_file_id and target_file_id.lower() != 'all':
        if target_file_id not in IDs:
            print(f"Error: Unknown file_id '{target_file_id}'. Available: {list(IDs.keys())}")
            return
        files_to_process = [target_file_id]
    else:
        files_to_process = list(IDs.keys())
        
    print(f"Starting index generation for: {files_to_process}")
    
    for file_id in files_to_process:
        print(f"\n=== Processing {file_id} ===")
        metadata = IDs[file_id]
        
        # Construct full path based on metadata location
        # We assume the location in IDs is relative to project root or needs BASE_DIR
        # The IDs dict has "location": "physionet.org/..."
        
        if "location" in metadata:
            file_path = BASE_DIR / metadata["location"]
        else:
            # Fallback or error if location not in metadata (though we saw it in filtering.py)
            # For now, we might need to hardcode or rely on it being there.
            # Based on previous view of filtering.py, it has "location".
            print(f"Skipping {file_id}: No location specified in metadata.")
            continue
            
        if not file_path.exists():
            print(f"Warning: File {file_path} not found. Skipping.")
            continue
            
        try:
            filter_obj = Filter(str(file_path), file_id)
            filter_obj.generate_byte_index(str(SUBJECT_IDS_PATH))
        except Exception as e:
            print(f"Error processing {file_id}: {e}")
            
    print("\nAll requested indexing operations completed.")

if __name__ == "__main__":
    create_index()
