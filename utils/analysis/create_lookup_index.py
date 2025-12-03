import os
from pathlib import Path
from utils.analysis.filtering import Filter

# Paths
BASE_DIR = Path("/home/bdg20b/mimic-project")
CHARTEVENTS_PATH = BASE_DIR / "physionet.org/files/mimiciv/3.1/icu/chartevents.csv.gz"
SUBJECT_IDS_PATH = BASE_DIR / "data/icu_unique_subject_ids.csv"

def create_index():
    print("Initializing Filter for chartevents...")
    # We use the file_id "chartevents" which is defined in filtering.py IDs dict
    # Note: The IDs dict in filtering.py must have "chartevents" with the correct metadata.
    # We assume the file path in the IDs dict matches or we pass it explicitly if the class supports it.
    # The current Filter __init__ takes file_path and file_id.
    
    # Check if file exists
    if not CHARTEVENTS_PATH.exists():
        print(f"Error: {CHARTEVENTS_PATH} not found.")
        return

    filter_obj = Filter(str(CHARTEVENTS_PATH), "chartevents")
    
    print("Generating byte index...")
    filter_obj.generate_byte_index(str(SUBJECT_IDS_PATH))
    
    print("Done.")

if __name__ == "__main__":
    create_index()
