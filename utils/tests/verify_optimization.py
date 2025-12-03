import sys
import os
import time
import pandas as pd
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.analysis.filtering import Filter
from utils.analysis.create_lookup_index import create_index

def verify():
    print("=== 1. Running Indexing Process ===")
    # This might take a while, but it's necessary to verify the full flow
    create_index()
    
    print("\n=== 2. Verifying Lookup Table ===")
    lookup_path = Path("data/icu_unique_subject_ids.csv")
    df = pd.read_csv(lookup_path)
    
    if 'chartevents_byteidx_start' in df.columns and 'chartevents_byteidx_end' in df.columns:
        print("SUCCESS: New columns found in lookup table.")
        print(df[['subject_id', 'chartevents_byteidx_start', 'chartevents_byteidx_end']].head())
    else:
        print("FAILURE: New columns NOT found in lookup table.")
        return

    print("\n=== 3. Testing Optimized Search ===")
    # Pick a subject that has data (start_byte != -1)
    valid_subject = df[df['chartevents_byteidx_start'] != -1].iloc[0]['subject_id']
    print(f"Testing lookup for subject: {valid_subject}")
    
    chartevents_path = "physionet.org/files/mimiciv/3.1/icu/chartevents.csv.gz"
    f = Filter(chartevents_path, "chartevents")
    
    start = time.time()
    result = f.search_subject(valid_subject)
    duration = time.time() - start
    
    print(f"Lookup took {duration:.4f} seconds")
    print(f"Result shape: {result.shape}")
    
    if not result.empty and duration < 0.5:
        print("SUCCESS: Lookup was fast and returned data.")
    elif result.empty:
        print("FAILURE: Result is empty.")
    else:
        print(f"WARNING: Lookup was slow ({duration:.4f}s). Optimization might not be working.")

if __name__ == "__main__":
    verify()
