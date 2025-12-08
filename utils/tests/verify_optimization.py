import sys
import os
import time
import pandas as pd
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.analysis.filtering import File_Filter
from utils.analysis.create_lookup_index import create_index

def verify(logger, file_id="chartevents"):
    """
    Verifies the optimization by checking lookup table and testing lookup speed.
    
    Args:
        logger: LoggerWrapper instance for logging
        file_id: The file ID to verify (default: chartevents)
    """
    logger.info(f"=== Verifying Optimization for {file_id} ===")
    
    lookup_path = Path("data/icu_unique_subject_ids.csv")
    if not lookup_path.exists():
        logger.error("Lookup table not found.")
        return
        
    df = pd.read_csv(lookup_path)
    
    start_col = f"{file_id}_byteidx_start"
    end_col = f"{file_id}_byteidx_end"
    
    if start_col in df.columns and end_col in df.columns:
        logger.success(f"Columns {start_col} and {end_col} found in lookup table.")
    else:
        logger.error(f"Columns {start_col} and {end_col} NOT found in lookup table.")
        return

    logger.info(f"\n=== Testing Optimized Search for {file_id} ===")
    # Pick a subject that has data (start_byte != -1)
    if start_col not in df.columns:
         logger.error("Cannot test search: columns missing.")
         return
         
    valid_subjects = df[df[start_col] != -1]
    if valid_subjects.empty:
        logger.error(f"No valid subjects with byte indices found for {file_id}.")
        return
        
    valid_subject = valid_subjects.iloc[0]['subject_id']
    logger.info(f"Testing lookup for subject: {valid_subject}")
    
    # We need to get the path from IDs dict
    from utils.analysis.filtering import IDs
    if file_id not in IDs:
        logger.error(f"Unknown file_id {file_id}")
        return
        
    file_path = IDs[file_id]["location"]
    # Prepend base dir if needed, assuming relative to project root
    # In filtering.py we saw it was relative.
    # But Filter expects full path? 
    # Let's try to construct it relative to CWD
    full_path = Path(file_path)
    if not full_path.exists():
         # Try with project root
         full_path = Path("/home/bdg20b/mimic-project") / file_path
         
    if not full_path.exists():
        logger.error(f"Data file not found at {full_path}")
        return

    # Fix: File_Filter expects file_id first, then file_path
    f = File_Filter(file_id, str(full_path))
    
    start = time.time()
    result = f.search_subject(valid_subject)
    duration = time.time() - start
    
    logger.info(f"Lookup took {duration:.4f} seconds")
    logger.info(f"Result shape: {result.shape}")
    
    if not result.empty and duration < 0.5:
        logger.success("Lookup was fast and returned data!")
    elif result.empty:
        logger.error("Result is empty.")
    else:
        logger.warning(f"Lookup was slow ({duration:.4f}s). Optimization might not be working.")
    
    logger.info("=== Verification Complete ===")

if __name__ == "__main__":
    from utils.logger import LoggerWrapper
    logger = LoggerWrapper(level="INFO")
    verify(logger)
