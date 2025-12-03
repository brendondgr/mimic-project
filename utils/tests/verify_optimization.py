import sys
import os
import time
import pandas as pd
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.analysis.filtering import Filter
from utils.analysis.create_lookup_index import create_index

def verify(logger):
    """
    Verifies the optimization by running the indexing process and testing lookup speed.
    
    Args:
        logger: LoggerWrapper instance for logging
    """
    logger.info("=== 1. Running Indexing Process ===")
    logger.info("This may take several minutes for a 3.3GB file...")
    
    # This might take a while, but it's necessary to verify the full flow
    create_index()
    
    logger.info("\n=== 2. Verifying Lookup Table ===")
    lookup_path = Path("data/icu_unique_subject_ids.csv")
    df = pd.read_csv(lookup_path)
    
    if 'chartevents_byteidx_start' in df.columns and 'chartevents_byteidx_end' in df.columns:
        logger.success("New columns found in lookup table.")
        logger.info(f"Sample data:\n{df[['subject_id', 'chartevents_byteidx_start', 'chartevents_byteidx_end']].head()}")
    else:
        logger.error("New columns NOT found in lookup table.")
        return

    logger.info("\n=== 3. Testing Optimized Search ===")
    # Pick a subject that has data (start_byte != -1)
    valid_subjects = df[df['chartevents_byteidx_start'] != -1]
    if valid_subjects.empty:
        logger.error("No valid subjects with byte indices found.")
        return
        
    valid_subject = valid_subjects.iloc[0]['subject_id']
    logger.info(f"Testing lookup for subject: {valid_subject}")
    
    chartevents_path = "physionet.org/files/mimiciv/3.1/icu/chartevents.csv.gz"
    f = Filter(chartevents_path, "chartevents")
    
    start = time.time()
    result = f.search_subject(valid_subject)
    duration = time.time() - start
    
    logger.info(f"Lookup took {duration:.4f} seconds")
    logger.info(f"Result shape: {result.shape}")
    
    if not result.empty and duration < 0.5:
        logger.success("Lookup was fast and returned data!")
        logger.info(f"Retrieved {len(result)} records in {duration:.4f}s")
    elif result.empty:
        logger.error("Result is empty.")
    else:
        logger.warning(f"Lookup was slow ({duration:.4f}s). Optimization might not be working.")
    
    logger.info("=== Verification Complete ===")

if __name__ == "__main__":
    from utils.logger import LoggerWrapper
    logger = LoggerWrapper(level="INFO")
    verify(logger)
