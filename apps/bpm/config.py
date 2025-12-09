"""Configuration for the BPM Flask Application."""

import os
from config.base_config import Config as BaseConfig

class Config(BaseConfig):
    """BPM app specific configuration.
    
    Inherits all universal settings from BaseConfig.
    """
    
    # ==================== App-Specific Static Path ====================
    STATIC_FOLDER = os.path.join(os.path.dirname(__file__), 'static')
    
    # ==================== Data Paths ====================
    # Path to the unique subject IDs CSV
    SUBJECT_IDS_FILE = os.path.join(os.getcwd(), 'data', 'icu_unique_subject_ids.csv')
