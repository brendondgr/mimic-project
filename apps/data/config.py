"""Configuration for the Data Flask Application."""

import os
from config.base_config import Config as BaseConfig


class Config(BaseConfig):
    """Data app specific configuration.
    
    Inherits all universal settings from BaseConfig.
    Add app-specific overrides or settings below.
    """
    
    # ==================== App-Specific Static Path ====================
    STATIC_FOLDER = os.path.join(os.path.dirname(__file__), 'static')
