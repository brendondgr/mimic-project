"""Base configuration for Flask applications."""

import os
from datetime import timedelta


class Config:
    """Universal base configuration class for all Flask apps."""
    # =================== Project Core Settings ===================
    ROOT_URL = '/home/bdg20b/mimic-project/'

    # ==================== Flask Core Settings ====================
    DEBUG = False
    TESTING = False
    PROPAGATE_EXCEPTIONS = True
    PRESERVE_CONTEXT_ON_EXCEPTION = True
    
    # ==================== Logging ====================
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
    
    # ==================== API Settings ====================
    JSON_API_SORT_KEYS = False
    RESTFUL_JSON = {
        'sort_keys': False,
    }
    
    # ==================== Static & Template Paths ====================
    # Note: STATIC_FOLDER is defined per-app in individual config files
    STATIC_URL_PATH = '/static'
    
    @staticmethod
    def init_app(app):
        """Initialize app with base configuration.
        
        Args:
            app: Flask application instance to initialize
        """
        # Ensure required directories exist
        os.makedirs(Config.LOG_DIR, exist_ok=True)
        os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
