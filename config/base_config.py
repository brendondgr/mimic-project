"""Base configuration for Flask applications."""


class Config:
    """Base configuration class for all Flask apps."""

    DEBUG = False
    TESTING = False
    JSON_SORT_KEYS = False

    @staticmethod
    def init_app(app):
        """Initialize app with base configuration."""
        pass
