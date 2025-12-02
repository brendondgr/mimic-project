"""Data Flask Application Factory."""

from flask import Flask
from config.base_config import Config as BaseConfig
from .routes import data_bp


def create_data_app():
    """Create and configure the Data Flask application.
    
    Returns:
        Flask: Configured Flask application instance
    """
    app = Flask(__name__)
    app.config.from_object('apps.data.config.Config')
    
    # Initialize base config
    BaseConfig.init_app(app)
    
    # Register blueprint
    app.register_blueprint(data_bp)
    
    return app
