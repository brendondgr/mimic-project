from flask import Flask
from config.base_config import Config as BaseConfig
from .routes import bpm_bp
import pandas as pd

def load_subject_ids(app):
    """Load subject IDs from CSV into memory."""
    try:
        csv_path = app.config.get('SUBJECT_IDS_FILE')
        if csv_path:
            df = pd.read_csv(csv_path)
            if 'subject_id' in df.columns:
                app.config['SUBJECT_IDS'] = df['subject_id'].tolist()
                print(f"[BPM App] Loaded {len(app.config['SUBJECT_IDS'])} subject IDs.")
            else:
                print("[BPM App] Error: 'subject_id' column not found in CSV.")
                app.config['SUBJECT_IDS'] = []
        else:
            print("[BPM App] Error: SUBJECT_IDS_FILE path not set in config.")
            app.config['SUBJECT_IDS'] = []
    except Exception as e:
        print(f"[BPM App] Error loading subject IDs: {e}")
        app.config['SUBJECT_IDS'] = []

def create_bpm_app():
    """Create and configure the BPM Flask application.
    
    Returns:
        Flask: Configured Flask application instance
    """
    app = Flask(__name__)
    app.config.from_object('apps.bpm.config.Config')
    
    # Initialize base config
    BaseConfig.init_app(app)
    
    # Register blueprint
    app.register_blueprint(bpm_bp)
    
    # Load data
    with app.app_context():
        load_subject_ids(app)
    
    return app
