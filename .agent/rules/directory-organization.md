---
trigger: model_decision
description: This rule should be applied when creating new files, adjusting folder structures, or creating new Flask Applications
---

# Project Structure

## Description
The goal of this project directory is to make it that multiple Flask apps can be stored into one to make utilizing Dataset Information easier.

## File Structure

The following is the general file structure of the project

```text
project_root/
│
├── main.py                          # Entry point with flag handling
│
├── apps/                            # All Flask applications
│   ├── __init__.py
│   │
│   ├── app1/                        # First Flask application
│   │   ├── __init__.py              # App factory (APP_NAME1())
│   │   ├── routes.py                # Routes/blueprints for app1
│   │   ├── config.py                # App-specific config
│       ├── static/
│   │   └── templates/               # App1-specific templates
│   │
│   ├── app2/                        # Second Flask application
│   │   ├── __init__.py              # App factory (APP_NAME2())
│   │   ├── routes.py                # Routes/blueprints for app2
│   │   ├── config.py                # App-specific config
│       ├── static/
│   │   └── templates/               # App2-specific templates
│   │
│   └── app3/                        # Third Flask application
│       ├── __init__.py
│       ├── routes.py
│       ├── config.py
│       ├── static/
│       └── templates/
│
├── utils/                           # General utilities
│   ├── __init__.py
│   ├── download/                    # Instructions for Downloading
│   ├── hardware/                    # Contains Python Scripts for Obtaining PC Info
│   └── logger.py                    # Logging Configuration
│
├── config/                          # Shared configurations
│   ├── __init__.py
│   └── base_config.py               # Base configuration class
│
└── requirements.txt
```

We structure the project in this fashion to ensure that Apps, folders, etc are created in a structured fashion.

## Apps
Apps will be created using Flask Blueprints. It should contain an init.py:
```python
from flask import Flask
from .routes import app1_bp
from utils.logging import setup_logging

def create_app1():
    app = Flask(__name__)
    app.config.from_object('apps.app1.config.Config')
    
    # Register blueprint
    app.register_blueprint(app1_bp)
    
    # Setup utilities
    setup_logging(app)
    
    return app
```

As well as a routes.py:
```python
from flask import Blueprint
from utils.helpers import some_utility_function

app1_bp = Blueprint('app1', __name__)

@app1_bp.route('/')
def index():
    return 'App1 Home'
```

These Python Scripts are just examples as to what is needed. Then, in `main.py`, alter --app flag, to take in our new app's name as an option, which will then open this particular Flask App.