"""Routes for the Data Flask Application."""

from flask import Blueprint

data_bp = Blueprint('data', __name__)


@data_bp.route('/')
def index():
    """Home route for the Data application."""
    return 'Data App Home'
