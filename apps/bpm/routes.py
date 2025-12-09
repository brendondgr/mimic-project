"""Routes for the BPM Flask Application."""

import json
import random
import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from flask import Blueprint, render_template, request, jsonify, current_app

# Import File_Filter
try:
    from utils.analysis.filters.file_filter import File_Filter
except ImportError:
    # Fallback if running from a different context, though in app it should work
    from utils.analysis.filters.file_filter import File_Filter

bpm_bp = Blueprint('bpm', __name__, template_folder='templates', static_folder='static')

@bpm_bp.route('/')
def index():
    """Render the main page."""
    # Get subject IDs from config
    available_ids = current_app.config.get('SUBJECT_IDS', [])
    # Limit to 5000 for frontend performance; 65k is too heavy for a simple select
    display_ids = available_ids[:5000] if available_ids else []
    return render_template('index.html', subject_ids=display_ids, total_count=len(available_ids))

@bpm_bp.route('/api/load-data', methods=['POST'])
def load_data():
    """Load and process BPM data for a subject."""
    try:
        data = request.get_json()
        subject_id = data.get('subject_id')
        is_random = data.get('random', False)
        
        # Valid subject ID logic
        available_ids = current_app.config.get('SUBJECT_IDS', [])
        
        if is_random:
            if not available_ids:
                return jsonify({'error': 'No subject IDs available to select from.'}), 500
            subject_id = random.choice(available_ids)
        else:
            if not subject_id:
                return jsonify({'error': 'Subject ID is required.'}), 400
            try:
                subject_id = int(subject_id)
            except ValueError:
                 return jsonify({'error': 'Subject ID must be a number.'}), 400
            
            # Optional: Validation against available list?
            # The plan says "Validate subject_id exists in list (if user-specified)"
            if available_ids and subject_id not in available_ids:
                 return jsonify({'error': f'Subject ID {subject_id} not found in the dataset.'}), 404

        # Load data using File_Filter
        try:
            # "Initialize File_Filter('chartevents')"
            ff = File_Filter("chartevents")
            # "Call file_filter.search_subject(subject_id)"
            df = ff.search_subject(subject_id)
        except Exception as e:
            return jsonify({'error': f'Error loading data: {str(e)}'}), 500
        
        if df.empty:
             return jsonify({'error': f'No data found for subject {subject_id}.'}), 404
        
        # Filter for BPM
        if 'valueuom' in df.columns:
            bpm_df = df[df['valueuom'] == 'bpm'].copy()
        else:
            # Fallback or check if 'itemid' corresponds to Heart Rate?
            # Plan says "Filter rows where valueuom == 'bpm'"
            # If column missing, empty
            bpm_df = pd.DataFrame()
        
        if bpm_df.empty:
            return jsonify({'error': f'No Heart Rate (BPM) data found for subject {subject_id}.'}), 404
            
        # Prepare Data
        # Sort by charttime
        if 'charttime' in bpm_df.columns:
            bpm_df['charttime'] = pd.to_datetime(bpm_df['charttime'])
            bpm_df = bpm_df.sort_values('charttime')
        
        valuenum = bpm_df['valuenum'].tolist()
        charttime = bpm_df['charttime'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist() if 'charttime' in bpm_df.columns else []

        # Create Visualizations
        graphs = create_plots(valuenum, charttime, subject_id)
        
        return jsonify({
            'subject_id': subject_id,
            'box_plot': graphs['box_plot'],
            'line_graph': graphs['line_graph']
        })

    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500

def create_plots(values, timestamps, subject_id):
    """Generate Plotly JSON for Box Plot and Line Graph adhering to dark theme."""
    
    # Theme Colors
    BG_COLOR = '#111827'
    TEXT_PRIMARY = '#F9FAFB'
    TEXT_SECONDARY = '#9CA3AF'
    GRID_COLOR = '#374151'
    SPINE_COLOR = '#D1D5DB'
    PRIMARY_COLOR = '#2563EB' # Box plot elements and Line color
    
    common_layout = dict(
        autosize=True,
        paper_bgcolor=BG_COLOR,
        plot_bgcolor=BG_COLOR,
        font=dict(family="Inter, sans-serif", color=TEXT_PRIMARY), # Match CSS font
        margin=dict(t=50, l=50, r=20, b=50),
        xaxis=dict(
            gridcolor=GRID_COLOR,
            gridwidth=0.5,
            linecolor=SPINE_COLOR,
            linewidth=0.8,
            tickcolor=SPINE_COLOR,
            tickfont=dict(size=10, color=TEXT_SECONDARY),
            title_font=dict(size=12, family="sans-serif", weight="bold")
        ),
        yaxis=dict(
            gridcolor=GRID_COLOR,
            gridwidth=0.5,
            linecolor=SPINE_COLOR,
            linewidth=0.8,
            tickcolor=SPINE_COLOR,
            tickfont=dict(size=10, color=TEXT_SECONDARY),
            title_font=dict(size=12, family="sans-serif", weight="bold")
        )
    )

    # 1. Box Plot
    box_fig = go.Figure()
    box_fig.add_trace(go.Box(
        y=values,
        name="BPM",
        marker_color=PRIMARY_COLOR,
        boxpoints='outliers', # specific
        line_width=1.5
    ))
    
    box_layout = common_layout.copy()
    box_layout['title'] = dict(
        text=f"Heart Rate Distribution",
        x=0, # Left aligned
        font=dict(size=18, family="sans-serif", weight="bold")
    )
    box_layout['yaxis'] = common_layout['yaxis'].copy()
    box_layout['yaxis']['title'] = "BPM"
    box_layout['xaxis'] = common_layout['xaxis'].copy()
    box_layout['xaxis']['showticklabels'] = False # Hide x tick labels for single box
    
    box_fig.update_layout(**box_layout)

    # 2. Line Graph
    line_fig = go.Figure()
    line_fig.add_trace(go.Scatter(
        x=timestamps,
        y=values,
        mode='lines',
        name='BPM',
        line=dict(color=PRIMARY_COLOR, width=2.5)
    ))
    
    line_layout = common_layout.copy()
    line_layout['title'] = dict(
        text=f"Heart Rate Over Time (Subject {subject_id})",
        x=0,
        font=dict(size=18, family="sans-serif", weight="bold")
    )
    line_layout['xaxis'] = common_layout['xaxis'].copy()
    line_layout['xaxis']['title'] = "Time"
    line_layout['yaxis'] = common_layout['yaxis'].copy()
    line_layout['yaxis']['title'] = "BPM"
    
    line_fig.update_layout(**line_layout)
    
    return {
        'box_plot': json.loads(json.dumps(box_fig, cls=plotly.utils.PlotlyJSONEncoder)),
        'line_graph': json.loads(json.dumps(line_fig, cls=plotly.utils.PlotlyJSONEncoder))
    }
