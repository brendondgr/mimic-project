"""Routes for the BPM Flask Application."""

import json
import random
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from scipy.interpolate import lagrange, CubicSpline, PchipInterpolator
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

def apply_interpolation(values, timestamps, method):
    """
    Apply interpolation to the data.
    Returns interpolated values and timestamps (dense format for smooth plotting).
    """
    if not values or not timestamps:
        return None, None
    
    try:
        # Create DataFrame to handle duplicates and sorting easily
        df = pd.DataFrame({'val': values, 'time': pd.to_datetime(timestamps)})
        
        # Drop rows with missing values
        df = df.dropna()
        
        if df.empty:
            return None, None
            
        # Group by time and take mean to handle duplicates (interpolation requires strictly increasing x)
        df = df.groupby('time')['val'].mean().reset_index()
        df = df.sort_values('time')
        
        times = df['time']
        y = df['val'].to_numpy()
        
        start_time = times.min()
        # Calculate seconds from start
        x = (times - start_time).dt.total_seconds().to_numpy()

        # Determine query points (denser grid for visualization)
        if len(x) < 2:
            return values, timestamps

        num_points = max(200, len(x) * 5)
        # If method is lagrange and N > 20, we must limit scope or points
        if method == 'lagrange' and len(x) > 20:
             # Limit to last 20 points
             sub_mask = np.arange(len(x)) >= (len(x) - 20)
             x_active = x[sub_mask]
             y_active = y[sub_mask]
             x_new = np.linspace(x_active.min(), x_active.max(), num_points)
             poly = lagrange(x_active, y_active)
             y_new = poly(x_new)
        else:
             x_active = x
             y_active = y
             x_new = np.linspace(x.min(), x.max(), num_points)
             
             if method == 'lagrange':
                 poly = lagrange(x_active, y_active)
                 y_new = poly(x_new)
             elif method == 'cubic_spline':
                 cs = CubicSpline(x_active, y_active, bc_type='natural')
                 y_new = cs(x_new)
             elif method == 'cubic_hermite':
                 pch = PchipInterpolator(x_active, y_active)
                 y_new = pch(x_new)
             else:
                 return None, None

        # Convert back to timestamps
        t_new = start_time + pd.to_timedelta(x_new, unit='s')
        return y_new.tolist(), t_new.strftime('%Y-%m-%d %H:%M:%S').tolist()

    except Exception as e:
        print(f"Interpolation error: {e}")
        return None, None

@bpm_bp.route('/api/load-data', methods=['POST'])
def load_data():
    """Load and process BPM data for a subject."""
    try:
        data = request.get_json()
        subject_id = data.get('subject_id')
        is_random = data.get('random', False)
        interpolation_method = data.get('interpolation_method', 'none')
        
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

        # Calculate Statistics
        stats = {}
        if not bpm_df.empty:
            stats = {
                'count': int(len(bpm_df)),
                'mean': round(bpm_df['valuenum'].mean(), 1),
                'median': round(bpm_df['valuenum'].median(), 1),
                'min': round(bpm_df['valuenum'].min(), 1),
                'max': round(bpm_df['valuenum'].max(), 1),
                'std': round(bpm_df['valuenum'].std(), 1) if len(bpm_df) > 1 else 0.0
            }

        # Handle Interpolation
        interpolated_values = None
        interpolated_timestamps = None
        
        if interpolation_method and interpolation_method != 'none':
            interpolated_values, interpolated_timestamps = apply_interpolation(valuenum, charttime, interpolation_method)

        # Create Visualizations
        graphs = create_line_plot(valuenum, charttime, subject_id, interpolated_values, interpolated_timestamps, interpolation_method)
        
        return jsonify({
            'subject_id': subject_id,
            'raw_data': {'values': valuenum, 'timestamps': charttime},
            'interpolated_data': {'values': interpolated_values, 'timestamps': interpolated_timestamps} if interpolated_values else None,
            'interpolation_method': interpolation_method,
            'line_graph': graphs['line_graph'],
            'statistics': stats
        })
    
    except Exception as e:
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500

@bpm_bp.route('/api/apply-interpolation', methods=['POST'])
def apply_interpolation_route():
    try:
        data = request.get_json()
        subject_id = data.get('subject_id')
        interpolation_method = data.get('interpolation_method')
        raw_data = data.get('raw_data', {})
        
        raw_values = raw_data.get('values', [])
        raw_timestamps = raw_data.get('timestamps', [])
        
        if not raw_values or not raw_timestamps:
             return jsonify({'error': 'Raw data missing'}), 400
             
        interpolated_values, interpolated_timestamps = apply_interpolation(raw_values, raw_timestamps, interpolation_method)
        
        graphs = create_line_plot(
            raw_values, 
            raw_timestamps, 
            subject_id, 
            interpolated_values, 
            interpolated_timestamps, 
            interpolation_method
        )
        
        return jsonify({
            'line_graph': graphs['line_graph'],
            'interpolated_data': {'values': interpolated_values, 'timestamps': interpolated_timestamps} if interpolated_values else None
        })
        
    except Exception as e:
        return jsonify({'error': f'Error applying interpolation: {str(e)}'}), 500

def create_line_plot(raw_values, raw_timestamps, subject_id, interpolated_values=None, interpolated_timestamps=None, method=None):
    """Generate Plotly JSON for Line Graph with optional Interpolation adhering to dark theme."""
    
    # Theme Colors
    BG_COLOR = '#111827'
    TEXT_PRIMARY = '#F9FAFB'
    TEXT_SECONDARY = '#9CA3AF'
    GRID_COLOR = '#374151'
    SPINE_COLOR = '#D1D5DB'
    PRIMARY_COLOR = '#2563EB' 
    ACCENT_COLOR = '#10B981' # Green for interpolation
    
    common_layout = dict(
        autosize=True,
        paper_bgcolor=BG_COLOR,
        plot_bgcolor=BG_COLOR,
        font=dict(family="Inter, sans-serif", color=TEXT_PRIMARY),
        margin=dict(t=50, l=40, r=20, b=40),
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

    # Line Graph
    line_fig = go.Figure()

    # Trace 1: Original Data (Scatter/Markers)
    # If interpolation is present, we clearly show original as markers
    # If no interpolation, can be lines+markers or just lines. User requested "Original data points as scatter plot"
    
    mode = 'markers' if interpolated_values else 'lines+markers'
    
    line_fig.add_trace(go.Scatter(
        x=raw_timestamps,
        y=raw_values,
        mode=mode,
        name='Original Data',
        marker=dict(color=PRIMARY_COLOR, size=8),
        line=dict(color=PRIMARY_COLOR, width=2)
    ))

    # Trace 2: Interpolated Data (Line)
    if interpolated_values and interpolated_timestamps:
        label = f"Interpolated ({method})"
        line_fig.add_trace(go.Scatter(
            x=interpolated_timestamps,
            y=interpolated_values,
            mode='lines',
            name=label,
            line=dict(color=ACCENT_COLOR, width=3, shape='spline' if 'cubic' in str(method) else 'linear')
        ))
    
    line_layout = common_layout.copy()
    line_layout['title'] = dict(
        text=f"Heart Rate Over Time (Subject {subject_id})",
        x=0.5,
        font=dict(size=18, family="sans-serif", weight="bold")
    )
    line_layout['xaxis'] = common_layout['xaxis'].copy()
    line_layout['xaxis']['title'] = "Time"
    line_layout['yaxis'] = common_layout['yaxis'].copy()
    line_layout['yaxis']['title'] = "BPM"
    line_layout['legend'] = dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1
    )
    
    line_fig.update_layout(**line_layout)
    
    return {
        'line_graph': json.loads(json.dumps(line_fig, cls=plotly.utils.PlotlyJSONEncoder))
    }
