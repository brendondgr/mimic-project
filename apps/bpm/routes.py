"""Routes for the BPM Flask Application."""

import json
import random
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from scipy.interpolate import BarycentricInterpolator, CubicSpline, PchipInterpolator
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
        start_time = times.min()
        
        # Calculate seconds from start
        x = (times - start_time).dt.total_seconds().to_numpy()
        y = df['val'].to_numpy()

        # Determine query points (denser grid for visualization)
        if len(x) < 2:
            return values, timestamps

        # Visualization Grid:
        # We want a smooth line, so we need a dense grid.
        # BUT we also want to ensure the line passes exactly through the original points (knots).
        # So we union the dense grid with the original x values.
        
        num_points = max(200, len(x) * 5)
        grid = np.linspace(x.min(), x.max(), num_points)
        x_new = np.unique(np.concatenate((x, grid)))
        x_new.sort()
        
        y_new = None
        
        # Method selection
        if method == 'lagrange':
             # Use BarycentricInterpolator for better numerical stability than lagrange()
             # To avoid extreme Runge's phenomenon with many data points,
             # we limit to 20 evenly-spaced points across the entire dataset.
             # This keeps polynomial degree manageable while covering the full time range.
             
             if len(x) > 20:
                 # Select 20 evenly-spaced indices across the entire dataset
                 indices = np.linspace(0, len(x) - 1, 20, dtype=int)
                 x_active = x[indices]
                 y_active = y[indices]
                 
                 # Interpolate over the entire range
                 poly = BarycentricInterpolator(x_active, y_active)
                 y_new = poly(x_new)
             else:
                 # Use all points if we have 20 or fewer
                 poly = BarycentricInterpolator(x, y)
                 y_new = poly(x_new)
                 
        elif method == 'cubic_spline':
             cs = CubicSpline(x, y, bc_type='natural')
             y_new = cs(x_new)
        elif method == 'cubic_hermite':
             pch = PchipInterpolator(x, y)
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
        if 'charttime' in bpm_df.columns:
            bpm_df['charttime'] = pd.to_datetime(bpm_df['charttime'])
            bpm_df = bpm_df.sort_values('charttime')
        
        # Outlier Detection (IQR Method)
        outliers_list = []
        if not bpm_df.empty and len(bpm_df) > 1:
            Q1 = bpm_df['valuenum'].quantile(0.25)
            Q3 = bpm_df['valuenum'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outlier_mask = (bpm_df['valuenum'] < lower_bound) | (bpm_df['valuenum'] > upper_bound)
            outliers_df = bpm_df[outlier_mask].copy()
            clean_df = bpm_df[~outlier_mask].copy()
            
            # Prepare outliers for table
            if not outliers_df.empty:
                o_vals = outliers_df['valuenum'].tolist()
                o_times = outliers_df['charttime'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
                for v, t in zip(o_vals, o_times):
                    outliers_list.append({'val': v, 'time': t})
        else:
            clean_df = bpm_df.copy()

        # Averaging Duplicates on Clean Data
        valuenum = []
        charttime = []
        point_types = []
        
        if not clean_df.empty:
            # Group by charttime to average duplicates
            grouped = clean_df.groupby('charttime')['valuenum'].agg(['mean', 'count']).reset_index()
            grouped = grouped.sort_values('charttime')
            
            valuenum = grouped['mean'].tolist()
            # If count > 1, it was averaged
            point_types = ['Averaged' if c > 1 else 'Original' for c in grouped['count']]
            charttime = grouped['charttime'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()

        # Calculate Statistics on the processed (clean + averaged) data? 
        # Usually stats should represent the valid dataset used for analysis
        stats = {}
        if valuenum:
            v_series = pd.Series(valuenum)
            stats = {
                'count': int(len(v_series)),
                'mean': round(v_series.mean(), 1),
                'median': round(v_series.median(), 1),
                'min': round(v_series.min(), 1),
                'max': round(v_series.max(), 1),
                'std': round(v_series.std(), 1) if len(v_series) > 1 else 0.0
            }

        # Handle Interpolation
        interpolated_values = None
        interpolated_timestamps = None
        
        if interpolation_method and interpolation_method != 'none':
            interpolated_values, interpolated_timestamps = apply_interpolation(valuenum, charttime, interpolation_method)

        # Create Visualizations and Table Data
        vis_data = create_line_plot(
            valuenum, 
            charttime, 
            subject_id, 
            interpolated_values, 
            interpolated_timestamps, 
            interpolation_method,
            point_types=point_types,
            outliers=outliers_list
        )
        
        return jsonify({
            'subject_id': subject_id,
            'raw_data': {
                'values': valuenum, 
                'timestamps': charttime,
                'types': point_types,
                'outliers': outliers_list
            },
            'interpolated_data': {'values': interpolated_values, 'timestamps': interpolated_timestamps} if interpolated_values else None,
            'interpolation_method': interpolation_method,
            'line_graph': vis_data['line_graph'],
            'table_data': vis_data['table_data'],
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
        point_types = raw_data.get('types', [])
        outliers = raw_data.get('outliers', [])
        
        if not raw_values or not raw_timestamps:
             return jsonify({'error': 'Raw data missing'}), 400
             
        interpolated_values, interpolated_timestamps = apply_interpolation(raw_values, raw_timestamps, interpolation_method)
        
        vis_data = create_line_plot(
            raw_values, 
            raw_timestamps, 
            subject_id, 
            interpolated_values, 
            interpolated_timestamps, 
            interpolation_method,
            point_types=point_types,
            outliers=outliers
        )
        
        return jsonify({
            'line_graph': vis_data['line_graph'],
            'table_data': vis_data['table_data'],
            'interpolated_data': {'values': interpolated_values, 'timestamps': interpolated_timestamps} if interpolated_values else None
        })
        
    except Exception as e:
        return jsonify({'error': f'Error applying interpolation: {str(e)}'}), 500

@bpm_bp.route('/api/save-graph', methods=['POST'])
def save_graph():
    try:
        data = request.get_json()
        image_data = data.get('image_data') # base64 string
        subject_id = data.get('subject_id')
        method = data.get('method', 'none')
        
        if not image_data or not subject_id:
            return jsonify({'error': 'Missing data'}), 400

        # Remove header if present (data:image/png;base64,)
        if ',' in image_data:
            image_data = image_data.split(',')[1]
            
        # Decode
        import base64
        import os
        img_bytes = base64.b64decode(image_data)
        
        # Define path: data/apps/bpm/ from root (cwd)
        save_dir = os.path.join(os.getcwd(), 'data', 'apps', 'bpm')
        os.makedirs(save_dir, exist_ok=True)
        
        # Filename: {method}_{subject_id}.png
        filename = f"{method}_{subject_id}.png"
        filepath = os.path.join(save_dir, filename)
        
        with open(filepath, 'wb') as f:
            f.write(img_bytes)
            
        return jsonify({'message': 'Graph saved successfully', 'path': filepath})
        
    except Exception as e:
        print(f"Save error: {e}")
        return jsonify({'error': f'Error saving graph: {str(e)}'}), 500

def create_line_plot(raw_values, raw_timestamps, subject_id, interpolated_values=None, interpolated_timestamps=None, method=None, point_types=None, outliers=None):
    """Generate Plotly JSON for Line Graph with relative time axes and generate table data."""
    
    # Helper to calculate relative times
    def get_relative_data(timestamps, start_time):
        if not timestamps:
            return [], []
        
        # Convert to datetime if not already
        ts = pd.to_datetime(timestamps)
        
        # Calculate elapsed hours
        # ts - start_time returns a TimedeltaIndex, which supports total_seconds() directly
        elapsed_seconds = (ts - start_time).total_seconds()
        elapsed_hours = elapsed_seconds / 3600.0
        
        # Create formatted strings: "Day X, Y.Yh"
        # Day 1 starts at 0h. 
        days = (elapsed_hours // 24).astype(int) + 1
        formatted = [f"Day {d}, {h:.1f}h" for d, h in zip(days, elapsed_hours)]
        
        return elapsed_hours.tolist(), formatted

    # Establish global start time from raw data
    raw_ts_dt = pd.to_datetime(raw_timestamps)
    if raw_ts_dt.empty:
        return {'line_graph': {}, 'table_data': []}
        
    start_time = raw_ts_dt.min()
    
    # Process Raw Data
    raw_hours, raw_labels = get_relative_data(raw_timestamps, start_time)
    
    # Process Interpolated Data
    interp_hours, interp_labels = [], []
    if interpolated_values and interpolated_timestamps:
        interp_hours, interp_labels = get_relative_data(interpolated_timestamps, start_time)

    # Theme Colors
    BG_COLOR = '#111827'
    TEXT_PRIMARY = '#F9FAFB'
    TEXT_SECONDARY = '#9CA3AF'
    GRID_COLOR = '#374151'
    SPINE_COLOR = '#D1D5DB'
    PRIMARY_COLOR = '#2563EB' 
    ACCENT_COLOR = '#10B981' 
    
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
            title_font=dict(size=12, family="sans-serif", weight="bold"),
            zeroline=False
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

    # Trace 1: Original Data
    mode = 'markers' if interpolated_values else 'lines+markers'
    
    line_fig.add_trace(go.Scatter(
        x=raw_hours,
        y=raw_values,
        mode=mode,
        name='Original Data',
        customdata=raw_labels,
        hovertemplate='%{customdata}<br>BPM: %{y:.1f}<extra></extra>',
        marker=dict(color=PRIMARY_COLOR, size=8),
        line=dict(color=PRIMARY_COLOR, width=2)
    ))

    # Trace 2: Interpolated Data
    if interpolated_values and interpolated_timestamps:
        label = f"Interpolated ({method})"
        line_fig.add_trace(go.Scatter(
            x=interp_hours,
            y=interpolated_values,
            mode='lines',
            name=label,
            customdata=interp_labels,
            hovertemplate='%{customdata}<br>BPM: %{y:.1f}<extra></extra>',
            line=dict(color=ACCENT_COLOR, width=3)
        ))
    
    line_layout = common_layout.copy()
    line_layout['title'] = dict(
        text=f"Heart Rate Over Time (Subject {subject_id})",
        x=0.5,
        font=dict(size=18, family="sans-serif", weight="bold")
    )
    line_layout['xaxis'] = common_layout['xaxis'].copy()
    line_layout['xaxis']['title'] = "Time (Hours from Start)"
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
    
    
    # Prepare Table Data
    table_rows = []
    
    # helper for constructing row
    def add_row(t_str, h, val, type_str):
        table_rows.append({
            'time': t_str,
            'hours': h,
            'bpm': val,
            'type': type_str
        })
    
    # Original / Averaged Data
    # point_types might be None if coming from old context, default to Original
    if point_types is None:
        point_types = ['Original'] * len(raw_values)
        
    for t_str, h, val, p_type in zip(raw_labels, raw_hours, raw_values, point_types):
        add_row(t_str, h, val, p_type)

    # Outliers
    # We need to calculate relative time for outliers too
    if outliers:
        o_vals = [o['val'] for o in outliers]
        o_times = [o['time'] for o in outliers]
        o_hours, o_labels = get_relative_data(o_times, start_time)
        
        for t_str, h, val in zip(o_labels, o_hours, o_vals):
            add_row(t_str, h, val, "Outlier")

    # Interpolated Data - ONLY if requested
    if interpolated_values:
         for t_str, h, val in zip(interp_labels, interp_hours, interpolated_values):
            add_row(t_str, h, val, f'Interpolated ({method})')
            
    # Sort by time
    table_rows.sort(key=lambda x: x['hours'])
    
    return {
        'line_graph': json.loads(json.dumps(line_fig, cls=plotly.utils.PlotlyJSONEncoder)),
        'table_data': table_rows
    }
