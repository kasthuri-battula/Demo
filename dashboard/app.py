"""
dashboard/app.py
================
Real-time anomaly detection dashboard using Streamlit.

Run with:
    streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# =============================================================================
# Page Configuration
# =============================================================================

st.set_page_config(
    page_title="Log Analytics Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# Helper Functions
# =============================================================================

@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_anomalies(file_path='data/advanced_anomalies.json'):
    """Load detected anomalies from JSON file."""
    try:
        with open(file_path, 'r') as f:
            anomalies = json.load(f)
        
        if not anomalies:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(anomalies)
        
        # Parse timestamps
        if 'detected_at' in df.columns:
            df['detected_at'] = pd.to_datetime(df['detected_at'])
        if 'window' in df.columns:
            df['window'] = pd.to_datetime(df['window'])
        
        return df
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading anomalies: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_metrics(file_path='data/metrics.csv'):
    """Load aggregated metrics from CSV file."""
    try:
        df = pd.read_csv(file_path)
        df['window'] = pd.to_datetime(df['window'])
        return df
    except FileNotFoundError:
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading metrics: {e}")
        return pd.DataFrame()


def get_severity_color(severity):
    """Get color for severity level."""
    colors = {
        'CRITICAL': '#d32f2f',
        'HIGH': '#ff9800',
        'MEDIUM': '#ffc107',
        'LOW': '#4caf50',
    }
    return colors.get(severity, '#9e9e9e')


def get_severity_emoji(severity):
    """Get emoji for severity level."""
    emojis = {
        'CRITICAL': '🔴',
        'HIGH': '🟠',
        'MEDIUM': '🟡',
        'LOW': '🟢',
    }
    return emojis.get(severity, '⚪')


# =============================================================================
# Dashboard Header
# =============================================================================

st.title("🔍 Log Analytics Dashboard")
st.markdown("Real-time anomaly detection and monitoring system")

# =============================================================================
# Sidebar Filters
# =============================================================================

st.sidebar.header("🎛️ Filters")

# Load data
anomalies_df = load_anomalies()
metrics_df = load_metrics()

if anomalies_df.empty:
    st.warning("⚠️ No anomalies detected. Run detection first:")
    st.code("python detection/advanced_detector.py --log-dir data/raw_logs")
    st.stop()

# Service filter
services = ['All'] + sorted(anomalies_df['service'].unique().tolist())
selected_service = st.sidebar.selectbox("Service", services)

# Severity filter
severities = ['All'] + ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
selected_severity = st.sidebar.selectbox("Severity", severities)

# Detection method filter
methods = ['All'] + sorted(anomalies_df['method'].unique().tolist())
selected_method = st.sidebar.selectbox("Detection Method", methods)

# Time range filter
if 'detected_at' in anomalies_df.columns:
    min_time = anomalies_df['detected_at'].min()
    max_time = anomalies_df['detected_at'].max()
    
    time_range = st.sidebar.slider(
        "Time Range",
        min_value=min_time.to_pydatetime(),
        max_value=max_time.to_pydatetime(),
        value=(min_time.to_pydatetime(), max_time.to_pydatetime()),
        format="HH:mm:ss"
    )

# Apply filters
filtered_df = anomalies_df.copy()

if selected_service != 'All':
    filtered_df = filtered_df[filtered_df['service'] == selected_service]

if selected_severity != 'All':
    filtered_df = filtered_df[filtered_df['severity'] == selected_severity]

if selected_method != 'All':
    filtered_df = filtered_df[filtered_df['method'] == selected_method]

if 'detected_at' in filtered_df.columns:
    filtered_df = filtered_df[
        (filtered_df['detected_at'] >= time_range[0]) &
        (filtered_df['detected_at'] <= time_range[1])
    ]

# =============================================================================
# Key Metrics (Top Row)
# =============================================================================

st.header("📊 Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Anomalies",
        len(filtered_df),
        delta=None,
        delta_color="inverse"
    )

with col2:
    critical_count = len(filtered_df[filtered_df['severity'] == 'CRITICAL'])
    high_count = len(filtered_df[filtered_df['severity'] == 'HIGH'])
    st.metric(
        "Critical + High",
        critical_count + high_count,
        delta=f"{critical_count} Critical",
        delta_color="inverse"
    )

with col3:
    if not filtered_df.empty:
        services_affected = filtered_df['service'].nunique()
        st.metric("Services Affected", services_affected)
    else:
        st.metric("Services Affected", 0)

with col4:
    if not filtered_df.empty and 'method' in filtered_df.columns:
        methods_used = filtered_df['method'].nunique()
        st.metric("Detection Methods", methods_used)
    else:
        st.metric("Detection Methods", 0)

# =============================================================================
# Charts Row 1: Severity Distribution & Timeline
# =============================================================================

st.header("📈 Analytics")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Severity Distribution")
    
    if not filtered_df.empty:
        severity_counts = filtered_df['severity'].value_counts()
        
        fig = px.pie(
            values=severity_counts.values,
            names=severity_counts.index,
            color=severity_counts.index,
            color_discrete_map={
                'CRITICAL': '#d32f2f',
                'HIGH': '#ff9800',
                'MEDIUM': '#ffc107',
                'LOW': '#4caf50',
            },
            hole=0.4
        )
        fig.update_traces(textinfo='percent+label+value')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data to display")

with col2:
    st.subheader("Anomalies Over Time")
    
    if not filtered_df.empty and 'window' in filtered_df.columns:
        # Group by time window
        timeline = filtered_df.groupby('window').size().reset_index(name='count')
        
        fig = px.line(
            timeline,
            x='window',
            y='count',
            markers=True,
            title="Anomaly Count by Time Window"
        )
        fig.update_traces(line_color='#2196f3')
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Anomaly Count",
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No time series data available")

# =============================================================================
# Charts Row 2: Service Breakdown & Detection Methods
# =============================================================================

col1, col2 = st.columns(2)

with col1:
    st.subheader("Anomalies by Service")
    
    if not filtered_df.empty:
        service_counts = filtered_df['service'].value_counts().head(10)
        
        fig = px.bar(
            x=service_counts.values,
            y=service_counts.index,
            orientation='h',
            color=service_counts.values,
            color_continuous_scale='Reds'
        )
        fig.update_layout(
            xaxis_title="Anomaly Count",
            yaxis_title="Service",
            showlegend=False,
            yaxis={'categoryorder': 'total ascending'}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data to display")

with col2:
    st.subheader("Detection Methods")
    
    if not filtered_df.empty and 'method' in filtered_df.columns:
        method_counts = filtered_df['method'].value_counts()
        
        fig = px.bar(
            x=method_counts.index,
            y=method_counts.values,
            color=method_counts.values,
            color_continuous_scale='Blues'
        )
        fig.update_layout(
            xaxis_title="Method",
            yaxis_title="Count",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data to display")

# =============================================================================
# Recent Anomalies Table
# =============================================================================

st.header("🚨 Recent Anomalies")

if not filtered_df.empty:
    # Sort by detection time (most recent first)
    if 'detected_at' in filtered_df.columns:
        display_df = filtered_df.sort_values('detected_at', ascending=False).head(20)
    else:
        display_df = filtered_df.head(20)
    
    # Select columns to display
    display_columns = ['service', 'name', 'severity', 'description', 'window']
    if 'method' in display_df.columns:
        display_columns.append('method')
    
    display_df = display_df[display_columns]
    
    # Style the dataframe
    def highlight_severity(row):
        color = get_severity_color(row['severity'])
        return [f'background-color: {color}33' if col == 'severity' else '' for col in row.index]
    
    styled_df = display_df.style.apply(highlight_severity, axis=1)
    
    st.dataframe(
        styled_df,
        use_container_width=True,
        height=400
    )
    
    # Download button
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Anomalies CSV",
        data=csv,
        file_name=f"anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
else:
    st.info("No anomalies match the current filters")

# =============================================================================
# System Metrics (if available)
# =============================================================================

if not metrics_df.empty:
    st.header("📊 System Metrics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Error Rate Over Time")
        
        # Filter metrics by selected service
        if selected_service != 'All':
            plot_metrics = metrics_df[metrics_df['service'] == selected_service]
        else:
            # Aggregate across all services
            plot_metrics = metrics_df.groupby('window').agg({
                'error_count': 'sum',
                'warning_count': 'sum',
                'total_logs': 'sum'
            }).reset_index()
        
        if not plot_metrics.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=plot_metrics['window'],
                y=plot_metrics['error_count'],
                mode='lines',
                name='Errors',
                line=dict(color='#f44336')
            ))
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Error Count",
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Warning Rate Over Time")
        
        if not plot_metrics.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=plot_metrics['window'],
                y=plot_metrics['warning_count'],
                mode='lines',
                name='Warnings',
                line=dict(color='#ff9800')
            ))
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Warning Count",
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# Footer
# =============================================================================

st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        <p>High-Throughput Log Analytics Platform | Milestone 4: Dashboard & Deployment</p>
        <p>Last updated: {}</p>
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    unsafe_allow_html=True
)

# Auto-refresh (optional)
if st.sidebar.checkbox("Auto-refresh (every 30s)", value=False):
    import time
    time.sleep(30)
    st.rerun()