import streamlit as st
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv

load_dotenv(dotenv_path="/app/.env")

LAKEFS_S3_PATH = f"s3a://{os.getenv('LAKEFS_REPOSITORY', 'egatdata')}/{os.getenv('LAKEFS_BRANCH', 'main')}/{os.getenv('LAKEFS_PATH_PARQUET', 'egat_realtime_power.parquet')}"

LAKEFS_STORAGE_OPTIONS = {
    "key": os.getenv("ACCESS_KEY"),
    "secret": os.getenv("SECRET_KEY"),
    "client_kwargs": {"endpoint_url": os.getenv("LAKEFS_ENDPOINT", "http://lakefsdb:8000")},
    "config_kwargs": {"s3": {"addressing_style": "path"}}
}

REFRESH_INTERVAL_DEFAULT = 30
ANOMALY_SENSITIVITY_DEFAULT = 10
MAX_DISPLAY_POINTS = 100

st.set_page_config(
    page_title="EGAT Realtime Power Dashboard (lakeFS)",
    layout="wide",
    menu_items={'Get Help': None, 'Report a bug': None, 'About': 'EGAT Realtime Power Generation Dashboard v2.1 (lakeFS)'}
)

def detect_anomalies(data, contamination=0.1):
    if len(data) < 10:
        return np.zeros(len(data), dtype=bool)
    scaled_data = StandardScaler().fit_transform(data.values.reshape(-1, 1))
    return IsolationForest(contamination=contamination, random_state=42, n_estimators=100).fit_predict(scaled_data) == -1

@st.cache_data(ttl=REFRESH_INTERVAL_DEFAULT)
def load_data():
    df = pd.read_parquet(LAKEFS_S3_PATH, storage_options=LAKEFS_STORAGE_OPTIONS)
    if 'scrape_time' not in df.columns:
        return pd.DataFrame()
    df['scrape_time'] = pd.to_datetime(df['scrape_time'])
    return df.sort_values(by='scrape_time', ascending=False)

def create_sidebar():
    st.sidebar.markdown("### 🎛️ Dashboard Controls")
    auto_refresh = st.sidebar.checkbox('Enable Auto-refresh', value=True)
    refresh_interval = st.sidebar.slider('Refresh Interval (seconds)', 5, 60, REFRESH_INTERVAL_DEFAULT)
    st.sidebar.markdown("### 🔍 Anomaly Detection")
    contamination = st.sidebar.slider('Sensitivity (%)', 1, 20, ANOMALY_SENSITIVITY_DEFAULT) / 100
    st.sidebar.markdown("### 👤 User Information")
    st.sidebar.text(f"User: {os.getenv('USERNAME', 'WatcharananPha')}")
    st.sidebar.text(f"UTC: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    return auto_refresh, refresh_interval, contamination

def display_metrics(latest_data, anomaly_detected):
    col1, col2, col3, col4 = st.columns(4)
    if latest_data and all(k in latest_data for k in ['current_value_MW', 'temperature_C', 'display_time']):
        col1.metric("⚡ Power Output", f"{latest_data['current_value_MW']:,.1f} MW")
        col2.metric("🌡️ Temperature", f"{latest_data['temperature_C']:.1f}°C")
        col3.metric("🕒 Last Update", latest_data['display_time'])
        col4.error("⚠️ Anomaly Detected!" if anomaly_detected else "✅ Normal Operation")
    else:
        col1.metric("⚡ Power Output", "N/A")
        col2.metric("🌡️ Temperature", "N/A")
        col3.metric("🕒 Last Update", "N/A")
        col4.info("Waiting for data...")

def display_charts(chart_data):
    chart_col1, chart_col2 = st.columns(2)
    if not chart_data.empty and all(k in chart_data.columns for k in ['scrape_time', 'current_value_MW', 'temperature_C']):
        with chart_col1:
            st.subheader("⚡ Power Generation (MW)")
            st.line_chart(chart_data.set_index('scrape_time')['current_value_MW'], use_container_width=True, height=300)
        with chart_col2:
            st.subheader("🌡️ Temperature (°C)")
            st.line_chart(chart_data.set_index('scrape_time')['temperature_C'], use_container_width=True, height=300, color='#FF4B4B')
    else:
        with chart_col1:
            st.subheader("⚡ Power Generation (MW)")
            st.info("No data to display chart.")
        with chart_col2:
            st.subheader("🌡️ Temperature (°C)")
            st.info("No data to display chart.")

def display_statistics(anomalies, chart_data):
    st.markdown("---")
    st.subheader("📊 System Statistics")
    stats_cols = st.columns(4)
    if len(anomalies) > 0 and not chart_data.empty:
        total_anomalies = anomalies.sum()
        anomaly_rate = (total_anomalies / len(anomalies)) * 100
        stats_cols[0].metric("Anomalies Detected", f"{int(total_anomalies)}")
        stats_cols[1].metric("Anomaly Rate", f"{anomaly_rate:.1f}%")
        stats_cols[2].metric("Avg Power", f"{chart_data['current_value_MW'].mean():,.1f} MW")
        stats_cols[3].metric("Peak Power", f"{chart_data['current_value_MW'].max():,.1f} MW")
    else:
        for col in stats_cols:
            col.info("N/A")

def display_dashboard():
    st.title("⚡ EGAT Realtime Power Generation Dashboard (via lakeFS)")
    auto_refresh, refresh_interval, contamination = create_sidebar()
    last_refresh_placeholder = st.empty()
    data_container = st.container()
    charts_container = st.container()

    while True:
        df = load_data()
        current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        latest_data_point = None
        chart_display_data = pd.DataFrame()
        detected_anomalies_on_recent = np.array([])

        if not df.empty:
            latest_data_point = df.iloc[0].to_dict()
            chart_display_data = df.head(MAX_DISPLAY_POINTS).sort_values('scrape_time', ascending=True)
            if 'current_value_MW' in chart_display_data:
                detected_anomalies_on_recent = detect_anomalies(chart_display_data['current_value_MW'], contamination)

        with data_container:
            if latest_data_point:
                anomaly_for_latest = detected_anomalies_on_recent[-1] if len(detected_anomalies_on_recent) > 0 and latest_data_point['scrape_time'] == chart_display_data['scrape_time'].iloc[-1] else False
                display_metrics(latest_data_point, anomaly_for_latest)
                st.subheader("📝 Recent Data (Latest 10)")
                df_display_table = df.head(10)
                anomalies_for_table = detect_anomalies(df_display_table['current_value_MW'], contamination) if 'current_value_MW' in df_display_table else np.zeros(len(df_display_table), dtype=bool)
                df_display_table['Status'] = ['⚠️ Anomaly' if a else '✅ Normal' for a in anomalies_for_table]
                st.dataframe(df_display_table[['scrape_time', 'display_time', 'current_value_MW', 'temperature_C', 'Status']], use_container_width=True, hide_index=True)
            else:
                st.info("⏳ Waiting for data from lakeFS...")
                display_metrics(None, False)

        with charts_container:
            display_charts(chart_display_data)
            display_statistics(detected_anomalies_on_recent, chart_display_data)

        last_refresh_placeholder.text(f"Last refreshed: {current_time_str}")

        if not auto_refresh:
            st.sidebar.info("Auto-refresh is disabled. Reload page or enable to see updates.")
            break

        time.sleep(refresh_interval)

if __name__ == "__main__":
    display_dashboard()