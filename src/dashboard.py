import streamlit as st
import pandas as pd
import snowflake.connector
import os
from PIL import Image
from pathlib import Path
from dotenv import load_dotenv
import matplotlib.pyplot as plt

# === CONFIG ===
st.set_page_config(page_title="ZTF Dashboard", layout="wide")
load_dotenv("../.env")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def query_df(sql):
    conn = get_connection()
    return pd.read_sql(sql, conn)

st.title("🛰️ ZTF Pipeline Dashboard")
tab1, tab2, tab3 = st.tabs(["📈 Overview", "🔍 Alert Lookup", "🖼️ Image Viewer"])

# === 📈 Overview Tab ===
with tab1:
    st.subheader("📊 Alert Counts by Date (Last 14 Days)")
    df_alerts = query_df("""
        SELECT
            CAST(UTC_LATEST_DETECTION AS DATE) AS alert_date,
            COUNT(*) AS alert_count
        FROM ztf_alerts_clean
        WHERE UTC_LATEST_DETECTION >= DATEADD(day, -14, CURRENT_DATE())
        GROUP BY alert_date
        ORDER BY alert_date
    """)
    st.bar_chart(df_alerts.set_index("ALERT_DATE"))

    st.subheader("📊 Daily Unique Object Counts")
    df_unique = query_df("""
        SELECT
            CAST(UTC_LATEST_DETECTION AS DATE) AS alert_date,
            COUNT(DISTINCT OBJECT_ID) AS unique_objects
        FROM ztf_alerts_clean
        WHERE UTC_LATEST_DETECTION >= DATEADD(day, -14, CURRENT_DATE())
        GROUP BY alert_date
        ORDER BY alert_date
    """)
    st.line_chart(df_unique.set_index("ALERT_DATE"))

    st.subheader("🧭 Detection Lag (First vs Latest Detection)")
    df_lag = query_df("""
        SELECT DATEDIFF(day, UTC_FIRST_DETECTION, UTC_LATEST_DETECTION) AS detection_lag
        FROM ztf_alerts_clean
        WHERE UTC_FIRST_DETECTION IS NOT NULL AND UTC_LATEST_DETECTION IS NOT NULL
    """)
    fig1, ax1 = plt.subplots()
    ax1.hist(df_lag["DETECTION_LAG"], bins=20)
    ax1.set_xlabel("Lag (days)")
    ax1.set_ylabel("Frequency")
    st.pyplot(fig1)

    st.subheader("🔭 Magnitude Distribution (G and R)")
    df_mag = query_df("""
        SELECT G_MAGNITUDE, R_MAGNITUDE
        FROM ztf_alerts_clean
        WHERE G_MAGNITUDE IS NOT NULL AND R_MAGNITUDE IS NOT NULL
    """)
    fig2, ax2 = plt.subplots()
    ax2.hist(df_mag["G_MAGNITUDE"], bins=30, alpha=0.5, label="G")
    ax2.hist(df_mag["R_MAGNITUDE"], bins=30, alpha=0.5, label="R")
    ax2.set_xlabel("Magnitude")
    ax2.set_ylabel("Frequency")
    ax2.legend()
    st.pyplot(fig2)

    st.subheader("🖼️ Image Stamp Counts")
    df_stamps = query_df("""
        SELECT COUNT(*) AS total_images, COUNT(DISTINCT object_id) AS unique_objects
        FROM ztf_stamps
    """)
    st.dataframe(df_stamps)

# === 🔍 Alert Lookup Tab ===
with tab2:
    object_id = st.text_input("Enter ZTF Object ID")
    if object_id:
        df = query_df(f"""
            SELECT *
            FROM ztf_alerts_clean
            WHERE object_id = '{object_id}'
            LIMIT 1
        """)
        if not df.empty:
            st.json(df.iloc[0].to_dict())
        else:
            st.warning("Object ID not found.")

# === 🖼️ Image Viewer Tab ===
with tab3:
    st.subheader("View Local Image Stamps")
    obj_id = st.text_input("ZTF Object ID for Images")
    date = st.text_input("Alert Date (YYYY-MM-DD)")

    if obj_id and date:
        base_path = Path(f"images/by_date/{date}")
        for t in ["science", "template", "difference"]:
            path = base_path / f"{obj_id}_{t}.png"
            if path.exists():
                st.image(str(path), caption=f"{t.title()} Image", width=300)
            else:
                st.warning(f"{t.title()} image not found.")
