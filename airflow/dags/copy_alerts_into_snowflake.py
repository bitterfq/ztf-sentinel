from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import boto3

sys.path.append("/opt/airflow/src")
from snowflake_utils import get_snowflake_connection

def copy_alerts_into_snowflake():
    conn = get_snowflake_connection()
    cs = conn.cursor()
    cs.execute("USE DATABASE ztf_data;")
    cs.execute("USE SCHEMA public;")

    s3 = boto3.client("s3")
    bucket = "ztf-pipeline-data"
    prefix = "alerts_partitioned/"


    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("alerts.jsonl"):
                keys.append(obj["Key"])

    if not keys:
        print("⚠️ No alerts.jsonl files found in S3.")
    else:
        print(f"✅ Found {len(keys)} alerts.jsonl files. Beginning COPY INTO...")

    for key in keys:
        stage_path = f"@ztf_stage/{key}"
        print(f"📦 Running COPY INTO for: {stage_path}")
        try:
            cs.execute(f"""
                COPY INTO ztf_alerts_raw(raw)
                FROM '{stage_path}'
                FILE_FORMAT = json_format
            """)
        except Exception as e:
            print(f"❌ COPY INTO failed for {key}: {e}")

    cs.close()
    conn.close()

default_args = {
    "owner": "ztf",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ztf_test_copy_alerts",
    default_args=default_args,
    schedule_interval=timedelta(hours=12),
    catchup=False,
    description="Upload ZTF alerts to Snowflake only",
    tags=["ztf", "snowflake"],
) as dag:

    upload_to_snowflake = PythonOperator(
        task_id="copy_alerts_to_snowflake_test",
        python_callable=copy_alerts_into_snowflake,
    )

    upload_to_snowflake
