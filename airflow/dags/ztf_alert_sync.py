from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import boto3

sys.path.append("/opt/airflow/src")
from snowflake_utils import get_snowflake_connection
from sync_to_s3 import main as sync_to_s3_main
from upsert_stamps import run_upsert as upsert_stamps_main

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
    dag_id="ztf_alert_sync",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs = 1,
    description="Upload ZTF alerts and stamps to Snowflake",
    tags=["ztf", "snowflake"],
) as dag:

    sync_s3_task = PythonOperator(
        task_id="sync_data_to_s3",
        python_callable=sync_to_s3_main
    )

    upload_to_snowflake = PythonOperator(
        task_id="copy_alerts_into_snowflake",
        python_callable=copy_alerts_into_snowflake,
    )

    upsert_stamps = PythonOperator(
        task_id="upsert_stamps_to_snowflake",
        python_callable=upsert_stamps_main
    )

    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/dbt/ztf_dbt && dbt run"
    )

    sync_s3_task >> [upload_to_snowflake, upsert_stamps] >> dbt_run
