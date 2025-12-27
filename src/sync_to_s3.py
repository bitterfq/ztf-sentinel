"""
sync_to_s3.py

This script synchronizes local directories containing ZTF alert and
image data to an S3 bucket.
It uploads only new or changed files by comparing local MD5 hashes
with S3 ETags, and logs all actions.
Intended for use in automated workflows such as Airflow DAGs.
"""

import os
import boto3
import hashlib
import logging
from botocore.exceptions import NoCredentialsError, ClientError
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv('/opt/airflow/.env')

# === CONFIG ===
BUCKET_NAME = 'ztf-pipeline-data'
LOCAL_DIRECTORIES = [
    ('/opt/airflow/data/alerts_partitioned', 'alerts_partitioned'),
    ('/opt/airflow/images/by_date', 'images/by_date')
]
LOG_FILE = '/opt/airflow/custom_logs/sync_to_s3.log'
MAX_WORKERS = 12  # Adjust based on Pi memory and CPU usage

# === SETUP LOGGING ===
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a'),
        logging.StreamHandler()
    ]
)

s3 = boto3.client('s3')

def md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def s3_etag_matches(local_path, bucket, key):
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        etag = response['ETag'].strip('"')
        return etag == md5(local_path)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

def upload_file_if_needed(file_path, s3_prefix, local_dir):
    file_path = Path(file_path)
    s3_key = os.path.join(s3_prefix, file_path.relative_to(local_dir).as_posix())
    try:
        if s3_etag_matches(file_path, BUCKET_NAME, s3_key):
            return None  # Skipped
        s3.upload_file(str(file_path), BUCKET_NAME, s3_key)
        return f"✅ Uploaded: {file_path} -> s3://{BUCKET_NAME}/{s3_key}"
    except NoCredentialsError:
        return "❌ AWS credentials not found."
    except Exception as e:
        return f"❌ Failed to upload {file_path}: {e}"

def upload_directory(local_dir, s3_prefix):
    local_dir = Path(local_dir)
    files = [f for f in local_dir.rglob('*') if f.is_file()]
    total = len(files)
    if total == 0:
        logging.info(f"📁 No files to upload in {local_dir}")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(upload_file_if_needed, f, s3_prefix, local_dir) for f in files]
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result:
                logging.info(result)
            if i % 100 == 0:
                logging.info(f"🧮 Processed {i}/{total} files...")

def main():
    logging.info("=" * 60)
    logging.info("🕒 Starting sync cycle")
    for local_path, s3_prefix in LOCAL_DIRECTORIES:
        if os.path.exists(local_path):
            logging.info(f"🔄 Syncing directory: {local_path}")
            upload_directory(local_path, s3_prefix)
        else:
            logging.warning(f"⚠️ Skipped missing path: {local_path}")
    logging.info("✅ Sync cycle complete")
    logging.info("=" * 60)
    logging.info("\n")

if __name__ == "__main__":
    main()
