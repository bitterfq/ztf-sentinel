import os
import json
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from fetch_stamps import fetch_and_save_all

# === CONFIG ===
ALERT_DIR = Path("data/alerts_partitioned")
IMAGE_DIR = Path("images/by_date")
LOG_PATH = Path("logs/repair_stamps.log")
MAX_WORKERS = 12

# === LOGGING SETUP ===
os.makedirs(LOG_PATH.parent, exist_ok=True)
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="[%(asctime)s] %(message)s"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("[%(asctime)s] %(message)s"))
logging.getLogger().addHandler(console)

# === HELPERS ===
def is_valid_stamp(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 1000

def expected_stamp_paths(alert_date: str, object_id: str) -> list:
    base = IMAGE_DIR / alert_date
    return [base / f"{object_id}_{stamp}.png" for stamp in ("science", "template", "difference")]

def read_jsonl(path: Path) -> list:
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]

def process_alert(alert, alert_date: str, i: int, total: int) -> str:
    try:
        object_id = alert["objectId"]
        paths = expected_stamp_paths(alert_date, object_id)
        if all(is_valid_stamp(p) for p in paths):
            return f"[{i}/{total}] ✅ {object_id} — All stamps present"

        output_dir = (IMAGE_DIR / alert_date).as_posix()
        fetch_and_save_all(object_id, output_dir)
        return f"[{i}/{total}] 🛠️ Repaired {object_id}"
    except Exception as e:
        return f"[{i}/{total}] ❌ Failed {alert.get('objectId', 'UNKNOWN')}: {e}"

def main():
    alerts = []
    for alert_dir in sorted(ALERT_DIR.glob("date=*")):
        alert_date = alert_dir.name.split("=")[1]
        jsonl_path = alert_dir / "alerts.jsonl"
        if not jsonl_path.exists():
            continue
        try:
            parsed = read_jsonl(jsonl_path)
            alerts.extend((a, alert_date) for a in parsed)
        except Exception as e:
            logging.warning(f"⚠️ Skipped {jsonl_path}: {e}")

    total = len(alerts)
    logging.info(f"🔎 Scanning {total} alerts for missing stamps...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_alert, alert, date, i + 1, total): (alert, date)
            for i, (alert, date) in enumerate(alerts)
        }
        for future in as_completed(futures):
            msg = future.result()
            logging.info(msg)

    logging.info("✅ All repairs complete.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("🛑 Interrupted by user.")
