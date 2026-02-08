"""Fetch and save ZTF stamps using the ALeRCE Python client."""
from alerce.core import Alerce
from astropy.io import fits
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
from pathlib import Path
import time
import os

# === Failure log path ===
FAILURE_LOG = Path("logs/failed_stamps.log")
FAILURE_LOG.parent.mkdir(parents=True, exist_ok=True)

def log_failure(msg):
    print(msg)
    with open(FAILURE_LOG, "a") as f:
        f.write(msg + "\n")

def try_fetch_stamps(object_id, retries=3, backoff=1):
    client = Alerce()
    for attempt in range(retries):
        try:
            stamps = client.get_stamps(object_id)
            if stamps and len(stamps) >= 3:
                return stamps
            raise ValueError(f"Less than 3 stamps for {object_id}")
        except Exception as e:
            if attempt == retries - 1:
                raise e
            wait = backoff * (2 ** attempt)
            print(f"⚠️ Retry {attempt + 1}/{retries} for {object_id} after {wait}s: {e}")
            time.sleep(wait)

def save_stamp_png(stamp, path):
    if stamp.data is not None and stamp.data.size > 0:
        plt.imshow(stamp.data, cmap="gray")
        plt.axis("off")
        plt.savefig(path, bbox_inches="tight", pad_inches=0.1)
        plt.close()
        print(f"🛰️ Saved {path.name}")
    else:
        log_failure(f"⚠️ Empty FITS stamp: {path.name}")

def fetch_and_save_all(object_id, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    stamp_types = ["science", "template", "difference"]
    filenames = [f"{object_id}_{t}.png" for t in stamp_types]
    paths = [Path(output_dir) / fname for fname in filenames]

    # ✅ Skip fetch if all files exist and are healthy
    skip_all = all(p.exists() and p.stat().st_size > 1000 for p in paths)
    if skip_all:
        print(f"✅ Skipping {object_id}, all stamps exist")
        return [{"image_type": t, "file_path": str(p)} for t, p in zip(stamp_types, paths)]

    try:
        stamps = try_fetch_stamps(object_id)
        for stamp, path in zip(stamps, paths):
            if path.exists() and path.stat().st_size > 1000:
                print(f"✅ Skipping existing {path.name}")
                continue
            save_stamp_png(stamp, path)
        return [{"image_type": t, "file_path": str(p)} for t, p in zip(stamp_types, paths)]
    except Exception as e:
        log_failure(f"❌ Failed to fetch/save for {object_id}: {e}")
        return None

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python fetch_stamps.py <ZTF_OBJECT_ID> [output_directory]")
    else:
        ztf_id = sys.argv[1]
        outdir = sys.argv[2] if len(sys.argv) > 2 else "."
        fetch_and_save_all(ztf_id, outdir)
