
import argparse
import csv
import hashlib
import json
import random

from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

INCOMING_DIR = ROOT / "incoming"
MANIFEST_DIR = ROOT / "batch_manifests"

COLUMNS = [
    "building_id",
    "meter_id",
    "reading_timestamp",
    "energy_consumption",
    "source_batch_id"
]


def generate_batch(batch_id, record_count=1000):

    random.seed(42)

    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

    output = INCOMING_DIR / f"{batch_id}.csv"

    rows = []

    for i in range(record_count):
        rows.append({
            "building_id": f"B{random.randint(1, 30):03}",
            "meter_id": f"M{random.randint(1, 150):03}",
            "reading_timestamp": (
                datetime(2026, 10, 1) +
                timedelta(minutes=15 * i)
            ).isoformat(),
            "energy_consumption": round(
                random.uniform(0, 200), 2
            ),
            "source_batch_id": batch_id
        })

    # Introduce controlled data-quality failures.
    for i in range(0, min(100, record_count), 20):
        rows[i]["energy_consumption"] = -10
        rows[i + 1]["meter_id"] = ""
        rows[i + 2]["energy_consumption"] = "abc"
        rows[i + 3]["reading_timestamp"] = ""
        rows[i + 4] = rows[i + 5].copy()

    random.shuffle(rows)

    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    # Calculate the SHA-256 hash of the complete delivery.
    content_hash = hashlib.sha256(output.read_bytes()).hexdigest()

    manifest = {
        "ingestion_batch_id": batch_id,
        "file_name": output.name,
        "file_path": str(output),
        "content_hash": content_hash,
        "record_count": len(rows),
        "created_at": datetime.now().isoformat()
    }

    manifest_path = MANIFEST_DIR / f"{batch_id}.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=4),
        encoding="utf-8"
    )

    print(f"Created batch: {batch_id}")
    print(f"Records: {len(rows)}")
    print(f"SHA-256: {content_hash}")
    print(f"File: {output}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--records", type=int, default=1000)

    args = parser.parse_args()

    generate_batch(args.batch_id, args.records)
