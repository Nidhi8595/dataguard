import csv, json, hashlib
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]

SOURCE_FILE = ROOT / "incoming" / "BATCH_001.csv"
OUTPUT_FILE = ROOT / "incoming" / "BATCH_004.csv"
MANIFEST_FILE = ROOT / "batch_manifests" / "BATCH_004.json"

# Read the original batch.
with SOURCE_FILE.open("r", newline="", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    fieldnames = reader.fieldnames
    rows = list(reader)

# Find the first valid reading to correct.
target = next(
    (
        row
        for row in rows
        if row["building_id"].strip()
        and row["meter_id"].strip()
        and row["reading_timestamp"].strip()
        and float(row["energy_consumption"]) >= 0
    ),
    None,
)

if target is None:
    raise ValueError("No valid reading found to correct.")

old_value = float(target["energy_consumption"])

# Reduce consumption by 10% to simulate a corrected reading.
new_value = round(old_value * 0.9, 2)

if new_value == old_value:
    new_value = round(old_value + 1, 2)

target["energy_consumption"] = str(new_value)
target["source_batch_id"] = "BATCH_004"

# Create a new delivery containing only the corrected reading.
with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow(target)

# Calculate the actual file hash.
content_hash = hashlib.sha256(OUTPUT_FILE.read_bytes()).hexdigest()

manifest = {
    "ingestion_batch_id": "BATCH_004",
    "file_name": "BATCH_004.csv",
    "file_path": "incoming/BATCH_004.csv",
    "content_hash": content_hash,
    "record_count": 1,
    "created_at": datetime.now(timezone.utc).isoformat(),
}

MANIFEST_FILE.write_text(json.dumps(manifest, indent=4), encoding="utf-8")

print("Correction batch created successfully!")
print(
    f"Reading key: {target['building_id']} / {target['meter_id']} / {target['reading_timestamp']}"
)
print(f"Original consumption: {old_value}")
print(f"Corrected consumption: {new_value}")
print(f"New batch: BATCH_004")
print(f"Content hash: {content_hash}")
