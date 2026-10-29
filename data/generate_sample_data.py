import csv
import random
from pathlib import Path
from datetime import datetime, timedelta

random.seed(42)

output = Path(__file__).parent / "sample" / "energy_readings.csv"
output.parent.mkdir(parents=True, exist_ok=True)

columns = [
    "building_id",
    "meter_id",
    "reading_timestamp",
    "energy_consumption",
    "source_batch_id",
]

rows = []

for i in range(1000):
    rows.append(
        {
            "building_id": f"B{random.randint(1, 30):03}",
            "meter_id": f"M{random.randint(1, 150):03}",
            "reading_timestamp": (
                datetime(2026, 10, 1) + timedelta(minutes=15 * i)
            ).isoformat(),
            "energy_consumption": round(random.uniform(0, 200), 2),
            "source_batch_id": f"BATCH{random.randint(1, 10):03}",
        }
    )

# Introduce varied data-quality failures
for i in range(0, 100, 20):
    rows[i]["energy_consumption"] = -10
    rows[i + 1]["meter_id"] = ""
    rows[i + 2]["energy_consumption"] = "abc"
    rows[i + 3]["reading_timestamp"] = ""
    rows[i + 4] = rows[i + 5].copy()

random.shuffle(rows)

with open(output, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)

print(f"Created {len(rows)} records: {output}")
