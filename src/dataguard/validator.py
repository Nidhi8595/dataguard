import csv
import json
import math
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def load_contract():
    path = ROOT / "config" / "data_contract.json"
    return json.loads(path.read_text(encoding="utf-8"))


def validate_schema(columns, contract):
    expected = set(contract["column_types"])
    actual = set(columns)
    errors = []

    if expected - actual:
        errors.append(f"Missing columns: {sorted(expected - actual)}")
    if actual - expected:
        errors.append(f"Unexpected columns: {sorted(actual - expected)}")

    return errors


def validate_record(row, contract, seen_keys):
    errors = []
    rules = contract["business_rules"]

    for column in contract["required_columns"]:
        if column not in row:
            errors.append(f"Missing column: {column}")
        elif rules["required_fields_not_null"] and not str(row[column] or "").strip():
            errors.append(f"Missing value: {column}")

    if errors:
        return errors

    for column, data_type in contract["column_types"].items():
        value = row[column]

        try:
            if data_type == "timestamp":
                datetime.fromisoformat(value)
            elif data_type == "double":
                number = float(value)
                if not math.isfinite(number):
                    raise ValueError()
            elif data_type == "string":
                if not isinstance(value, str):
                    raise ValueError()
            else:
                errors.append(f"Unsupported type: {data_type}")
        except (ValueError, TypeError):
            errors.append(f"Invalid {data_type}: {column}")

    try:
        consumption = float(row["energy_consumption"])
        if math.isfinite(consumption) and consumption < rules["energy_consumption_min"]:
            errors.append("Negative energy consumption")
    except (ValueError, TypeError):
        pass

    key_columns = rules["unique_key"]
    key = tuple(row[column] for column in key_columns)

    if all(str(value).strip() for value in key):
        if key in seen_keys:
            errors.append("Duplicate reading")
        else:
            seen_keys.add(key)

    return errors


def validate_csv():
    contract = load_contract()
    path = ROOT / "data" / "sample" / "energy_readings.csv"

    with path.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        schema_errors = validate_schema(reader.fieldnames or [], contract)
        accepted, rejected = [], []
        seen_keys = set()

        for row in reader:
            errors = validate_record(row, contract, seen_keys)
            result = {"record": row, "rejection_reasons": errors}
            (rejected if errors else accepted).append(result)

    print("Schema errors:", schema_errors)
    print("Total records:", len(accepted) + len(rejected))
    print("Accepted records:", len(accepted))
    print("Rejected records:", len(rejected))
    print("\nSample rejection reasons:")

    for item in rejected[:10]:
        print(item["rejection_reasons"])

    return accepted, rejected, schema_errors


if __name__ == "__main__":
    validate_csv()
