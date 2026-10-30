from dataguard.validator import load_contract, validate_record, validate_schema

contract = load_contract()


def valid_record():
    return {
        "building_id": "B001",
        "meter_id": "M001",
        "reading_timestamp": "2026-10-28T09:00:00",
        "energy_consumption": "12.5",
        "source_batch_id": "BATCH001",
    }


def test_valid_record():
    assert validate_record(valid_record(), contract, set()) == []


def test_negative_consumption():
    row = valid_record()
    row["energy_consumption"] = "-5"
    assert "Negative energy consumption" in validate_record(row, contract, set())


def test_missing_meter_id():
    row = valid_record()
    row["meter_id"] = ""
    assert "Missing value: meter_id" in validate_record(row, contract, set())


def test_duplicate_reading():
    row = valid_record()

    seen = set()
    validate_record(row, contract, seen)
    assert "Duplicate reading" in validate_record(row, contract, seen)


def test_invalid_timestamp():
    row = valid_record()
    row["reading_timestamp"] = "invalid-date"
    assert "Invalid timestamp: reading_timestamp" in validate_record(
        row, contract, set()
    )


def test_invalid_numeric_value():
    row = valid_record()
    row["energy_consumption"] = "abc"
    assert "Invalid double: energy_consumption" in validate_record(row, contract, set())


def test_missing_schema_column():
    columns = list(contract["column_types"])
    columns.remove("meter_id")
    assert validate_schema(columns, contract)


def test_unexpected_schema_column():
    columns = list(contract["column_types"]) + ["temperature"]
    assert validate_schema(columns, contract)
