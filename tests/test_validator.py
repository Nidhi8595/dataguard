from dataguard.validator import load_contract, validate_record


def test_valid_record():
    contract = load_contract()
    seen = set()
    row = {
        "building_id": "B001",
        "meter_id": "M001",
        "reading_timestamp": "2026-10-28T09:00:00",
        "energy_consumption": "12.5",
        "source_batch_id": "BATCH001",
    }

    assert validate_record(row, contract, seen) == []


def test_negative_consumption():
    contract = load_contract()
    row = {
        "building_id": "B001",
        "meter_id": "M001",
        "reading_timestamp": "2026-10-28T09:00:00",
        "energy_consumption": "-5",
        "source_batch_id": "BATCH001",
    }

    errors = validate_record(row, contract, set())
    assert "Negative energy consumption" in errors


def test_missing_meter_id():
    contract = load_contract()
    row = {
        "building_id": "B001",
        "meter_id": "",
        "reading_timestamp": "2026-10-28T09:00:00",
        "energy_consumption": "12.5",
        "source_batch_id": "BATCH001",
    }

    errors = validate_record(row, contract, set())
    assert "Missing value: meter_id" in errors


def test_duplicate_reading():
    contract = load_contract()
    row = {
        "building_id": "B001",
        "meter_id": "M001",
        "reading_timestamp": "2026-10-28T09:00:00",
        "energy_consumption": "12.5",
        "source_batch_id": "BATCH001",
    }

    seen = set()
    validate_record(row, contract, seen)
    errors = validate_record(row, contract, seen)

    assert "Duplicate reading" in errors
