import json, pytest

from dataguard.batch_registry import register_batch, update_batch_status, get_batches


def create_manifest(tmp_path, batch_id, content_hash):
    manifest = {
        "ingestion_batch_id": batch_id,
        "file_name": f"{batch_id}.csv",
        "file_path": f"incoming/{batch_id}.csv",
        "content_hash": content_hash,
        "record_count": 1000,
    }

    path = tmp_path / f"{batch_id}_{content_hash}.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def test_register_new_batch(tmp_path):
    manifest = create_manifest(tmp_path, "BATCH_001", "hash_a")
    db = tmp_path / "test.db"

    result = register_batch(manifest, db)

    assert result["result"] == "REGISTERED"
    assert result["status"] == "RECEIVED"


def test_detect_duplicate_content(tmp_path):
    db = tmp_path / "test.db"

    first = create_manifest(tmp_path, "BATCH_001", "hash_a")
    second = create_manifest(tmp_path, "BATCH_003", "hash_a")

    register_batch(first, db)
    result = register_batch(second, db)

    assert result["result"] == "DUPLICATE_CONTENT"
    assert result["duplicate_of"] == "BATCH_001"


def test_same_batch_registration_is_idempotent(tmp_path):
    db = tmp_path / "test.db"
    manifest = create_manifest(tmp_path, "BATCH_001", "hash_a")

    register_batch(manifest, db)
    result = register_batch(manifest, db)

    assert result["result"] == "ALREADY_REGISTERED"


def test_same_batch_id_with_different_content_is_rejected(tmp_path):
    db = tmp_path / "test.db"

    first = create_manifest(tmp_path, "BATCH_001", "hash_a")
    changed = create_manifest(tmp_path, "BATCH_001", "hash_b")

    register_batch(first, db)

    with pytest.raises(ValueError, match="different content"):
        register_batch(changed, db)


def test_failed_batch_can_be_retried(tmp_path):
    db = tmp_path / "test.db"
    manifest = create_manifest(tmp_path, "BATCH_001", "hash_a")

    register_batch(manifest, db)

    update_batch_status("BATCH_001", "PROCESSING", db)
    update_batch_status("BATCH_001", "FAILED", db)
    update_batch_status("BATCH_001", "PROCESSING", db)
    update_batch_status("BATCH_001", "PROCESSED", db)

    batches = get_batches(db)

    assert batches[0]["status"] == "PROCESSED"
    assert batches[0]["processed_at"] is not None


def test_processed_batch_cannot_be_reprocessed(tmp_path):
    db = tmp_path / "test.db"
    manifest = create_manifest(tmp_path, "BATCH_001", "hash_a")

    register_batch(manifest, db)

    update_batch_status("BATCH_001", "PROCESSING", db)
    update_batch_status("BATCH_001", "PROCESSED", db)

    with pytest.raises(ValueError, match="Invalid transition"):
        update_batch_status("BATCH_001", "PROCESSING", db)


def test_corrected_delivery_is_registered_as_new_batch(tmp_path):
    db = tmp_path / "test.db"

    original = create_manifest(tmp_path, "BATCH_001", "original_hash")
    corrected = create_manifest(tmp_path, "BATCH_004", "corrected_hash")

    register_batch(original, db)
    result = register_batch(corrected, db)

    assert result["result"] == "REGISTERED"
    assert result["batch_id"] == "BATCH_004"
