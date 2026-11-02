import json
import sqlite3

from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = ROOT / "data" / "batch_registry.db"


def connect(db_path=DEFAULT_DB):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

    connection.execute("""
        CREATE TABLE IF NOT EXISTS batch_registry (
            ingestion_batch_id TEXT PRIMARY KEY,
            file_name TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            record_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            duplicate_of_batch_id TEXT,
            received_at TEXT NOT NULL,
            processed_at TEXT
        )
    """)

    return connection


def register_batch(manifest_path, db_path=DEFAULT_DB):

    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))

    batch_id = manifest["ingestion_batch_id"]
    content_hash = manifest["content_hash"]

    if not batch_id or not content_hash:
        raise ValueError("Batch ID and content hash are required")

    with connect(db_path) as connection:
        existing = connection.execute(
            """
            SELECT *
            FROM batch_registry
            WHERE ingestion_batch_id = ?
            """,
            (batch_id,),
        ).fetchone()

        if existing:
            if existing["content_hash"] != content_hash:
                raise ValueError("Batch ID already exists with different content")

            return {
                "result": "ALREADY_REGISTERED",
                "batch_id": batch_id,
                "status": existing["status"],
            }

        duplicate = connection.execute(
            """
            SELECT ingestion_batch_id
            FROM batch_registry
            WHERE content_hash = ?
              AND status != 'DUPLICATE'
            ORDER BY received_at
            LIMIT 1
            """,
            (content_hash,),
        ).fetchone()

        status = "DUPLICATE" if duplicate else "RECEIVED"

        connection.execute(
            """
            INSERT INTO batch_registry (
                ingestion_batch_id,
                file_name,
                content_hash,
                record_count,
                status,
                duplicate_of_batch_id,
                received_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                batch_id,
                manifest["file_name"],
                content_hash,
                manifest["record_count"],
                status,
                duplicate["ingestion_batch_id"] if duplicate else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    return {
        "result": "DUPLICATE_CONTENT" if duplicate else "REGISTERED",
        "batch_id": batch_id,
        "status": status,
        "duplicate_of": (duplicate["ingestion_batch_id"] if duplicate else None),
    }


def update_batch_status(batch_id, new_status, db_path=DEFAULT_DB):

    allowed_transitions = {
        "RECEIVED": {"PROCESSING"},
        "PROCESSING": {"PROCESSED", "FAILED"},
        "FAILED": {"PROCESSING"},
        "PROCESSED": set(),
        "DUPLICATE": set(),
    }

    if new_status not in {"PROCESSING", "PROCESSED", "FAILED"}:
        raise ValueError(f"Invalid status: {new_status}")

    with connect(db_path) as connection:
        batch = connection.execute(
            """
            SELECT status
            FROM batch_registry
            WHERE ingestion_batch_id = ?
            """,
            (batch_id,),
        ).fetchone()

        if not batch:
            raise ValueError(f"Unknown batch: {batch_id}")

        current_status = batch["status"]

        if new_status not in allowed_transitions[current_status]:
            raise ValueError(f"Invalid transition: {current_status} -> {new_status}")

        processed_at = (
            datetime.now(timezone.utc).isoformat()
            if new_status == "PROCESSED"
            else None
        )

        connection.execute(
            """
            UPDATE batch_registry
            SET status = ?,
                processed_at = COALESCE(?, processed_at)
            WHERE ingestion_batch_id = ?
            """,
            (new_status, processed_at, batch_id),
        )

    return new_status


def get_batches(db_path=DEFAULT_DB):

    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM batch_registry
            ORDER BY received_at
            """
        ).fetchall()

    return [dict(row) for row in rows]
