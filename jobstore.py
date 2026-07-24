"""
Job store — SQLite persistence for background jobs.

Keeps jobs durable across restarts (a queue in memory would forget everything
the moment the worker crashes — which is exactly when you care most).
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent / "jobs.db"

# Job lifecycle:  queued -> running -> succeeded | failed
STATUSES = {"queued", "running", "succeeded", "failed"}


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id               TEXT PRIMARY KEY,
                idempotency_key  TEXT UNIQUE,
                type             TEXT NOT NULL,
                payload          TEXT NOT NULL,
                status           TEXT NOT NULL DEFAULT 'queued',
                attempts         INTEGER NOT NULL DEFAULT 0,
                max_attempts     INTEGER NOT NULL DEFAULT 3,
                result           TEXT,
                error            TEXT,
                created_at       REAL NOT NULL,
                updated_at       REAL NOT NULL
            )
            """
        )
        conn.commit()


def _row_to_job(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "type": row["type"],
        "status": row["status"],
        "attempts": row["attempts"],
        "max_attempts": row["max_attempts"],
        "payload": json.loads(row["payload"]),
        "result": json.loads(row["result"]) if row["result"] else None,
        "error": row["error"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def find_by_idempotency_key(key: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
    return _row_to_job(row) if row else None


def create_job(
    job_id: str,
    job_type: str,
    payload: dict[str, Any],
    idempotency_key: str | None,
    max_attempts: int = 3,
) -> dict[str, Any]:
    now = time.time()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO jobs
                (id, idempotency_key, type, payload, status, attempts,
                 max_attempts, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'queued', 0, ?, ?, ?)
            """,
            (job_id, idempotency_key, job_type, json.dumps(payload),
             max_attempts, now, now),
        )
        conn.commit()
    return get_job(job_id)  # type: ignore[return-value]


def get_job(job_id: str) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _row_to_job(row) if row else None


def claim_next_queued() -> dict[str, Any] | None:
    """Atomically move one queued job to running (so it runs exactly once per worker)."""
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            "UPDATE jobs SET status = 'running', attempts = attempts + 1, updated_at = ? WHERE id = ?",
            (time.time(), row["id"]),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
    return _row_to_job(row)


def mark_succeeded(job_id: str, result: dict[str, Any]) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE jobs SET status = 'succeeded', result = ?, error = NULL, updated_at = ? WHERE id = ?",
            (json.dumps(result), time.time(), job_id),
        )
        conn.commit()


def mark_retry_or_fail(job_id: str, error: str) -> str:
    """Requeue if attempts remain, otherwise fail. Returns the new status."""
    with _connect() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return "failed"
        if row["attempts"] < row["max_attempts"]:
            new_status = "queued"
        else:
            new_status = "failed"
        conn.execute(
            "UPDATE jobs SET status = ?, error = ?, updated_at = ? WHERE id = ?",
            (new_status, error, time.time(), job_id),
        )
        conn.commit()
    return new_status


def reset_stuck_jobs() -> int:
    """On startup, requeue jobs left 'running' by a crashed worker (crash recovery)."""
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE jobs SET status = 'queued', updated_at = ? WHERE status = 'running'",
            (time.time(),),
        )
        conn.commit()
        return cur.rowcount


def counts() -> dict[str, int]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"
        ).fetchall()
    out = {s: 0 for s in STATUSES}
    for r in rows:
        out[r["status"]] = r["n"]
    return out
