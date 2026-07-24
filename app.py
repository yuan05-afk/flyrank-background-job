"""
FlyRank · Your first background job
Accept fast (202), work in the background, report status.

    POST /jobs           -> 202 + {job_id}       (returns instantly)
    GET  /jobs/{id}       -> job status + result
    GET  /jobs            -> queue counts
    GET  /health

Send an Idempotency-Key header on POST to make retries safe: the same key
returns the same job instead of creating a second one.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

import jobstore
from worker import Worker

_worker: Worker | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    jobstore.init_db()
    requeued = jobstore.reset_stuck_jobs()
    if requeued:
        print(f"[startup] recovered {requeued} stuck job(s) from a previous crash", flush=True)
    global _worker
    _worker = Worker()
    _worker.start()
    yield
    if _worker:
        _worker.stop()


app = FastAPI(
    title="Background Job API",
    version="1.0.0",
    description=(
        "Accept-fast job queue. POST returns **202** immediately; a background "
        "worker does the slow work with retries + backoff; poll the status endpoint. "
        "Idempotency, crash recovery, and alerting on permanent failure are built in."
    ),
    lifespan=lifespan,
)


class JobIn(BaseModel):
    type: str = Field("ai_call", description="Handler to run.")
    prompt: str = Field("", description="Example payload for the demo AI call.")
    work_seconds: float = Field(2, ge=0, le=30, description="Simulated latency.")
    flaky: bool = Field(False, description="Fail ~60% of attempts to show retries.")
    force_fail: bool = Field(False, description="Always fail to show the alert path.")
    max_attempts: int = Field(3, ge=1, le=10)


def _public_view(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": job["id"],
        "type": job["type"],
        "status": job["status"],
        "attempts": job["attempts"],
        "max_attempts": job["max_attempts"],
        "result": job["result"],
        "error": job["error"],
    }


@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok", "worker": "running" if _worker and _worker.is_alive() else "down"}


@app.get("/jobs", tags=["jobs"], summary="Queue counts")
def list_counts():
    return jobstore.counts()


@app.post("/jobs", status_code=202, tags=["jobs"], summary="Enqueue a job (returns 202 instantly)")
def enqueue(
    body: JobIn,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    # Idempotency: same key -> same job, never a duplicate.
    if idempotency_key:
        existing = jobstore.find_by_idempotency_key(idempotency_key)
        if existing:
            return JSONResponse(
                status_code=202,
                content={
                    "job_id": existing["id"],
                    "status": existing["status"],
                    "idempotent_replay": True,
                    "status_url": f"/jobs/{existing['id']}",
                },
            )

    job_id = str(uuid.uuid4())
    payload = {
        "prompt": body.prompt,
        "work_seconds": body.work_seconds,
        "flaky": body.flaky,
        "force_fail": body.force_fail,
    }
    jobstore.create_job(
        job_id=job_id,
        job_type=body.type,
        payload=payload,
        idempotency_key=idempotency_key,
        max_attempts=body.max_attempts,
    )
    return {
        "job_id": job_id,
        "status": "queued",
        "idempotent_replay": False,
        "status_url": f"/jobs/{job_id}",
    }


@app.get("/jobs/{job_id}", tags=["jobs"], summary="Job status + result")
def job_status(job_id: str):
    job = jobstore.get_job(job_id)
    if job is None:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    return _public_view(job)
