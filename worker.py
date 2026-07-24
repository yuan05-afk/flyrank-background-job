"""
Worker — the thing that does the slow work off the request path.

Non-negotiables baked in:
  * Retries with exponential backoff (transient failures happen).
  * Idempotency handled at enqueue time (see app.py) so re-sends don't double-run.
  * Alerts: when a job exhausts its retries, we "page someone" (here: a loud log
    line + optional webhook). In production this is PagerDuty / Slack / email.
"""

from __future__ import annotations

import os
import random
import threading
import time
import urllib.request

import jobstore

POLL_INTERVAL = 0.25          # seconds between queue checks
BACKOFF_BASE = 0.5            # seconds; delay = BACKOFF_BASE * 2**(attempt-1)
ALERT_WEBHOOK = os.environ.get("ALERT_WEBHOOK")  # optional


def _alert(message: str) -> None:
    """Someone must find out when a job dies for good."""
    print(f"[ALERT] {message}", flush=True)
    if ALERT_WEBHOOK:
        try:
            data = f'{{"text": "{message}"}}'.encode()
            req = urllib.request.Request(
                ALERT_WEBHOOK, data=data,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception as exc:  # never let alerting crash the worker
            print(f"[ALERT] webhook failed: {exc}", flush=True)


def _run_ai_call(payload: dict) -> dict:
    """
    Stand-in for a slow A6-style AI call. Sleeps to simulate latency and can be
    told to fail (payload {"force_fail": true}) or flake (payload {"flaky": true}).
    """
    prompt = payload.get("prompt", "")
    time.sleep(float(payload.get("work_seconds", 2)))

    if payload.get("force_fail"):
        raise RuntimeError("model provider returned 500")
    if payload.get("flaky") and random.random() < 0.6:
        raise RuntimeError("transient timeout talking to model")

    word_count = len(prompt.split())
    return {
        "summary": f"Processed prompt of {word_count} words.",
        "echo": prompt[:120],
        "model": "demo-llm-v1",
    }


HANDLERS = {
    "ai_call": _run_ai_call,
}


def _process(job: dict) -> None:
    job_id = job["id"]
    handler = HANDLERS.get(job["type"])
    if handler is None:
        jobstore.mark_retry_or_fail(job_id, f"no handler for type '{job['type']}'")
        _alert(f"job {job_id} has unknown type '{job['type']}'")
        return

    try:
        result = handler(job["payload"])
        jobstore.mark_succeeded(job_id, result)
        print(f"[worker] job {job_id} succeeded on attempt {job['attempts']}", flush=True)
    except Exception as exc:
        status = jobstore.mark_retry_or_fail(job_id, str(exc))
        if status == "queued":
            delay = BACKOFF_BASE * (2 ** (job["attempts"] - 1))
            print(
                f"[worker] job {job_id} failed (attempt {job['attempts']}): {exc} "
                f"-> retrying in {delay:.1f}s",
                flush=True,
            )
            time.sleep(delay)
        else:
            _alert(
                f"job {job_id} FAILED permanently after {job['attempts']} attempts: {exc}"
            )


class Worker(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self._stop = threading.Event()

    def run(self) -> None:
        print("[worker] started", flush=True)
        while not self._stop.is_set():
            job = jobstore.claim_next_queued()
            if job is None:
                time.sleep(POLL_INTERVAL)
                continue
            _process(job)

    def stop(self) -> None:
        self._stop.set()
