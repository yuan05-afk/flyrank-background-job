# Background Job API

Move slow work off the request path. The endpoint **accepts fast (202)**, a background **worker** does the work, and a **status endpoint** reports the result.

```
POST /jobs        -> 202 { job_id, status_url }   # returns instantly
GET  /jobs/{id}   -> { status, attempts, result, error }
GET  /jobs        -> queue counts
GET  /health      -> worker liveness
```

## Why this pattern

A request that waits 30s for an AI call ties up a connection, times out proxies, and gives the user a spinner. Instead: accept the work, hand back a ticket (`job_id`), and let a worker grind through it. This is how every SaaS runs report generation, video encoding, and model calls.

## Run

```bash
pip install -r requirements.txt && uvicorn app:app --reload --port 8010
```

Swagger: http://localhost:8010/docs

## The three non-negotiables

Real job systems must survive three facts of life. This one does:

| Fact | Handling |
|------|----------|
| **Jobs run twice** | `Idempotency-Key` header — the same key returns the same job instead of creating a duplicate. |
| **Jobs fail** | Retries with **exponential backoff** (`0.5 · 2^(attempt-1)`), up to `max_attempts`. |
| **Someone must know** | On permanent failure the worker emits a loud `[ALERT]` line (and POSTs to `ALERT_WEBHOOK` if set). |

Bonus: on startup any job left `running` by a crashed worker is **requeued** (crash recovery), and jobs are stored in **SQLite** so the queue survives a restart.

## Demo payload knobs

```jsonc
POST /jobs
{
  "prompt": "summarize the onboarding flow",
  "work_seconds": 2,     // simulated latency
  "flaky": false,        // fail ~60% of attempts (shows retries recovering)
  "force_fail": true,    // always fail (shows the alert path)
  "max_attempts": 3
}
```

## Proof (from `_checkpoint.py`)

```
POST /jobs -> 202 in 0.028s        # instant, not 2s
final: status=succeeded attempts=1
idempotency same id? True
force_fail final: failed attempts=3   # + [ALERT] logged
flaky final: succeeded attempts=3     # retries recovered it
```

## Files

```
app.py         # FastAPI routes (accept + status)
worker.py      # background thread: retries, backoff, alerts
jobstore.py    # SQLite persistence + atomic claim + crash recovery
```

## License

MIT — FlyRank Backend internship.
