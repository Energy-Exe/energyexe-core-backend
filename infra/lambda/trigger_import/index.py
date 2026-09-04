"""Fire a scheduled import by calling the backend's public trigger endpoint.

Why this exists: EventBridge API Destinations enforce a hard 5-second
invocation timeout, but `/import-jobs/trigger/{job}` answers only when the
import has finished (40s-4min; the response is the terminal job row). Calling
it directly from EventBridge meant every delivery was recorded as failed
despite succeeding server-side, and retried — 9 imports an hour plus a permanent
false alarm.

A Lambda can wait for the real response, so retries here are driven by the
actual outcome rather than by a timeout. Uses only the stdlib: no layer, no
dependencies, and no VPC (the API is public via the ALB).

Retry rules (2026-09-05, the endpoint dedups re-triggers by job key):
  * transport error / 5xx / job "failed"  -> retry in-invocation with backoff,
    while the function's remaining time allows a full attempt;
  * read timeout (import still running server-side) -> do NOT re-POST here;
    raise so Lambda's async retry checks back in a few minutes. That POST hits
    the endpoint's in-flight guard and gets the live/finished row;
  * response status "running"/"pending" -> the guard answered: treat as ok.

Event shape: {"job_name": "taipower-hourly"}  — optionally "api_url".
Raising propagates failure to Lambda's async retry machinery and, once those are
exhausted, to the on-failure SQS destination that alarms.
"""

import json
import os
import time
import urllib.error
import urllib.request

DEFAULT_API_URL = os.environ.get("API_URL", "").rstrip("/")

# The import is idempotent — its upsert key is
# (source, source_type, identifier, period_start) — so retrying is always safe.
ATTEMPTS = int(os.environ.get("ATTEMPTS", "3"))
BACKOFF_SECONDS = int(os.environ.get("BACKOFF_SECONDS", "20"))
# Must stay under the ALB idle timeout (300s) and the function timeout (300s).
READ_TIMEOUT_SECONDS = int(os.environ.get("READ_TIMEOUT_SECONDS", "270"))
# Never start an attempt that cannot get a real answer before the function dies.
MIN_ATTEMPT_SECONDS = 30
SAFETY_MARGIN_SECONDS = 10
IN_FLIGHT_STATUSES = ("running", "pending")


def _remaining_seconds(context) -> float:
    fn = getattr(context, "get_remaining_time_in_millis", None)
    if not callable(fn):
        return float(READ_TIMEOUT_SECONDS)
    return fn() / 1000.0 - SAFETY_MARGIN_SECONDS


def _is_read_timeout(exc) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    return isinstance(getattr(exc, "reason", None), TimeoutError)


def _post(url: str, timeout: float = READ_TIMEOUT_SECONDS) -> dict:
    request = urllib.request.Request(url, method="POST")
    request.add_header("X-Triggered-By", "eventbridge-lambda")

    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        if response.status < 200 or response.status >= 300:
            raise RuntimeError(f"HTTP {response.status}: {body[:400]}")
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            # A 2xx that isn't JSON still means the call landed; don't fail on it.
            return {"raw": body[:400]}


def handler(event, context=None):
    job_name = (event or {}).get("job_name")
    if not job_name:
        raise ValueError("event must include job_name")

    api_url = ((event or {}).get("api_url") or DEFAULT_API_URL).rstrip("/")
    if not api_url:
        raise ValueError("API_URL env var or event.api_url is required")

    url = f"{api_url}/api/v1/import-jobs/trigger/{job_name}"

    last_error = None
    for attempt in range(1, ATTEMPTS + 1):
        remaining = _remaining_seconds(context)
        if remaining < MIN_ATTEMPT_SECONDS:
            print(f"attempt {attempt}/{ATTEMPTS} skipped for {job_name}: {remaining:.0f}s left")
            break
        try:
            result = _post(url, timeout=min(READ_TIMEOUT_SECONDS, remaining))
        except (urllib.error.URLError, urllib.error.HTTPError, RuntimeError, TimeoutError) as exc:
            last_error = exc
            detail = getattr(exc, "read", None)
            if callable(detail):
                try:
                    last_error = f"{exc} :: {detail().decode('utf-8')[:300]}"
                except Exception:
                    pass
            print(f"attempt {attempt}/{ATTEMPTS} failed for {job_name}: {last_error}")
            if _is_read_timeout(exc):
                # The import is still running server-side. A second POST now
                # would only hit the in-flight guard; let the async retry check
                # back later instead of burning the rest of this invocation.
                break
            # A deploy takes the single backend task down for ~1-2 min, which is
            # exactly what these in-invocation retries are for.
            if attempt < ATTEMPTS:
                time.sleep(BACKOFF_SECONDS)
            continue

        # The endpoint returns 200 with the job row even when the import itself
        # failed, so the HTTP status alone is not the success signal.
        status = result.get("status")
        if status in IN_FLIGHT_STATUSES:
            # The endpoint's guard found this job already in flight (a previous
            # attempt or invocation started it). Nothing to retry.
            print(f"{job_name} already in flight: job_id={result.get('id')} status={status}")
            return {"job_name": job_name, "job_id": result.get("id"), "status": status}
        if status == "failed":
            last_error = f"job reported failed: {result.get('error_message')!r}"
            print(f"attempt {attempt}/{ATTEMPTS} {last_error}")
            if attempt < ATTEMPTS:
                time.sleep(BACKOFF_SECONDS)
            continue

        print(
            f"{job_name} ok: job_id={result.get('id')} status={status} "
            f"records_imported={result.get('records_imported')} "
            f"duration_s={result.get('duration_seconds')}"
        )
        return {
            "job_name": job_name,
            "job_id": result.get("id"),
            "status": status,
            "records_imported": result.get("records_imported"),
        }

    # Exhausted in-invocation attempts: raise so Lambda's async retries take
    # over, and the on-failure SQS destination alarms if those run out too.
    raise RuntimeError(f"{job_name} failed after {ATTEMPTS} attempts: {last_error}")
