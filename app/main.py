import logging
import os

import httpx
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Histogram
from pydantic import BaseModel
from shared.metrics import install_metrics

SERVICE_NAME = "dispatcher"
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
SERVICE_ENV = os.getenv("APP_ENV", "prod")
WORKER_URL = os.getenv("WORKER_URL", "http://127.0.0.1:8002")
WORKER_TIMEOUT_SECONDS = float(os.getenv("WORKER_TIMEOUT_SECONDS", "180"))
WORKER_TIMEOUT_MIN_SECONDS = float(os.getenv("WORKER_TIMEOUT_MIN_SECONDS", "420"))
WORKER_CONNECT_TIMEOUT_SECONDS = float(os.getenv("WORKER_CONNECT_TIMEOUT_SECONDS", "10"))
WORKER_WRITE_TIMEOUT_SECONDS = float(os.getenv("WORKER_WRITE_TIMEOUT_SECONDS", "30"))
WORKER_POOL_TIMEOUT_SECONDS = float(os.getenv("WORKER_POOL_TIMEOUT_SECONDS", "30"))
WORKER_READ_TIMEOUT_SECONDS = max(WORKER_TIMEOUT_SECONDS, WORKER_TIMEOUT_MIN_SECONDS)

logger = logging.getLogger(__name__)

dispatcher_jobs_dispatched_total = Counter(
    "dispatcher_jobs_dispatched_total",
    "Total jobs dispatched",
)
dispatcher_jobs_failed_total = Counter(
    "dispatcher_jobs_failed_total",
    "Total jobs failed while dispatching to worker",
)
dispatcher_dispatch_duration_seconds = Histogram(
    "dispatcher_dispatch_duration_seconds",
    "Dispatcher->worker roundtrip latency",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120),
)


class DispatchRequest(BaseModel):
    job_id: str
    claim: str
    room_id: str | None = None
    source: str | None = None


app = FastAPI(title="Luxia Dispatcher", version=SERVICE_VERSION)
install_metrics(app, service_name=SERVICE_NAME, version=SERVICE_VERSION, env=SERVICE_ENV)
logger.info(
    "[Dispatcher] timeouts read=%.1fs(min=%.1fs) connect=%.1fs write=%.1fs pool=%.1fs",
    WORKER_READ_TIMEOUT_SECONDS,
    WORKER_TIMEOUT_MIN_SECONDS,
    WORKER_CONNECT_TIMEOUT_SECONDS,
    WORKER_WRITE_TIMEOUT_SECONDS,
    WORKER_POOL_TIMEOUT_SECONDS,
)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/dispatch/test")
async def dispatch_test() -> dict[str, object]:
    dispatcher_jobs_dispatched_total.inc()
    return {"status": "ok", "service": SERVICE_NAME, "message": "dispatch counter incremented"}


@app.post("/dispatch/submit")
async def dispatch_submit(payload: DispatchRequest) -> dict[str, object]:
    dispatcher_jobs_dispatched_total.inc()
    try:
        with dispatcher_dispatch_duration_seconds.time():
            timeout = httpx.Timeout(
                connect=WORKER_CONNECT_TIMEOUT_SECONDS,
                read=WORKER_READ_TIMEOUT_SECONDS,
                write=WORKER_WRITE_TIMEOUT_SECONDS,
                pool=WORKER_POOL_TIMEOUT_SECONDS,
            )
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(
                    f"{WORKER_URL}/worker/verify",
                    json={
                        "job_id": payload.job_id,
                        "claim": payload.claim,
                        "room_id": payload.room_id,
                        "source": payload.source or SERVICE_NAME,
                    },
                )
                response.raise_for_status()
                worker_result = response.json()
    except Exception as exc:
        dispatcher_jobs_failed_total.inc()
        raise HTTPException(status_code=502, detail=f"Worker call failed: {exc}") from exc

    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "job_id": payload.job_id,
        "room_id": payload.room_id,
        "result": worker_result,
    }


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": SERVICE_NAME, "status": "running"}
