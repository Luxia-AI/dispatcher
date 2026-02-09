import os

from fastapi import FastAPI
from prometheus_client import Counter

from shared.metrics import install_metrics

SERVICE_NAME = "dispatcher"
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
SERVICE_ENV = os.getenv("APP_ENV", "prod")

dispatcher_jobs_dispatched_total = Counter(
    "dispatcher_jobs_dispatched_total",
    "Total jobs dispatched",
)

app = FastAPI(title="Luxia Dispatcher", version=SERVICE_VERSION)
install_metrics(app, service_name=SERVICE_NAME, version=SERVICE_VERSION, env=SERVICE_ENV)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/dispatch/test")
async def dispatch_test() -> dict[str, object]:
    dispatcher_jobs_dispatched_total.inc()
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "message": "dispatch counter incremented",
    }


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": SERVICE_NAME, "status": "running"}
