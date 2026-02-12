import asyncio
import contextlib
import json
import logging
import os
import uuid

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Histogram
from pydantic import BaseModel
from shared.metrics import install_metrics

from app.utils.config import POSTS_TOPIC, get_kafka_config

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
ENABLE_KAFKA = os.getenv("ENABLE_KAFKA", "false").strip().lower() in {"1", "true", "yes", "on"}
KAFKA_RESULTS_TOPIC = os.getenv("RESULTS_TOPIC", "jobs.results")
KAFKA_CONSUMER_GROUP = os.getenv("DISPATCHER_GROUP", "dispatcher-posts")

logger = logging.getLogger(__name__)
_kafka_consumer: AIOKafkaConsumer | None = None
_kafka_producer: AIOKafkaProducer | None = None
_kafka_task: asyncio.Task | None = None

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
    return await _dispatch_to_worker(payload)


async def _dispatch_to_worker(payload: DispatchRequest) -> dict[str, object]:
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


async def _kafka_loop() -> None:
    if _kafka_consumer is None or _kafka_producer is None:
        logger.warning("[Dispatcher] Kafka loop started without consumer/producer")
        return
    logger.info("[Dispatcher] Kafka consume loop started topic=%s group=%s", POSTS_TOPIC, KAFKA_CONSUMER_GROUP)
    async for msg in _kafka_consumer:
        try:
            raw = json.loads(msg.value.decode("utf-8"))
            payload = DispatchRequest(
                job_id=str(raw.get("job_id") or str(uuid.uuid4())),
                claim=str(raw.get("claim") or raw.get("content") or "").strip(),
                room_id=raw.get("room_id"),
                source=raw.get("source"),
            )
            logger.info(
                "[Dispatcher][Kafka] consumed ok topic=%s room_id=%s job_id=%s",
                POSTS_TOPIC,
                str(payload.room_id or ""),
                payload.job_id,
            )
            result = await _dispatch_to_worker(payload)
            out = result.get("result", result)
            if isinstance(out, dict):
                out.setdefault("job_id", payload.job_id)
                out.setdefault("room_id", payload.room_id)
            else:
                out = {
                    "status": "error",
                    "job_id": payload.job_id,
                    "room_id": payload.room_id,
                    "message": "Invalid worker response payload",
                }
            await _kafka_producer.send_and_wait(KAFKA_RESULTS_TOPIC, json.dumps(out).encode("utf-8"))
            logger.info(
                "[Dispatcher][Kafka] result publish ok topic=%s room_id=%s job_id=%s status=%s",
                KAFKA_RESULTS_TOPIC,
                str(payload.room_id or ""),
                payload.job_id,
                str(out.get("status") if isinstance(out, dict) else ""),
            )
        except Exception as exc:
            err_payload = {
                "status": "error",
                "job_id": raw.get("job_id") if isinstance(raw, dict) else None,
                "room_id": raw.get("room_id") if isinstance(raw, dict) else None,
                "message": f"Kafka dispatch failed: {type(exc).__name__}: {exc}",
            }
            with contextlib.suppress(Exception):
                await _kafka_producer.send_and_wait(KAFKA_RESULTS_TOPIC, json.dumps(err_payload).encode("utf-8"))
            logger.exception("[Dispatcher] Kafka consume/dispatch failure")


@app.on_event("startup")
async def startup_kafka() -> None:
    global _kafka_consumer, _kafka_producer, _kafka_task
    if not ENABLE_KAFKA:
        logger.info("[Dispatcher] Kafka loop disabled (ENABLE_KAFKA=false)")
        return
    cfg = get_kafka_config()
    _kafka_consumer = AIOKafkaConsumer(
        POSTS_TOPIC,
        group_id=KAFKA_CONSUMER_GROUP,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        **cfg,
    )
    _kafka_producer = AIOKafkaProducer(**cfg)
    await _kafka_consumer.start()
    await _kafka_producer.start()
    _kafka_task = asyncio.create_task(_kafka_loop())
    logger.info("[Dispatcher] Kafka enabled topic_in=%s topic_out=%s", POSTS_TOPIC, KAFKA_RESULTS_TOPIC)


@app.on_event("shutdown")
async def shutdown_kafka() -> None:
    global _kafka_consumer, _kafka_producer, _kafka_task
    if _kafka_task is not None:
        _kafka_task.cancel()
        with contextlib.suppress(Exception):
            await _kafka_task
        _kafka_task = None
    if _kafka_consumer is not None:
        with contextlib.suppress(Exception):
            await _kafka_consumer.stop()
        _kafka_consumer = None
    if _kafka_producer is not None:
        with contextlib.suppress(Exception):
            await _kafka_producer.stop()
        _kafka_producer = None
