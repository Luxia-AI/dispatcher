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
KAFKA_RESULTS_PUBLISH_ATTEMPTS = max(
    1, int(os.getenv("KAFKA_RESULTS_PUBLISH_ATTEMPTS", "3"))
)
KAFKA_RESULTS_PUBLISH_BACKOFF_SECONDS = max(
    0.0, float(os.getenv("KAFKA_RESULTS_PUBLISH_BACKOFF_SECONDS", "1.0"))
)
SOCKETHUB_BASE_URL = os.getenv("SOCKETHUB_URL", "").strip().rstrip("/")
SOCKETHUB_RESULT_CALLBACK_URL = os.getenv(
    "SOCKETHUB_RESULT_CALLBACK_URL",
    (
        f"{SOCKETHUB_BASE_URL}/internal/dispatch-result"
        if SOCKETHUB_BASE_URL
        else ""
    ),
).strip()
SOCKETHUB_RESULT_CALLBACK_TOKEN = os.getenv(
    "SOCKETHUB_RESULT_CALLBACK_TOKEN", ""
).strip()

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
        raw: dict[str, object] | None = None
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
            published = await _publish_result_with_retry(
                out, payload.room_id, payload.job_id
            )
            if not published:
                fallback_ok = await _emit_result_fallback(
                    out, payload.room_id, payload.job_id
                )
                if not fallback_ok:
                    logger.error(
                        "[Dispatcher] Worker completed but result delivery failed room_id=%s job_id=%s",
                        str(payload.room_id or ""),
                        payload.job_id,
                    )
        except Exception as exc:
            err_payload = {
                "status": "error",
                "job_id": raw.get("job_id") if isinstance(raw, dict) else None,
                "room_id": raw.get("room_id") if isinstance(raw, dict) else None,
                "message": f"Kafka dispatch failed: {type(exc).__name__}: {exc}",
            }
            if not (
                await _publish_result_with_retry(
                    err_payload,
                    err_payload.get("room_id"),
                    err_payload.get("job_id"),
                )
            ):
                await _emit_result_fallback(
                    err_payload,
                    err_payload.get("room_id"),
                    err_payload.get("job_id"),
                )
            logger.exception("[Dispatcher] Kafka consume/dispatch failure")


async def _publish_result_with_retry(
    result_payload: dict[str, object],
    room_id: object,
    job_id: object,
) -> bool:
    if _kafka_producer is None:
        logger.warning("[Dispatcher][Kafka] result publish skipped: producer unavailable")
        return False

    encoded = json.dumps(result_payload).encode("utf-8")
    for attempt in range(1, KAFKA_RESULTS_PUBLISH_ATTEMPTS + 1):
        try:
            await _kafka_producer.send_and_wait(KAFKA_RESULTS_TOPIC, encoded)
            logger.info(
                "[Dispatcher][Kafka] result publish ok topic=%s room_id=%s job_id=%s status=%s attempt=%d/%d",
                KAFKA_RESULTS_TOPIC,
                str(room_id or ""),
                str(job_id or ""),
                str(result_payload.get("status") or ""),
                attempt,
                KAFKA_RESULTS_PUBLISH_ATTEMPTS,
            )
            return True
        except Exception as exc:
            logger.warning(
                "[Dispatcher][Kafka] result publish failed topic=%s room_id=%s job_id=%s attempt=%d/%d error=%s",
                KAFKA_RESULTS_TOPIC,
                str(room_id or ""),
                str(job_id or ""),
                attempt,
                KAFKA_RESULTS_PUBLISH_ATTEMPTS,
                exc,
            )
            if attempt < KAFKA_RESULTS_PUBLISH_ATTEMPTS:
                await asyncio.sleep(
                    KAFKA_RESULTS_PUBLISH_BACKOFF_SECONDS * attempt
                )
    return False


async def _emit_result_fallback(
    result_payload: dict[str, object],
    room_id: object,
    job_id: object,
) -> bool:
    if not SOCKETHUB_RESULT_CALLBACK_URL:
        logger.warning(
            "[Dispatcher][Fallback] skipped callback room_id=%s job_id=%s reason=SOCKETHUB_RESULT_CALLBACK_URL not set",
            str(room_id or ""),
            str(job_id or ""),
        )
        return False

    callback_headers = {}
    if SOCKETHUB_RESULT_CALLBACK_TOKEN:
        callback_headers["x-dispatcher-token"] = SOCKETHUB_RESULT_CALLBACK_TOKEN

    try:
        timeout = httpx.Timeout(connect=5.0, read=20.0, write=10.0, pool=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                SOCKETHUB_RESULT_CALLBACK_URL,
                json=result_payload,
                headers=callback_headers,
            )
            response.raise_for_status()
        logger.info(
            "[Dispatcher][Fallback] callback ok room_id=%s job_id=%s status=%s",
            str(room_id or ""),
            str(job_id or ""),
            str(result_payload.get("status") or ""),
        )
        return True
    except Exception as exc:
        logger.warning(
            "[Dispatcher][Fallback] callback failed room_id=%s job_id=%s error=%s",
            str(room_id or ""),
            str(job_id or ""),
            exc,
        )
        return False


def _validate_kafka_runtime_config() -> None:
    cfg = get_kafka_config()
    bootstrap = str(cfg.get("bootstrap_servers", ""))
    request_timeout_ms = int(cfg.get("request_timeout_ms", 0))
    retries = KAFKA_RESULTS_PUBLISH_ATTEMPTS
    retry_backoff_ms = int(cfg.get("retry_backoff_ms", 0))
    security_protocol = str(cfg.get("security_protocol", "PLAINTEXT"))
    using_event_hubs = "servicebus.windows.net" in bootstrap.lower()

    logger.info(
        "[Dispatcher][KafkaConfig] bootstrap=%s security_protocol=%s request_timeout_ms=%d publish_attempts=%d retry_backoff_ms=%d",
        bootstrap,
        security_protocol,
        request_timeout_ms,
        retries,
        retry_backoff_ms,
    )
    if using_event_hubs and security_protocol != "SASL_SSL":
        logger.warning(
            "[Dispatcher][KafkaConfig] Event Hubs bootstrap detected but security protocol is not SASL_SSL"
        )
    if using_event_hubs and ":9093" not in bootstrap:
        logger.warning(
            "[Dispatcher][KafkaConfig] Event Hubs bootstrap should usually include :9093"
        )
    if request_timeout_ms < 60000:
        logger.warning(
            "[Dispatcher][KafkaConfig] request_timeout_ms=%d may be too low for Event Hubs latency spikes",
            request_timeout_ms,
        )
    if retries < 3:
        logger.warning(
            "[Dispatcher][KafkaConfig] publish_attempts=%d may be too low for transient broker timeouts",
            retries,
        )


@app.on_event("startup")
async def startup_kafka() -> None:
    global _kafka_consumer, _kafka_producer, _kafka_task
    if not ENABLE_KAFKA:
        logger.info("[Dispatcher] Kafka loop disabled (ENABLE_KAFKA=false)")
        return
    _validate_kafka_runtime_config()
    if SOCKETHUB_RESULT_CALLBACK_URL:
        logger.info(
            "[Dispatcher][Fallback] callback enabled url=%s",
            SOCKETHUB_RESULT_CALLBACK_URL,
        )
    else:
        logger.warning(
            "[Dispatcher][Fallback] callback disabled; set SOCKETHUB_URL or SOCKETHUB_RESULT_CALLBACK_URL for non-Kafka delivery fallback"
        )
    cfg = get_kafka_config()
    _kafka_consumer = AIOKafkaConsumer(
        POSTS_TOPIC,
        group_id=KAFKA_CONSUMER_GROUP,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        **cfg,
    )
    _kafka_producer = AIOKafkaProducer(**cfg)
    try:
        await _kafka_consumer.start()
        await _kafka_producer.start()
    except Exception:
        with contextlib.suppress(Exception):
            await _kafka_consumer.stop()
        _kafka_consumer = None
        _kafka_producer = None
        raise
    with contextlib.suppress(Exception):
        post_partitions = _kafka_consumer.partitions_for_topic(POSTS_TOPIC)
        logger.info(
            "[Dispatcher][KafkaConfig] topic=%s partitions=%s",
            POSTS_TOPIC,
            sorted(post_partitions) if post_partitions else [],
        )
    try:
        results_partitions = await _kafka_producer.partitions_for(KAFKA_RESULTS_TOPIC)
        logger.info(
            "[Dispatcher][KafkaConfig] results_topic=%s partitions=%s",
            KAFKA_RESULTS_TOPIC,
            sorted(results_partitions) if results_partitions else [],
        )
        if not results_partitions:
            logger.warning(
                "[Dispatcher][KafkaConfig] results topic has no visible partitions; check Event Hubs topic/entity and permissions"
            )
    except Exception as exc:
        logger.warning(
            "[Dispatcher][KafkaConfig] unable to validate results topic metadata topic=%s error=%s",
            KAFKA_RESULTS_TOPIC,
            exc,
        )
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
