import json

from aiokafka import AIOKafkaProducer

from app.utils.config import DLQ_TOPIC, JOBS_TOPIC
from app.utils.metrics import dispatch_latency, jobs_dispatched, jobs_failed


class KafkaPublisher:
    def __init__(self, producer: AIOKafkaProducer):
        self.producer = producer

    async def publish_job(self, job_payload: dict):
        with dispatch_latency.time():
            await self.producer.send_and_wait(
                JOBS_TOPIC, json.dumps(job_payload).encode("utf-8")
            )
        jobs_dispatched.inc()

    async def publish_dlq(self, payload: dict):
        await self.producer.send_and_wait(
            DLQ_TOPIC, json.dumps(payload).encode("utf-8")
        )
        jobs_failed.inc()
