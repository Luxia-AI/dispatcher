import json
import logging

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from app.algorithms.build_job import build_worker_job
from app.publishers.kafka import KafkaPublisher
from app.utils.config import POSTS_TOPIC
from app.utils.metrics import jobs_received, jobs_received_by_domain
from app.utils.schemas import PostEnvelope

logger = logging.getLogger("dispatcher")


class KafkaPostConsumer:
    def __init__(self, consumer: AIOKafkaConsumer, publisher: KafkaPublisher):
        self.consumer = consumer
        self.publisher = publisher

    async def start_loop(self):
        logger.info("Kafka consumer starting for topic %s", POSTS_TOPIC)

        async for msg in self.consumer:
            jobs_received.inc()

            try:
                data = json.loads(msg.value.decode("utf-8"))
                post = PostEnvelope(**data)
            except (ValidationError, json.JSONDecodeError) as e:
                logger.error("Invalid message: %s", e)
                await self.publisher.publish_dlq(
                    {
                        "error": str(e),
                        "raw": msg.value.decode("utf-8"),
                    }
                )
                continue

            job = await build_worker_job(post)
            routed_domain = job.assigned_worker_group.replace("-workers", "")
            jobs_received_by_domain.labels(domain=routed_domain).inc()
            await self.publisher.publish_job(job.dict())

            logger.info("Dispatched job %s for post %s", job.job_id, post.post_id)
