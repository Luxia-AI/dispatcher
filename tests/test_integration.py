"""Integration tests for the complete workflow."""

import json
from unittest.mock import MagicMock, patch

import pytest

from app.algorithms.build_job import build_worker_job
from app.publishers.kafka import KafkaPublisher
from app.utils.schemas import PostEnvelope


class TestEndToEndWorkflow:
    """Test complete end-to-end workflow."""

    @pytest.mark.asyncio
    async def test_complete_workflow_health_domain(self, mock_kafka_producer):
        """Test complete workflow: post -> routing -> job -> publish."""
        # 1. Create incoming post
        post = PostEnvelope(
            post_id="e2e-test-1",
            room_id="room-1",
            text="Breaking news: New COVID-19 treatment approved by FDA",
            meta={},
        )

        # 2. Mock LLM to return health domain
        mock_content = MagicMock()
        mock_content.text = "health"
        mock_message = MagicMock()
        mock_message.content = [mock_content]

        mock_client = MagicMock()
        mock_client.messages.create = MagicMock(return_value=mock_message)

        with patch("app.algorithms.routing._get_groq_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            # 3. Build worker job
            job = await build_worker_job(post)

            # 4. Verify routing
            assert job.assigned_worker_group == "health-workers"
            assert job.post.post_id == "e2e-test-1"

            # 5. Publish job
            publisher = KafkaPublisher(mock_kafka_producer)
            await publisher.publish_job(job.dict())

            # 6. Verify job was published
            mock_kafka_producer.send_and_wait.assert_called_once()
            call_args = mock_kafka_producer.send_and_wait.call_args
            payload = json.loads(call_args[0][1].decode("utf-8"))

            assert payload["assigned_worker_group"] == "health-workers"
            assert payload["post"]["post_id"] == "e2e-test-1"

    @pytest.mark.asyncio
    async def test_workflow_with_explicit_domain(self, mock_kafka_producer):
        """Test workflow with explicit domain bypasses LLM."""
        post = PostEnvelope(
            post_id="e2e-test-2",
            text="Random text that might confuse LLM",
            meta={"domain": "technology"},
        )

        job = await build_worker_job(post)
        assert job.assigned_worker_group == "tech-workers"

        publisher = KafkaPublisher(mock_kafka_producer)
        await publisher.publish_job(job.dict())

        mock_kafka_producer.send_and_wait.assert_called_once()
