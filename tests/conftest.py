"""Pytest configuration and shared fixtures."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.utils.schemas import PostEnvelope, WorkerJob


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_post():
    """Sample post envelope for testing."""
    return PostEnvelope(
        post_id="test-post-123",
        room_id="room-456",
        text="Breaking: New vaccine shows 95% effectiveness against COVID-19",
        meta={},
    )


@pytest.fixture
def sample_post_with_domain():
    """Sample post with explicit domain."""
    return PostEnvelope(
        post_id="test-post-456",
        room_id="room-789",
        text="Stock markets rally today",
        meta={"domain": "finance"},
    )


@pytest.fixture
def sample_worker_job():
    """Sample worker job for testing."""
    return WorkerJob(
        job_id="job-123",
        post=PostEnvelope(post_id="post-123", text="Test post", meta={}),
        assigned_worker_group="health-workers",
        attempt=0,
    )


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer."""
    producer = AsyncMock(spec=AIOKafkaProducer)
    producer.send_and_wait = AsyncMock(return_value=None)
    return producer


@pytest.fixture
def mock_kafka_consumer():
    """Mock Kafka consumer."""
    consumer = AsyncMock(spec=AIOKafkaConsumer)
    return consumer


@pytest.fixture
def mock_groq_client():
    """Mock Groq client for LLM testing."""
    client = MagicMock()

    # Mock response structure
    mock_content = MagicMock()
    mock_content.text = "health"

    mock_message = MagicMock()
    mock_message.content = [mock_content]

    client.messages.create = MagicMock(return_value=mock_message)

    return client


@pytest.fixture
def sample_kafka_message():
    """Create a sample Kafka message."""
    msg = MagicMock()
    post_data = {"post_id": "test-123", "text": "COVID-19 vaccine news", "meta": {}}
    msg.value = json.dumps(post_data).encode("utf-8")
    return msg


@pytest.fixture
def invalid_kafka_message():
    """Create an invalid Kafka message."""
    msg = MagicMock()
    msg.value = b"invalid json {{"
    return msg
