"""Test data schemas and validation."""

import pytest
from pydantic import ValidationError

from app.utils.schemas import PostEnvelope, WorkerJob


class TestPostEnvelope:
    """Test PostEnvelope schema."""

    def test_valid_post_envelope(self):
        """Test creating a valid post envelope."""
        post = PostEnvelope(
            post_id="123",
            text="Test post content",
            room_id="room-1",
            meta={"key": "value"},
        )

        assert post.post_id == "123"
        assert post.text == "Test post content"
        assert post.room_id == "room-1"
        assert post.meta == {"key": "value"}
        assert post.timestamp is not None

    def test_post_envelope_minimal(self):
        """Test creating post with minimal fields."""
        post = PostEnvelope(post_id="123", text="Test")

        assert post.post_id == "123"
        assert post.text == "Test"
        assert post.room_id is None
        assert post.meta == {}

    def test_post_envelope_missing_required(self):
        """Test validation fails without required fields."""
        with pytest.raises(ValidationError):
            PostEnvelope(post_id="123")  # Missing text

        with pytest.raises(ValidationError):
            PostEnvelope(text="Test")  # Missing post_id


class TestWorkerJob:
    """Test WorkerJob schema."""

    def test_valid_worker_job(self, sample_post):
        """Test creating a valid worker job."""
        job = WorkerJob(
            job_id="job-123", post=sample_post, assigned_worker_group="health-workers"
        )

        assert job.job_id == "job-123"
        assert job.post == sample_post
        assert job.assigned_worker_group == "health-workers"
        assert job.attempt == 0
        assert job.created_at is not None

    def test_worker_job_serialization(self, sample_post):
        """Test job can be serialized to dict."""
        job = WorkerJob(
            job_id="job-789", post=sample_post, assigned_worker_group="tech-workers"
        )

        job_dict = job.dict()
        assert isinstance(job_dict, dict)
        assert job_dict["job_id"] == "job-789"
        assert "post" in job_dict
        assert job_dict["assigned_worker_group"] == "tech-workers"
