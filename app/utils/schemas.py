from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class PostEnvelope(BaseModel):
    post_id: str
    room_id: str | None = None
    text: str
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    meta: dict[str, Any] = {}


class WorkerJob(BaseModel):
    job_id: str
    post: PostEnvelope
    assigned_worker_group: str
    attempt: int = 0
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
