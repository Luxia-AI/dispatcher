import uuid

from app.algorithms.routing import choose_worker_group
from app.utils.schemas import PostEnvelope, WorkerJob


async def build_worker_job(post: PostEnvelope) -> WorkerJob:
    worker_group = await choose_worker_group(post)

    return WorkerJob(
        job_id=str(uuid.uuid4()), post=post, assigned_worker_group=worker_group
    )
