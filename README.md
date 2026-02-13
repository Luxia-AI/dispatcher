# Dispatcher Service

`dispatcher` coordinates claim jobs between ingress and worker execution.

## Responsibilities

- Accept dispatch requests
- Call worker verify endpoint with timeout/retry boundaries
- Optionally consume and publish Kafka events
- Provide fallback callback delivery path

## Entry Points

- API and orchestration: `dispatcher/app/main.py`
- Config: `dispatcher/app/utils/config.py`
- Schemas: `dispatcher/app/utils/schemas.py`

## Dependencies

- FastAPI + httpx
- aiokafka
- shared metrics middleware

## Canonical Docs

- `docs/system-overview.md`
- `docs/request-lifecycle.md`
- `docs/interfaces-and-contracts.md`
- `docs/deployment-and-operations.md`

Last verified against code: February 13, 2026
