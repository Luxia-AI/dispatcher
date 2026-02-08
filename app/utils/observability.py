import os
from typing import Optional

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_TRACING_INITIALIZED = False


def setup_tracing(app) -> None:
    global _TRACING_INITIALIZED
    if _TRACING_INITIALIZED:
        return

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    service_name = os.getenv("OTEL_SERVICE_NAME", "dispatcher")

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
    _TRACING_INITIALIZED = True


def get_trace_context() -> dict[str, Optional[str]]:
    span = trace.get_current_span()
    span_context = span.get_span_context()
    if not span_context or not span_context.is_valid:
        return {"trace_id": None, "span_id": None, "parent_span_id": None}

    parent = span.parent
    return {
        "trace_id": f"{span_context.trace_id:032x}",
        "span_id": f"{span_context.span_id:016x}",
        "parent_span_id": f"{parent.span_id:016x}" if parent else None,
    }
