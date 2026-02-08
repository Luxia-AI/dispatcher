from prometheus_client import Counter, Histogram

jobs_received = Counter("dispatcher_jobs_received_total", "Inbound posts received")
jobs_dispatched = Counter("dispatcher_jobs_dispatched_total", "Jobs dispatched")
jobs_failed = Counter("dispatcher_jobs_failed_total", "Jobs failed")
dispatch_latency = Histogram("dispatcher_dispatch_latency", "Dispatch latency seconds")
jobs_received_by_domain = Counter(
    "dispatcher_jobs_received_by_domain_total",
    "Inbound posts received by routed domain",
    ["domain"],
)
jobs_dispatched_by_result = Counter(
    "dispatcher_jobs_dispatched_by_result_total",
    "Dispatch results by status",
    ["result_status"],
)
