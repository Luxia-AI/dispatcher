from prometheus_client import Counter, Histogram

jobs_received = Counter("dispatcher_jobs_received_total", "Inbound posts received")
jobs_dispatched = Counter("dispatcher_jobs_dispatched_total", "Jobs dispatched")
jobs_failed = Counter("dispatcher_jobs_failed_total", "Jobs failed")
dispatch_latency = Histogram("dispatcher_dispatch_latency", "Dispatch latency seconds")
