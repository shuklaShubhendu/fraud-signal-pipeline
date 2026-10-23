"""Prometheus metrics for the fraud pipeline."""

from prometheus_client import Counter, Gauge, Histogram, Info

# ──────────────────────────── Pipeline Info ────────────────────────────
pipeline_info = Info("fraud_pipeline", "Fraud signal pipeline metadata")
pipeline_info.info({"version": "1.0.0", "service": "fraud-signal-pipeline"})

# ──────────────────────────── Throughput ────────────────────────────
transactions_produced_total = Counter(
    "transactions_produced_total",
    "Total transactions produced to Kafka",
)

transactions_processed_total = Counter(
    "transactions_processed_total",
    "Total transactions processed by the consumer",
    ["status"],  # success, error, duplicate
)

transactions_processing_errors_total = Counter(
    "transactions_processing_errors_total",
    "Total processing errors",
    ["error_type"],  # validation, scoring, database, unknown
)

# ──────────────────────────── Latency ────────────────────────────
processing_latency_seconds = Histogram(
    "processing_latency_seconds",
    "End-to-end processing latency per transaction",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5],
)

scoring_latency_seconds = Histogram(
    "scoring_latency_seconds",
    "Latency of fraud scoring (rules + model)",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
)

risk_api_latency_seconds = Histogram(
    "risk_api_latency_seconds",
    "Latency of external risk API calls",
    buckets=[0.01, 0.05, 0.1, 0.2, 0.3, 0.5, 1.0],
)

db_write_latency_seconds = Histogram(
    "db_write_latency_seconds",
    "Latency of database upsert operations",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1],
)

# ──────────────────────────── Fraud Detection ────────────────────────────
fraud_detected_total = Counter(
    "fraud_detected_total",
    "Total transactions flagged as fraud",
    ["detection_method"],  # rule, model, combined
)

fraud_score_histogram = Histogram(
    "fraud_score_distribution",
    "Distribution of fraud scores",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# ──────────────────────────── Resiliency ────────────────────────────
circuit_breaker_state = Gauge(
    "circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half-open)",
)

degraded_mode_total = Counter(
    "degraded_mode_total",
    "Total requests served in degraded mode (rules-only scoring)",
)

retry_attempts_total = Counter(
    "retry_attempts_total",
    "Total retry attempts to external services",
    ["service"],  # risk_api, database
)

dead_letter_total = Counter(
    "dead_letter_total",
    "Total messages sent to dead-letter topic",
    ["reason"],  # validation_error, processing_error, poison_message
)

# ──────────────────────────── Consumer Health ────────────────────────────
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Current Kafka consumer lag (messages behind)",
)

database_errors_total = Counter(
    "database_errors_total",
    "Total database connection/write errors",
)

active_connections = Gauge(
    "active_db_connections",
    "Current active database connections",
)

# ──────────────────────────── Alerts ────────────────────────────
alerts_generated_total = Counter(
    "alerts_generated_total",
    "Total alerts generated",
    ["alert_type", "severity"],
)
