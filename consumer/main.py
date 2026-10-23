"""Kafka consumer — the core of the fraud signal pipeline.

Consumes transactions from Kafka, validates with Pydantic, scores
for fraud using rules + IsolationForest + risk API, and upserts
results to PostgreSQL with idempotency guarantees.

Key design decisions:
- Batch processing via getmany() for throughput
- Manual offset commit after successful upsert (at-least-once + idempotent)
- Periodic lag computation (every 5s, not every message)
- Bounded asyncio.gather for concurrent processing within a batch

Also exposes a Prometheus metrics endpoint on PROMETHEUS_PORT.
"""

import asyncio
import json
import signal
import time
from datetime import datetime

from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server
from pydantic import BaseModel, Field, ValidationError

from config import settings
from consumer.db import close_db, insert_alert, upsert_transaction
from consumer.dead_letter import close_dead_letter_producer, send_to_dead_letter
from consumer.resiliency import call_risk_api
from consumer.scoring import anomaly_model, score_transaction
from observability.logging import get_logger, setup_logging
from observability.metrics import (
    alerts_generated_total,
    kafka_consumer_lag,
    processing_latency_seconds,
    transactions_processed_total,
    transactions_processing_errors_total,
)
from observability.tracing import setup_tracing

setup_logging(settings.log_level)
logger = get_logger("consumer")

shutdown_event = asyncio.Event()

# Batch processing settings
BATCH_TIMEOUT_MS = 500  # Wait up to 500ms for a batch
MAX_BATCH_SIZE = 100  # Process up to 100 messages per batch
MAX_CONCURRENT = 20  # Max concurrent message processing within a batch
LAG_CHECK_INTERVAL = 5.0  # Check consumer lag every 5 seconds


# ──────────────────────────── Pydantic Schema ────────────────────────────


class TransactionMessage(BaseModel):
    """Schema for incoming Kafka transaction messages."""

    transaction_id: str
    card_id: str
    amount: float = Field(gt=0)
    currency: str = Field(default="USD", max_length=3)
    merchant: str
    merchant_category: str | None = None
    country: str = Field(max_length=3)
    timestamp: datetime
    is_fraud_injected: bool = False
    fraud_pattern: str | None = None


# ──────────────────────────── Message Processing ────────────────────────────


async def process_message(raw_value: bytes, tracer) -> None:
    """Process a single Kafka message through the full pipeline.

    Flow: deserialize → validate → risk API → score → upsert → alert
    """
    with tracer.start_as_current_span("process_transaction") as span:
        start_time = time.monotonic()

        # Step 1: Deserialize
        try:
            data = json.loads(raw_value)
        except json.JSONDecodeError as e:
            transactions_processing_errors_total.labels(error_type="validation").inc()
            await send_to_dead_letter(raw_value, "json_decode_error", str(e))
            return

        transaction_id = data.get("transaction_id", "unknown")
        span.set_attribute("transaction.id", transaction_id)

        # Step 2: Validate with Pydantic
        try:
            txn = TransactionMessage(**data)
        except ValidationError as e:
            transactions_processing_errors_total.labels(error_type="validation").inc()
            await send_to_dead_letter(raw_value, "validation_error", str(e), transaction_id)
            return

        span.set_attribute("transaction.amount", txn.amount)
        span.set_attribute("transaction.country", txn.country)
        span.set_attribute("transaction.card_id", txn.card_id)

        try:
            # Step 3: Call risk API (with resiliency)
            with tracer.start_as_current_span("call_risk_api"):
                risk_result = await call_risk_api(
                    {
                        "transaction_id": txn.transaction_id,
                        "amount": txn.amount,
                        "country": txn.country,
                        "card_id": txn.card_id,
                        "merchant": txn.merchant,
                    }
                )

            span.set_attribute("risk_api.degraded", risk_result.degraded)

            # Step 4: Score transaction
            with tracer.start_as_current_span("score_transaction"):
                score_result = score_transaction(
                    amount=txn.amount,
                    country=txn.country,
                    timestamp=txn.timestamp,
                    card_id=txn.card_id,
                    risk_api_score=risk_result.score,
                )

            span.set_attribute("fraud.score", score_result["fraud_score"])
            span.set_attribute("fraud.is_fraud", score_result["is_fraud"])

            # Step 5: Upsert to PostgreSQL (idempotent)
            with tracer.start_as_current_span("db_upsert"):
                is_new = await upsert_transaction(
                    transaction_id=txn.transaction_id,
                    card_id=txn.card_id,
                    amount=txn.amount,
                    currency=txn.currency,
                    merchant=txn.merchant,
                    merchant_category=txn.merchant_category,
                    country=txn.country,
                    timestamp=txn.timestamp,
                    is_fraud=score_result["is_fraud"],
                    fraud_score=score_result["fraud_score"],
                    fraud_reasons=score_result["reasons"],
                    risk_api_score=risk_result.score,
                    degraded=risk_result.degraded,
                )

            status = "success" if is_new else "duplicate"
            transactions_processed_total.labels(status=status).inc()

            # Step 6: Generate alert if fraud detected
            if score_result["is_fraud"] and is_new:
                severity = "critical" if score_result["fraud_score"] >= 0.85 else "high"
                with tracer.start_as_current_span("create_alert"):
                    alert_id = await insert_alert(
                        transaction_id=txn.transaction_id,
                        alert_type="fraud_detected",
                        severity=severity,
                        details={
                            "fraud_score": score_result["fraud_score"],
                            "reasons": score_result["reasons"],
                            "detection_method": score_result["detection_method"],
                            "amount": txn.amount,
                            "country": txn.country,
                            "degraded": risk_result.degraded,
                        },
                    )

                alerts_generated_total.labels(alert_type="fraud_detected", severity=severity).inc()

                logger.warning(
                    "fraud_alert_created",
                    alert_id=alert_id,
                    transaction_id=txn.transaction_id,
                    fraud_score=score_result["fraud_score"],
                    reasons=score_result["reasons"],
                    amount=txn.amount,
                )

            elapsed = time.monotonic() - start_time
            processing_latency_seconds.observe(elapsed)

        except Exception as e:
            transactions_processing_errors_total.labels(error_type="processing").inc()
            logger.error(
                "processing_error",
                transaction_id=transaction_id,
                error=str(e),
                error_type=type(e).__name__,
            )
            await send_to_dead_letter(raw_value, "processing_error", str(e), transaction_id)


# ──────────────────────────── Consumer Loop ────────────────────────────


async def _update_lag(consumer):
    """Compute and update consumer lag metric."""
    try:
        partitions = consumer.assignment()
        if not partitions:
            return
        end_offsets = await consumer.end_offsets(partitions)
        total_lag = 0
        for tp in partitions:
            pos = await consumer.position(tp)
            total_lag += end_offsets[tp] - pos
        kafka_consumer_lag.set(total_lag)
    except Exception:
        pass  # Non-critical


async def consume():
    """Main consumer loop — reads batches from Kafka and processes concurrently."""
    # Initialize tracing
    tracer = setup_tracing(
        service_name=settings.otel_service_name,
        otlp_endpoint=settings.otel_exporter_otlp_endpoint,
    )

    # Start Prometheus metrics server
    start_http_server(settings.prometheus_port)
    logger.info("prometheus_metrics_started", port=settings.prometheus_port)

    # Initialize scoring model
    anomaly_model.load_or_train()

    consumer = AIOKafkaConsumer(
        settings.kafka_topic_transactions,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # Manual commit after successful processing
        value_deserializer=lambda v: v,  # Keep as bytes
    )

    await consumer.start()
    logger.info(
        "consumer_started",
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic_transactions,
        group_id=settings.kafka_consumer_group,
        batch_size=MAX_BATCH_SIZE,
        concurrency=MAX_CONCURRENT,
    )

    last_lag_check = 0.0

    try:
        while not shutdown_event.is_set():
            # Fetch a batch of messages
            batch = await consumer.getmany(
                timeout_ms=BATCH_TIMEOUT_MS,
                max_records=MAX_BATCH_SIZE,
            )

            if not batch:
                continue

            # Process messages concurrently within the batch (bounded)
            tasks = []
            for tp, messages in batch.items():
                for msg in messages:
                    tasks.append(process_message(msg.value, tracer))

            # Bounded concurrency: process MAX_CONCURRENT at a time
            semaphore = asyncio.Semaphore(MAX_CONCURRENT)

            async def _bounded(coro):
                async with semaphore:
                    await coro

            await asyncio.gather(*[_bounded(t) for t in tasks], return_exceptions=True)

            # Manual commit after successful batch processing
            # This gives us at-least-once delivery + idempotent upsert
            await consumer.commit()

            # Periodic lag check (every 5 seconds, not every message)
            now = time.monotonic()
            if now - last_lag_check >= LAG_CHECK_INTERVAL:
                await _update_lag(consumer)
                last_lag_check = now

    finally:
        await consumer.stop()
        await close_dead_letter_producer()
        await close_db()
        logger.info("consumer_stopped")


def handle_shutdown(sig, frame):
    """Handle graceful shutdown."""
    logger.info("shutdown_signal_received", signal=sig)
    shutdown_event.set()


def main():
    """Entry point for the consumer service."""
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info("consumer_service_starting")
    asyncio.run(consume())


if __name__ == "__main__":
    main()
