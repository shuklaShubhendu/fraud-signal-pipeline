"""Synthetic transaction generator — produces to Kafka/Redpanda.

Generates realistic payment transactions with ~2% injected fraud patterns.
Runs continuously, producing configurable msg/s to the transactions topic.
"""

import asyncio
import json
import signal
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

from config import settings
from generator.patterns import generate_legitimate_transaction, maybe_inject_fraud
from observability.logging import get_logger, setup_logging
from observability.metrics import transactions_produced_total

setup_logging(settings.log_level)
logger = get_logger("generator")

# Configuration
MESSAGES_PER_SECOND = 50
CARD_POOL_SIZE = 500  # Simulated cardholders

# Pre-generate card IDs for realistic repeat usage
CARD_IDS = [f"CARD-{uuid.uuid4().hex[:12].upper()}" for _ in range(CARD_POOL_SIZE)]

shutdown_event = asyncio.Event()


def handle_shutdown(sig, frame):
    """Handle graceful shutdown signals."""
    logger.info("shutdown_signal_received", signal=sig)
    shutdown_event.set()


async def produce_transactions():
    """Main producer loop — generates and sends transactions to Kafka."""
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        compression_type="gzip",
    )

    await producer.start()
    logger.info(
        "producer_started",
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic_transactions,
        rate=f"{MESSAGES_PER_SECOND} msg/s",
    )

    try:
        batch_count = 0
        while not shutdown_event.is_set():
            batch_start = asyncio.get_event_loop().time()
            produced = 0

            for _ in range(MESSAGES_PER_SECOND):
                if shutdown_event.is_set():
                    break

                import random
                card_id = random.choice(CARD_IDS)
                now = datetime.now(timezone.utc)

                # Check for fraud injection (~2% of transactions)
                fraud_batch = maybe_inject_fraud(card_id, now)

                if fraud_batch:
                    for txn in fraud_batch:
                        txn["transaction_id"] = f"TXN-{uuid.uuid4().hex[:16].upper()}"
                        await producer.send_and_wait(
                            settings.kafka_topic_transactions,
                            value=txn,
                            key=txn["card_id"],
                        )
                        transactions_produced_total.inc()
                        produced += 1
                else:
                    txn = generate_legitimate_transaction(card_id, now)
                    txn["transaction_id"] = f"TXN-{uuid.uuid4().hex[:16].upper()}"
                    await producer.send_and_wait(
                        settings.kafka_topic_transactions,
                        value=txn,
                        key=txn["card_id"],
                    )
                    transactions_produced_total.inc()
                    produced += 1

            batch_count += 1
            elapsed = asyncio.get_event_loop().time() - batch_start

            if batch_count % 10 == 0:
                logger.info(
                    "producer_batch",
                    batch=batch_count,
                    produced=produced,
                    elapsed_ms=round(elapsed * 1000, 1),
                )

            # Maintain target rate
            sleep_time = max(0, 1.0 - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    finally:
        await producer.stop()
        logger.info("producer_stopped", total_batches=batch_count)


def main():
    """Entry point for the generator service."""
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    logger.info("generator_starting")
    asyncio.run(produce_transactions())


if __name__ == "__main__":
    main()
