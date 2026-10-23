"""Dead-letter topic handler for poison messages.

Messages that fail validation or processing after all retries are
sent to the dead-letter topic for manual inspection and reprocessing.
"""

import json

from aiokafka import AIOKafkaProducer

from config import settings
from observability.logging import get_logger
from observability.metrics import dead_letter_total

logger = get_logger("consumer.dead_letter")

_producer: AIOKafkaProducer | None = None


async def get_dead_letter_producer() -> AIOKafkaProducer:
    """Lazy-init a Kafka producer for dead-letter messages."""
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await _producer.start()
        logger.info("dead_letter_producer_started")
    return _producer


async def send_to_dead_letter(
    original_message: bytes | str,
    reason: str,
    error_details: str,
    transaction_id: str | None = None,
):
    """Send a failed message to the dead-letter topic.

    Wraps the original message with error metadata for debugging.
    """
    producer = await get_dead_letter_producer()

    envelope = {
        "original_message": original_message.decode("utf-8")
        if isinstance(original_message, bytes)
        else original_message,
        "transaction_id": transaction_id,
        "reason": reason,
        "error_details": error_details,
    }

    await producer.send_and_wait(
        settings.kafka_topic_dead_letter,
        value=envelope,
    )

    dead_letter_total.labels(reason=reason).inc()
    logger.warning(
        "message_sent_to_dead_letter",
        transaction_id=transaction_id,
        reason=reason,
        error=error_details,
    )


async def close_dead_letter_producer():
    """Gracefully close the dead-letter producer."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None
        logger.info("dead_letter_producer_stopped")
