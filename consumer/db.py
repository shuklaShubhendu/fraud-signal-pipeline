"""PostgreSQL database layer with idempotent upserts.

Uses SQLAlchemy async with asyncpg for non-blocking DB operations.
Upserts on transaction_id ensure idempotency — duplicate Kafka messages
won't create duplicate rows.
"""

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import settings
from observability.logging import get_logger
from observability.metrics import (
    active_connections,
    database_errors_total,
    db_write_latency_seconds,
)

logger = get_logger("consumer.db")

engine = create_async_engine(
    settings.database_url,
    pool_size=10,
    max_overflow=5,
    pool_pre_ping=True,
    pool_recycle=300,
)

async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def upsert_transaction(
    transaction_id: str,
    card_id: str,
    amount: float,
    currency: str,
    merchant: str,
    merchant_category: str | None,
    country: str,
    timestamp: datetime,
    is_fraud: bool,
    fraud_score: float,
    fraud_reasons: list[str],
    risk_api_score: float | None,
    degraded: bool,
) -> bool:
    """Upsert a transaction into PostgreSQL.

    Uses INSERT ... ON CONFLICT (transaction_id) DO UPDATE for idempotency.
    Returns True if inserted (new), False if updated (duplicate).
    """
    upsert_sql = text("""
        INSERT INTO transactions (
            transaction_id, card_id, amount, currency, merchant,
            merchant_category, country, timestamp, is_fraud, fraud_score,
            fraud_reasons, risk_api_score, degraded, processed_at
        ) VALUES (
            :transaction_id, :card_id, :amount, :currency, :merchant,
            :merchant_category, :country, :timestamp, :is_fraud, :fraud_score,
            :fraud_reasons, :risk_api_score, :degraded, NOW()
        )
        ON CONFLICT (transaction_id) DO UPDATE SET
            fraud_score = EXCLUDED.fraud_score,
            is_fraud = EXCLUDED.is_fraud,
            fraud_reasons = EXCLUDED.fraud_reasons,
            risk_api_score = EXCLUDED.risk_api_score,
            degraded = EXCLUDED.degraded,
            processed_at = NOW()
        RETURNING (xmax = 0) AS is_insert
    """)

    try:
        with db_write_latency_seconds.time():
            async with async_session() as session:
                active_connections.inc()
                try:
                    result = await session.execute(
                        upsert_sql,
                        {
                            "transaction_id": transaction_id,
                            "card_id": card_id,
                            "amount": amount,
                            "currency": currency,
                            "merchant": merchant,
                            "merchant_category": merchant_category,
                            "country": country,
                            "timestamp": timestamp,
                            "is_fraud": is_fraud,
                            "fraud_score": fraud_score,
                            "fraud_reasons": fraud_reasons,
                            "risk_api_score": risk_api_score,
                            "degraded": degraded,
                        },
                    )
                    row = result.fetchone()
                    is_new = row[0] if row else True
                    await session.commit()

                    if not is_new:
                        logger.debug(
                            "duplicate_transaction_upserted",
                            transaction_id=transaction_id,
                        )
                    return is_new
                finally:
                    active_connections.dec()

    except Exception as e:
        database_errors_total.inc()
        logger.error(
            "database_upsert_failed",
            transaction_id=transaction_id,
            error=str(e),
        )
        raise


async def insert_alert(
    transaction_id: str,
    alert_type: str,
    severity: str,
    details: dict,
) -> int:
    """Insert a fraud alert linked to a transaction. Returns alert ID."""
    import json

    insert_sql = text("""
        INSERT INTO alerts (transaction_id, alert_type, severity, details)
        VALUES (:transaction_id, :alert_type, :severity, :details::jsonb)
        RETURNING id
    """)

    try:
        async with async_session() as session:
            result = await session.execute(
                insert_sql,
                {
                    "transaction_id": transaction_id,
                    "alert_type": alert_type,
                    "severity": severity,
                    "details": json.dumps(details),
                },
            )
            alert_id = result.scalar_one()
            await session.commit()
            return alert_id
    except Exception as e:
        database_errors_total.inc()
        logger.error("alert_insert_failed", error=str(e))
        raise


async def close_db():
    """Gracefully close the database engine."""
    await engine.dispose()
    logger.info("database_connection_closed")
