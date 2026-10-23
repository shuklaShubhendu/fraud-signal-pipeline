"""FastAPI routes for the fraud signal pipeline API.

Endpoints:
- GET  /health                      — Health check
- GET  /transactions/{id}           — Get transaction by ID
- GET  /alerts                      — List alerts with filters
- POST /webhooks/alert              — Alertmanager webhook receiver
- POST /copilot/incident/{alert_id} — RAG copilot for incident response
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from consumer.db import async_session
from observability.logging import get_logger

logger = get_logger("api.routes")
router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint — returns service status and basic stats."""
    try:
        async with async_session() as session:
            result = await session.execute(text("SELECT COUNT(*) FROM transactions"))
            txn_count = result.scalar()
            result = await session.execute(text("SELECT COUNT(*) FROM alerts"))
            alert_count = result.scalar()
    except Exception as e:
        return {
            "status": "degraded",
            "database": "unreachable",
            "error": str(e),
        }

    return {
        "status": "healthy",
        "service": "fraud-signal-pipeline",
        "version": "1.0.0",
        "database": "connected",
        "transactions_count": txn_count,
        "alerts_count": alert_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/transactions/{transaction_id}")
async def get_transaction(transaction_id: str):
    """Get a specific transaction by ID with its fraud scoring details."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT transaction_id, card_id, amount, currency, merchant,
                       merchant_category, country, timestamp, is_fraud, fraud_score,
                       fraud_reasons, risk_api_score, degraded, processed_at
                FROM transactions
                WHERE transaction_id = :txn_id
            """),
            {"txn_id": transaction_id},
        )
        row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Transaction {transaction_id} not found")

    return {
        "transaction_id": row[0],
        "card_id": row[1],
        "amount": float(row[2]),
        "currency": row[3],
        "merchant": row[4],
        "merchant_category": row[5],
        "country": row[6],
        "timestamp": row[7].isoformat() if row[7] else None,
        "is_fraud": row[8],
        "fraud_score": float(row[9]) if row[9] else 0.0,
        "fraud_reasons": row[10] or [],
        "risk_api_score": float(row[11]) if row[11] else None,
        "degraded": row[12],
        "processed_at": row[13].isoformat() if row[13] else None,
    }


@router.get("/alerts")
async def list_alerts(
    since: datetime | None = Query(None, description="Filter alerts since this timestamp (ISO format)"),
    severity: str | None = Query(None, description="Filter by severity (critical, high, medium, low)"),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of alerts to return"),
    acknowledged: bool | None = Query(None, description="Filter by acknowledged status"),
):
    """List fraud alerts with optional filters."""
    conditions = []
    params = {}

    if since:
        conditions.append("a.created_at >= :since")
        params["since"] = since

    if severity:
        conditions.append("a.severity = :severity")
        params["severity"] = severity

    if acknowledged is not None:
        conditions.append("a.acknowledged = :acknowledged")
        params["acknowledged"] = acknowledged

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    query = text(f"""
        SELECT a.id, a.transaction_id, a.alert_type, a.severity, a.details,
               a.acknowledged, a.created_at,
               t.amount, t.country, t.card_id, t.fraud_score
        FROM alerts a
        LEFT JOIN transactions t ON a.transaction_id = t.transaction_id
        {where_clause}
        ORDER BY a.created_at DESC
        LIMIT :limit
    """)
    params["limit"] = limit

    async with async_session() as session:
        result = await session.execute(query, params)
        rows = result.fetchall()

    return {
        "count": len(rows),
        "alerts": [
            {
                "alert_id": row[0],
                "transaction_id": row[1],
                "alert_type": row[2],
                "severity": row[3],
                "details": row[4],
                "acknowledged": row[5],
                "created_at": row[6].isoformat() if row[6] else None,
                "transaction": {
                    "amount": float(row[7]) if row[7] else None,
                    "country": row[8],
                    "card_id": row[9],
                    "fraud_score": float(row[10]) if row[10] else None,
                },
            }
            for row in rows
        ],
    }


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: int):
    """Mark an alert as acknowledged."""
    async with async_session() as session:
        result = await session.execute(
            text("UPDATE alerts SET acknowledged = TRUE WHERE id = :id RETURNING id"),
            {"id": alert_id},
        )
        updated = result.fetchone()
        await session.commit()

    if not updated:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")

    return {"status": "acknowledged", "alert_id": alert_id}


# ──────────────────────────── Stats ────────────────────────────


@router.get("/stats")
async def pipeline_stats():
    """Get pipeline statistics — useful for README numbers."""
    async with async_session() as session:
        result = await session.execute(
            text("""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE is_fraud) as fraud_count,
                AVG(fraud_score) as avg_score,
                MAX(fraud_score) as max_score,
                COUNT(*) FILTER (WHERE degraded) as degraded_count,
                COUNT(DISTINCT card_id) as unique_cards
            FROM transactions
        """)
        )
        row = result.fetchone()

    return {
        "total_transactions": row[0],
        "fraud_detected": row[1],
        "fraud_rate": f"{(row[1] / max(row[0], 1)) * 100:.2f}%",
        "avg_fraud_score": round(float(row[2] or 0), 4),
        "max_fraud_score": round(float(row[3] or 0), 4),
        "degraded_mode_count": row[4],
        "unique_cards": row[5],
    }


# ──────────────────────────── Webhooks ────────────────────────────


@router.post("/webhooks/alert")
async def receive_alertmanager_webhook(payload: dict):
    """Receive alerts from Alertmanager."""
    logger.info(
        "alertmanager_webhook_received",
        status=payload.get("status"),
        alerts_count=len(payload.get("alerts", [])),
    )
    return {"status": "received"}
