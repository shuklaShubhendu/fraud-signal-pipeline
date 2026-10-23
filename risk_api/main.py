"""Mock external risk API — simulates unreliable third-party service.

Intentionally returns random 500 errors and slow responses to test
the consumer's resiliency patterns (retries, timeouts, circuit breaker).

Failure rates:
- ~15% chance of HTTP 500
- ~10% chance of slow response (400-800ms, exceeding 300ms timeout)
- ~75% normal fast responses
"""

import asyncio
import random

import uvicorn
from fastapi import FastAPI, HTTPException
from prometheus_client import make_asgi_app

from observability.logging import get_logger, setup_logging

setup_logging("INFO")
logger = get_logger("risk_api")

app = FastAPI(title="Mock Risk API", version="1.0.0")

# Prometheus metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.post("/score")
async def score_transaction(transaction: dict):
    """Score a transaction for risk.

    Simulates an unreliable external service:
    - 15% chance of 500 error
    - 10% chance of slow response (400-800ms)
    - 75% fast response with random risk score
    """
    roll = random.random()

    # ~15% failure rate
    if roll < 0.15:
        logger.warning(
            "simulated_500_error",
            transaction_id=transaction.get("transaction_id"),
        )
        raise HTTPException(status_code=500, detail="Internal Server Error (simulated)")

    # ~10% slow response (will trigger timeout in consumer)
    if roll < 0.25:
        delay = random.uniform(0.4, 0.8)
        logger.info(
            "simulated_slow_response",
            delay_ms=round(delay * 1000),
            transaction_id=transaction.get("transaction_id"),
        )
        await asyncio.sleep(delay)

    # Generate risk score based on transaction attributes
    base_score = random.uniform(0.0, 0.3)

    amount = transaction.get("amount", 0)
    country = transaction.get("country", "USA")

    # Higher risk for large amounts
    if amount > 2000:
        base_score += random.uniform(0.2, 0.4)
    elif amount > 500:
        base_score += random.uniform(0.05, 0.15)

    # Higher risk for certain countries
    high_risk = {"NGA", "RUS", "PRK", "IRN", "VEN"}
    if country in high_risk:
        base_score += random.uniform(0.2, 0.4)

    risk_score = min(1.0, round(base_score, 4))

    return {
        "risk_score": risk_score,
        "risk_level": "high" if risk_score > 0.7 else "medium" if risk_score > 0.4 else "low",
        "transaction_id": transaction.get("transaction_id"),
    }


@app.get("/health")
async def health():
    """Health check."""
    return {"status": "healthy", "service": "mock-risk-api"}


def main():
    """Run the mock risk API server."""
    uvicorn.run(
        "risk_api.main:app",
        host="0.0.0.0",
        port=8001,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
