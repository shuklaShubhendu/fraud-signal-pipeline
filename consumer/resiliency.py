"""Resiliency patterns for external service calls.

Implements the full resiliency stack from the Visa JD:
- Timeouts (300ms default)
- Retries with exponential backoff (tenacity)
- Circuit breaker (custom async state machine — pybreaker doesn't support asyncio)
- Graceful degradation (fallback to rules-only scoring)
"""

import asyncio
import time
from enum import IntEnum

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import settings
from observability.logging import get_logger
from observability.metrics import (
    circuit_breaker_state,
    degraded_mode_total,
    retry_attempts_total,
    risk_api_latency_seconds,
)

logger = get_logger("consumer.resiliency")


class BreakerState(IntEnum):
    """Circuit breaker states for Prometheus metric."""

    CLOSED = 0
    OPEN = 1
    HALF_OPEN = 2


# ──────────────────────────── Async Circuit Breaker ────────────────────────────


class AsyncCircuitBreaker:
    """Async-native circuit breaker state machine.

    pybreaker 1.2.0 has no asyncio support (only Tornado's call_async),
    so async functions silently pass through without tracking failures.
    This class implements the full state machine:

        CLOSED  ──(fail_max failures)──►  OPEN
        OPEN    ──(reset_timeout elapsed)──►  HALF_OPEN
        HALF_OPEN  ──(1 success)──►  CLOSED
        HALF_OPEN  ──(1 failure)──►  OPEN
    """

    def __init__(self, fail_max: int = 5, reset_timeout: float = 30.0, name: str = "breaker"):
        self.fail_max = fail_max
        self.reset_timeout = reset_timeout
        self.name = name
        self._state = BreakerState.CLOSED
        self._fail_count = 0
        self._open_until = 0.0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> BreakerState:
        """Current breaker state (may transition OPEN → HALF_OPEN on read)."""
        if self._state == BreakerState.OPEN and time.monotonic() >= self._open_until:
            return BreakerState.HALF_OPEN
        return self._state

    @property
    def is_open(self) -> bool:
        return self.state == BreakerState.OPEN

    def _transition(self, new_state: BreakerState):
        old = self._state
        self._state = new_state
        circuit_breaker_state.set(int(new_state))
        if old != new_state:
            logger.warning(
                "circuit_breaker_state_change",
                old_state=old.name,
                new_state=new_state.name,
                name=self.name,
            )

    async def call(self, coro_func, *args, **kwargs):
        """Execute an async function through the circuit breaker.

        Raises CircuitBreakerOpenError if the breaker is OPEN.
        """
        async with self._lock:
            current = self.state

            if current == BreakerState.OPEN:
                raise CircuitBreakerOpenError(
                    f"Circuit breaker '{self.name}' is OPEN (resets in "
                    f"{max(0, self._open_until - time.monotonic()):.1f}s)"
                )

            # CLOSED or HALF_OPEN — allow the call through

        # Run outside the lock so we don't hold it during I/O
        try:
            result = await coro_func(*args, **kwargs)
        except Exception:
            await self._record_failure()
            raise
        else:
            await self._record_success()
            return result

    async def _record_failure(self):
        async with self._lock:
            self._fail_count += 1
            logger.warning(
                "circuit_breaker_failure",
                name=self.name,
                fail_count=self._fail_count,
                fail_max=self.fail_max,
            )
            if self._fail_count >= self.fail_max:
                self._open_until = time.monotonic() + self.reset_timeout
                self._transition(BreakerState.OPEN)

    async def _record_success(self):
        async with self._lock:
            if self._state == BreakerState.HALF_OPEN:
                logger.info("circuit_breaker_recovered", name=self.name)
            self._fail_count = 0
            self._transition(BreakerState.CLOSED)

    def close(self):
        """Manually close the breaker (useful in tests)."""
        self._state = BreakerState.CLOSED
        self._fail_count = 0
        self._open_until = 0.0
        circuit_breaker_state.set(0)


class CircuitBreakerOpenError(Exception):
    """Raised when a call is attempted while the circuit breaker is OPEN."""

    pass


# Global circuit breaker instance
risk_api_breaker = AsyncCircuitBreaker(
    fail_max=5,
    reset_timeout=30.0,
    name="risk_api",
)


# ──────────────────────────── Risk API Client ────────────────────────────


class RiskAPIError(Exception):
    """Raised when risk API call fails."""

    pass


class RiskAPITimeout(RiskAPIError):
    """Raised when risk API call times out."""

    pass


class RiskAPIResult:
    """Result from risk API call."""

    def __init__(self, score: float | None, degraded: bool, error: str | None = None):
        self.score = score
        self.degraded = degraded
        self.error = error


@retry(
    stop=stop_after_attempt(settings.risk_api_max_retries),
    wait=wait_exponential(multiplier=0.1, min=0.05, max=1.0),
    retry=retry_if_exception_type(
        (httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException, asyncio.TimeoutError)
    ),
    before_sleep=lambda retry_state: retry_attempts_total.labels(service="risk_api").inc(),
    reraise=True,
)
async def _call_risk_api_with_retry(transaction_data: dict) -> float:
    """Call risk API with retries and timeout.

    Timeout: 300ms (configurable via RISK_API_TIMEOUT_MS)
    Retries: 3 attempts with exponential backoff (50ms → 100ms → 200ms)
    """
    timeout = settings.risk_api_timeout_ms / 1000.0

    async with httpx.AsyncClient(timeout=timeout) as client:
        with risk_api_latency_seconds.time():
            response = await client.post(
                f"{settings.risk_api_url}/score",
                json=transaction_data,
            )
            response.raise_for_status()
            data = response.json()
            return data["risk_score"]


async def call_risk_api(transaction_data: dict) -> RiskAPIResult:
    """Call risk API with full resiliency stack.

    Flow:
    1. Check circuit breaker state
    2. If closed/half-open: call API with retries + timeout
    3. If open or all retries fail: graceful degradation (rules-only)

    Returns RiskAPIResult with score (or None if degraded).
    """
    try:
        score = await risk_api_breaker.call(_call_risk_api_with_retry, transaction_data)
        return RiskAPIResult(score=score, degraded=False)

    except CircuitBreakerOpenError:
        # Circuit breaker is OPEN — graceful degradation
        degraded_mode_total.inc()
        logger.warning(
            "graceful_degradation",
            reason="circuit_breaker_open",
            transaction_id=transaction_data.get("transaction_id"),
        )
        return RiskAPIResult(score=None, degraded=True, error="circuit_breaker_open")

    except (httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException, asyncio.TimeoutError) as e:
        # All retries exhausted — graceful degradation
        degraded_mode_total.inc()
        logger.warning(
            "graceful_degradation",
            reason="retries_exhausted",
            error=str(e),
            transaction_id=transaction_data.get("transaction_id"),
        )
        return RiskAPIResult(score=None, degraded=True, error=f"retries_exhausted: {type(e).__name__}")

    except Exception as e:
        # Unexpected error — graceful degradation
        degraded_mode_total.inc()
        logger.error(
            "risk_api_unexpected_error",
            error=str(e),
            transaction_id=transaction_data.get("transaction_id"),
        )
        return RiskAPIResult(score=None, degraded=True, error=f"unexpected: {str(e)}")
