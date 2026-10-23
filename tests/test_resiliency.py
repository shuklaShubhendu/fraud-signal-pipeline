"""Resiliency tests — verify circuit breaker, retries, and degraded mode.

These tests validate that the consumer handles external service failures
correctly by falling back to rules-only scoring (graceful degradation).
"""

import asyncio
from unittest.mock import MagicMock, patch

import httpx
import pytest

from consumer.resiliency import (
    AsyncCircuitBreaker,
    BreakerState,
    CircuitBreakerOpenError,
    RiskAPIResult,
    call_risk_api,
    risk_api_breaker,
)


@pytest.fixture(autouse=True)
def reset_circuit_breaker():
    """Reset circuit breaker state before each test."""
    risk_api_breaker.close()
    yield


class TestGracefulDegradation:
    """Test that failures result in degraded mode, not crashes."""

    @pytest.mark.asyncio
    async def test_connection_error_returns_degraded(self):
        """When risk API is unreachable, should return degraded result."""
        with patch("consumer.resiliency._call_risk_api_with_retry") as mock:
            mock.side_effect = httpx.ConnectError("Connection refused")

            result = await call_risk_api({"transaction_id": "TXN-001", "amount": 100})

            assert isinstance(result, RiskAPIResult)
            assert result.degraded is True
            assert result.score is None
            assert result.error is not None

    @pytest.mark.asyncio
    async def test_timeout_returns_degraded(self):
        """When risk API times out (httpx.TimeoutException), should degrade."""
        with patch("consumer.resiliency._call_risk_api_with_retry") as mock:
            mock.side_effect = httpx.TimeoutException("Read timed out")

            result = await call_risk_api({"transaction_id": "TXN-002", "amount": 200})

            assert result.degraded is True
            assert result.score is None

    @pytest.mark.asyncio
    async def test_asyncio_timeout_returns_degraded(self):
        """asyncio.TimeoutError should also degrade."""
        with patch("consumer.resiliency._call_risk_api_with_retry") as mock:
            mock.side_effect = asyncio.TimeoutError()

            result = await call_risk_api({"transaction_id": "TXN-002b", "amount": 200})

            assert result.degraded is True
            assert result.score is None

    @pytest.mark.asyncio
    async def test_http_500_returns_degraded(self):
        """When risk API returns 500, should return degraded result."""
        with patch("consumer.resiliency._call_risk_api_with_retry") as mock:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.request = MagicMock()
            mock.side_effect = httpx.HTTPStatusError(
                "Server Error",
                request=mock_response.request,
                response=mock_response,
            )

            result = await call_risk_api({"transaction_id": "TXN-003", "amount": 300})

            assert result.degraded is True
            assert result.score is None


class TestCircuitBreaker:
    """Test async circuit breaker state machine."""

    @pytest.mark.asyncio
    async def test_breaker_opens_after_failures(self):
        """Circuit breaker should open after 5 consecutive failures
        and the 6th call should NOT reach the mock (breaker blocks it)."""
        breaker = AsyncCircuitBreaker(fail_max=5, reset_timeout=30.0, name="test")

        call_count = 0

        async def failing_func():
            nonlocal call_count
            call_count += 1
            raise httpx.ConnectError("Connection refused")

        # First 5 calls go through and fail
        for i in range(5):
            with pytest.raises(httpx.ConnectError):
                await breaker.call(failing_func)

        assert breaker.state == BreakerState.OPEN
        assert call_count == 5  # All 5 calls reached the function

        # 6th call should be blocked by breaker (mock not called)
        with pytest.raises(CircuitBreakerOpenError):
            await breaker.call(failing_func)

        assert call_count == 5  # Still 5 — breaker blocked the 6th

    @pytest.mark.asyncio
    async def test_breaker_recovers_on_success(self):
        """After reset_timeout, breaker enters half-open and recovers on success."""
        breaker = AsyncCircuitBreaker(fail_max=2, reset_timeout=0.1, name="test-recover")

        async def failing():
            raise httpx.ConnectError("down")

        async def succeeding():
            return 0.5

        # Open the breaker
        for _ in range(2):
            with pytest.raises(httpx.ConnectError):
                await breaker.call(failing)

        assert breaker.state == BreakerState.OPEN

        # Wait for reset_timeout
        await asyncio.sleep(0.15)
        assert breaker.state == BreakerState.HALF_OPEN

        # Successful call should close it
        result = await breaker.call(succeeding)
        assert result == 0.5
        assert breaker.state == BreakerState.CLOSED

    @pytest.mark.asyncio
    async def test_successful_call_returns_score(self):
        """Successful API call should return actual score."""
        with patch("consumer.resiliency._call_risk_api_with_retry") as mock:
            mock.return_value = 0.75

            result = await call_risk_api({"transaction_id": "TXN-SUCCESS"})

            assert result.degraded is False
            assert result.score == 0.75

    @pytest.mark.asyncio
    async def test_degraded_result_has_error_info(self):
        """Degraded results should include error information."""
        with patch("consumer.resiliency._call_risk_api_with_retry") as mock:
            mock.side_effect = httpx.ConnectError("Connection refused")

            result = await call_risk_api({"transaction_id": "TXN-ERR"})

            assert result.error is not None
            assert len(result.error) > 0

    @pytest.mark.asyncio
    async def test_breaker_state_values(self):
        """Verify state enum values match Prometheus metric expectations."""
        assert BreakerState.CLOSED == 0
        assert BreakerState.OPEN == 1
        assert BreakerState.HALF_OPEN == 2
