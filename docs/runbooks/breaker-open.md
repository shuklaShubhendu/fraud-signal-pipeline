# Runbook: Circuit Breaker Open (Degraded Mode)

## Symptoms
- Alertmanager fires `CircuitBreakerOpen` alert
- `circuit_breaker_state` metric = 1 (open)
- `degraded_mode_total` counter is incrementing
- Consumer logs show `graceful_degradation` events
- Transaction responses include `degraded: true`
- Scoring uses rules-only (no risk API score)

## Severity
**Critical** — fraud detection accuracy is reduced without risk API scores

## What's Happening
The circuit breaker has opened because the external Risk API has failed
too many consecutive times (5 failures). The consumer has gracefully
degraded to rules-only scoring (without risk API input). This is
**expected behavior** — the pipeline continues processing transactions
with reduced accuracy rather than failing entirely.

## Diagnosis Steps

### 1. Confirm circuit breaker state
```bash
curl -s http://localhost:8002/metrics | grep circuit_breaker_state
```

### 2. Check Risk API health
```bash
curl http://localhost:8001/health
docker compose logs risk-api --tail 20
```

### 3. Check how many transactions are in degraded mode
```bash
curl -s http://localhost:8002/metrics | grep degraded_mode_total
```

### 4. Check retry attempts
```bash
curl -s http://localhost:8002/metrics | grep retry_attempts_total
```

### 5. Check Risk API error rate
```bash
curl -s http://localhost:8002/metrics | grep risk_api_latency
```

## Root Causes & Fixes

### Cause 1: Risk API is down
- **Fix**: Restart the Risk API service
- **Command**: `docker compose restart risk-api`
- **Wait**: Circuit breaker will auto-recover after 30s (half-open → test → closed)

### Cause 2: Risk API is overloaded / slow
- **Check**: `risk_api_latency_seconds` p95 > 300ms
- **Fix**: Scale Risk API or increase timeout
- **Command**: Adjust `RISK_API_TIMEOUT_MS` in .env (e.g., to 500)

### Cause 3: Network issue between consumer and Risk API
- **Check**: `docker network inspect new_project_default`
- **Fix**: Restart Docker networking
- **Command**: `docker compose down && docker compose up -d`

## Circuit Breaker Recovery

The circuit breaker will **automatically recover**:
1. After `reset_timeout` (30 seconds), it enters **half-open** state
2. In half-open state, it allows one test request through
3. If the test request succeeds → **closed** (normal operation resumes)
4. If the test request fails → **open** (back to degraded mode for another 30s)

**No manual intervention needed** for circuit breaker recovery — just fix the underlying cause.

## Monitoring Recovery
Watch these metrics to confirm recovery:
```bash
# Should change from 1 (open) to 2 (half-open) to 0 (closed)
watch -n 5 'curl -s http://localhost:8002/metrics | grep circuit_breaker_state'

# Should stop incrementing
watch -n 5 'curl -s http://localhost:8002/metrics | grep degraded_mode_total'
```

## Impact Assessment
While in degraded mode:
- Transactions **continue processing** (no data loss)
- Fraud scoring uses **rules + IsolationForest only** (no risk API input)
- Detection accuracy may be slightly reduced
- All degraded transactions are marked with `degraded=true`

## Escalation
If Risk API cannot be restored within 15 minutes:
- Review degraded transaction fraud scores for accuracy
- Consider adjusting fraud threshold temporarily
- Notify fraud analysis team of reduced detection capability
