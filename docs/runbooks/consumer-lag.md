# Runbook: High Consumer Lag

## Symptoms
- Alertmanager fires `HighConsumerLag` alert
- Consumer lag metric `kafka_consumer_lag` exceeds 1000 messages
- Transactions appear delayed in the API
- Grafana dashboard shows growing lag trend

## Severity
**Warning** → **Critical** if lag exceeds 5000 or grows for >10 minutes

## Diagnosis Steps

### 1. Check consumer health
```bash
docker compose logs consumer --tail 50
curl http://localhost:8000/health
```

### 2. Check consumer lag metric
```bash
curl -s http://localhost:8002/metrics | grep kafka_consumer_lag
```

### 3. Check for error spikes
```bash
curl -s http://localhost:8002/metrics | grep transactions_processing_errors
```

### 4. Check resource utilization
```bash
docker stats consumer --no-stream
```

### 5. Check if producer rate exceeds consumer throughput
```bash
# Compare produced vs processed rates
curl -s http://localhost:8002/metrics | grep -E 'transactions_(produced|processed)_total'
```

## Root Causes & Fixes

### Cause 1: Consumer processing too slow
- **Check**: High `processing_latency_seconds` p95
- **Fix**: Scale consumer replicas or optimize scoring logic
- **Command**: `docker compose up -d --scale consumer=3`

### Cause 2: Database writes are slow
- **Check**: High `db_write_latency_seconds`
- **Fix**: Check PostgreSQL connections and indexes
- **Command**: `docker exec postgres psql -U pipeline -d fraud_pipeline -c "SELECT * FROM pg_stat_activity;"`

### Cause 3: Risk API causing delays
- **Check**: High `risk_api_latency_seconds` or circuit breaker open
- **Fix**: Circuit breaker should auto-degrade; check if degraded mode is working
- **Command**: `curl -s http://localhost:8002/metrics | grep degraded_mode`

### Cause 4: Kafka/Redpanda partition issue
- **Check**: Uneven partition assignment
- **Fix**: Rebalance consumers
- **Command**: `docker exec redpanda rpk group describe fraud-consumer-group`

## Rollback
If lag was caused by a recent deployment:
```bash
docker compose down consumer
git checkout HEAD~1 -- consumer/
docker compose up -d --build consumer
```

## Escalation
If lag persists >30 minutes after fixes, escalate to on-call with:
- Current lag value
- Consumer error logs
- Processing latency P95
- Risk API circuit breaker state
