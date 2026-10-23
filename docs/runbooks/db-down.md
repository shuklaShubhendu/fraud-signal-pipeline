# Runbook: Database Down / Connection Errors

## Symptoms
- Alertmanager fires `DatabaseErrors` alert
- `database_errors_total` metric is incrementing
- API returns 500 on `/transactions` or `/alerts` endpoints
- Consumer logs show `database_upsert_failed` errors
- Health endpoint shows `database: unreachable`

## Severity
**Critical** — transactions are being lost to dead-letter topic

## Diagnosis Steps

### 1. Check PostgreSQL container
```bash
docker compose ps postgres
docker compose logs postgres --tail 30
```

### 2. Test database connectivity
```bash
docker exec postgres pg_isready -U pipeline -d fraud_pipeline
```

### 3. Check connection pool
```bash
docker exec postgres psql -U pipeline -d fraud_pipeline -c \
  "SELECT count(*) as total, state FROM pg_stat_activity GROUP BY state;"
```

### 4. Check disk space
```bash
docker exec postgres df -h /var/lib/postgresql/data
```

### 5. Check for locks
```bash
docker exec postgres psql -U pipeline -d fraud_pipeline -c \
  "SELECT pid, query, state, wait_event_type FROM pg_stat_activity WHERE state != 'idle' ORDER BY query_start;"
```

## Root Causes & Fixes

### Cause 1: PostgreSQL container crashed
- **Fix**: Restart the container
- **Command**: `docker compose restart postgres`
- **Verify**: `docker exec postgres pg_isready -U pipeline -d fraud_pipeline`

### Cause 2: Connection pool exhausted
- **Check**: All connections in `active` state
- **Fix**: Kill idle connections and restart consumer
- **Command**:
  ```bash
  docker exec postgres psql -U pipeline -d fraud_pipeline -c \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle' AND query_start < NOW() - interval '5 minutes';"
  docker compose restart consumer
  ```

### Cause 3: Disk full
- **Fix**: Clean up old data or expand volume
- **Command**:
  ```bash
  docker exec postgres psql -U pipeline -d fraud_pipeline -c \
    "DELETE FROM transactions WHERE processed_at < NOW() - interval '7 days';"
  docker exec postgres psql -U pipeline -d fraud_pipeline -c "VACUUM FULL;"
  ```

### Cause 4: Schema migration issue
- **Fix**: Re-apply init.sql
- **Command**: `docker exec -i postgres psql -U pipeline -d fraud_pipeline < init.sql`

## Recovery
After database is back:
1. Check dead-letter topic for messages that need reprocessing
2. Verify `transactions_processed_total` is incrementing again
3. Confirm consumer lag is decreasing

## Rollback
```bash
docker compose down postgres
docker volume rm new_project_postgres_data
docker compose up -d postgres
# Wait for init, then restart consumer
sleep 5
docker compose restart consumer
```

## Escalation
If database cannot be recovered within 15 minutes:
- Capture: `docker compose logs postgres`
- Note: last successful processing timestamp
- Contact: database admin / on-call
