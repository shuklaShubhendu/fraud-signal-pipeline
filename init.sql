CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL,
    transaction_id VARCHAR(64) PRIMARY KEY,
    card_id VARCHAR(32) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    merchant VARCHAR(128) NOT NULL,
    merchant_category VARCHAR(64),
    country VARCHAR(3) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    is_fraud BOOLEAN DEFAULT FALSE,
    fraud_score NUMERIC(5, 4) DEFAULT 0.0,
    fraud_reasons TEXT[] DEFAULT '{}',
    risk_api_score NUMERIC(5, 4),
    degraded BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_card_id ON transactions(card_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud ON transactions(is_fraud) WHERE is_fraud = TRUE;
CREATE INDEX IF NOT EXISTS idx_transactions_processed_at ON transactions(processed_at);

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) REFERENCES transactions(transaction_id),
    alert_type VARCHAR(32) NOT NULL,
    severity VARCHAR(16) NOT NULL DEFAULT 'medium',
    details JSONB NOT NULL DEFAULT '{}',
    acknowledged BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
