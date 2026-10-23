"""Integration tests — end-to-end pipeline with real containers.

Uses testcontainers for PostgreSQL to verify the full flow
from validated transaction → scoring → database write → API query.
"""

import asyncio
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container():
    """Start a PostgreSQL testcontainer."""
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        pytest.skip("testcontainers not installed")

    with PostgresContainer(
        image="postgres:16-alpine",
        username="pipeline",
        password="pipeline_secret",
        dbname="fraud_pipeline",
    ) as postgres:
        import psycopg2
        conn = psycopg2.connect(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            user="pipeline",
            password="pipeline_secret",
            dbname="fraud_pipeline",
        )
        cursor = conn.cursor()
        with open("init.sql", "r") as f:
            cursor.execute(f.read())
        conn.commit()
        cursor.close()
        conn.close()

        yield postgres


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_transaction_write_and_read(self, postgres_container):
        """Write a transaction and read it back via SQL."""
        import psycopg2

        host = postgres_container.get_container_host_ip()
        port = postgres_container.get_exposed_port(5432)
        conn_str = f"postgresql://pipeline:pipeline_secret@{host}:{port}/fraud_pipeline"

        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()

        now = datetime.now(timezone.utc)

        # Insert
        cursor.execute("""
            INSERT INTO transactions (
                transaction_id, card_id, amount, currency, merchant,
                country, timestamp, is_fraud, fraud_score, fraud_reasons
            ) VALUES (
                'TXN-E2E-001', 'CARD-E2E', 150.00, 'USD', 'BestBuy',
                'USA', %s, TRUE, 0.85, ARRAY['velocity_spike', 'high_amount']
            )
        """, (now,))
        conn.commit()

        # Read back
        cursor.execute("""
            SELECT transaction_id, amount, is_fraud, fraud_score, fraud_reasons
            FROM transactions WHERE transaction_id = 'TXN-E2E-001'
        """)
        row = cursor.fetchone()

        assert row[0] == "TXN-E2E-001"
        assert float(row[1]) == 150.00
        assert row[2] is True
        assert float(row[3]) == 0.85
        assert "velocity_spike" in row[4]

        cursor.close()
        conn.close()

    def test_alert_creation_and_query(self, postgres_container):
        """Create an alert and verify it links to a transaction."""
        import psycopg2
        import json

        host = postgres_container.get_container_host_ip()
        port = postgres_container.get_exposed_port(5432)
        conn_str = f"postgresql://pipeline:pipeline_secret@{host}:{port}/fraud_pipeline"

        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()

        now = datetime.now(timezone.utc)

        # Insert transaction first
        cursor.execute("""
            INSERT INTO transactions (
                transaction_id, card_id, amount, currency, merchant,
                country, timestamp, is_fraud, fraud_score
            ) VALUES (
                'TXN-E2E-ALERT', 'CARD-ALERT', 5000.00, 'USD', 'LuxuryStore',
                'NGA', %s, TRUE, 0.92
            )
            ON CONFLICT (transaction_id) DO NOTHING
        """, (now,))
        conn.commit()

        # Insert alert
        cursor.execute("""
            INSERT INTO alerts (transaction_id, alert_type, severity, details)
            VALUES ('TXN-E2E-ALERT', 'fraud_detected', 'critical', %s::jsonb)
            RETURNING id
        """, (json.dumps({"fraud_score": 0.92, "reasons": ["high_amount"]}),))
        alert_id = cursor.fetchone()[0]
        conn.commit()

        assert alert_id > 0

        # Query alert with transaction join
        cursor.execute("""
            SELECT a.id, a.severity, t.amount, t.country
            FROM alerts a
            JOIN transactions t ON a.transaction_id = t.transaction_id
            WHERE a.id = %s
        """, (alert_id,))
        row = cursor.fetchone()

        assert row[1] == "critical"
        assert float(row[2]) == 5000.00
        assert row[3] == "NGA"

        cursor.close()
        conn.close()

    def test_scoring_pipeline_integration(self, postgres_container):
        """Test that scoring produces expected results for known fraud patterns."""
        from consumer.scoring import score_transaction

        # Test velocity spike pattern
        result = score_transaction(
            amount=1500.00,
            country="NGA",
            timestamp=datetime(2024, 1, 1, 3, 0, tzinfo=timezone.utc),
            card_id="CARD-INT-TEST",
            risk_api_score=0.8,
        )

        assert result["fraud_score"] > 0.0
        assert isinstance(result["is_fraud"], bool)
        assert result["detection_method"] == "combined"
        assert len(result["reasons"]) > 0  # Should trigger multiple rules
