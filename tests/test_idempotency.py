"""Integration test for PostgreSQL idempotency.

Uses testcontainers to spin up a real PostgreSQL instance
and verifies that duplicate transaction_id inserts don't
create duplicate rows (upsert behavior).
"""

from datetime import datetime, timezone

import pytest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def _get_sync_connection_string(host, port):
    """Build sync connection string for testcontainer Postgres."""
    return f"postgresql://pipeline:pipeline_secret@{host}:{port}/fraud_pipeline"


def _get_async_connection_string(host, port):
    """Build async connection string for testcontainer Postgres."""
    return f"postgresql+asyncpg://pipeline:pipeline_secret@{host}:{port}/fraud_pipeline"


@pytest.fixture(scope="module")
def postgres_container():
    """Start a PostgreSQL testcontainer for integration tests."""
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
        # Apply schema
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


@pytest.fixture
def db_config(postgres_container):
    """Get database connection config from testcontainer."""
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    return {
        "host": host,
        "port": port,
        "async_url": _get_async_connection_string(host, port),
        "sync_url": _get_sync_connection_string(host, port),
    }


class TestIdempotency:
    """Test that duplicate messages don't create duplicate rows."""

    def test_upsert_creates_new_row(self, db_config):
        """First insert should create a new row."""
        import psycopg2

        conn = psycopg2.connect(db_config["sync_url"])
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO transactions (
                transaction_id, card_id, amount, currency, merchant,
                country, timestamp, is_fraud, fraud_score
            ) VALUES (
                'TXN-IDEM-001', 'CARD-001', 99.99, 'USD', 'Amazon',
                'USA', %s, FALSE, 0.1
            )
            ON CONFLICT (transaction_id) DO UPDATE SET
                fraud_score = EXCLUDED.fraud_score
            RETURNING (xmax = 0) AS is_insert
        """,
            (datetime.now(timezone.utc),),
        )

        row = cursor.fetchone()
        assert row[0] is True  # is_insert = True (new row)
        conn.commit()

        # Verify row exists
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE transaction_id = 'TXN-IDEM-001'")
        assert cursor.fetchone()[0] == 1

        cursor.close()
        conn.close()

    def test_duplicate_upsert_does_not_duplicate(self, db_config):
        """Sending the same transaction_id twice should not create two rows."""
        import psycopg2

        conn = psycopg2.connect(db_config["sync_url"])
        cursor = conn.cursor()

        now = datetime.now(timezone.utc)

        # First insert
        cursor.execute(
            """
            INSERT INTO transactions (
                transaction_id, card_id, amount, currency, merchant,
                country, timestamp, is_fraud, fraud_score
            ) VALUES (
                'TXN-IDEM-002', 'CARD-002', 50.00, 'USD', 'Walmart',
                'USA', %s, FALSE, 0.2
            )
            ON CONFLICT (transaction_id) DO UPDATE SET
                fraud_score = EXCLUDED.fraud_score
        """,
            (now,),
        )
        conn.commit()

        # Second insert (duplicate) with different fraud_score
        cursor.execute(
            """
            INSERT INTO transactions (
                transaction_id, card_id, amount, currency, merchant,
                country, timestamp, is_fraud, fraud_score
            ) VALUES (
                'TXN-IDEM-002', 'CARD-002', 50.00, 'USD', 'Walmart',
                'USA', %s, FALSE, 0.9
            )
            ON CONFLICT (transaction_id) DO UPDATE SET
                fraud_score = EXCLUDED.fraud_score
            RETURNING (xmax = 0) AS is_insert
        """,
            (now,),
        )

        row = cursor.fetchone()
        assert row[0] is False  # is_insert = False (updated, not inserted)
        conn.commit()

        # Verify still only one row
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE transaction_id = 'TXN-IDEM-002'")
        assert cursor.fetchone()[0] == 1

        # Verify score was updated
        cursor.execute("SELECT fraud_score FROM transactions WHERE transaction_id = 'TXN-IDEM-002'")
        score = float(cursor.fetchone()[0])
        assert score == 0.9  # Updated to new score

        cursor.close()
        conn.close()

    def test_multiple_duplicates_still_one_row(self, db_config):
        """Sending the same transaction 5 times should still result in one row."""
        import psycopg2

        conn = psycopg2.connect(db_config["sync_url"])
        cursor = conn.cursor()

        now = datetime.now(timezone.utc)

        for i in range(5):
            cursor.execute(
                """
                INSERT INTO transactions (
                    transaction_id, card_id, amount, currency, merchant,
                    country, timestamp, is_fraud, fraud_score
                ) VALUES (
                    'TXN-IDEM-003', 'CARD-003', 100.00, 'USD', 'Target',
                    'USA', %s, FALSE, %s
                )
                ON CONFLICT (transaction_id) DO UPDATE SET
                    fraud_score = EXCLUDED.fraud_score
            """,
                (now, float(i) / 10),
            )
            conn.commit()

        cursor.execute("SELECT COUNT(*) FROM transactions WHERE transaction_id = 'TXN-IDEM-003'")
        assert cursor.fetchone()[0] == 1

        cursor.close()
        conn.close()
