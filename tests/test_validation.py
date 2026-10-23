"""Unit tests for Pydantic transaction validation.

Ensures the TransactionMessage schema correctly validates
incoming Kafka messages and rejects malformed data.
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from consumer.main import TransactionMessage


class TestTransactionValidation:
    """Test Pydantic transaction message validation."""

    def test_valid_transaction(self):
        """Valid transaction should parse successfully."""
        txn = TransactionMessage(
            transaction_id="TXN-ABC123",
            card_id="CARD-001",
            amount=99.99,
            currency="USD",
            merchant="Amazon",
            merchant_category="retail",
            country="USA",
            timestamp=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        )
        assert txn.transaction_id == "TXN-ABC123"
        assert txn.amount == 99.99
        assert txn.is_fraud_injected is False

    def test_minimum_required_fields(self):
        """Transaction with only required fields should be valid."""
        txn = TransactionMessage(
            transaction_id="TXN-MIN",
            card_id="CARD-002",
            amount=1.0,
            merchant="Test",
            country="USA",
            timestamp=datetime.now(timezone.utc),
        )
        assert txn.currency == "USD"  # Default
        assert txn.merchant_category is None  # Optional

    def test_negative_amount_rejected(self):
        """Negative amounts should fail validation."""
        with pytest.raises(ValidationError) as exc_info:
            TransactionMessage(
                transaction_id="TXN-NEG",
                card_id="CARD-003",
                amount=-50.0,
                merchant="Test",
                country="USA",
                timestamp=datetime.now(timezone.utc),
            )
        assert "amount" in str(exc_info.value).lower()

    def test_zero_amount_rejected(self):
        """Zero amount should fail validation (gt=0)."""
        with pytest.raises(ValidationError):
            TransactionMessage(
                transaction_id="TXN-ZERO",
                card_id="CARD-004",
                amount=0.0,
                merchant="Test",
                country="USA",
                timestamp=datetime.now(timezone.utc),
            )

    def test_missing_required_field(self):
        """Missing required field should fail validation."""
        with pytest.raises(ValidationError):
            TransactionMessage(
                transaction_id="TXN-MISS",
                # card_id is missing
                amount=100.0,
                merchant="Test",
                country="USA",
                timestamp=datetime.now(timezone.utc),
            )

    def test_country_code_too_long(self):
        """Country code longer than 3 chars should fail."""
        with pytest.raises(ValidationError):
            TransactionMessage(
                transaction_id="TXN-LONG",
                card_id="CARD-005",
                amount=100.0,
                merchant="Test",
                country="TOOLONG",
                timestamp=datetime.now(timezone.utc),
            )

    def test_iso_timestamp_parsing(self):
        """ISO format timestamps should parse correctly."""
        txn = TransactionMessage(
            transaction_id="TXN-ISO",
            card_id="CARD-006",
            amount=100.0,
            merchant="Test",
            country="USA",
            timestamp="2024-01-15T10:30:00+00:00",
        )
        assert txn.timestamp.year == 2024
        assert txn.timestamp.month == 1

    def test_fraud_injected_flag(self):
        """is_fraud_injected should default to False."""
        txn = TransactionMessage(
            transaction_id="TXN-FLAG",
            card_id="CARD-007",
            amount=100.0,
            merchant="Test",
            country="USA",
            timestamp=datetime.now(timezone.utc),
        )
        assert txn.is_fraud_injected is False
        assert txn.fraud_pattern is None

    def test_with_fraud_pattern(self):
        """Fraud-injected transaction should carry pattern info."""
        txn = TransactionMessage(
            transaction_id="TXN-FRAUD",
            card_id="CARD-008",
            amount=5000.0,
            merchant="Luxury Store",
            country="NGA",
            timestamp=datetime.now(timezone.utc),
            is_fraud_injected=True,
            fraud_pattern="velocity_spike",
        )
        assert txn.is_fraud_injected is True
        assert txn.fraud_pattern == "velocity_spike"
