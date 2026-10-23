"""Unit tests for fraud scoring engine.

Tests both rule-based scoring and IsolationForest model predictions
to ensure fraud detection works correctly.
"""

from datetime import datetime, timezone

import pytest

from consumer.scoring import (
    AnomalyModel,
    apply_rules,
    score_amount,
    score_card_testing,
    score_high_risk_country,
    score_odd_hours,
    score_transaction,
    score_velocity,
)


class TestRuleBasedScoring:
    """Test individual rule-based scoring functions."""

    def test_velocity_spike_detected(self):
        """High velocity count should trigger velocity rule."""
        score, reason = score_velocity(100.0, "CARD-001", {"CARD-001": 6})
        assert score == 0.8
        assert "velocity_spike" in reason

    def test_velocity_elevated(self):
        """Moderate velocity should trigger elevated warning."""
        score, reason = score_velocity(100.0, "CARD-001", {"CARD-001": 4})
        assert score == 0.4
        assert "elevated_velocity" in reason

    def test_velocity_normal(self):
        """Normal velocity should not trigger."""
        score, reason = score_velocity(100.0, "CARD-001", {"CARD-001": 1})
        assert score == 0.0
        assert reason is None

    def test_high_amount_flagged(self):
        """Amounts over $5000 should be flagged."""
        score, reason = score_amount(7500.0)
        assert score == 0.7
        assert "high_amount" in reason

    def test_elevated_amount_flagged(self):
        """Amounts over $2000 should trigger elevated."""
        score, reason = score_amount(3000.0)
        assert score == 0.3
        assert "elevated_amount" in reason

    def test_normal_amount_passes(self):
        """Normal amounts should not trigger."""
        score, reason = score_amount(50.0)
        assert score == 0.0
        assert reason is None

    def test_odd_hour_detected(self):
        """Transactions at 3 AM should be flagged."""
        ts = datetime(2024, 1, 1, 3, 30, tzinfo=timezone.utc)
        score, reason = score_odd_hours(ts)
        assert score == 0.5
        assert "odd_hour" in reason

    def test_normal_hour_passes(self):
        """Transactions at 2 PM should not trigger."""
        ts = datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc)
        score, reason = score_odd_hours(ts)
        assert score == 0.0
        assert reason is None

    def test_card_testing_detected(self):
        """Small round amounts ($1.00) should trigger card testing."""
        score, reason = score_card_testing(1.00)
        assert score == 0.6
        assert "card_testing" in reason

    def test_card_testing_not_triggered_for_normal(self):
        """Normal amounts should not trigger card testing."""
        score, reason = score_card_testing(42.50)
        assert score == 0.0
        assert reason is None

    def test_high_risk_country_flagged(self):
        """Transactions from NGA should be flagged."""
        score, reason = score_high_risk_country("NGA")
        assert score == 0.5
        assert "high_risk_country" in reason

    def test_normal_country_passes(self):
        """Transactions from USA should not trigger."""
        score, reason = score_high_risk_country("USA")
        assert score == 0.0
        assert reason is None


class TestApplyRules:
    """Test combined rule application."""

    def test_multiple_rules_triggered(self):
        """Multiple rules should all contribute reasons."""
        ts = datetime(2024, 1, 1, 3, 0, tzinfo=timezone.utc)
        score, reasons = apply_rules(
            amount=6000.0,
            country="NGA",
            timestamp=ts,
            card_id="CARD-001",
            recent_counts={"CARD-001": 7},
        )
        assert score >= 0.7  # At least one high-scoring rule
        assert len(reasons) >= 3  # Multiple rules should fire

    def test_clean_transaction(self):
        """Normal transaction should have zero score."""
        ts = datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc)
        score, reasons = apply_rules(
            amount=50.0,
            country="USA",
            timestamp=ts,
            card_id="CARD-001",
            recent_counts={"CARD-001": 1},
        )
        assert score == 0.0
        assert len(reasons) == 0


class TestAnomalyModel:
    """Test IsolationForest anomaly detection."""

    @pytest.fixture(autouse=True)
    def setup_model(self):
        """Train model before each test."""
        self.model = AnomalyModel()
        self.model.train_on_synthetic()

    def test_normal_transaction_low_score(self):
        """Normal transaction should get a low anomaly score."""
        score = self.model.predict(
            amount=50.0,
            hour=14,
            is_high_risk=False,
            velocity_count=1,
        )
        assert 0.0 <= score <= 1.0
        # Normal transactions should generally score low
        assert score < 0.8

    def test_anomalous_transaction_higher_score(self):
        """Anomalous transaction should score higher than normal."""
        normal_score = self.model.predict(50.0, 14, False, 1)
        anomaly_score = self.model.predict(9999.0, 3, True, 8)
        # Anomalous should score higher than normal
        assert anomaly_score > normal_score

    def test_score_in_valid_range(self):
        """All scores should be between 0 and 1."""
        for amount in [1, 50, 500, 5000, 9999]:
            for hour in [3, 12, 22]:
                score = self.model.predict(amount, hour, False, 1)
                assert 0.0 <= score <= 1.0


class TestScoreTransaction:
    """Test the combined scoring function."""

    def test_returns_required_fields(self):
        """score_transaction should return all required fields."""
        result = score_transaction(
            amount=100.0,
            country="USA",
            timestamp=datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc),
            card_id="CARD-TEST-001",
        )
        assert "fraud_score" in result
        assert "is_fraud" in result
        assert "reasons" in result
        assert "detection_method" in result
        assert isinstance(result["fraud_score"], float)
        assert isinstance(result["is_fraud"], bool)
        assert isinstance(result["reasons"], list)

    def test_with_risk_api_score(self):
        """When risk API score is provided, detection_method should be 'combined'."""
        result = score_transaction(
            amount=100.0,
            country="USA",
            timestamp=datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc),
            card_id="CARD-TEST-002",
            risk_api_score=0.5,
        )
        assert result["detection_method"] == "combined"
        assert result["risk_api_score"] == 0.5

    def test_without_risk_api_score(self):
        """Without risk API score, detection_method should be 'rules_and_model'."""
        result = score_transaction(
            amount=100.0,
            country="USA",
            timestamp=datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc),
            card_id="CARD-TEST-003",
        )
        assert result["detection_method"] == "rules_and_model"
        assert result["risk_api_score"] is None
