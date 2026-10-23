"""Fraud scoring engine — rules-based + IsolationForest anomaly detection.

Two-layer scoring approach:
1. Rule-based checks (fast, explainable)
2. IsolationForest model (catches novel patterns)

Final score = weighted combination with explainable reasons list.
"""

import pickle
from collections import deque
from datetime import datetime
from pathlib import Path

import numpy as np
from sklearn.ensemble import IsolationForest

from observability.logging import get_logger
from observability.metrics import (
    fraud_detected_total,
    fraud_score_histogram,
    scoring_latency_seconds,
)

logger = get_logger("consumer.scoring")

MODEL_PATH = Path(__file__).parent / "model" / "isolation_forest.pkl"
FRAUD_THRESHOLD = 0.65  # Score above this = flagged as fraud


# ──────────────────────────── Rule-Based Scoring ────────────────────────────


def score_velocity(amount: float, card_id: str, recent_counts: dict) -> tuple[float, str | None]:
    """Check for velocity anomalies (many transactions in short window)."""
    count = recent_counts.get(card_id, 0)
    if count > 5:
        return 0.8, "velocity_spike: >5 transactions in 2-minute window"
    elif count > 3:
        return 0.4, "elevated_velocity: >3 transactions in 2-minute window"
    return 0.0, None


def score_amount(amount: float) -> tuple[float, str | None]:
    """Flag unusually high amounts."""
    if amount > 5000:
        return 0.7, f"high_amount: ${amount:.2f} exceeds $5000 threshold"
    elif amount > 2000:
        return 0.3, f"elevated_amount: ${amount:.2f} exceeds $2000 threshold"
    return 0.0, None


def score_odd_hours(timestamp: datetime) -> tuple[float, str | None]:
    """Flag transactions at suspicious hours (2-5 AM local)."""
    hour = timestamp.hour
    if 2 <= hour <= 5:
        return 0.5, f"odd_hour_transaction: transaction at {hour}:00"
    return 0.0, None


def score_card_testing(amount: float) -> tuple[float, str | None]:
    """Detect card testing patterns (very small round amounts)."""
    if amount <= 2.0 and amount == round(amount, 0):
        return 0.6, f"card_testing_pattern: small round amount ${amount:.2f}"
    return 0.0, None


def score_high_risk_country(country: str) -> tuple[float, str | None]:
    """Flag transactions from high-risk countries."""
    high_risk = {"NGA", "RUS", "PRK", "IRN", "VEN"}
    if country in high_risk:
        return 0.5, f"high_risk_country: transaction from {country}"
    return 0.0, None


def apply_rules(
    amount: float,
    country: str,
    timestamp: datetime,
    card_id: str,
    recent_counts: dict,
) -> tuple[float, list[str]]:
    """Apply all rules, return max score and list of triggered reasons."""
    checks = [
        score_velocity(amount, card_id, recent_counts),
        score_amount(amount),
        score_odd_hours(timestamp),
        score_card_testing(amount),
        score_high_risk_country(country),
    ]

    max_score = 0.0
    reasons = []
    for score, reason in checks:
        if reason:
            reasons.append(reason)
            max_score = max(max_score, score)

    return max_score, reasons


# ──────────────────────────── Model-Based Scoring ────────────────────────────


class AnomalyModel:
    """Wrapper around IsolationForest for transaction anomaly scoring."""

    def __init__(self):
        self.model: IsolationForest | None = None
        self._trained = False

    def train_on_synthetic(self):
        """Train IsolationForest on synthetic normal transaction features."""
        logger.info("training_isolation_forest")

        rng = np.random.RandomState(42)
        n_samples = 10000

        # Feature vector: [amount, hour, is_high_risk_country, velocity_count]
        normal_amounts = rng.lognormal(mean=3.5, sigma=1.2, size=n_samples).clip(1, 5000)
        normal_hours = rng.normal(loc=14, scale=4, size=n_samples).clip(0, 23).astype(int)
        normal_risk = rng.binomial(1, 0.05, size=n_samples)  # 5% from high-risk
        normal_velocity = rng.poisson(lam=1.5, size=n_samples).clip(0, 10)

        X_train = np.column_stack([normal_amounts, normal_hours, normal_risk, normal_velocity])

        self.model = IsolationForest(
            n_estimators=100,
            contamination=0.02,
            random_state=42,
            n_jobs=-1,
        )
        self.model.fit(X_train)
        self._trained = True

        # Save model
        MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(MODEL_PATH, "wb") as f:
            pickle.dump(self.model, f)

        logger.info("isolation_forest_trained", samples=n_samples)

    def load_or_train(self):
        """Load saved model or train a new one."""
        if MODEL_PATH.exists():
            with open(MODEL_PATH, "rb") as f:
                self.model = pickle.load(f)
            self._trained = True
            logger.info("isolation_forest_loaded", path=str(MODEL_PATH))
        else:
            self.train_on_synthetic()

    def predict(
        self,
        amount: float,
        hour: int,
        is_high_risk: bool,
        velocity_count: int,
    ) -> float:
        """Score a transaction (0.0 = normal, 1.0 = anomalous)."""
        if not self._trained:
            self.load_or_train()

        features = np.array([[amount, hour, int(is_high_risk), velocity_count]])
        # decision_function returns negative for anomalies
        raw_score = -self.model.decision_function(features)[0]
        # Normalize to 0-1 range
        normalized = max(0.0, min(1.0, (raw_score + 0.5) / 1.0))
        return round(float(normalized), 4)


# Global model instance
anomaly_model = AnomalyModel()

# In-memory velocity tracker (card_id -> deque of timestamps)
# In production, this would use Redis with TTL
VELOCITY_WINDOW_SECONDS = 120  # 2-minute sliding window
_velocity_window: dict[str, deque[float]] = {}


def update_velocity(card_id: str) -> int:
    """Record a transaction timestamp and return count in the sliding window.

    Prunes entries older than VELOCITY_WINDOW_SECONDS so the fraud rate
    doesn't inflate over time.
    """
    import time as _time

    now = _time.monotonic()

    if card_id not in _velocity_window:
        _velocity_window[card_id] = deque()

    window = _velocity_window[card_id]

    # Prune entries older than the window
    while window and (now - window[0]) > VELOCITY_WINDOW_SECONDS:
        window.popleft()

    window.append(now)
    return len(window)


def get_velocity_count(card_id: str) -> int:
    """Get the current velocity count for a card without recording."""
    import time as _time

    now = _time.monotonic()
    window = _velocity_window.get(card_id)
    if not window:
        return 0
    # Prune stale entries on read too
    while window and (now - window[0]) > VELOCITY_WINDOW_SECONDS:
        window.popleft()
    return len(window)


def reset_velocity():
    """Reset velocity window (useful in tests)."""
    _velocity_window.clear()


# ──────────────────────────── Combined Scoring ────────────────────────────


def score_transaction(
    amount: float,
    country: str,
    timestamp: datetime,
    card_id: str,
    risk_api_score: float | None = None,
) -> dict:
    """Combined fraud scoring using rules + model + optional risk API score.

    Returns:
        {
            "fraud_score": float (0-1),
            "is_fraud": bool,
            "reasons": list[str],
            "detection_method": str,
        }
    """
    with scoring_latency_seconds.time():
        velocity_count = update_velocity(card_id)
        high_risk_countries = {"NGA", "RUS", "PRK", "IRN", "VEN"}

        # Layer 1: Rules (pass velocity_count as a dict for the card)
        rule_score, reasons = apply_rules(
            amount,
            country,
            timestamp,
            card_id,
            recent_counts={card_id: velocity_count},
        )

        # Layer 2: IsolationForest
        model_score = anomaly_model.predict(
            amount=amount,
            hour=timestamp.hour,
            is_high_risk=country in high_risk_countries,
            velocity_count=velocity_count,
        )

        # Combine scores (weighted average)
        weights = {"rules": 0.4, "model": 0.4, "risk_api": 0.2}

        if risk_api_score is not None:
            final_score = (
                weights["rules"] * rule_score + weights["model"] * model_score + weights["risk_api"] * risk_api_score
            )
            detection_method = "combined"
        else:
            # No risk API score (degraded mode)
            final_score = 0.5 * rule_score + 0.5 * model_score
            detection_method = "rules_and_model"

        final_score = round(float(min(1.0, final_score)), 4)

        if model_score > 0.7 and "anomaly_model: high anomaly score" not in reasons:
            reasons.append(f"anomaly_model: isolation forest score {model_score:.3f}")

        is_fraud = bool(final_score >= FRAUD_THRESHOLD)

        # Record metrics
        fraud_score_histogram.observe(final_score)
        if is_fraud:
            fraud_detected_total.labels(detection_method=detection_method).inc()

        return {
            "fraud_score": final_score,
            "is_fraud": is_fraud,
            "reasons": reasons,
            "detection_method": detection_method,
            "rule_score": rule_score,
            "model_score": model_score,
            "risk_api_score": risk_api_score,
        }
