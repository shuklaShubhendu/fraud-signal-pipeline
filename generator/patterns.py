"""Fraud pattern injection for synthetic transaction generation.

Implements realistic fraud patterns:
- Velocity spikes (many transactions in short window)
- Impossible travel (transactions from distant countries in minutes)
- Odd-hour high amounts (large purchases at 2-5 AM)
- Round-amount probing (small round amounts to test stolen cards)
"""

import random
from datetime import datetime, timedelta, timezone

# Legitimate merchants and countries
MERCHANTS = [
    "Amazon", "Walmart", "Target", "BestBuy", "Starbucks",
    "McDonalds", "Shell Gas", "Uber", "Netflix", "Spotify",
    "Apple Store", "Google Play", "Steam", "Costco", "HomeDepot",
    "Zara", "Nike", "Adidas", "Sephora", "Whole Foods",
]

MERCHANT_CATEGORIES = [
    "retail", "grocery", "food", "gas", "entertainment",
    "subscription", "travel", "electronics", "clothing", "health",
]

COUNTRIES = ["USA", "CAN", "GBR", "DEU", "FRA", "JPN", "AUS", "IND", "BRA", "MEX"]
FRAUD_COUNTRIES = ["NGA", "RUS", "PRK", "IRN", "VEN"]  # High-risk origins

# Distance groups for impossible-travel detection
COUNTRY_GROUPS = {
    "americas": ["USA", "CAN", "MEX", "BRA"],
    "europe": ["GBR", "DEU", "FRA"],
    "asia": ["JPN", "IND"],
    "oceania": ["AUS"],
}


def generate_legitimate_transaction(card_id: str, timestamp: datetime) -> dict:
    """Generate a normal-looking transaction."""
    return {
        "card_id": card_id,
        "amount": round(random.uniform(1.50, 500.00), 2),
        "currency": "USD",
        "merchant": random.choice(MERCHANTS),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "country": random.choice(COUNTRIES),
        "timestamp": timestamp.isoformat(),
        "is_fraud_injected": False,
        "fraud_pattern": None,
    }


def inject_velocity_spike(card_id: str, base_time: datetime) -> list[dict]:
    """Generate 5-8 rapid transactions in under 2 minutes (velocity spike).

    Real fraud pattern: stolen card used for rapid purchases before block.
    """
    count = random.randint(5, 8)
    transactions = []
    for i in range(count):
        t = base_time + timedelta(seconds=random.randint(5, 90))
        transactions.append({
            "card_id": card_id,
            "amount": round(random.uniform(50.00, 2000.00), 2),
            "currency": "USD",
            "merchant": random.choice(MERCHANTS),
            "merchant_category": random.choice(MERCHANT_CATEGORIES),
            "country": random.choice(COUNTRIES[:3]),  # Same region
            "timestamp": t.isoformat(),
            "is_fraud_injected": True,
            "fraud_pattern": "velocity_spike",
        })
    return transactions


def inject_impossible_travel(card_id: str, base_time: datetime) -> list[dict]:
    """Generate transactions from distant countries within minutes.

    Real fraud pattern: card cloned and used simultaneously in different countries.
    """
    group1 = random.choice(list(COUNTRY_GROUPS.values()))
    group2 = random.choice(list(COUNTRY_GROUPS.values()))
    while group1 == group2:
        group2 = random.choice(list(COUNTRY_GROUPS.values()))

    return [
        {
            "card_id": card_id,
            "amount": round(random.uniform(100.00, 1500.00), 2),
            "currency": "USD",
            "merchant": random.choice(MERCHANTS),
            "merchant_category": random.choice(MERCHANT_CATEGORIES),
            "country": random.choice(group1),
            "timestamp": base_time.isoformat(),
            "is_fraud_injected": True,
            "fraud_pattern": "impossible_travel",
        },
        {
            "card_id": card_id,
            "amount": round(random.uniform(100.00, 1500.00), 2),
            "currency": "USD",
            "merchant": random.choice(MERCHANTS),
            "merchant_category": random.choice(MERCHANT_CATEGORIES),
            "country": random.choice(group2),
            "timestamp": (base_time + timedelta(minutes=random.randint(1, 5))).isoformat(),
            "is_fraud_injected": True,
            "fraud_pattern": "impossible_travel",
        },
    ]


def inject_odd_hour_high_amount(card_id: str, base_time: datetime) -> list[dict]:
    """Generate a large transaction at 2-5 AM.

    Real fraud pattern: fraudsters operate at odd hours to avoid detection.
    """
    odd_hour = random.randint(2, 5)
    timestamp = base_time.replace(hour=odd_hour, minute=random.randint(0, 59))

    return [{
        "card_id": card_id,
        "amount": round(random.uniform(2000.00, 9999.99), 2),
        "currency": "USD",
        "merchant": random.choice(MERCHANTS),
        "merchant_category": random.choice(["electronics", "retail"]),
        "country": random.choice(FRAUD_COUNTRIES + COUNTRIES[:2]),
        "timestamp": timestamp.isoformat(),
        "is_fraud_injected": True,
        "fraud_pattern": "odd_hour_high_amount",
    }]


def inject_card_testing(card_id: str, base_time: datetime) -> list[dict]:
    """Generate small round-amount transactions (card testing / probing).

    Real fraud pattern: test if stolen card details work with tiny charges.
    """
    count = random.randint(3, 6)
    transactions = []
    for i in range(count):
        transactions.append({
            "card_id": card_id,
            "amount": round(random.choice([1.00, 2.00, 5.00, 0.50, 0.01]), 2),
            "currency": "USD",
            "merchant": random.choice(MERCHANTS),
            "merchant_category": "retail",
            "country": random.choice(COUNTRIES),
            "timestamp": (base_time + timedelta(seconds=random.randint(30, 300))).isoformat(),
            "is_fraud_injected": True,
            "fraud_pattern": "card_testing",
        })
    return transactions


FRAUD_INJECTORS = [
    inject_velocity_spike,
    inject_impossible_travel,
    inject_odd_hour_high_amount,
    inject_card_testing,
]


def maybe_inject_fraud(card_id: str, timestamp: datetime, fraud_rate: float = 0.02) -> list[dict] | None:
    """With `fraud_rate` probability, inject a fraud pattern.

    Returns list of fraudulent transactions, or None for no fraud.
    """
    if random.random() < fraud_rate:
        injector = random.choice(FRAUD_INJECTORS)
        return injector(card_id, timestamp)
    return None
