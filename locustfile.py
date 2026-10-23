"""Locust load test — smoke test for ~1000 msg/s throughput.

Simulates concurrent API queries against the running pipeline
to measure p50/p95/p99 latency under load.

Usage:
    locust -f locustfile.py --headless -u 50 -r 10 -t 60s --host http://localhost:8000
"""

import random

from locust import HttpUser, between, task


class FraudPipelineUser(HttpUser):
    """Simulated user querying the fraud pipeline API."""

    wait_time = between(0.1, 0.5)  # 100-500ms between requests

    @task(5)
    def health_check(self):
        """GET /health — most frequent check."""
        self.client.get("/health")

    @task(3)
    def get_stats(self):
        """GET /stats — pipeline statistics."""
        self.client.get("/stats")

    @task(2)
    def list_alerts(self):
        """GET /alerts — list recent alerts."""
        self.client.get("/alerts?limit=10")

    @task(2)
    def list_alerts_by_severity(self):
        """GET /alerts filtered by severity."""
        severity = random.choice(["critical", "high", "medium"])
        self.client.get(f"/alerts?severity={severity}&limit=20")

    @task(1)
    def get_random_transaction(self):
        """GET /transactions/{id} — may 404, that's expected."""
        txn_id = f"TXN-{random.randint(1, 10000):08X}"
        with self.client.get(
            f"/transactions/{txn_id}",
            catch_response=True,
        ) as response:
            if response.status_code == 404:
                response.success()  # 404 is expected for random IDs
