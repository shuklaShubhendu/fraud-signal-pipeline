"""Prometheus metrics tool for the RAG copilot.

Provides a LangChain-compatible tool that fetches live metrics
from Prometheus for a given service, enabling the copilot to
include real-time data in incident summaries.
"""

import httpx

from observability.logging import get_logger

logger = get_logger("copilot.tools")

PROMETHEUS_URL = "http://prometheus:9090"


async def fetch_recent_metrics(metric_names: list[str] | None = None, minutes: int = 15) -> dict:
    """Fetch recent metrics from Prometheus for incident analysis.

    Args:
        metric_names: Specific metrics to query. If None, fetches key pipeline metrics.
        minutes: How far back to look (default 15 minutes).

    Returns:
        Dictionary of metric_name -> current values/rates.
    """
    if metric_names is None:
        metric_names = [
            "transactions_processed_total",
            "transactions_processing_errors_total",
            "processing_latency_seconds",
            "circuit_breaker_state",
            "degraded_mode_total",
            "kafka_consumer_lag",
            "database_errors_total",
            "fraud_detected_total",
            "dead_letter_total",
            "risk_api_latency_seconds",
        ]

    results = {}

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            for metric in metric_names:
                try:
                    # Instant query for current value
                    response = await client.get(
                        f"{PROMETHEUS_URL}/api/v1/query",
                        params={"query": metric},
                    )
                    data = response.json()

                    if data.get("status") == "success" and data.get("data", {}).get("result"):
                        values = []
                        for result in data["data"]["result"]:
                            labels = result.get("metric", {})
                            value = result.get("value", [None, None])[1]
                            values.append(
                                {
                                    "labels": {k: v for k, v in labels.items() if k != "__name__"},
                                    "value": value,
                                }
                            )
                        results[metric] = values
                    else:
                        results[metric] = "no_data"

                    # Also get rate for counters
                    if "total" in metric:
                        rate_response = await client.get(
                            f"{PROMETHEUS_URL}/api/v1/query",
                            params={"query": f"rate({metric}[{minutes}m])"},
                        )
                        rate_data = rate_response.json()
                        if rate_data.get("status") == "success" and rate_data.get("data", {}).get("result"):
                            rate_values = []
                            for result in rate_data["data"]["result"]:
                                labels = result.get("metric", {})
                                value = result.get("value", [None, None])[1]
                                rate_values.append(
                                    {
                                        "labels": {k: v for k, v in labels.items() if k != "__name__"},
                                        "rate_per_second": value,
                                    }
                                )
                            results[f"{metric}_rate"] = rate_values

                except Exception as e:
                    results[metric] = f"error: {str(e)}"

    except Exception as e:
        logger.error("prometheus_fetch_failed", error=str(e))
        return {"error": f"Failed to connect to Prometheus: {str(e)}"}

    return results


def format_metrics_for_llm(metrics: dict) -> str:
    """Format metrics dictionary into a human-readable string for the LLM."""
    lines = ["## Current Pipeline Metrics (last 15 minutes)\n"]

    for metric_name, values in metrics.items():
        if isinstance(values, str):
            lines.append(f"**{metric_name}**: {values}")
        elif isinstance(values, list):
            lines.append(f"**{metric_name}**:")
            for v in values:
                labels = v.get("labels", {})
                value = v.get("value") or v.get("rate_per_second", "N/A")
                label_str = ", ".join(f"{k}={val}" for k, val in labels.items()) if labels else "total"
                lines.append(f"  - [{label_str}]: {value}")
        lines.append("")

    return "\n".join(lines)
