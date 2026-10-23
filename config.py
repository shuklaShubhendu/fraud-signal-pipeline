"""Shared configuration for all services."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Kafka / Redpanda
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic_transactions: str = "transactions"
    kafka_topic_dead_letter: str = "dead-letter"
    kafka_consumer_group: str = "fraud-consumer-group"

    # PostgreSQL
    database_url: str = "postgresql+asyncpg://pipeline:pipeline_secret@localhost:5432/fraud_pipeline"
    database_url_sync: str = "postgresql://pipeline:pipeline_secret@localhost:5432/fraud_pipeline"

    # Risk API
    risk_api_url: str = "http://localhost:8001"
    risk_api_timeout_ms: int = 300
    risk_api_max_retries: int = 3

    # Observability
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    otel_service_name: str = "fraud-pipeline"
    prometheus_port: int = 8002
    log_level: str = "INFO"

    # Gemini
    google_api_key: str = ""
    gemini_chat_model: str = "gemini-2.5-flash"
    gemini_embedding_model: str = "models/gemini-embedding-001"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
