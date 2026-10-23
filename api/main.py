"""FastAPI application entry point.

Fraud Signal Pipeline API:
- Transaction queries
- Alert management
- Pipeline health & stats
- Alertmanager webhook
- RAG Copilot for incident response
"""

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import make_asgi_app

from api.routes import router
from config import settings
from observability.logging import setup_logging
from observability.tracing import setup_tracing

# Initialize observability
setup_logging(settings.log_level)
setup_tracing(
    service_name=f"{settings.otel_service_name}-api",
    otlp_endpoint=settings.otel_exporter_otlp_endpoint,
)

# Create FastAPI app
app = FastAPI(
    title="Fraud Signal Pipeline API",
    description="Real-time fraud detection pipeline with rules, ML, and resiliency patterns",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# OpenTelemetry instrumentation
FastAPIInstrumentor.instrument_app(app)

# Register routes
app.include_router(router)

# Try to register copilot routes (optional — requires GOOGLE_API_KEY)
try:
    from copilot.rag import copilot_router
    app.include_router(copilot_router, prefix="/copilot", tags=["copilot"])
except ImportError:
    pass
except Exception:
    pass


def main():
    """Run the API server."""
    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
