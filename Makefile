.PHONY: up down demo test lint clean logs

# ─── Infrastructure ───
up:
	docker compose up -d --build
	@echo ""
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║  Fraud Signal Pipeline — Services Starting...       ║"
	@echo "╠══════════════════════════════════════════════════════╣"
	@echo "║  API:          http://localhost:8000/docs            ║"
	@echo "║  Grafana:      http://localhost:3000                 ║"
	@echo "║  Jaeger:       http://localhost:16686                ║"
	@echo "║  Prometheus:   http://localhost:9090                 ║"
	@echo "║  Alertmanager: http://localhost:9093                 ║"
	@echo "╚══════════════════════════════════════════════════════╝"

down:
	docker compose down -v

# ─── Demo ───
demo: up
	@echo "Waiting for services to stabilize..."
	@sleep 10
	@echo ""
	@echo "Pipeline is running! Transactions are being generated."
	@echo "  → Watch Grafana: http://localhost:3000"
	@echo "  → Query API:    curl http://localhost:8000/health"
	@echo "  → View traces:  http://localhost:16686"

# ─── Development ───
test:
	docker compose up -d redpanda postgres
	@sleep 5
	pytest tests/ -v --tb=short

lint:
	ruff check .
	ruff format --check .

format:
	ruff format .

# ─── Logs ───
logs:
	docker compose logs -f consumer generator api

logs-consumer:
	docker compose logs -f consumer

logs-generator:
	docker compose logs -f generator

# ─── Performance ───
loadtest:
	locust -f locustfile.py --headless -u 50 -r 10 -t 60s --host http://localhost:8000

# ─── Cleanup ───
clean: down
	docker system prune -f
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
