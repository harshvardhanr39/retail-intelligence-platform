.PHONY: setup start stop generate pipeline test clean

# Load environment variables
include .env
export

setup:
	python3.11 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	cd frontend && npm install
	@echo "✅ Setup complete. Run: source .venv/bin/activate"

start:
	cd docker && docker compose up -d
	@echo "✅ All services started"
	@echo "  Airflow: http://localhost:8080"
	@echo "  Kafka UI: http://localhost:8085"
	@echo "  Grafana: http://localhost:3001"

stop:
	cd docker && docker compose down

generate:
	.venv/bin/python generators/generate_orders.py
	.venv/bin/python generators/generate_inventory.py
	.venv/bin/python generators/generate_events.py
	@echo "✅ Data generated"

pipeline:
	.venv/bin/python pipeline/extractors/orders_extractor.py
	.venv/bin/python pipeline/extractors/products_extractor.py
	.venv/bin/python pipeline/extractors/inventory_extractor.py
	cd dbt_project && ../.venv/bin/dbt run --profiles-dir .
	cd dbt_project && ../.venv/bin/dbt test --profiles-dir .
	@echo "✅ Pipeline complete"

test:
	.venv/bin/pytest tests/ -v --cov=pipeline --cov-report=html

backend:
	cd backend && ../.venv/bin/uvicorn app.main:app --reload --port 8000

frontend-dev:
	cd frontend && npm run dev

clean:
	docker compose -f docker/docker-compose.yml down -v
	rm -rf data/landing/* data/bronze/*
	@echo "✅ Cleaned all data"