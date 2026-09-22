# RetailLake - one-command workflows.  Run `make help` to list them.
# Run from the repository root. Needs Docker, and a .env file (cp .env.example .env).

SHELL := /bin/bash
DATE  ?= $(shell date +%F)
DC     = docker compose --env-file .env -f docker/docker-compose.yml
AIRFLOW_EXEC = docker exec retaillake-airflow-scheduler airflow
PSQL   = docker exec -i retaillake-postgres psql -U $${POSTGRES_USER:-retaillake} -d retaildb

.DEFAULT_GOAL := help
.PHONY: help up down seed unpause bronze silver gold pipeline demo quality test lint e2e clean

help:  ## List the available commands
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-10s %s\n", $$1, $$2}'

up:  ## Start the whole stack (Postgres, MinIO, Airflow, dbt, Superset)
	$(DC) up -d

down:  ## Stop the stack (data volumes are kept)
	$(DC) down

seed:  ## Regenerate the mock source data (fixed seed 42 -> identical every time)
	python scripts/seed_mock_sources.py --seed 42 --out-dir data/sample

unpause:  ## Un-pause the DAGs (a paused child DAG makes the master DAG wait forever)
	$(AIRFLOW_EXEC) dags unpause retail_pipeline
	$(AIRFLOW_EXEC) dags unpause retail_bronze_ingestion
	$(AIRFLOW_EXEC) dags unpause retail_silver_clean

bronze:  ## Raw landing in MinIO + typed Bronze tables (DATE=YYYY-MM-DD, default today)
	$(AIRFLOW_EXEC) dags test retail_bronze_ingestion $(DATE)

silver:  ## Validate, quarantine, hash PII, deduplicate -> Silver
	$(AIRFLOW_EXEC) dags test retail_silver_clean $(DATE)

gold:  ## Build the Gold star schema + run every dbt test
	docker exec retaillake-dbt dbt build

pipeline: bronze silver gold  ## Full run: Raw -> Bronze -> Silver -> Gold

demo: up seed unpause pipeline quality  ## Everything, from a cold start, ending with the quality summary

quality:  ## Show the data-quality scorecard and why rows were quarantined
	@echo "--- pass rate per source ---"
	@$(PSQL) -c "SELECT source_name, rows_in, rows_clean, rows_quarantined, pass_rate_pct FROM gold.quality_scorecard ORDER BY 1;"
	@echo "--- quarantine reasons ---"
	@$(PSQL) -c "SELECT source_name, failed_rule, quarantined_count FROM gold.quality_summary ORDER BY 1, 3 DESC;"

test:  ## Fast unit tests (no Docker needed): pip install -r requirements-dev.txt
	python -m pytest tests/unit -q

lint:  ## Lint the Python code with ruff
	ruff check .

e2e:  ## End-to-end test (needs Postgres + S3 endpoint + Airflow + dbt; see tests/e2e)
	RETAIL_E2E=1 python -m pytest tests/dags tests/e2e -q

clean:  ## Remove local caches (keeps Docker volumes and data)
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov dbt_smart_retail/target dbt_smart_retail/logs
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
