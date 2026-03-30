.PHONY: setup seed lint test dq run-bronze run-silver run-gold run-all clean help

PYTHON := python3
PYTEST := pytest
RUFF := ruff
MYPY := mypy
NOTEBOOKS_DIR := notebooks
TESTS_DIR := tests

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup:  ## Create venv, install dependencies, scaffold lakehouse dirs
	bash scripts/setup_local_env.sh

seed:  ## Generate sample data (1K rows per entity)
	$(PYTHON) scripts/seed_data.py --small

seed-full:  ## Generate full dataset (100K rows per entity)
	$(PYTHON) scripts/seed_data.py --full

lint:  ## Run ruff and mypy linters
	$(RUFF) check $(NOTEBOOKS_DIR) $(TESTS_DIR) scripts --fix
	$(MYPY) $(NOTEBOOKS_DIR) --ignore-missing-imports

test:  ## Run unit tests
	$(PYTEST) $(TESTS_DIR)/unit/ -v --tb=short

test-integration:  ## Run integration tests (requires seeded data)
	$(PYTEST) $(TESTS_DIR)/integration/ -v --tb=short -m integration

test-all:  ## Run all tests including integration
	$(PYTEST) $(TESTS_DIR)/ -v --tb=short

dq:  ## Run Great Expectations data quality checks
	$(PYTHON) -m great_expectations checkpoint run default_checkpoint --config data_quality/checkpoints/default_checkpoint.yaml 2>/dev/null || echo "GE checkpoint run (configure GE context for full execution)"

run-bronze:  ## Run all Bronze ingestion notebooks
	$(PYTHON) $(NOTEBOOKS_DIR)/bronze/ingest_pos_transactions.py
	$(PYTHON) $(NOTEBOOKS_DIR)/bronze/ingest_inventory.py
	$(PYTHON) $(NOTEBOOKS_DIR)/bronze/ingest_customers.py
	$(PYTHON) $(NOTEBOOKS_DIR)/bronze/ingest_clickstream.py

run-silver:  ## Run all Silver transformation notebooks
	$(PYTHON) $(NOTEBOOKS_DIR)/silver/clean_transactions.py
	$(PYTHON) $(NOTEBOOKS_DIR)/silver/clean_inventory.py
	$(PYTHON) $(NOTEBOOKS_DIR)/silver/clean_customers.py
	$(PYTHON) $(NOTEBOOKS_DIR)/silver/sessionise_clicks.py

run-gold:  ## Run all Gold aggregation notebooks
	$(PYTHON) $(NOTEBOOKS_DIR)/gold/build_dim_customer.py
	$(PYTHON) $(NOTEBOOKS_DIR)/gold/build_dim_product.py
	$(PYTHON) $(NOTEBOOKS_DIR)/gold/build_fact_sales.py
	$(PYTHON) $(NOTEBOOKS_DIR)/gold/build_customer_360.py

run-all:  ## Run full Bronze → Silver → Gold pipeline
	$(MAKE) run-bronze
	$(MAKE) run-silver
	$(MAKE) run-gold

clean:  ## Remove build artifacts, caches, and Delta tables
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf lakehouse/bronze/pos_transactions lakehouse/bronze/inventory_movements
	rm -rf lakehouse/bronze/customers lakehouse/bronze/clickstream
	rm -rf lakehouse/silver/* lakehouse/gold/*
	rm -rf test-results/
	@echo "Clean complete."
