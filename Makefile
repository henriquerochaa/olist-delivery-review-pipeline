BATCH_ID ?= olist-mvp-2026-10-25
BRONZE_DIR ?= data/bronze/$(BATCH_ID)
DBT_PROJECT := dbt_pipeline_qualidade
DBT_VARS := {"bronze_dir": "$(abspath $(BRONZE_DIR))"}

.PHONY: bronze transform pipeline test lint format typecheck

bronze:
	uv run olist-bronze --batch-id "$(BATCH_ID)"

transform:
	uv run dbt build --project-dir "$(DBT_PROJECT)" --profiles-dir "$(DBT_PROJECT)" --vars '$(DBT_VARS)'

pipeline:
	uv run olist-bronze --batch-id "$(BATCH_ID)"
	uv run dbt build --project-dir "$(DBT_PROJECT)" --profiles-dir "$(DBT_PROJECT)" --vars '$(DBT_VARS)'

test:
	uv run pytest -q

lint:
	uv run ruff check src tests

format:
	uv run ruff format --check src tests

typecheck:
	uv run ty check src tests
