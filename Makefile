BATCH_ID ?= olist-mvp-2026-10-25
BRONZE_DIR ?= data/bronze/$(BATCH_ID)
DBT_PROJECT := dbt_pipeline_qualidade
DBT_VARS := {"bronze_dir": "$(abspath $(BRONZE_DIR))"}

.PHONY: bronze transform pipeline airflow airflow-test test lint format typecheck

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
	uv run ruff check src tests airflow/dags

format:
	uv run ruff format --check src tests airflow/dags

typecheck:
	uv run ty check src tests


AIRFLOW_HOME := $(abspath .airflow)
AIRFLOW_DAGS := $(abspath airflow/dags)

# Airflow roda em ambiente uv separado do pipeline de dados.
airflow:
	AIRFLOW_HOME="$(AIRFLOW_HOME)" AIRFLOW__CORE__DAGS_FOLDER="$(AIRFLOW_DAGS)" uv run --project airflow airflow db migrate
	AIRFLOW_HOME="$(AIRFLOW_HOME)" AIRFLOW__CORE__DAGS_FOLDER="$(AIRFLOW_DAGS)" uv run --project airflow airflow standalone

airflow-test:
	AIRFLOW_HOME="$(AIRFLOW_HOME)" AIRFLOW__CORE__DAGS_FOLDER="$(AIRFLOW_DAGS)" uv run --project airflow airflow db migrate
	AIRFLOW_HOME="$(AIRFLOW_HOME)" AIRFLOW__CORE__DAGS_FOLDER="$(AIRFLOW_DAGS)" uv run --project airflow airflow dags test olist_delivery_review_pipeline 2026-10-29
