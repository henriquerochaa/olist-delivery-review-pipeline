"""Airflow DAG que orquestra a execução local do Projeto 1."""

from __future__ import annotations

import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dag(
    dag_id="olist_delivery_review_pipeline",
    description="Ingestão Bronze seguida da transformação e qualidade dbt.",
    start_date=pendulum.datetime(2026, 10, 29, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=3),
    default_args={"retries": 0, "execution_timeout": timedelta(hours=2)},
    params={
        "batch_id": Param(
            "olist-mvp-2026-10-25",
            type="string",
            pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$",
            description="Identificador do lote Bronze a construir ou substituir.",
        )
    },
    tags=["projeto1", "olist", "batch"],
)
def olist_delivery_review_pipeline():
    @task
    def ingest_bronze() -> None:
        batch_id = get_current_context()["params"]["batch_id"]
        subprocess.run(
            ["make", "bronze", f"BATCH_ID={batch_id}"],
            cwd=PROJECT_ROOT,
            check=True,
        )

    @task
    def build_and_test_models() -> None:
        batch_id = get_current_context()["params"]["batch_id"]
        subprocess.run(
            ["make", "transform", f"BATCH_ID={batch_id}"],
            cwd=PROJECT_ROOT,
            check=True,
        )

    ingest_bronze() >> build_and_test_models()


olist_delivery_review_pipeline()
