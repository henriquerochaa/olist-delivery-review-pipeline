"""Verificações estáticas da orquestração, sem importar nem iniciar o Airflow.

O Airflow vive em um ambiente `uv` separado; por isso a DAG é lida como código
(AST) e comparada com o Makefile e com a validação de `batch_id` da Bronze.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = PROJECT_ROOT / "airflow" / "dags" / "olist_delivery_review_pipeline.py"
BRONZE_FILE = PROJECT_ROOT / "src" / "projeto1_pipeline_qualidade_dados" / "bronze.py"
MAKEFILE = PROJECT_ROOT / "Makefile"


def _calls(tree: ast.AST, name: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id == name)
            or (isinstance(node.func, ast.Attribute) and node.func.attr == name)
        )
    ]


def _keywords(call: ast.Call) -> dict[str, ast.expr]:
    return {kw.arg: kw.value for kw in call.keywords if kw.arg}


@pytest.fixture(scope="module")
def dag_tree() -> ast.Module:
    return ast.parse(DAG_FILE.read_text(encoding="utf-8"))


def test_dag_is_manual_single_run_without_backfill_or_retries(
    dag_tree: ast.Module,
) -> None:
    dag_decorator = next(
        node
        for node in ast.walk(dag_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "dag"
    )
    options = _keywords(dag_decorator)

    assert ast.literal_eval(options["dag_id"]) == "olist_delivery_review_pipeline"
    assert ast.literal_eval(options["schedule"]) is None
    assert ast.literal_eval(options["catchup"]) is False
    assert ast.literal_eval(options["max_active_runs"]) == 1
    assert ast.literal_eval(options["max_active_tasks"]) == 1
    default_args = options["default_args"]
    assert isinstance(default_args, ast.Dict)
    retries = {
        ast.literal_eval(key): value
        for key, value in zip(default_args.keys, default_args.values, strict=True)
        if key is not None
    }["retries"]
    assert ast.literal_eval(retries) == 0


def test_dag_runs_bronze_then_transform_through_make(dag_tree: ast.Module) -> None:
    runs = _calls(dag_tree, "run")
    commands = []
    for call in runs:
        arguments = call.args[0]
        assert isinstance(arguments, ast.List)
        commands.append(ast.literal_eval(arguments.elts[1]))

    assert commands == ["bronze", "transform"]
    assert all(ast.literal_eval(_keywords(call)["check"]) is True for call in runs), (
        "uma falha de etapa deve falhar a task"
    )
    dependency = next(
        node
        for node in ast.walk(dag_tree)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.RShift)
    )
    assert ast.unparse(dependency) == "ingest_bronze() >> build_and_test_models()"


def test_dag_batch_id_pattern_matches_bronze_validation(dag_tree: ast.Module) -> None:
    param = _calls(dag_tree, "Param")[0]
    dag_pattern = ast.literal_eval(_keywords(param)["pattern"])
    bronze_source = BRONZE_FILE.read_text(encoding="utf-8")
    bronze_pattern = re.search(r're\.fullmatch\(r"([^"]+)", batch_id\)', bronze_source)

    assert bronze_pattern is not None
    assert dag_pattern.strip("^$") == bronze_pattern.group(1)
    for valid in ["olist-mvp-2026-10-25", "a", "lote_1.v2"]:
        assert re.fullmatch(dag_pattern, valid)
    for invalid in ["", "../x", "a b", ".oculto", "x" * 101]:
        assert not re.fullmatch(dag_pattern, invalid)


def test_make_targets_used_by_dag_exist_and_pipeline_stops_on_failure() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")

    for target in ("bronze", "transform", "pipeline"):
        assert re.search(rf"^{target}:", makefile, re.MULTILINE)
    pipeline = re.search(r"^pipeline:\n((?:\t.*\n)+)", makefile, re.MULTILINE)
    assert pipeline is not None
    steps = [line.strip() for line in pipeline.group(1).splitlines()]
    assert steps[0].startswith("uv run olist-bronze")
    assert steps[1].startswith("uv run dbt build")
    assert not any(step.startswith("-") for step in steps), (
        "prefixo '-' faria o make ignorar falhas"
    )
