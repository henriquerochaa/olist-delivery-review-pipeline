"""Testes da publicação Gold, sem Postgres real: usam DuckDB temporário e um destino falso."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self

import duckdb
import pytest

from projeto1_pipeline_qualidade_dados import publish
from projeto1_pipeline_qualidade_dados.publish import PublishError


def _make_duckdb(path: Path, *, summary_orders: int = 2) -> None:
    connection = duckdb.connect(str(path))
    connection.execute(
        """create table fct_delivery_reviews as select * from (values
        ('o1', 'delivered', timestamp '2026-01-02', timestamp '2026-01-03',
         'early', -1::bigint, 5.0, 1::bigint),
        ('o2', 'canceled', null, timestamp '2026-01-03',
         'not_delivered', null, null, 0::bigint)
        ) t(order_id, order_status, delivered_to_customer_at, estimated_delivery_at,
            delivery_bucket, delivery_delay_days, average_review_score, review_count)"""
    )
    connection.execute(
        f"""create table mart_delivery_review_summary as select
        'early' as delivery_bucket, {summary_orders}::bigint as order_count,
        1::bigint as reviewed_order_count, 1::hugeint as review_record_count,
        5.0 as average_score_per_order, -1.0 as average_delay_days"""
    )
    connection.close()


BUILT_AT = datetime(2026, 11, 2, 14, 0, tzinfo=UTC)


def _make_dbt_results(
    path: Path, *, statuses: tuple[str, ...] = ("pass", "success")
) -> None:
    results = [{"unique_id": f"node.{i}", "status": s} for i, s in enumerate(statuses)]
    meta = {"generated_at": "2026-11-02T14:00:00.000000Z"}
    path.write_text(
        json.dumps({"metadata": meta, "results": results}), encoding="utf-8"
    )


class FakeCopy:
    def __init__(self, sink: list[tuple[Any, ...]]) -> None:
        self.sink = sink

    def write_row(self, row: tuple[Any, ...]) -> None:
        self.sink.append(row)


class FakeCursor:
    def __init__(self, store: dict[str, Any], break_count: bool) -> None:
        self.store = store
        self.break_count = break_count
        self.current = ""
        self.pending_max = False
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, query: Any, params: tuple[Any, ...] | None = None) -> None:
        text = query.as_string() if hasattr(query, "as_string") else str(query)
        self.pending_max = text.startswith("select max(gold_built_at)")
        if text.startswith("insert into") and params:
            self.store["logged"] = params[0]
        if text.startswith("select count"):
            return
        self.current = text

    @contextmanager
    def copy(self, query: object):
        sink: list[tuple[Any, ...]] = []
        yield FakeCopy(sink)
        self.store["last"] = len(sink)

    def fetchone(self) -> tuple[Any]:
        if self.pending_max:
            return (self.store.get("last_published"),)
        return (self.store["last"] + (1 if self.break_count else 0),)


class FakeConnection:
    def __init__(
        self, *, break_count: bool = False, last_published: datetime | None = None
    ) -> None:
        self.store: dict[str, Any] = {"last_published": last_published}
        self.break_count = break_count
        self.rolled_back = False

    @contextmanager
    def transaction(self):
        try:
            yield
        except Exception:
            self.rolled_back = True
            raise

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.store, self.break_count)


def test_validate_identifier_rejects_unsafe_schema() -> None:
    assert publish.validate_identifier("gold") == "gold"
    for bad in ["Gold", "gold; drop table x", "1gold", "", "a-b"]:
        with pytest.raises(PublishError):
            publish.validate_identifier(bad)


def test_read_gold_rows_returns_all_rows(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    rows = publish.read_gold_rows(db)
    assert len(rows["fct_delivery_reviews"]) == 2
    assert len(rows["mart_delivery_review_summary"]) == 1


def test_missing_duckdb_blocks(tmp_path: Path) -> None:
    with pytest.raises(PublishError, match="não encontrado"):
        publish.read_gold_rows(tmp_path / "nao_existe.duckdb")


def test_inconsistent_source_blocks_before_connecting(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db, summary_orders=99)
    # URL inválida: se tentasse conectar, o erro seria outro.
    with pytest.raises(PublishError, match="não reconcilia"):
        publish.publish_gold(db, "postgresql://invalido")


def test_missing_database_url_blocks(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    with pytest.raises(PublishError, match=publish.DATABASE_URL_ENV):
        publish.publish_gold(db, "")


def test_load_tables_reports_counts(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    rows = publish.read_gold_rows(db)
    connection = FakeConnection()
    counts = publish.load_tables(connection, "gold", rows, BUILT_AT)  # ty: ignore[invalid-argument-type]
    assert counts == {"fct_delivery_reviews": 2, "mart_delivery_review_summary": 1}
    assert not connection.rolled_back


def test_count_mismatch_rolls_back(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    rows = publish.read_gold_rows(db)
    connection = FakeConnection(break_count=True)
    with pytest.raises(PublishError, match="destino"):
        publish.load_tables(connection, "gold", rows, BUILT_AT)  # ty: ignore[invalid-argument-type]
    assert connection.rolled_back


def test_main_without_env_returns_1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    monkeypatch.delenv(publish.DATABASE_URL_ENV, raising=False)
    assert publish.main(["--duckdb", str(db)]) == 1
    assert publish.DATABASE_URL_ENV in capsys.readouterr().err


def test_main_never_prints_driver_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    # Senha com % inválido: o driver repete o trecho na mensagem de erro.
    results = tmp_path / "run_results.json"
    _make_dbt_results(results)
    monkeypatch.setenv(publish.DATABASE_URL_ENV, "postgresql://u:abc%*segredo@h/db")
    assert publish.main(["--duckdb", str(db), "--dbt-results", str(results)]) == 1
    assert "segredo" not in capsys.readouterr().err


def test_load_tables_logs_build_time(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    connection = FakeConnection()
    publish.load_tables(connection, "gold", publish.read_gold_rows(db), BUILT_AT)  # ty: ignore[invalid-argument-type]
    assert connection.store["logged"] == BUILT_AT


def test_older_gold_than_last_published_is_refused(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    newer = datetime(2026, 11, 3, tzinfo=UTC)
    connection = FakeConnection(last_published=newer)
    with pytest.raises(PublishError, match="mais antiga"):
        publish.load_tables(connection, "gold", publish.read_gold_rows(db), BUILT_AT)  # ty: ignore[invalid-argument-type]
    assert connection.rolled_back
    assert "logged" not in connection.store


def test_republishing_same_gold_is_allowed(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    connection = FakeConnection(last_published=BUILT_AT)
    publish.load_tables(connection, "gold", publish.read_gold_rows(db), BUILT_AT)  # ty: ignore[invalid-argument-type]
    assert not connection.rolled_back


def test_failed_dbt_item_blocks_before_connecting(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    results = tmp_path / "run_results.json"
    _make_dbt_results(results, statuses=("pass", "fail"))
    with pytest.raises(PublishError, match="sem sucesso"):
        publish.publish_gold(db, "postgresql://invalido", "gold", results)


def test_missing_dbt_results_blocks_before_connecting(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    with pytest.raises(PublishError, match="Resultado do dbt"):
        publish.publish_gold(db, "postgresql://invalido", "gold", tmp_path / "nao.json")
