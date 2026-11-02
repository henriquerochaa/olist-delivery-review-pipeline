"""Testes da publicação Gold, sem Postgres real: usam DuckDB temporário e um destino falso."""

from __future__ import annotations

from contextlib import contextmanager
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


class FakeCopy:
    def __init__(self, sink: list[tuple[Any, ...]]) -> None:
        self.sink = sink

    def write_row(self, row: tuple[Any, ...]) -> None:
        self.sink.append(row)


class FakeCursor:
    def __init__(self, store: dict[str, int], break_count: bool) -> None:
        self.store = store
        self.break_count = break_count
        self.current = ""
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, query: Any) -> None:
        text = query.as_string() if hasattr(query, "as_string") else str(query)
        if text.startswith("select count"):
            return
        self.current = text

    @contextmanager
    def copy(self, query: object):
        sink: list[tuple[Any, ...]] = []
        yield FakeCopy(sink)
        self.store["last"] = len(sink)

    def fetchone(self) -> tuple[int]:
        return (self.store["last"] + (1 if self.break_count else 0),)


class FakeConnection:
    def __init__(self, *, break_count: bool = False) -> None:
        self.store: dict[str, int] = {}
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
    counts = publish.load_tables(connection, "gold", rows)  # ty: ignore[invalid-argument-type]
    assert counts == {"fct_delivery_reviews": 2, "mart_delivery_review_summary": 1}
    assert not connection.rolled_back


def test_count_mismatch_rolls_back(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    _make_duckdb(db)
    rows = publish.read_gold_rows(db)
    connection = FakeConnection(break_count=True)
    with pytest.raises(PublishError, match="destino"):
        publish.load_tables(connection, "gold", rows)  # ty: ignore[invalid-argument-type]
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
    monkeypatch.setenv(publish.DATABASE_URL_ENV, "postgresql://u:abc%*segredo@h/db")
    assert publish.main(["--duckdb", str(db)]) == 1
    assert "segredo" not in capsys.readouterr().err
