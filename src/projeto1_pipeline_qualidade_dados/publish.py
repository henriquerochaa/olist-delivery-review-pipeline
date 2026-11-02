"""Publicação das tabelas Gold do DuckDB local em um Postgres (Neon).

A carga é um full refresh dentro de uma única transação: ou as duas tabelas
são substituídas e reconciliadas, ou nada muda no destino. A connection string
vem de uma variável de ambiente e nunca é impressa.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, LiteralString

import duckdb
import psycopg
from psycopg import sql

DATABASE_URL_ENV = "NEON_DATABASE_URL"
DEFAULT_DUCKDB_PATH = "data/warehouse.duckdb"
DEFAULT_SCHEMA = "gold"
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

# Tabela Gold -> (coluna, tipo Postgres). A ordem define a ordem do COPY.
GOLD_TABLES: dict[str, list[tuple[str, LiteralString]]] = {
    "fct_delivery_reviews": [
        ("order_id", "text"),
        ("order_status", "text"),
        ("delivered_to_customer_at", "timestamp"),
        ("estimated_delivery_at", "timestamp"),
        ("delivery_bucket", "text"),
        ("delivery_delay_days", "bigint"),
        ("average_review_score", "double precision"),
        ("review_count", "bigint"),
    ],
    "mart_delivery_review_summary": [
        ("delivery_bucket", "text"),
        ("order_count", "bigint"),
        ("reviewed_order_count", "bigint"),
        ("review_record_count", "bigint"),
        ("average_score_per_order", "double precision"),
        ("average_delay_days", "double precision"),
    ],
}


class PublishError(RuntimeError):
    """Falha esperada de publicação (configuração ou reconciliação)."""


def validate_identifier(name: str) -> str:
    """Aceita só nomes simples de schema, para evitar SQL inesperado."""
    if not _IDENTIFIER.fullmatch(name):
        raise PublishError(f"Nome de schema inválido: {name!r}")
    return name


def read_gold_rows(duckdb_path: Path) -> dict[str, list[tuple[Any, ...]]]:
    """Lê as tabelas Gold do DuckDB, na ordem de colunas de GOLD_TABLES."""
    if not duckdb_path.exists():
        raise PublishError(f"Banco DuckDB não encontrado: {duckdb_path}")
    rows: dict[str, list[tuple[Any, ...]]] = {}
    connection = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        for table, columns in GOLD_TABLES.items():
            names = ", ".join(name for name, _ in columns)
            rows[table] = connection.execute(f"select {names} from {table}").fetchall()
    finally:
        connection.close()
    return rows


def check_source_consistency(rows: dict[str, list[tuple[Any, ...]]]) -> None:
    """Bloqueia a publicação se a origem já estiver inconsistente."""
    facts = rows["fct_delivery_reviews"]
    summary = rows["mart_delivery_review_summary"]
    if not facts or not summary:
        raise PublishError("Tabela Gold vazia na origem; nada foi publicado.")
    summarized_orders = sum(row[1] for row in summary)
    if summarized_orders != len(facts):
        raise PublishError(
            f"Resumo ({summarized_orders}) não reconcilia com a fato ({len(facts)})."
        )


def _create_table_sql(schema: str, table: str) -> sql.Composed:
    columns = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(pg_type))
        for name, pg_type in GOLD_TABLES[table]
    )
    return sql.SQL("create table if not exists {}.{} ({})").format(
        sql.Identifier(schema), sql.Identifier(table), columns
    )


def _copy_sql(schema: str, table: str) -> sql.Composed:
    columns = sql.SQL(", ").join(sql.Identifier(name) for name, _ in GOLD_TABLES[table])
    return sql.SQL("copy {}.{} ({}) from stdin").format(
        sql.Identifier(schema), sql.Identifier(table), columns
    )


def load_tables(
    connection: psycopg.Connection,
    schema: str,
    rows: dict[str, list[tuple[Any, ...]]],
) -> dict[str, int]:
    """Substitui as tabelas numa transação e reconcilia contagens antes do commit."""
    counts: dict[str, int] = {}
    with connection.transaction(), connection.cursor() as cursor:
        for table, table_rows in rows.items():
            cursor.execute(_create_table_sql(schema, table))
            cursor.execute(
                sql.SQL("truncate {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )
            with cursor.copy(_copy_sql(schema, table)) as copy:
                for row in table_rows:
                    copy.write_row(row)
            cursor.execute(
                sql.SQL("select count(*) from {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )
            result = cursor.fetchone()
            loaded = int(result[0]) if result else -1
            if loaded != len(table_rows):
                # A exceção desfaz a transação inteira.
                raise PublishError(
                    f"{table}: origem {len(table_rows)} linhas, destino {loaded}."
                )
            counts[table] = loaded
    return counts


def publish_gold(
    duckdb_path: Path,
    database_url: str,
    schema: str = DEFAULT_SCHEMA,
) -> dict[str, int]:
    """Valida a origem, carrega o destino e devolve as contagens publicadas."""
    schema = validate_identifier(schema)
    if not database_url:
        raise PublishError(f"Defina a variável de ambiente {DATABASE_URL_ENV}.")
    rows = read_gold_rows(duckdb_path)
    check_source_consistency(rows)
    with psycopg.connect(database_url, connect_timeout=15) as connection:
        return load_tables(connection, schema, rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publica a Gold no Postgres (Neon).")
    parser.add_argument("--duckdb", default=DEFAULT_DUCKDB_PATH)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    args = parser.parse_args(argv)
    try:
        counts = publish_gold(
            Path(args.duckdb),
            os.environ.get(DATABASE_URL_ENV, ""),
            args.schema,
        )
    except PublishError as error:
        print(f"Publicação falhou: {error}", file=sys.stderr)
        return 1
    except (psycopg.Error, duckdb.Error) as error:
        # A mensagem do driver pode repetir trechos da connection string
        # (inclusive a senha); por isso só o tipo do erro é impresso.
        print(
            f"Publicação falhou ({type(error).__name__}). Confira a "
            f"{DATABASE_URL_ENV}, a rede e as permissões do papel.",
            file=sys.stderr,
        )
        return 1
    print(json.dumps({"status": "published", "schema": args.schema, "rows": counts}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
