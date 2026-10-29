"""Carregamento dos contratos e validação estrutural dos CSVs de entrada."""

from __future__ import annotations

import csv
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml


class ContractDefinitionError(ValueError):
    """Indica que o índice ou um contrato está inconsistente."""


def _matches_logical_type(value: str, logical_type: str) -> bool:
    """Confere o tipo sem converter nem substituir o valor de origem."""
    try:
        if logical_type == "string":
            return True
        if logical_type == "integer":
            int(value)
            return True
        if logical_type == "decimal":
            return Decimal(value).is_finite()
        if logical_type == "timestamp_without_timezone":
            pattern = r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?"
            if re.fullmatch(pattern, value) is None:
                return False
            return datetime.fromisoformat(value).tzinfo is None
    except (InvalidOperation, ValueError):
        return False
    return False


def load_contracts(index_path: Path) -> tuple[Path, list[dict[str, Any]]]:
    """Carrega os oito contratos listados no índice e retorna a pasta de origem."""
    index_path = index_path.resolve()
    try:
        index = yaml.safe_load(index_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ContractDefinitionError(
            f"Não foi possível ler {index_path}: {exc}"
        ) from exc

    contract_files = index.get("contract_files") if isinstance(index, dict) else None
    included_files = index.get("included_files") if isinstance(index, dict) else None
    if not isinstance(contract_files, list) or not isinstance(included_files, list):
        raise ContractDefinitionError(
            "O índice precisa declarar contract_files e included_files como listas."
        )
    if len(contract_files) != 8 or len(included_files) != 8:
        raise ContractDefinitionError(
            "O MVP deve declarar exatamente oito arquivos e oito contratos."
        )
    if any(
        not isinstance(name, str) or Path(name).name != name
        for name in contract_files + included_files
    ):
        raise ContractDefinitionError(
            "O índice deve listar nomes de arquivos sem caminhos."
        )
    if len(set(contract_files)) != 8 or len(set(included_files)) != 8:
        raise ContractDefinitionError(
            "O índice não pode repetir arquivos ou contratos."
        )
    if set(included_files) != {
        f.removesuffix(".yaml") + ".csv" for f in contract_files
    }:
        raise ContractDefinitionError(
            "Os CSVs incluídos no índice não correspondem aos arquivos de contrato."
        )
    if "olist_geolocation_dataset.csv" in included_files:
        raise ContractDefinitionError(
            "Geolocalização está fora do MVP e não pode estar incluída."
        )

    contracts: list[dict[str, Any]] = []
    source_files: set[str] = set()
    for contract_file in contract_files:
        if Path(contract_file).name != contract_file:
            raise ContractDefinitionError(
                "contract_files deve conter apenas nomes de arquivos."
            )
        path = index_path.parent / contract_file
        try:
            contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            raise ContractDefinitionError(
                f"Não foi possível ler {path}: {exc}"
            ) from exc
        if not isinstance(contract, dict):
            raise ContractDefinitionError(f"Contrato inválido: {path}")
        source_file = contract.get("source_file")
        columns = contract.get("columns")
        key = contract.get("primary_key")
        if (
            not isinstance(source_file, str)
            or Path(source_file).name != source_file
            or not isinstance(columns, dict)
            or not isinstance(key, list)
        ):
            raise ContractDefinitionError(
                f"Contrato sem source_file, columns ou primary_key válidos: {path}"
            )
        if source_file not in included_files:
            raise ContractDefinitionError(
                f"{source_file} não está declarado em included_files."
            )
        if source_file in source_files:
            raise ContractDefinitionError(f"Há mais de um contrato para {source_file}.")
        if not key or len(set(key)) != len(key) or not set(key).issubset(columns):
            raise ContractDefinitionError(
                f"A chave de {source_file} contém colunas não contratadas."
            )
        for name, definition in columns.items():
            if (
                not isinstance(definition, dict)
                or not isinstance(definition.get("nullable"), bool)
                or definition.get("logical_type")
                not in {"string", "integer", "decimal", "timestamp_without_timezone"}
            ):
                raise ContractDefinitionError(
                    f"Definição inválida para {source_file}.{name}."
                )
        if any(columns[name]["nullable"] for name in key):
            raise ContractDefinitionError(
                f"A chave de {source_file} não pode conter campos anuláveis."
            )
        source_files.add(source_file)
        contracts.append(contract)

    if source_files != set(included_files):
        raise ContractDefinitionError(
            "Os contratos não cobrem exatamente os oito CSVs incluídos."
        )

    source_dir = index.get("source_directory")
    if not isinstance(source_dir, str):
        raise ContractDefinitionError("O índice precisa declarar source_directory.")
    return (index_path.parent / source_dir).resolve(), contracts


def validate_sources(raw_dir: Path, contracts: list[dict[str, Any]]) -> dict[str, Any]:
    """Valida arquivos, colunas, nulos e unicidade sem expor valores de linhas."""
    file_results: list[dict[str, Any]] = []
    for contract in contracts:
        source_file = contract["source_file"]
        path = raw_dir / source_file
        columns = contract["columns"]
        key = contract["primary_key"]
        result: dict[str, Any] = {
            "source_file": source_file,
            "rows_read": 0,
            "source_columns": [],
            "extra_columns": [],
            "null_counts": {},
            "issues": [],
        }
        if not path.is_file():
            result["issues"].append(
                {"rule": "file_exists", "message": "Arquivo obrigatório ausente."}
            )
            file_results.append(result)
            continue

        try:
            with path.open("r", encoding="utf-8-sig", newline="") as stream:
                reader = csv.reader(stream)
                header = next(reader, None)
                if header is None:
                    result["issues"].append(
                        {
                            "rule": "header_present",
                            "message": "Arquivo vazio, sem cabeçalho.",
                        }
                    )
                    file_results.append(result)
                    continue
                result["source_columns"] = header
                duplicate_headers = sorted(
                    {name for name in header if header.count(name) > 1}
                )
                if duplicate_headers:
                    result["issues"].append(
                        {"rule": "unique_headers", "columns": duplicate_headers}
                    )
                missing = sorted(set(columns) - set(header))
                result["extra_columns"] = [
                    name for name in header if name not in columns
                ]
                if missing:
                    result["issues"].append(
                        {"rule": "required_columns", "missing_columns": missing}
                    )
                if any(
                    name.startswith("_")
                    and name in {"_batch_id", "_ingested_at", "_source_file"}
                    for name in header
                ):
                    result["issues"].append(
                        {
                            "rule": "reserved_metadata_names",
                            "message": "A origem usa nome reservado para metadados Bronze.",
                        }
                    )

                positions = {name: index for index, name in enumerate(header)}
                seen_keys: set[tuple[str, ...]] = set()
                null_rows: dict[str, list[int]] = {}
                invalid_type_rows: dict[str, list[int]] = {}
                duplicate_key_rows: list[int] = []
                malformed_rows: list[int] = []
                for row_number, row in enumerate(reader, start=2):
                    result["rows_read"] += 1
                    if len(row) != len(header):
                        malformed_rows.append(row_number)
                        continue
                    for name, definition in columns.items():
                        if name not in positions:
                            continue
                        value = row[positions[name]]
                        if value == "":
                            result["null_counts"][name] = (
                                result["null_counts"].get(name, 0) + 1
                            )
                            if not definition["nullable"]:
                                null_rows.setdefault(name, []).append(row_number)
                        elif not _matches_logical_type(
                            value, definition["logical_type"]
                        ):
                            invalid_type_rows.setdefault(name, []).append(row_number)
                    if all(name in positions for name in key):
                        key_values = tuple(row[positions[name]] for name in key)
                        if any(value == "" for value in key_values):
                            continue
                        if key_values in seen_keys:
                            duplicate_key_rows.append(row_number)
                        else:
                            seen_keys.add(key_values)

                for name, rows in sorted(null_rows.items()):
                    result["issues"].append(
                        {
                            "rule": "not_null",
                            "column": name,
                            "count": len(rows),
                            "sample_row_numbers": rows[:10],
                        }
                    )
                for name, rows in sorted(invalid_type_rows.items()):
                    result["issues"].append(
                        {
                            "rule": "logical_type",
                            "column": name,
                            "type": columns[name]["logical_type"],
                            "count": len(rows),
                            "sample_row_numbers": rows[:10],
                        }
                    )
                if duplicate_key_rows:
                    result["issues"].append(
                        {
                            "rule": "unique_key",
                            "columns": key,
                            "count": len(duplicate_key_rows),
                            "sample_row_numbers": duplicate_key_rows[:10],
                        }
                    )
                if malformed_rows:
                    result["issues"].append(
                        {
                            "rule": "row_width",
                            "count": len(malformed_rows),
                            "sample_row_numbers": malformed_rows[:10],
                        }
                    )
        except (OSError, UnicodeError, csv.Error) as exc:
            result["issues"].append({"rule": "csv_readable", "message": str(exc)})
        file_results.append(result)

    issue_count = sum(len(result["issues"]) for result in file_results)
    error_count = sum(
        issue.get("count", 1) for result in file_results for issue in result["issues"]
    )
    return {
        "passed": issue_count == 0,
        "files": file_results,
        "issue_count": issue_count,
        "error_count": error_count,
    }
