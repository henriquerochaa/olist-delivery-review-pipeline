"""Carregamento dos contratos e validação estrutural dos CSVs de entrada."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

LOGICAL_TYPES = {"string", "integer", "decimal", "timestamp_without_timezone"}
# Nomes que a Bronze acrescenta; a origem não pode usá-los.
RESERVED_METADATA_COLUMNS = {"_batch_id", "_ingested_at", "_source_file"}
EXPECTED_FILE_COUNT = 8
SAMPLE_SIZE = 10  # linhas de exemplo por achado, para não inflar o relatório

Issue = dict[str, Any]


class ContractDefinitionError(ValueError):
    """Indica que o índice ou um contrato está inconsistente."""


# ---------------------------------------------------------------------------
# Tipos lógicos
# ---------------------------------------------------------------------------


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
            # O regex limita o formato; fromisoformat rejeita datas impossíveis.
            pattern = r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?"
            if re.fullmatch(pattern, value) is None:
                return False
            return datetime.fromisoformat(value).tzinfo is None
    except (InvalidOperation, ValueError):
        return False
    return False


# ---------------------------------------------------------------------------
# Definição dos contratos
# ---------------------------------------------------------------------------


def _read_yaml(path: Path) -> Any:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ContractDefinitionError(f"Não foi possível ler {path}: {exc}") from exc


def _is_plain_filename(name: object) -> bool:
    """Aceita só nomes de arquivo, sem pastas (barra `../` e caminhos absolutos)."""
    return isinstance(name, str) and Path(name).name == name


def _validate_index(index: object) -> tuple[list[str], list[str]]:
    """Valida o índice e devolve (arquivos de contrato, CSVs incluídos)."""
    contract_files = index.get("contract_files") if isinstance(index, dict) else None
    included_files = index.get("included_files") if isinstance(index, dict) else None
    if not isinstance(contract_files, list) or not isinstance(included_files, list):
        raise ContractDefinitionError(
            "O índice precisa declarar contract_files e included_files como listas."
        )
    if (
        len(contract_files) != EXPECTED_FILE_COUNT
        or len(included_files) != EXPECTED_FILE_COUNT
    ):
        raise ContractDefinitionError(
            "O MVP deve declarar exatamente oito arquivos e oito contratos."
        )
    if not all(_is_plain_filename(name) for name in contract_files + included_files):
        raise ContractDefinitionError(
            "O índice deve listar nomes de arquivos sem caminhos."
        )
    if (
        len(set(contract_files)) != EXPECTED_FILE_COUNT
        or len(set(included_files)) != EXPECTED_FILE_COUNT
    ):
        raise ContractDefinitionError(
            "O índice não pode repetir arquivos ou contratos."
        )
    expected_csvs = {f.removesuffix(".yaml") + ".csv" for f in contract_files}
    if set(included_files) != expected_csvs:
        raise ContractDefinitionError(
            "Os CSVs incluídos no índice não correspondem aos arquivos de contrato."
        )
    if "olist_geolocation_dataset.csv" in included_files:
        raise ContractDefinitionError(
            "Geolocalização está fora do MVP e não pode estar incluída."
        )
    return contract_files, included_files


def _check_key_columns(key: list[str], columns: dict[str, Any], source: str) -> None:
    if not key or len(set(key)) != len(key) or not set(key).issubset(columns):
        raise ContractDefinitionError(
            f"A chave de {source} contém colunas não contratadas."
        )


def _check_column_definitions(columns: dict[str, Any], source: str) -> None:
    for name, definition in columns.items():
        if (
            not isinstance(definition, dict)
            or not isinstance(definition.get("nullable"), bool)
            or definition.get("logical_type") not in LOGICAL_TYPES
        ):
            raise ContractDefinitionError(f"Definição inválida para {source}.{name}.")


def _check_key_not_nullable(
    key: list[str], columns: dict[str, Any], source: str
) -> None:
    if any(columns[name]["nullable"] for name in key):
        raise ContractDefinitionError(
            f"A chave de {source} não pode conter campos anuláveis."
        )


def _validate_contract(
    contract: object,
    path: Path,
    included_files: list[str],
    already_seen: set[str],
) -> dict[str, Any]:
    """Valida um contrato individual e o devolve já tipado como dict."""
    if not isinstance(contract, dict):
        raise ContractDefinitionError(f"Contrato inválido: {path}")
    source_file = contract.get("source_file")
    columns = contract.get("columns")
    key = contract.get("primary_key")
    if (
        not isinstance(source_file, str)
        or not _is_plain_filename(source_file)
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
    if source_file in already_seen:
        raise ContractDefinitionError(f"Há mais de um contrato para {source_file}.")
    _check_key_columns(key, columns, source_file)
    _check_column_definitions(columns, source_file)
    _check_key_not_nullable(key, columns, source_file)
    return contract


def load_contracts(index_path: Path) -> tuple[Path, list[dict[str, Any]]]:
    """Carrega os oito contratos listados no índice e retorna a pasta de origem."""
    index_path = index_path.resolve()
    index = _read_yaml(index_path)
    contract_files, included_files = _validate_index(index)

    contracts: list[dict[str, Any]] = []
    source_files: set[str] = set()
    for contract_file in contract_files:
        path = index_path.parent / contract_file
        contract = _validate_contract(
            _read_yaml(path), path, included_files, source_files
        )
        source_files.add(contract["source_file"])
        contracts.append(contract)

    if source_files != set(included_files):
        raise ContractDefinitionError(
            "Os contratos não cobrem exatamente os oito CSVs incluídos."
        )

    source_dir = index.get("source_directory")
    if not isinstance(source_dir, str):
        raise ContractDefinitionError("O índice precisa declarar source_directory.")
    return (index_path.parent / source_dir).resolve(), contracts


# ---------------------------------------------------------------------------
# Validação do conteúdo dos CSVs
# ---------------------------------------------------------------------------


@dataclass
class _RowFindings:
    """Achados acumulados enquanto as linhas são lidas (números de linha do arquivo)."""

    rows_read: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)
    null_rows: dict[str, list[int]] = field(default_factory=dict)
    invalid_type_rows: dict[str, list[int]] = field(default_factory=dict)
    duplicate_key_rows: list[int] = field(default_factory=list)
    malformed_rows: list[int] = field(default_factory=list)


def _header_issues(header: list[str], columns: dict[str, Any]) -> list[Issue]:
    issues: list[Issue] = []
    duplicates = sorted({name for name in header if header.count(name) > 1})
    if duplicates:
        issues.append({"rule": "unique_headers", "columns": duplicates})
    missing = sorted(set(columns) - set(header))
    if missing:
        issues.append({"rule": "required_columns", "missing_columns": missing})
    if RESERVED_METADATA_COLUMNS & set(header):
        issues.append(
            {
                "rule": "reserved_metadata_names",
                "message": "A origem usa nome reservado para metadados Bronze.",
            }
        )
    return issues


def _check_row_values(
    row: list[str],
    row_number: int,
    positions: dict[str, int],
    columns: dict[str, Any],
    findings: _RowFindings,
) -> None:
    """Registra campos vazios e valores que não respeitam o tipo lógico."""
    for name, definition in columns.items():
        if name not in positions:
            continue
        value = row[positions[name]]
        if value == "":
            findings.null_counts[name] = findings.null_counts.get(name, 0) + 1
            if not definition["nullable"]:
                findings.null_rows.setdefault(name, []).append(row_number)
        elif not _matches_logical_type(value, definition["logical_type"]):
            findings.invalid_type_rows.setdefault(name, []).append(row_number)


def _check_row_key(
    row: list[str],
    row_number: int,
    positions: dict[str, int],
    key: list[str],
    seen_keys: set[tuple[str, ...]],
    findings: _RowFindings,
) -> None:
    """Detecta chave repetida; chave com campo vazio já é achado de not_null."""
    if not all(name in positions for name in key):
        return
    key_values = tuple(row[positions[name]] for name in key)
    if any(value == "" for value in key_values):
        return
    if key_values in seen_keys:
        findings.duplicate_key_rows.append(row_number)
    else:
        seen_keys.add(key_values)


def _scan_rows(
    reader: Any,
    header: list[str],
    contract: dict[str, Any],
    findings: _RowFindings,
) -> None:
    positions = {name: index for index, name in enumerate(header)}
    seen_keys: set[tuple[str, ...]] = set()
    # start=2: a linha 1 do arquivo é o cabeçalho.
    for row_number, row in enumerate(reader, start=2):
        findings.rows_read += 1
        if len(row) != len(header):
            findings.malformed_rows.append(row_number)
            continue
        _check_row_values(row, row_number, positions, contract["columns"], findings)
        _check_row_key(
            row, row_number, positions, contract["primary_key"], seen_keys, findings
        )


def _row_issues(findings: _RowFindings, contract: dict[str, Any]) -> list[Issue]:
    """Transforma os achados acumulados em itens do relatório."""
    columns = contract["columns"]
    issues: list[Issue] = []
    for name, rows in sorted(findings.null_rows.items()):
        issues.append(
            {
                "rule": "not_null",
                "column": name,
                "count": len(rows),
                "sample_row_numbers": rows[:SAMPLE_SIZE],
            }
        )
    for name, rows in sorted(findings.invalid_type_rows.items()):
        issues.append(
            {
                "rule": "logical_type",
                "column": name,
                "type": columns[name]["logical_type"],
                "count": len(rows),
                "sample_row_numbers": rows[:SAMPLE_SIZE],
            }
        )
    if findings.duplicate_key_rows:
        issues.append(
            {
                "rule": "unique_key",
                "columns": contract["primary_key"],
                "count": len(findings.duplicate_key_rows),
                "sample_row_numbers": findings.duplicate_key_rows[:SAMPLE_SIZE],
            }
        )
    if findings.malformed_rows:
        issues.append(
            {
                "rule": "row_width",
                "count": len(findings.malformed_rows),
                "sample_row_numbers": findings.malformed_rows[:SAMPLE_SIZE],
            }
        )
    return issues


def _validate_file(raw_dir: Path, contract: dict[str, Any]) -> dict[str, Any]:
    """Valida um CSV contra o seu contrato, sem parar no primeiro problema."""
    source_file = contract["source_file"]
    path = raw_dir / source_file
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
        return result

    findings = _RowFindings()
    try:
        # utf-8-sig descarta o BOM do Excel; newline="" deixa o módulo csv
        # tratar quebras de linha dentro de campos entre aspas.
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
                return result
            result["source_columns"] = header
            result["extra_columns"] = [
                name for name in header if name not in contract["columns"]
            ]
            result["issues"].extend(_header_issues(header, contract["columns"]))
            _scan_rows(reader, header, contract, findings)
            result["issues"].extend(_row_issues(findings, contract))
    except (OSError, UnicodeError, csv.Error) as exc:
        result["issues"].append({"rule": "csv_readable", "message": str(exc)})
    result["rows_read"] = findings.rows_read
    result["null_counts"] = findings.null_counts
    return result


def validate_sources(raw_dir: Path, contracts: list[dict[str, Any]]) -> dict[str, Any]:
    """Valida arquivos, colunas, nulos e unicidade sem expor valores de linhas."""
    file_results = [_validate_file(raw_dir, contract) for contract in contracts]
    # issue_count conta regras violadas; error_count conta ocorrências.
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
