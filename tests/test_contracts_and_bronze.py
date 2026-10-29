from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest
import yaml

from projeto1_pipeline_qualidade_dados.bronze import IngestionBlocked, ingest_batch
from projeto1_pipeline_qualidade_dados.contracts import load_contracts, validate_sources

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INDEX_PATH = PROJECT_ROOT / "contracts" / "olist_mvp.yaml"


def _write_contract_fixture(root: Path) -> tuple[Path, list[dict]]:
    raw_dir = root / "raw"
    contract_dir = root / "contracts"
    raw_dir.mkdir(parents=True)
    contract_dir.mkdir()
    index = yaml.safe_load(INDEX_PATH.read_text(encoding="utf-8"))
    for filename in index["contract_files"]:
        shutil.copy2(INDEX_PATH.parent / filename, contract_dir / filename)
    index["source_directory"] = "../raw"
    index_path = contract_dir / "olist_mvp.yaml"
    index_path.write_text(yaml.safe_dump(index, sort_keys=False), encoding="utf-8")
    loaded_raw, contracts = load_contracts(index_path)
    _write_csvs(loaded_raw, contracts)
    return loaded_raw, contracts


def _valid_value(logical_type: str, column: str, row_number: int) -> str:
    if logical_type == "integer":
        return str(row_number)
    if logical_type == "decimal":
        return "12.50"
    if logical_type == "timestamp_without_timezone":
        return "2017-01-02 03:04:05"
    return f"{column}-{row_number}"


def _write_csvs(
    raw_dir: Path,
    contracts: list[dict],
    rows: int = 1,
    extra_source_file: str | None = None,
) -> None:
    for contract in contracts:
        columns = contract["columns"]
        keys = set(contract["primary_key"])
        header = list(columns)
        has_extra = contract["source_file"] == extra_source_file
        if has_extra:
            header.append("upstream_extension")
        with (raw_dir / contract["source_file"]).open(
            "w", encoding="utf-8", newline=""
        ) as stream:
            writer = csv.DictWriter(stream, fieldnames=header)
            writer.writeheader()
            for row_number in range(1, rows + 1):
                row = {
                    name: _valid_value(definition["logical_type"], name, row_number)
                    for name, definition in columns.items()
                }
                for key in keys:
                    row[key] = _valid_value(
                        columns[key]["logical_type"], key, row_number
                    )
                if has_extra:
                    row["upstream_extension"] = f"kept-{row_number}"
                writer.writerow(row)


def test_extra_columns_are_accepted_and_reported(tmp_path: Path) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    contract = contracts[0]
    source = raw_dir / contract["source_file"]
    with source.open("a", encoding="utf-8") as stream:
        stream.write("\n")
    frame = pd.read_csv(source, dtype="string", keep_default_na=False)
    frame["new_source_field"] = "extra"
    frame.to_csv(source, index=False)

    result = validate_sources(raw_dir, [contract])

    assert result["passed"] is True
    assert result["files"][0]["extra_columns"] == ["new_source_field"]


def test_missing_required_column_and_non_nullable_null_block(tmp_path: Path) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    contract = contracts[0]
    source = raw_dir / contract["source_file"]
    columns = list(contract["columns"])
    columns.remove(contract["primary_key"][0])
    with source.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerow([""] * len(columns))

    result = validate_sources(raw_dir, [contract])

    rules = {issue["rule"] for issue in result["files"][0]["issues"]}
    assert result["passed"] is False
    assert "required_columns" in rules
    assert "not_null" in rules


def test_ingestion_rerun_replaces_same_batch_without_duplicates(tmp_path: Path) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    _write_csvs(
        raw_dir,
        contracts,
        rows=2,
        extra_source_file=contracts[0]["source_file"],
    )
    index_path = tmp_path / "contracts" / "olist_mvp.yaml"
    output_dir = tmp_path / "bronze"

    first = ingest_batch(
        index_path=index_path, output_dir=output_dir, batch_id="fixture-batch"
    )
    (output_dir / "fixture-batch" / "stale.marker").touch()
    second = ingest_batch(
        index_path=index_path, output_dir=output_dir, batch_id="fixture-batch"
    )

    final_dir = output_dir / "fixture-batch"
    report = json.loads(
        (final_dir / "ingestion_report.json").read_text(encoding="utf-8")
    )
    parquet_files = list(final_dir.glob("*.parquet"))
    assert first["status"] == second["status"] == "succeeded"
    assert len(parquet_files) == 8
    assert not (final_dir / "stale.marker").exists()
    assert report["total_bronze_rows"] == 16
    assert report["issue_count"] == report["error_count"] == 0
    assert all(len(pd.read_parquet(path)) == 2 for path in parquet_files)
    extra_parquet = final_dir / f"{Path(contracts[0]['source_file']).stem}.parquet"
    assert pd.read_parquet(extra_parquet)["upstream_extension"].tolist() == [
        "kept-1",
        "kept-2",
    ]


def test_invalid_input_blocks_parquet_write_and_saves_report(tmp_path: Path) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    contract = contracts[0]
    source = raw_dir / contract["source_file"]
    with source.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    rows[0][contract["primary_key"][0]] = ""
    with source.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(contract["columns"]))
        writer.writeheader()
        writer.writerows(rows)

    output_dir = tmp_path / "bronze"
    with pytest.raises(IngestionBlocked):
        ingest_batch(
            index_path=tmp_path / "contracts" / "olist_mvp.yaml",
            output_dir=output_dir,
            batch_id="invalid-fixture",
        )

    assert not (output_dir / "invalid-fixture").exists()
    report = json.loads(
        (output_dir / "_reports" / "invalid-fixture.json").read_text(encoding="utf-8")
    )
    assert report["status"] == "blocked"
    assert report["error_count"] > 0
