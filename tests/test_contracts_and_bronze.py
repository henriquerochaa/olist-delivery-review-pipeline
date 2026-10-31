from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest
import yaml

from projeto1_pipeline_qualidade_dados.bronze import IngestionBlocked, ingest_batch
from projeto1_pipeline_qualidade_dados.contracts import (
    ContractDefinitionError,
    load_contracts,
    validate_sources,
)

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


# --- Regras de validação adicionais -------------------------------------------------


def _contract_for(contracts: list[dict], source_file: str) -> dict:
    return next(item for item in contracts if item["source_file"] == source_file)


def _read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        return list(reader.fieldnames or []), list(reader)


def _write_rows(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _rules(result: dict) -> set[str]:
    return {issue["rule"] for issue in result["files"][0]["issues"]}


def test_real_contract_index_loads_eight_contracts() -> None:
    _, contracts = load_contracts(INDEX_PATH)

    assert len(contracts) == 8
    assert "olist_geolocation_dataset.csv" not in {
        contract["source_file"] for contract in contracts
    }


def test_duplicate_primary_key_blocks_validation(tmp_path: Path) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    contract = _contract_for(contracts, "olist_orders_dataset.csv")
    source = raw_dir / contract["source_file"]
    header, rows = _read_rows(source)
    _write_rows(source, header, [rows[0], dict(rows[0])])

    result = validate_sources(raw_dir, [contract])

    assert result["passed"] is False
    assert "unique_key" in _rules(result)
    issue = next(i for i in result["files"][0]["issues"] if i["rule"] == "unique_key")
    assert issue["count"] == 1


def test_composite_key_allows_repeated_part_but_not_repeated_pair(
    tmp_path: Path,
) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    contract = _contract_for(contracts, "olist_order_items_dataset.csv")
    assert len(contract["primary_key"]) > 1
    source = raw_dir / contract["source_file"]
    header, rows = _read_rows(source)
    other_item = dict(rows[0])
    other_item["order_item_id"] = "2"
    _write_rows(source, header, [rows[0], other_item])
    assert validate_sources(raw_dir, [contract])["passed"] is True

    _write_rows(source, header, [rows[0], dict(rows[0])])
    assert "unique_key" in _rules(validate_sources(raw_dir, [contract]))


@pytest.mark.parametrize(
    ("source_file", "column", "bad_value"),
    [
        ("olist_order_items_dataset.csv", "order_item_id", "abc"),
        ("olist_order_items_dataset.csv", "price", "NaN"),
        ("olist_orders_dataset.csv", "order_purchase_timestamp", "2017-13-45 99:00:00"),
        ("olist_orders_dataset.csv", "order_purchase_timestamp", "02/01/2017"),
        (
            "olist_orders_dataset.csv",
            "order_purchase_timestamp",
            "2017-01-02 03:04:05+00:00",
        ),
    ],
)
def test_invalid_logical_type_is_reported(
    tmp_path: Path, source_file: str, column: str, bad_value: str
) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    contract = _contract_for(contracts, source_file)
    source = raw_dir / source_file
    header, rows = _read_rows(source)
    rows[0][column] = bad_value
    _write_rows(source, header, rows)

    result = validate_sources(raw_dir, [contract])

    issues = [i for i in result["files"][0]["issues"] if i["rule"] == "logical_type"]
    assert result["passed"] is False
    assert [issue["column"] for issue in issues] == [column]
    assert issues[0]["sample_row_numbers"] == [2]
    assert bad_value not in json.dumps(result)


def test_missing_source_file_blocks_validation_and_ingestion(tmp_path: Path) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    contract = _contract_for(contracts, "olist_sellers_dataset.csv")
    (raw_dir / contract["source_file"]).unlink()

    result = validate_sources(raw_dir, [contract])
    assert result["passed"] is False
    assert _rules(result) == {"file_exists"}

    output_dir = tmp_path / "bronze"
    with pytest.raises(IngestionBlocked):
        ingest_batch(
            index_path=tmp_path / "contracts" / "olist_mvp.yaml",
            output_dir=output_dir,
            batch_id="missing-file",
        )
    assert not (output_dir / "missing-file").exists()
    assert (output_dir / "_reports" / "missing-file.json").is_file()


def test_malformed_row_width_is_reported(tmp_path: Path) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    contract = _contract_for(contracts, "olist_customers_dataset.csv")
    source = raw_dir / contract["source_file"]
    with source.open("a", encoding="utf-8", newline="") as stream:
        stream.write("only-one-field\n")

    result = validate_sources(raw_dir, [contract])

    issue = next(i for i in result["files"][0]["issues"] if i["rule"] == "row_width")
    assert result["passed"] is False
    assert issue["count"] == 1
    assert issue["sample_row_numbers"] == [3]


def test_empty_file_and_duplicate_headers_are_reported(tmp_path: Path) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    empty = _contract_for(contracts, "olist_sellers_dataset.csv")
    (raw_dir / empty["source_file"]).write_text("", encoding="utf-8")
    assert _rules(validate_sources(raw_dir, [empty])) == {"header_present"}

    duplicated = _contract_for(contracts, "olist_customers_dataset.csv")
    source = raw_dir / duplicated["source_file"]
    header, _ = _read_rows(source)
    source.write_text(
        ",".join([*header, header[0]]) + "\n" + ",".join(["x"] * (len(header) + 1)),
        encoding="utf-8",
    )
    assert "unique_headers" in _rules(validate_sources(raw_dir, [duplicated]))


def test_reserved_metadata_column_in_source_is_rejected(tmp_path: Path) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    contract = _contract_for(contracts, "olist_sellers_dataset.csv")
    source = raw_dir / contract["source_file"]
    header, rows = _read_rows(source)
    for row in rows:
        row["_batch_id"] = "from-source"
    _write_rows(source, [*header, "_batch_id"], rows)

    assert "reserved_metadata_names" in _rules(validate_sources(raw_dir, [contract]))


@pytest.mark.parametrize(
    "batch_id", ["", "../escape", "a/b", ".hidden", "with space", "x" * 101]
)
def test_unsafe_batch_id_is_rejected_before_any_write(
    tmp_path: Path, batch_id: str
) -> None:
    _write_contract_fixture(tmp_path)
    output_dir = tmp_path / "bronze"

    with pytest.raises(ValueError, match="batch_id"):
        ingest_batch(
            index_path=tmp_path / "contracts" / "olist_mvp.yaml",
            output_dir=output_dir,
            batch_id=batch_id,
        )

    assert not output_dir.exists()


def test_bronze_keeps_ids_as_text_adds_metadata_and_nulls_only_where_allowed(
    tmp_path: Path,
) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    contract = _contract_for(contracts, "olist_products_dataset.csv")
    source = raw_dir / contract["source_file"]
    header, rows = _read_rows(source)
    nullable_column = next(
        name for name in header if contract["columns"][name]["nullable"]
    )
    rows[0]["product_id"] = "00012345"
    rows[0][nullable_column] = ""
    _write_rows(source, header, rows)
    output_dir = tmp_path / "bronze"

    ingest_batch(
        index_path=tmp_path / "contracts" / "olist_mvp.yaml",
        output_dir=output_dir,
        batch_id="types-batch",
        ingested_at="2026-10-31T00:00:00Z",
    )
    frame = pd.read_parquet(
        output_dir / "types-batch" / "olist_products_dataset.parquet"
    )

    assert frame.loc[0, "product_id"] == "00012345"
    assert pd.isna(frame.loc[0, nullable_column])
    assert frame.loc[0, "_batch_id"] == "types-batch"
    assert frame.loc[0, "_ingested_at"] == "2026-10-31T00:00:00Z"
    assert frame.loc[0, "_source_file"] == "olist_products_dataset.csv"


def test_blocked_rerun_keeps_last_good_batch_and_records_the_failure(
    tmp_path: Path,
) -> None:
    raw_dir, contracts = _write_contract_fixture(tmp_path)
    index_path = tmp_path / "contracts" / "olist_mvp.yaml"
    output_dir = tmp_path / "bronze"
    ingest_batch(index_path=index_path, output_dir=output_dir, batch_id="stable")
    before = {
        path.name: path.read_bytes()
        for path in (output_dir / "stable").glob("*.parquet")
    }

    contract = _contract_for(contracts, "olist_orders_dataset.csv")
    source = raw_dir / contract["source_file"]
    header, rows = _read_rows(source)
    _write_rows(source, header, [rows[0], dict(rows[0])])
    with pytest.raises(IngestionBlocked):
        ingest_batch(index_path=index_path, output_dir=output_dir, batch_id="stable")

    after = {
        path.name: path.read_bytes()
        for path in (output_dir / "stable").glob("*.parquet")
    }
    report = json.loads(
        (output_dir / "_reports" / "stable.json").read_text(encoding="utf-8")
    )
    assert after == before
    assert report["status"] == "blocked"
    assert not list(output_dir.glob(".stable.*"))


def test_contract_index_rejects_geolocation_and_nullable_keys(tmp_path: Path) -> None:
    _write_contract_fixture(tmp_path)
    index_path = tmp_path / "contracts" / "olist_mvp.yaml"
    original = index_path.read_text(encoding="utf-8")

    index = yaml.safe_load(original)
    index["included_files"][0] = "olist_geolocation_dataset.csv"
    index_path.write_text(yaml.safe_dump(index), encoding="utf-8")
    with pytest.raises(ContractDefinitionError):
        load_contracts(index_path)

    index_path.write_text(original, encoding="utf-8")
    orders_path = tmp_path / "contracts" / "olist_orders_dataset.yaml"
    orders = yaml.safe_load(orders_path.read_text(encoding="utf-8"))
    orders["columns"]["order_id"]["nullable"] = True
    orders_path.write_text(yaml.safe_dump(orders), encoding="utf-8")
    with pytest.raises(ContractDefinitionError):
        load_contracts(index_path)
