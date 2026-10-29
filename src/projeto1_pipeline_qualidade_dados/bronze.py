"""Ingestão idempotente dos CSVs Olist para Parquet Bronze."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from projeto1_pipeline_qualidade_dados.contracts import (
    ContractDefinitionError,
    load_contracts,
    validate_sources,
)


class IngestionBlocked(RuntimeError):
    """Ingestão bloqueada por falhas de contrato."""


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def ingest_batch(
    *,
    index_path: Path,
    output_dir: Path,
    batch_id: str,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    """Valida os oito CSVs e substitui o diretório do mesmo lote."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", batch_id):
        raise ValueError(
            "batch_id deve ter 1–100 caracteres: letras, números, ponto, hífen ou sublinhado."
        )

    raw_dir, contracts = load_contracts(index_path)
    validation = validate_sources(raw_dir, contracts)
    timestamp = ingested_at or datetime.now(UTC).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    output_dir = output_dir.resolve()
    report: dict[str, Any] = {
        "batch_id": batch_id,
        "ingested_at": timestamp,
        "status": "blocked" if not validation["passed"] else "validated",
        "source_directory": str(raw_dir),
        "total_source_rows": sum(file["rows_read"] for file in validation["files"]),
        "files": validation["files"],
        "issue_count": validation["issue_count"],
        "error_count": validation["error_count"],
        "parquet_files": [],
    }
    if not validation["passed"]:
        _write_json(output_dir / "_reports" / f"{batch_id}.json", report)
        raise IngestionBlocked(
            f"Contrato bloqueou o lote {batch_id}; relatório salvo em _reports/{batch_id}.json"
        )

    stage_dir = output_dir / f".{batch_id}.staging-{uuid.uuid4().hex}"
    final_dir = output_dir / batch_id
    backup_dir = output_dir / f".{batch_id}.backup-{uuid.uuid4().hex}"
    output_dir.mkdir(parents=True, exist_ok=True)
    stage_dir.mkdir()
    try:
        for contract, file_result in zip(contracts, validation["files"], strict=True):
            source_file = contract["source_file"]
            frame = pd.read_csv(
                raw_dir / source_file,
                dtype="string",
                keep_default_na=False,
                na_filter=False,
                encoding="utf-8-sig",
            )
            expected_rows = file_result["rows_read"]
            if len(frame) != expected_rows:
                raise RuntimeError(
                    f"Contagem mudou entre validação e leitura: {source_file}"
                )

            # Campo vazio em coluna anulável representa nulo na origem; os demais
            # campos permanecem textuais para preservar IDs, datas e valores brutos.
            for column, definition in contract["columns"].items():
                if definition["nullable"]:
                    frame[column] = frame[column].mask(frame[column].eq(""), pd.NA)

            frame["_batch_id"] = batch_id
            frame["_ingested_at"] = timestamp
            frame["_source_file"] = source_file
            parquet_path = stage_dir / f"{Path(source_file).stem}.parquet"
            frame.to_parquet(parquet_path, index=False, engine="pyarrow")
            report["parquet_files"].append(f"{batch_id}/{parquet_path.name}")
            file_result["bronze_rows"] = len(frame)

        report["status"] = "succeeded"
        report["total_bronze_rows"] = sum(
            file["bronze_rows"] for file in report["files"]
        )
        report["issue_count"] = 0
        report["error_count"] = 0
        _write_json(stage_dir / "ingestion_report.json", report)

        if final_dir.exists():
            final_dir.replace(backup_dir)
        try:
            stage_dir.replace(final_dir)
        except OSError:
            if backup_dir.exists() and not final_dir.exists():
                backup_dir.replace(final_dir)
            raise
        if backup_dir.exists():
            shutil.rmtree(backup_dir, ignore_errors=True)
        return report
    except Exception as exc:
        if stage_dir.exists():
            shutil.rmtree(stage_dir, ignore_errors=True)
        report["status"] = "failed"
        report["error_count"] = report.get("error_count", 0) + 1
        report["execution_error"] = str(exc)
        _write_json(output_dir / "_reports" / f"{batch_id}.json", report)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validar contratos e ingerir CSVs Olist na Bronze."
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Identificador estável para reprocessamento idempotente.",
    )
    parser.add_argument(
        "--contracts", type=Path, default=Path("contracts/olist_mvp.yaml")
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/bronze"))
    args = parser.parse_args()
    try:
        report = ingest_batch(
            index_path=args.contracts,
            output_dir=args.output_dir,
            batch_id=args.batch_id,
        )
    except (
        ContractDefinitionError,
        IngestionBlocked,
        OSError,
        ValueError,
        RuntimeError,
    ) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "status": report["status"],
                "batch_id": report["batch_id"],
                "files": len(report["files"]),
                "total_bronze_rows": report["total_bronze_rows"],
                "report": str(
                    args.output_dir / args.batch_id / "ingestion_report.json"
                ),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
