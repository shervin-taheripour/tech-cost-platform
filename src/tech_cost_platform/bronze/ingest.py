"""CSV-to-Delta bronze ingestion."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import yaml
from pydantic import BaseModel, ConfigDict, ValidationError as PydanticValidationError

from ..delta_tables import build_arrow_table, write_delta_table
from ..runtime import repo_root, resolve_repo_path
from .contracts import (
    ApplicationContract,
    BronzeContract,
    BusinessUnitContract,
    CostCenterContract,
    GLCostContract,
    ResourceTowerContract,
    UsageMetricContract,
)
from .schema import (
    APPLICATION_SCHEMA,
    BUSINESS_UNIT_SCHEMA,
    COST_CENTER_SCHEMA,
    GL_COST_SCHEMA,
    RESOURCE_TOWER_SCHEMA,
    USAGE_METRIC_SCHEMA,
)

class BronzeConfig(BaseModel):
    """Runtime config for bronze ingestion."""

    model_config = ConfigDict(frozen=True)

    source_dir: str = "data/source"
    bronze_dir: str = "data/bronze"


class BronzeRuntimeConfig(BaseModel):
    """Minimal config required by the bronze stage."""

    bronze: BronzeConfig = BronzeConfig()


class BronzeValidationError(ValueError):
    """Raised when a source file fails the ingestion contract."""


@dataclass(frozen=True)
class TableSpec:
    table_name: str
    filename: str
    contract_model: type[BronzeContract]
    arrow_schema: pa.Schema

    @property
    def source_columns(self) -> list[str]:
        return list(self.contract_model.model_fields)

    @property
    def storage_schema(self) -> pa.Schema:
        return self.arrow_schema.append(pa.field("_source_file", pa.string()))


TABLE_SPECS = (
    TableSpec("gl_costs", "gl_costs.csv", GLCostContract, GL_COST_SCHEMA),
    TableSpec("cost_centers", "cost_centers.csv", CostCenterContract, COST_CENTER_SCHEMA),
    TableSpec("resource_towers", "resource_towers.csv", ResourceTowerContract, RESOURCE_TOWER_SCHEMA),
    TableSpec("applications", "applications.csv", ApplicationContract, APPLICATION_SCHEMA),
    TableSpec("business_units", "business_units.csv", BusinessUnitContract, BUSINESS_UNIT_SCHEMA),
    TableSpec("usage_metrics", "usage_metrics.csv", UsageMetricContract, USAGE_METRIC_SCHEMA),
)
TABLE_SPEC_BY_NAME = {spec.table_name: spec for spec in TABLE_SPECS}


def load_bronze_config(config_path: Path | None = None) -> BronzeRuntimeConfig:
    """Load the bronze runtime config from config.yaml."""
    resolved_path = config_path or repo_root() / "config.yaml"
    with resolved_path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    return BronzeRuntimeConfig.model_validate(raw_config)


def resolve_directory(path_value: str | Path) -> Path:
    """Resolve repo-relative paths into absolute paths."""
    return resolve_repo_path(path_value)


def validate_csv_header(source_path: Path, expected_columns: list[str]) -> None:
    """Reject files whose header does not match the contract exactly."""
    with source_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise BronzeValidationError(f"{source_path.name} is empty.") from exc

    if header != expected_columns:
        raise BronzeValidationError(
            f"{source_path.name} header mismatch. Expected {expected_columns}, got {header}."
        )


def _normalize_raw_payload(raw_payload: Mapping[str, str]) -> dict[str, object]:
    """Convert blank CSV cells to None before contract validation."""
    return {
        column_name: (None if value == "" else value)
        for column_name, value in raw_payload.items()
    }


def read_validated_rows(source_path: Path, spec: TableSpec) -> list[dict[str, object]]:
    """Read and validate source rows against the table contract."""
    errors: list[str] = []
    validated_rows: list[dict[str, object]] = []

    with source_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_number, raw_row in enumerate(reader, start=2):
            try:
                row = spec.contract_model.model_validate(_normalize_raw_payload(raw_row))
            except PydanticValidationError as exc:
                errors.append(f"{source_path.name}:{row_number}: {exc}")
                continue
            payload = row.model_dump(mode="python")
            payload["_source_file"] = source_path.name
            validated_rows.append(payload)

    if errors:
        joined_errors = "\n".join(errors[:5])
        raise BronzeValidationError(
            f"Bronze validation failed for {spec.table_name}.\n{joined_errors}"
        )

    return validated_rows


def prepare_bronze_tables(
    *,
    source_dir: Path,
    source_overrides: Mapping[str, Path] | None = None,
) -> dict[str, pa.Table]:
    """Read and validate every source table before any Delta write happens."""
    prepared: dict[str, pa.Table] = {}

    for spec in TABLE_SPECS:
        override_path = source_overrides.get(spec.table_name) if source_overrides else None
        source_path = Path(override_path) if override_path else source_dir / spec.filename
        if not source_path.exists():
            raise FileNotFoundError(f"Expected source file for {spec.table_name}: {source_path}")

        validate_csv_header(source_path, spec.source_columns)
        validated_rows = read_validated_rows(source_path, spec)
        prepared[spec.table_name] = build_arrow_table(validated_rows, spec.storage_schema)

    return prepared


def write_bronze_tables(prepared_tables: Mapping[str, pa.Table], bronze_dir: Path) -> dict[str, Path]:
    """Write validated bronze tables to Delta."""
    bronze_dir.mkdir(parents=True, exist_ok=True)
    output_paths: dict[str, Path] = {}

    for spec in TABLE_SPECS:
        output_path = bronze_dir / spec.table_name
        output_paths[spec.table_name] = write_delta_table(
            prepared_tables[spec.table_name],
            output_path,
            sort_columns=[*spec.source_columns],
        )

    return output_paths


def ingest_bronze_sources(
    *,
    config_path: Path | None = None,
    source_dir: str | Path | None = None,
    bronze_dir: str | Path | None = None,
    source_overrides: Mapping[str, Path] | None = None,
) -> dict[str, Path]:
    """Run the full bronze ingest from CSV sources into Delta tables."""
    config = load_bronze_config(config_path)
    resolved_source_dir = resolve_directory(source_dir or config.bronze.source_dir)
    resolved_bronze_dir = resolve_directory(bronze_dir or config.bronze.bronze_dir)
    prepared_tables = prepare_bronze_tables(
        source_dir=resolved_source_dir,
        source_overrides=source_overrides,
    )
    return write_bronze_tables(prepared_tables, resolved_bronze_dir)


def parse_args() -> argparse.Namespace:
    """Parse CLI args for the bronze stage."""
    parser = argparse.ArgumentParser(description="Ingest synthetic CSV sources into bronze Delta tables.")
    parser.add_argument("--config", type=Path, help="Optional path to config.yaml.")
    parser.add_argument("--source-dir", type=Path, help="Optional source CSV directory override.")
    parser.add_argument("--bronze-dir", type=Path, help="Optional bronze Delta directory override.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint for bronze ingest."""
    args = parse_args()
    output_paths = ingest_bronze_sources(
        config_path=args.config,
        source_dir=args.source_dir,
        bronze_dir=args.bronze_dir,
    )
    print(f"[tech-cost-platform] bronze tables_written={len(output_paths)}")
    for spec in TABLE_SPECS:
        print(f"[tech-cost-platform] bronze table={spec.table_name} path={output_paths[spec.table_name]}")
    return 0
