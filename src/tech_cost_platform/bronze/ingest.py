"""CSV-to-Delta bronze ingestion."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError as PydanticValidationError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..spark import build_spark_session, repo_root
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


class SparkConfig(BaseModel):
    """Spark settings reused for bronze ingest."""

    app_name: str = "tech-cost-platform"
    master: str = "local[*]"


class BronzeConfig(BaseModel):
    """Runtime config for bronze ingestion."""

    model_config = ConfigDict(frozen=True)

    source_dir: str = "data/source"
    bronze_dir: str = "data/bronze"


class BronzeRuntimeConfig(BaseModel):
    """Minimal config required by the bronze stage."""

    spark: SparkConfig = SparkConfig()
    bronze: BronzeConfig = BronzeConfig()


class BronzeValidationError(ValueError):
    """Raised when a source file fails the ingestion contract."""


@dataclass(frozen=True)
class TableSpec:
    table_name: str
    filename: str
    contract_model: type[BronzeContract]
    spark_schema: object

    @property
    def source_columns(self) -> list[str]:
        return list(self.contract_model.model_fields)


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
    path = Path(path_value)
    return path if path.is_absolute() else repo_root() / path


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


def read_source_dataframe(spark: SparkSession, source_path: Path, spec: TableSpec) -> DataFrame:
    """Read a CSV source file with an explicit Spark schema."""
    return (
        spark.read.format("csv")
        .options(header=True, mode="PERMISSIVE", nullValue="", enforceSchema=True)
        .schema(spec.spark_schema)
        .load(str(source_path))
        .withColumn("_source_file", F.lit(source_path.name))
    )


def validate_dataframe_rows(dataframe: DataFrame, spec: TableSpec, source_path: Path) -> None:
    """Validate collected rows on the driver with the table's Pydantic contract."""
    errors: list[str] = []

    for row_number, row in enumerate(dataframe.select(*spec.source_columns).collect(), start=2):
        payload = row.asDict(recursive=True)
        try:
            spec.contract_model.model_validate(payload)
        except PydanticValidationError as exc:
            errors.append(f"{source_path.name}:{row_number}: {exc}")

    if errors:
        joined_errors = "\n".join(errors[:5])
        raise BronzeValidationError(
            f"Bronze validation failed for {spec.table_name}.\n{joined_errors}"
        )


def prepare_bronze_tables(
    spark: SparkSession,
    *,
    source_dir: Path,
    source_overrides: Mapping[str, Path] | None = None,
) -> dict[str, DataFrame]:
    """Read and validate every source table before any Delta write happens."""
    prepared: dict[str, DataFrame] = {}

    for spec in TABLE_SPECS:
        override_path = source_overrides.get(spec.table_name) if source_overrides else None
        source_path = Path(override_path) if override_path else source_dir / spec.filename
        if not source_path.exists():
            raise FileNotFoundError(f"Expected source file for {spec.table_name}: {source_path}")

        validate_csv_header(source_path, spec.source_columns)
        dataframe = read_source_dataframe(spark, source_path, spec)
        validate_dataframe_rows(dataframe, spec, source_path)
        prepared[spec.table_name] = dataframe

    return prepared


def write_bronze_tables(prepared_tables: Mapping[str, DataFrame], bronze_dir: Path) -> dict[str, Path]:
    """Write validated bronze tables to Delta."""
    bronze_dir.mkdir(parents=True, exist_ok=True)
    output_paths: dict[str, Path] = {}

    for spec in TABLE_SPECS:
        output_path = bronze_dir / spec.table_name
        (
            prepared_tables[spec.table_name]
            .select(*spec.source_columns, "_source_file")
            .write.format("delta")
            .mode("overwrite")
            .save(str(output_path))
        )
        output_paths[spec.table_name] = output_path

    return output_paths


def ingest_bronze_sources(
    *,
    config_path: Path | None = None,
    source_dir: str | Path | None = None,
    bronze_dir: str | Path | None = None,
    source_overrides: Mapping[str, Path] | None = None,
    spark: SparkSession | None = None,
    warehouse_dir: str | Path | None = None,
) -> dict[str, Path]:
    """Run the full bronze ingest from CSV sources into Delta tables."""
    config = load_bronze_config(config_path)
    resolved_source_dir = resolve_directory(source_dir or config.bronze.source_dir)
    resolved_bronze_dir = resolve_directory(bronze_dir or config.bronze.bronze_dir)
    resolved_warehouse_dir = (
        Path(warehouse_dir)
        if warehouse_dir is not None
        else resolved_bronze_dir.parent / "warehouse"
    )

    owns_session = spark is None
    active_spark = spark or build_spark_session(
        app_name=f"{config.spark.app_name}-bronze",
        master=config.spark.master,
        warehouse_dir=resolved_warehouse_dir,
    )

    try:
        prepared_tables = prepare_bronze_tables(
            active_spark,
            source_dir=resolved_source_dir,
            source_overrides=source_overrides,
        )
        return write_bronze_tables(prepared_tables, resolved_bronze_dir)
    finally:
        if owns_session:
            active_spark.stop()


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
