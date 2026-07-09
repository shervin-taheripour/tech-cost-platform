"""Bronze-to-silver Delta build orchestration."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import DataFrame, SparkSession

from ..bronze.ingest import BronzeConfig, SparkConfig, resolve_directory
from ..delta_io import write_delta_table_staged
from ..spark import build_spark_session, repo_root
from .conform import SILVER_TABLE_NAMES, conform_bronze_tables, read_bronze_tables
from .dq import SilverDQReport, run_silver_dq_checks, summarize_failed_checks


class SilverConfig(BaseModel):
    """Runtime config for the silver stage."""

    model_config = ConfigDict(frozen=True)

    silver_dir: str = "data/silver"


class SilverRuntimeConfig(BaseModel):
    """Minimal config required by the silver stage."""

    spark: SparkConfig = Field(default_factory=SparkConfig)
    bronze: BronzeConfig = Field(default_factory=BronzeConfig)
    silver: SilverConfig = Field(default_factory=SilverConfig)


class SilverValidationError(ValueError):
    """Raised when required silver inputs are missing or malformed."""


class SilverDataQualityError(SilverValidationError):
    """Raised when the silver DQ suite detects governed data issues."""

    def __init__(self, report: SilverDQReport):
        self.report = report
        super().__init__(summarize_failed_checks(report))


@dataclass(frozen=True)
class SilverBuildResult:
    """Successful silver build output paths plus the passing DQ report."""

    output_paths: dict[str, Path]
    dq_report: SilverDQReport


def load_silver_config(config_path: Path | None = None) -> SilverRuntimeConfig:
    """Load silver runtime config from config.yaml."""
    resolved_path = config_path or repo_root() / "config.yaml"
    with resolved_path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    return SilverRuntimeConfig.model_validate(raw_config)


def write_silver_tables(tables: dict[str, DataFrame], silver_dir: Path) -> dict[str, Path]:
    """Write conformed silver tables to Delta via the shared staged-write helper."""
    silver_dir.mkdir(parents=True, exist_ok=True)
    output_paths: dict[str, Path] = {}

    for table_name in SILVER_TABLE_NAMES:
        output_path = silver_dir / table_name
        output_paths[table_name] = write_delta_table_staged(tables[table_name], output_path)

    return output_paths


def build_silver_tables(
    *,
    config_path: Path | None = None,
    bronze_dir: str | Path | None = None,
    silver_dir: str | Path | None = None,
    spark: SparkSession | None = None,
    warehouse_dir: str | Path | None = None,
) -> SilverBuildResult:
    """Read bronze Delta, validate, conform, and write silver Delta outputs."""
    config = load_silver_config(config_path)
    resolved_bronze_dir = resolve_directory(bronze_dir or config.bronze.bronze_dir)
    resolved_silver_dir = resolve_directory(silver_dir or config.silver.silver_dir)
    resolved_warehouse_dir = (
        Path(warehouse_dir)
        if warehouse_dir is not None
        else resolved_silver_dir.parent / "warehouse"
    )

    owns_session = spark is None
    active_spark = spark or build_spark_session(
        app_name=f"{config.spark.app_name}-silver",
        master=config.spark.master,
        warehouse_dir=resolved_warehouse_dir,
    )

    try:
        bronze_tables = read_bronze_tables(active_spark, resolved_bronze_dir)
        conformance = conform_bronze_tables(bronze_tables)
        dq_report = run_silver_dq_checks(conformance)
        if not dq_report.passed:
            raise SilverDataQualityError(dq_report)
        output_paths = write_silver_tables(dict(conformance.tables), resolved_silver_dir)
        return SilverBuildResult(output_paths=output_paths, dq_report=dq_report)
    except FileNotFoundError as exc:
        raise SilverValidationError(str(exc)) from exc
    finally:
        if owns_session:
            active_spark.stop()


def parse_args() -> argparse.Namespace:
    """Parse CLI args for the silver stage."""
    parser = argparse.ArgumentParser(description="Build conformed silver Delta tables from bronze.")
    parser.add_argument("--config", type=Path, help="Optional path to config.yaml.")
    parser.add_argument("--bronze-dir", type=Path, help="Optional bronze Delta directory override.")
    parser.add_argument("--silver-dir", type=Path, help="Optional silver Delta directory override.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint for the silver stage."""
    args = parse_args()
    result = build_silver_tables(
        config_path=args.config,
        bronze_dir=args.bronze_dir,
        silver_dir=args.silver_dir,
    )
    print(
        f"[tech-cost-platform] silver status=completed "
        f"dq_checks={len(result.dq_report.checks)} tables_written={len(result.output_paths)}"
    )
    for table_name in SILVER_TABLE_NAMES:
        print(f"[tech-cost-platform] silver table={table_name} path={result.output_paths[table_name]}")
    return 0
