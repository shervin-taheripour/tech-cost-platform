"""Offline tests for the bronze ingestion layer."""

from __future__ import annotations

import csv
import shutil
from decimal import Decimal
from pathlib import Path

import pytest

from tech_cost_platform.bronze.ingest import (
    TABLE_SPECS,
    BronzeValidationError,
    ingest_bronze_sources,
)
from tech_cost_platform.spark import build_spark_session, repo_root

EXPECTED_GL_TOTAL_EUR = Decimal("61813.95")


def csv_row_count(path: Path) -> int:
    """Return the number of data rows in a CSV file."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


@pytest.fixture(scope="session")
def bronze_test_root() -> Path:
    """Use a project-local path for Spark test output on Windows."""
    root = repo_root() / "data" / "test-runs" / "bronze-tests"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture(scope="session")
def spark(bronze_test_root: Path):
    """Build one local Spark session for the bronze tests."""
    session = build_spark_session(
        app_name="tech-cost-platform-bronze-tests",
        warehouse_dir=bronze_test_root / "warehouse",
    )
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def case_dir(bronze_test_root: Path, request: pytest.FixtureRequest) -> Path:
    """Give each test its own project-local output directory."""
    path = bronze_test_root / request.node.name
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    yield path
    shutil.rmtree(path, ignore_errors=True)


def test_bronze_creates_all_delta_tables_and_reconciles_counts(spark, case_dir: Path) -> None:
    """All six source tables should land in bronze Delta with matching row counts."""
    bronze_dir = case_dir / "bronze"
    output_paths = ingest_bronze_sources(
        bronze_dir=bronze_dir,
        warehouse_dir=case_dir / "warehouse",
        spark=spark,
    )

    source_dir = repo_root() / "data" / "source"
    for spec in TABLE_SPECS:
        delta_frame = spark.read.format("delta").load(str(output_paths[spec.table_name]))
        assert output_paths[spec.table_name].exists()
        assert delta_frame.count() == csv_row_count(source_dir / spec.filename)


def test_bronze_preserves_gl_total_and_lineage_anchor(spark, case_dir: Path) -> None:
    """Bronze should preserve the GL aggregate, ids, and source columns."""
    bronze_dir = case_dir / "bronze"
    output_paths = ingest_bronze_sources(
        bronze_dir=bronze_dir,
        warehouse_dir=case_dir / "warehouse",
        spark=spark,
    )

    dataframe = spark.read.format("delta").load(str(output_paths["gl_costs"]))
    columns = set(dataframe.columns)
    gl_rows = dataframe.where("gl_line_id = 'GL-000001'").select(
        "gl_line_id", "period", "gl_account", "cost_center_id", "amount_eur", "description"
    )
    aggregate = dataframe.selectExpr("CAST(sum(amount_eur) AS DECIMAL(18,2)) AS total").collect()[0]["total"]

    assert columns.issuperset(
        {"gl_line_id", "period", "gl_account", "cost_center_id", "amount_eur", "description", "_source_file"}
    )
    assert dataframe.select("gl_line_id").distinct().count() == dataframe.count()
    assert aggregate == EXPECTED_GL_TOTAL_EUR
    assert gl_rows.collect()[0]["gl_line_id"] == "GL-000001"
    assert gl_rows.collect()[0]["cost_center_id"] == "CC-BIZ-APPS"


def test_bronze_preserves_intentional_null_tower_id(spark, case_dir: Path) -> None:
    """The unmapped residual anchor must survive bronze as a null tower_id."""
    bronze_dir = case_dir / "bronze"
    output_paths = ingest_bronze_sources(
        bronze_dir=bronze_dir,
        warehouse_dir=case_dir / "warehouse",
        spark=spark,
    )

    dataframe = spark.read.format("delta").load(str(output_paths["cost_centers"]))
    row = dataframe.where("cost_center_id = 'CC-LEGACY'").select("tower_id").collect()[0]

    assert row["tower_id"] is None
    assert dataframe.where("tower_id IS NULL").count() >= 1


def test_bronze_rejects_malformed_input_without_writing(spark, case_dir: Path) -> None:
    """Malformed source input should fail validation before any Delta tables are written."""
    bronze_dir = case_dir / "bronze"
    malformed_path = repo_root() / "tests" / "fixtures" / "gl_costs_malformed.csv"

    with pytest.raises(BronzeValidationError):
        ingest_bronze_sources(
            bronze_dir=bronze_dir,
            warehouse_dir=case_dir / "warehouse",
            source_overrides={"gl_costs": malformed_path},
            spark=spark,
        )

    for spec in TABLE_SPECS:
        assert not (bronze_dir / spec.table_name).exists()
