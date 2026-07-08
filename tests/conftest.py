"""Shared test fixtures for self-contained offline runs."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from uuid import uuid4

import pytest
from pyspark.sql import SparkSession

from tech_cost_platform.bronze.ingest import ingest_bronze_sources
from tech_cost_platform.silver.build import SilverBuildResult, build_silver_tables
from tech_cost_platform.spark import build_spark_session, repo_root
from tech_cost_platform.synth.generate import DEFAULT_SYNTH_CONFIG, generate_source_exports
from tech_cost_platform.synth.schema import SynthConfig


@dataclass(frozen=True)
class BronzeRun:
    """Per-test bronze sandbox with isolated write locations."""

    source_dir: Path
    bronze_dir: Path
    warehouse_dir: Path
    spark: SparkSession

    def ingest(self, *, source_overrides: Mapping[str, Path] | None = None) -> dict[str, Path]:
        """Run bronze ingest into this run's isolated directories."""
        return ingest_bronze_sources(
            source_dir=self.source_dir,
            bronze_dir=self.bronze_dir,
            warehouse_dir=self.warehouse_dir,
            source_overrides=source_overrides,
            spark=self.spark,
        )


@dataclass(frozen=True)
class SilverRun:
    """Per-test silver sandbox with isolated bronze/silver write locations."""

    source_dir: Path
    bronze_dir: Path
    silver_dir: Path
    warehouse_dir: Path
    spark: SparkSession

    def ingest_bronze(self, *, source_overrides: Mapping[str, Path] | None = None) -> dict[str, Path]:
        """Prepare bronze inputs for this silver test run."""
        return ingest_bronze_sources(
            source_dir=self.source_dir,
            bronze_dir=self.bronze_dir,
            warehouse_dir=self.warehouse_dir,
            source_overrides=source_overrides,
            spark=self.spark,
        )

    def build_from_bronze(self) -> SilverBuildResult:
        """Build silver from the bronze Delta already present in this run."""
        return build_silver_tables(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
            warehouse_dir=self.warehouse_dir,
            spark=self.spark,
        )

    def build(self, *, source_overrides: Mapping[str, Path] | None = None) -> SilverBuildResult:
        """Run bronze ingest and then build silver in this run's isolated directories."""
        self.ingest_bronze(source_overrides=source_overrides)
        return self.build_from_bronze()


@pytest.fixture(scope="session")
def test_workspace() -> Path:
    """Create a project-local gitignored workspace for the whole test session."""
    path = repo_root() / "data" / "test-runs" / f"pytest-{uuid4().hex[:8]}"
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def synth_data(test_workspace: Path) -> Path:
    """Generate deterministic source CSVs once into the shared test workspace."""
    source_dir = test_workspace / "source"
    config = SynthConfig(
        seed=DEFAULT_SYNTH_CONFIG.seed,
        period=DEFAULT_SYNTH_CONFIG.period,
        output_dir=source_dir.resolve().as_posix(),
    )
    generate_source_exports(config=config)
    return source_dir


@pytest.fixture(scope="session")
def spark(test_workspace: Path):
    """Share one Spark session across bronze test runs to avoid startup churn."""
    session = build_spark_session(
        app_name="tech-cost-platform-tests",
        warehouse_dir=test_workspace / "spark-session-warehouse",
        extra_conf={"spark.local.dir": str((test_workspace / "spark-local").resolve())},
    )
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture(name="bronze_tables")
def fixture_bronze_tables(
    test_workspace: Path,
    synth_data: Path,
    spark: SparkSession,
    request: pytest.FixtureRequest,
):
    """Return a factory that creates isolated bronze write sandboxes per call."""
    case_root = test_workspace / "bronze" / request.node.name
    shutil.rmtree(case_root, ignore_errors=True)
    case_root.mkdir(parents=True, exist_ok=True)

    run_index = 0

    def create_run() -> BronzeRun:
        nonlocal run_index
        run_index += 1
        run_root = case_root / f"run-{run_index:02d}"
        return BronzeRun(
            source_dir=synth_data,
            bronze_dir=run_root / "bronze",
            warehouse_dir=run_root / "warehouse",
            spark=spark,
        )

    yield create_run
    shutil.rmtree(case_root, ignore_errors=True)


@pytest.fixture
def bronze_ingest(bronze_tables):
    """Alias the bronze factory name used in the handoff text."""
    return bronze_tables


@pytest.fixture(name="silver")
def fixture_silver(
    test_workspace: Path,
    synth_data: Path,
    spark: SparkSession,
    request: pytest.FixtureRequest,
):
    """Return a factory that creates isolated silver write sandboxes per call."""
    case_root = test_workspace / "silver" / request.node.name
    shutil.rmtree(case_root, ignore_errors=True)
    case_root.mkdir(parents=True, exist_ok=True)

    run_index = 0

    def create_run() -> SilverRun:
        nonlocal run_index
        run_index += 1
        run_root = case_root / f"run-{run_index:02d}"
        return SilverRun(
            source_dir=synth_data,
            bronze_dir=run_root / "bronze",
            silver_dir=run_root / "silver",
            warehouse_dir=run_root / "warehouse",
            spark=spark,
        )

    yield create_run
    shutil.rmtree(case_root, ignore_errors=True)
