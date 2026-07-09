"""Shared test fixtures for self-contained offline runs."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Mapping
from uuid import uuid4

import pytest

from tech_cost_platform.bronze.ingest import ingest_bronze_sources
from tech_cost_platform.silver.build import SilverBuildResult, build_silver_tables
from tech_cost_platform.runtime import repo_root
from tech_cost_platform.synth.generate import DEFAULT_SYNTH_CONFIG, generate_source_exports
from tech_cost_platform.synth.schema import SynthConfig

if TYPE_CHECKING:
    from tech_cost_platform.engine import AllocationResult
    from tech_cost_platform.gold import GoldReportsResult
    from tech_cost_platform.lineage import LineageBuildResult
    from tech_cost_platform.residual import ResidualReportResult


@dataclass(frozen=True)
class BronzeRun:
    """Per-test bronze sandbox with isolated write locations."""

    source_dir: Path
    bronze_dir: Path

    def ingest(self, *, source_overrides: Mapping[str, Path] | None = None) -> dict[str, Path]:
        """Run bronze ingest into this run's isolated directories."""
        return ingest_bronze_sources(
            source_dir=self.source_dir,
            bronze_dir=self.bronze_dir,
            source_overrides=source_overrides,
        )


@dataclass(frozen=True)
class SilverRun:
    """Per-test silver sandbox with isolated bronze/silver write locations."""

    source_dir: Path
    bronze_dir: Path
    silver_dir: Path

    def ingest_bronze(self, *, source_overrides: Mapping[str, Path] | None = None) -> dict[str, Path]:
        """Prepare bronze inputs for this silver test run."""
        return ingest_bronze_sources(
            source_dir=self.source_dir,
            bronze_dir=self.bronze_dir,
            source_overrides=source_overrides,
        )

    def build_from_bronze(self) -> SilverBuildResult:
        """Build silver from the bronze Delta already present in this run."""
        return build_silver_tables(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
        )

    def build(self, *, source_overrides: Mapping[str, Path] | None = None) -> SilverBuildResult:
        """Run bronze ingest and then build silver in this run's isolated directories."""
        self.ingest_bronze(source_overrides=source_overrides)
        return self.build_from_bronze()


@dataclass(frozen=True)
class EngineRun:
    """Per-test engine sandbox with isolated bronze, silver, and gold write locations."""

    source_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path

    def ingest_bronze(self, *, source_overrides: Mapping[str, Path] | None = None) -> dict[str, Path]:
        """Prepare bronze inputs for this engine test run."""
        return ingest_bronze_sources(
            source_dir=self.source_dir,
            bronze_dir=self.bronze_dir,
            source_overrides=source_overrides,
        )

    def build_silver(self) -> SilverBuildResult:
        """Build silver inputs for the allocation engine from this run's bronze data."""
        return build_silver_tables(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
        )

    def run_allocation(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
    ) -> "AllocationResult":
        """Run the gold allocation engine from this run's prepared silver inputs."""
        from tech_cost_platform.engine import run_allocation

        return run_allocation(
            silver_dir=self.silver_dir,
            gold_dir=self.gold_dir,
            rule_version_id=rule_version_id,
            rules_dir=rules_dir,
        )

    def build(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
        source_overrides: Mapping[str, Path] | None = None,
    ) -> "AllocationResult":
        """Run bronze ingest, silver build, and gold allocation in this run."""
        self.ingest_bronze(source_overrides=source_overrides)
        self.build_silver()
        return self.run_allocation(rule_version_id=rule_version_id, rules_dir=rules_dir)


@dataclass(frozen=True)
class ResidualRun:
    """Per-test residual sandbox with isolated bronze, silver, and gold write locations."""

    source_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path

    def ingest_bronze(self, *, source_overrides: Mapping[str, Path] | None = None) -> dict[str, Path]:
        """Prepare bronze inputs for this residual test run."""
        return ingest_bronze_sources(
            source_dir=self.source_dir,
            bronze_dir=self.bronze_dir,
            source_overrides=source_overrides,
        )

    def build_silver(self) -> SilverBuildResult:
        """Build silver inputs for residual reporting."""
        return build_silver_tables(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
        )

    def run_allocation(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
    ) -> "AllocationResult":
        """Build gold allocation and residual inputs."""
        from tech_cost_platform.engine import run_allocation

        return run_allocation(
            silver_dir=self.silver_dir,
            gold_dir=self.gold_dir,
            rule_version_id=rule_version_id,
            rules_dir=rules_dir,
        )

    def build_residual(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
    ) -> "ResidualReportResult":
        """Build residual report outputs from prepared silver and gold inputs."""
        from tech_cost_platform.residual import build_residual_outputs

        return build_residual_outputs(
            silver_dir=self.silver_dir,
            gold_dir=self.gold_dir,
            rule_version_id=rule_version_id,
            rules_dir=rules_dir,
        )

    def build(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
        source_overrides: Mapping[str, Path] | None = None,
    ) -> "ResidualReportResult":
        """Run bronze, silver, gold, and residual reporting in this run."""
        self.ingest_bronze(source_overrides=source_overrides)
        self.build_silver()
        self.run_allocation(rule_version_id=rule_version_id, rules_dir=rules_dir)
        return self.build_residual(rule_version_id=rule_version_id, rules_dir=rules_dir)


@dataclass(frozen=True)
class LineageRun:
    """Per-test lineage sandbox with isolated bronze, silver, gold, and example locations."""

    source_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    examples_dir: Path

    def ingest_bronze(self, *, source_overrides: Mapping[str, Path] | None = None) -> dict[str, Path]:
        """Prepare bronze inputs for this lineage test run."""
        return ingest_bronze_sources(
            source_dir=self.source_dir,
            bronze_dir=self.bronze_dir,
            source_overrides=source_overrides,
        )

    def build_silver(self) -> SilverBuildResult:
        """Build silver inputs for lineage reporting."""
        return build_silver_tables(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
        )

    def run_allocation(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
    ) -> "AllocationResult":
        """Build gold allocation and residual inputs."""
        from tech_cost_platform.engine import run_allocation

        return run_allocation(
            silver_dir=self.silver_dir,
            gold_dir=self.gold_dir,
            rule_version_id=rule_version_id,
            rules_dir=rules_dir,
        )

    def build_residual(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
    ) -> "ResidualReportResult":
        """Build residual report outputs from prepared silver and gold inputs."""
        from tech_cost_platform.residual import build_residual_outputs

        return build_residual_outputs(
            silver_dir=self.silver_dir,
            gold_dir=self.gold_dir,
            rule_version_id=rule_version_id,
            rules_dir=rules_dir,
        )

    def build_lineage(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
    ) -> "LineageBuildResult":
        """Build lineage outputs from prepared silver, gold, and residual inputs."""
        from tech_cost_platform.lineage import build_lineage_outputs

        return build_lineage_outputs(
            silver_dir=self.silver_dir,
            gold_dir=self.gold_dir,
            examples_dir=self.examples_dir,
            rule_version_id=rule_version_id,
            rules_dir=rules_dir,
        )

    def build(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
        source_overrides: Mapping[str, Path] | None = None,
    ) -> "LineageBuildResult":
        """Run bronze, silver, gold, residual, and lineage reporting in this run."""
        self.ingest_bronze(source_overrides=source_overrides)
        self.build_silver()
        self.run_allocation(rule_version_id=rule_version_id, rules_dir=rules_dir)
        self.build_residual(rule_version_id=rule_version_id, rules_dir=rules_dir)
        return self.build_lineage(rule_version_id=rule_version_id, rules_dir=rules_dir)


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


@pytest.fixture(name="bronze_tables")
def fixture_bronze_tables(
    test_workspace: Path,
    synth_data: Path,
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
        )

    yield create_run
    shutil.rmtree(case_root, ignore_errors=True)


@pytest.fixture(name="engine")
def fixture_engine(
    test_workspace: Path,
    synth_data: Path,
    request: pytest.FixtureRequest,
):
    """Return a factory that creates isolated engine write sandboxes per call."""
    case_root = test_workspace / "engine" / request.node.name
    shutil.rmtree(case_root, ignore_errors=True)
    case_root.mkdir(parents=True, exist_ok=True)

    run_index = 0

    def create_run() -> EngineRun:
        nonlocal run_index
        run_index += 1
        run_root = case_root / f"run-{run_index:02d}"
        return EngineRun(
            source_dir=synth_data,
            bronze_dir=run_root / "bronze",
            silver_dir=run_root / "silver",
            gold_dir=run_root / "gold",
        )

    yield create_run
    shutil.rmtree(case_root, ignore_errors=True)


@pytest.fixture(name="residual")
def fixture_residual(
    test_workspace: Path,
    synth_data: Path,
    request: pytest.FixtureRequest,
):
    """Return a factory that creates isolated residual write sandboxes per call."""
    case_root = test_workspace / "residual" / request.node.name
    shutil.rmtree(case_root, ignore_errors=True)
    case_root.mkdir(parents=True, exist_ok=True)

    run_index = 0

    def create_run() -> ResidualRun:
        nonlocal run_index
        run_index += 1
        run_root = case_root / f"run-{run_index:02d}"
        return ResidualRun(
            source_dir=synth_data,
            bronze_dir=run_root / "bronze",
            silver_dir=run_root / "silver",
            gold_dir=run_root / "gold",
        )

    yield create_run
    shutil.rmtree(case_root, ignore_errors=True)


@dataclass(frozen=True)
class GoldReportsRun:
    """Per-test gold reports sandbox with isolated bronze, silver, gold, and example locations."""

    source_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    examples_dir: Path

    def ingest_bronze(self, *, source_overrides: Mapping[str, Path] | None = None) -> dict[str, Path]:
        """Prepare bronze inputs for this gold reports test run."""
        return ingest_bronze_sources(
            source_dir=self.source_dir,
            bronze_dir=self.bronze_dir,
            source_overrides=source_overrides,
        )

    def build_silver(self) -> "SilverBuildResult":
        """Build silver inputs for gold reports."""
        return build_silver_tables(
            bronze_dir=self.bronze_dir,
            silver_dir=self.silver_dir,
        )

    def run_allocation(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
    ) -> "AllocationResult":
        """Build gold allocation and residual inputs."""
        from tech_cost_platform.engine import run_allocation

        return run_allocation(
            silver_dir=self.silver_dir,
            gold_dir=self.gold_dir,
            rule_version_id=rule_version_id,
            rules_dir=rules_dir,
        )

    def build_residual(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
    ) -> "ResidualReportResult":
        """Build residual report outputs."""
        from tech_cost_platform.residual import build_residual_outputs

        return build_residual_outputs(
            silver_dir=self.silver_dir,
            gold_dir=self.gold_dir,
            rule_version_id=rule_version_id,
            rules_dir=rules_dir,
        )

    def build_lineage(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
    ) -> "LineageBuildResult":
        """Build lineage outputs."""
        from tech_cost_platform.lineage import build_lineage_outputs

        return build_lineage_outputs(
            silver_dir=self.silver_dir,
            gold_dir=self.gold_dir,
            examples_dir=self.examples_dir,
            rule_version_id=rule_version_id,
            rules_dir=rules_dir,
        )

    def build_reports(self) -> "GoldReportsResult":
        """Build gold report views from existing gold and silver data."""
        from tech_cost_platform.gold import build_gold_reports

        return build_gold_reports(
            silver_dir=self.silver_dir,
            gold_dir=self.gold_dir,
        )

    def build(
        self,
        *,
        rule_version_id: str = "v1_transactions",
        rules_dir: Path | None = None,
        source_overrides: Mapping[str, Path] | None = None,
    ) -> "GoldReportsResult":
        """Run bronze, silver, gold, residual, lineage, and reports in this run."""
        self.ingest_bronze(source_overrides=source_overrides)
        self.build_silver()
        self.run_allocation(rule_version_id=rule_version_id, rules_dir=rules_dir)
        self.build_residual(rule_version_id=rule_version_id, rules_dir=rules_dir)
        self.build_lineage(rule_version_id=rule_version_id, rules_dir=rules_dir)
        return self.build_reports()


@pytest.fixture(name="lineage")
def fixture_lineage(
    test_workspace: Path,
    synth_data: Path,
    request: pytest.FixtureRequest,
):
    """Return a factory that creates isolated lineage write sandboxes per call."""
    case_root = test_workspace / "lineage" / request.node.name
    shutil.rmtree(case_root, ignore_errors=True)
    case_root.mkdir(parents=True, exist_ok=True)

    run_index = 0

    def create_run() -> LineageRun:
        nonlocal run_index
        run_index += 1
        run_root = case_root / f"run-{run_index:02d}"
        return LineageRun(
            source_dir=synth_data,
            bronze_dir=run_root / "bronze",
            silver_dir=run_root / "silver",
            gold_dir=run_root / "gold",
            examples_dir=run_root / "examples",
        )

    yield create_run
    shutil.rmtree(case_root, ignore_errors=True)


@pytest.fixture(name="gold_reports")
def fixture_gold_reports(
    test_workspace: Path,
    synth_data: Path,
    request: pytest.FixtureRequest,
):
    """Return a factory that creates isolated gold reports write sandboxes per call."""
    case_root = test_workspace / "gold_reports" / request.node.name
    shutil.rmtree(case_root, ignore_errors=True)
    case_root.mkdir(parents=True, exist_ok=True)

    run_index = 0

    def create_run() -> GoldReportsRun:
        nonlocal run_index
        run_index += 1
        run_root = case_root / f"run-{run_index:02d}"
        return GoldReportsRun(
            source_dir=synth_data,
            bronze_dir=run_root / "bronze",
            silver_dir=run_root / "silver",
            gold_dir=run_root / "gold",
            examples_dir=run_root / "examples",
        )

    yield create_run
    shutil.rmtree(case_root, ignore_errors=True)
