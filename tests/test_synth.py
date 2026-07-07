"""Offline tests for the synthetic source-data generator."""

from __future__ import annotations

import csv
import hashlib
from decimal import Decimal
from pathlib import Path

from tech_cost_platform.synth.generate import (
    DEFAULT_GL_TOTAL_EUR,
    DEFAULT_SYNTH_CONFIG,
    TABLE_FILENAMES,
    generate_source_exports,
)
from tech_cost_platform.synth.schema import (
    APPLICATION_COLUMNS,
    BUSINESS_UNIT_COLUMNS,
    COST_CENTER_COLUMNS,
    GL_COST_COLUMNS,
    RESOURCE_TOWER_COLUMNS,
    USAGE_METRIC_COLUMNS,
    SynthConfig,
)

EXPECTED_COLUMNS = {
    "gl_costs.csv": GL_COST_COLUMNS,
    "cost_centers.csv": COST_CENTER_COLUMNS,
    "resource_towers.csv": RESOURCE_TOWER_COLUMNS,
    "applications.csv": APPLICATION_COLUMNS,
    "business_units.csv": BUSINESS_UNIT_COLUMNS,
    "usage_metrics.csv": USAGE_METRIC_COLUMNS,
}


def synth_config(output_dir: Path) -> SynthConfig:
    """Return a test config rooted in the temp directory."""
    return SynthConfig(
        seed=DEFAULT_SYNTH_CONFIG.seed,
        period=DEFAULT_SYNTH_CONFIG.period,
        output_dir=output_dir.resolve().as_posix(),
    )


def file_hash(path: Path) -> str:
    """Return a stable content hash for a generated file."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Read a generated CSV file."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def generate_into(output_dir: Path) -> dict[str, Path]:
    """Generate source files into the supplied output directory."""
    return generate_source_exports(config=synth_config(output_dir))


def test_synth_emits_all_expected_files_and_columns(tmp_path: Path) -> None:
    """The generator should emit all six source exports with the specified columns."""
    output_paths = generate_into(tmp_path / "source-a")

    assert set(path.name for path in output_paths.values()) == set(TABLE_FILENAMES.values())
    for path in output_paths.values():
        rows = read_csv_rows(path)
        assert rows
        assert list(rows[0].keys()) == EXPECTED_COLUMNS[path.name]


def test_synth_is_byte_deterministic_for_default_seed(tmp_path: Path) -> None:
    """Two runs with the same seed should produce byte-identical outputs."""
    first_run = generate_into(tmp_path / "source-a")
    second_run = generate_into(tmp_path / "source-b")

    first_hashes = {path.name: file_hash(path) for path in first_run.values()}
    second_hashes = {path.name: file_hash(path) for path in second_run.values()}

    assert first_hashes == second_hashes


def test_synth_referential_integrity(tmp_path: Path) -> None:
    """Foreign keys should resolve across all generated source tables."""
    output_paths = generate_into(tmp_path / "source")

    cost_centers = read_csv_rows(output_paths["cost_centers"])
    resource_towers = read_csv_rows(output_paths["resource_towers"])
    applications = read_csv_rows(output_paths["applications"])
    business_units = read_csv_rows(output_paths["business_units"])
    gl_costs = read_csv_rows(output_paths["gl_costs"])
    usage_metrics = read_csv_rows(output_paths["usage_metrics"])

    cost_center_ids = {row["cost_center_id"] for row in cost_centers}
    tower_ids = {row["tower_id"] for row in resource_towers}
    app_ids = {row["app_id"] for row in applications}
    bu_ids = {row["bu_id"] for row in business_units}

    for row in cost_centers:
        tower_id = row["tower_id"]
        if tower_id:
            assert tower_id in tower_ids

    for row in gl_costs:
        assert row["cost_center_id"] in cost_center_ids

    for row in usage_metrics:
        if row["step"] == "tower_to_app":
            assert row["from_id"] in tower_ids
            assert row["to_id"] in app_ids
        elif row["step"] == "app_to_bu":
            assert row["from_id"] in app_ids
            assert row["to_id"] in bu_ids
        else:
            raise AssertionError(f"Unexpected step: {row['step']}")


def test_synth_encodes_design_intent_cases(tmp_path: Path) -> None:
    """The engineered awkward cases should exist for later packets."""
    output_paths = generate_into(tmp_path / "source")

    cost_centers = {
        row["cost_center_id"]: row for row in read_csv_rows(output_paths["cost_centers"])
    }
    gl_costs = read_csv_rows(output_paths["gl_costs"])
    usage_metrics = read_csv_rows(output_paths["usage_metrics"])

    unmapped_rows = [
        row for row in gl_costs if cost_centers[row["cost_center_id"]]["tower_id"] == ""
    ]
    assert unmapped_rows

    tower_to_app_apps = {
        row["to_id"] for row in usage_metrics if row["step"] == "tower_to_app"
    }
    app_to_bu_apps = {
        row["from_id"] for row in usage_metrics if row["step"] == "app_to_bu"
    }
    shared_unattributable_apps = tower_to_app_apps - app_to_bu_apps
    assert "APP-EMAIL" in shared_unattributable_apps

    analytics_storage = [
        Decimal(row["value"])
        for row in usage_metrics
        if row["step"] == "app_to_bu"
        and row["from_id"] == "APP-ANALYTICS"
        and row["metric_name"] == "storage_gb"
    ]
    analytics_named_users = [
        Decimal(row["value"])
        for row in usage_metrics
        if row["step"] == "app_to_bu"
        and row["from_id"] == "APP-ANALYTICS"
        and row["metric_name"] == "named_users"
    ]
    assert sum(analytics_storage, start=Decimal("0.00")) == Decimal("0.00")
    assert sum(analytics_named_users, start=Decimal("0.00")) > Decimal("0.00")

    def proportions(metric_name: str) -> dict[str, Decimal]:
        rows = [
            row
            for row in usage_metrics
            if row["step"] == "app_to_bu"
            and row["from_id"] == "APP-BILLING"
            and row["metric_name"] == metric_name
        ]
        total = sum((Decimal(row["value"]) for row in rows), start=Decimal("0.00"))
        return {row["to_id"]: (Decimal(row["value"]) / total) for row in rows}

    transactions = proportions("transactions")
    named_users = proportions("named_users")
    top_transactions_bu, top_transactions_share = max(
        transactions.items(), key=lambda item: item[1]
    )
    top_named_users_bu, top_named_users_share = max(named_users.items(), key=lambda item: item[1])

    assert top_transactions_bu != top_named_users_bu
    assert abs(top_transactions_share - top_named_users_share) >= Decimal("0.20")


def test_synth_gl_total_is_locked(tmp_path: Path) -> None:
    """The seeded GL aggregate should stay fixed for downstream reconciliation tests."""
    output_paths = generate_into(tmp_path / "source")
    gl_costs = read_csv_rows(output_paths["gl_costs"])
    total = sum((Decimal(row["amount_eur"]) for row in gl_costs), start=Decimal("0.00"))

    assert total == DEFAULT_GL_TOTAL_EUR
