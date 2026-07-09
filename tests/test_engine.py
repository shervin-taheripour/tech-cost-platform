"""Integration tests for the allocation engine cascade."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from tech_cost_platform.delta_tables import read_delta_table
from tech_cost_platform.engine import run_allocation
from tech_cost_platform.synth.generate import DEFAULT_GL_TOTAL_EUR


def load_gold_rows(gold_dir: Path, table_name: str) -> list[dict[str, object]]:
    """Read a gold Delta table from disk as Python rows."""
    return read_delta_table(gold_dir / table_name).to_pylist()


def aggregate_terminal_amounts(gold_dir: Path) -> dict[str, Decimal]:
    """Return reconciled terminal totals by GL line across allocation and residual outputs."""
    totals: dict[str, Decimal] = {}
    for row in load_gold_rows(gold_dir, "allocation"):
        totals[row["gl_line_id"]] = totals.get(row["gl_line_id"], Decimal("0.00")) + row[
            "allocated_amount_eur"
        ]
    for row in load_gold_rows(gold_dir, "residual"):
        totals[row["gl_line_id"]] = totals.get(row["gl_line_id"], Decimal("0.00")) + row["amount_eur"]
    return totals


def write_rule_version(test_workspace: Path, version_id: str, payload: dict[str, object]) -> Path:
    """Write a temporary governed rule version for engine integration tests."""
    rules_dir = test_workspace / "engine-rules" / version_id
    rules_dir.mkdir(parents=True, exist_ok=True)
    rule_path = rules_dir / f"{version_id}.yaml"
    rule_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return rules_dir


def build_rule_payload(
    *,
    version_id: str,
    tower_metric: str = "cpu_hours",
    app_metric: str = "transactions",
) -> dict[str, object]:
    """Return a minimal governed-valid rule payload for engine test variants."""
    return {
        "version_id": version_id,
        "description": f"Engine integration test rule {version_id}.",
        "created": "2026-07-09",
        "gl_to_tower": {"basis": "cost_center_mapping"},
        "tower_to_app": {"strategy": "consumption", "metric_name": tower_metric},
        "app_to_bu": {"strategy": "consumption", "metric_name": app_metric},
    }


@pytest.fixture
def engine_outputs(test_workspace: Path, engine):
    """Prepare one shared engine integration workspace and reuse its outputs across tests."""
    run = engine()
    run.ingest_bronze()
    run.build_silver()

    email_rule_dir = write_rule_version(
        test_workspace,
        "v_test_email_unattributable",
        build_rule_payload(
            version_id="v_test_email_unattributable",
            tower_metric="named_users",
            app_metric="named_users",
        ),
    )
    storage_rule_dir = write_rule_version(
        test_workspace,
        "v_test_storage_driver_zero",
        build_rule_payload(
            version_id="v_test_storage_driver_zero",
            tower_metric="cpu_hours",
            app_metric="storage_gb",
        ),
    )

    return {
        "run": run,
        "v1": run_allocation(
            silver_dir=run.silver_dir,
            gold_dir=run.gold_dir / "v1",
            rule_version_id="v1_transactions",
        ),
        "v2": run_allocation(
            silver_dir=run.silver_dir,
            gold_dir=run.gold_dir / "v2",
            rule_version_id="v2_named_users",
        ),
        "email": run_allocation(
            silver_dir=run.silver_dir,
            gold_dir=run.gold_dir / "email",
            rule_version_id="v_test_email_unattributable",
            rules_dir=email_rule_dir,
        ),
        "storage": run_allocation(
            silver_dir=run.silver_dir,
            gold_dir=run.gold_dir / "storage",
            rule_version_id="v_test_storage_driver_zero",
            rules_dir=storage_rule_dir,
        ),
        "v1_repeat": run_allocation(
            silver_dir=run.silver_dir,
            gold_dir=run.gold_dir / "v1-repeat",
            rule_version_id="v1_transactions",
        ),
    }


def test_full_cascade_writes_allocation_and_residual_and_reconciles(engine_outputs) -> None:
    """The shipped v1 cascade should write both gold outputs and reconcile exactly."""
    result = engine_outputs["v1"]
    allocation_rows = load_gold_rows(result.output_paths["allocation"].parent, "allocation")
    residual_rows = load_gold_rows(result.output_paths["residual"].parent, "residual")
    allocated_total = sum((row["allocated_amount_eur"] for row in allocation_rows), start=Decimal("0.00"))
    residual_total = sum((row["amount_eur"] for row in residual_rows), start=Decimal("0.00"))

    assert set(result.output_paths) == {"allocation", "residual"}
    assert result.rule_version == "v1_transactions"
    assert result.output_paths["allocation"].exists()
    assert result.output_paths["residual"].exists()
    assert allocated_total + residual_total == DEFAULT_GL_TOTAL_EUR


def test_no_double_counting_preserves_each_gl_line_total_once_in_aggregate(engine_outputs) -> None:
    """Each GL line should be fully accounted for exactly once across terminal outcomes."""
    expected = {
        row["gl_line_id"]: row["amount_eur"]
        for row in read_delta_table(engine_outputs["run"].silver_dir / "fact_gl_cost").to_pylist()
    }
    terminal_totals = aggregate_terminal_amounts(engine_outputs["v1"].output_paths["allocation"].parent)

    assert terminal_totals == expected


def test_seeded_residual_cases_surface_with_expected_reason_codes(engine_outputs) -> None:
    """The governed residual cases should exit at the intended step and reason."""
    unmapped_rows = [
        row
        for row in load_gold_rows(engine_outputs["v1"].output_paths["residual"].parent, "residual")
        if row["cost_center_id"] == "CC-LEGACY" and row["failed_step"] == "gl_to_tower"
    ]
    email_rows = [
        row
        for row in load_gold_rows(engine_outputs["email"].output_paths["residual"].parent, "residual")
        if row["app_id"] == "APP-EMAIL" and row["failed_step"] == "app_to_bu"
    ]
    storage_rows = [
        row
        for row in load_gold_rows(engine_outputs["storage"].output_paths["residual"].parent, "residual")
        if row["app_id"] == "APP-ANALYTICS" and row["failed_step"] == "app_to_bu"
    ]

    assert {row["reason_code"] for row in unmapped_rows} == {"unmapped"}
    assert len(email_rows) >= 1
    assert {row["reason_code"] for row in email_rows} == {"shared_unattributable"}
    assert len(storage_rows) >= 1
    assert {row["reason_code"] for row in storage_rows} == {"driver_zero"}


def test_rule_versions_v1_and_v2_diverge_at_bu_level_but_both_reconcile(engine_outputs) -> None:
    """The shipped driver comparison pair should change BU allocations, not totals."""

    def bu_totals(gold_dir: Path) -> dict[str, Decimal]:
        totals: dict[str, Decimal] = {}
        for row in load_gold_rows(gold_dir, "allocation"):
            totals[row["bu_id"]] = totals.get(row["bu_id"], Decimal("0.00")) + row["allocated_amount_eur"]
        return totals

    def reconcile_total(gold_dir: Path) -> Decimal:
        allocation_total = sum(
            (row["allocated_amount_eur"] for row in load_gold_rows(gold_dir, "allocation")),
            start=Decimal("0.00"),
        )
        residual_total = sum(
            (row["amount_eur"] for row in load_gold_rows(gold_dir, "residual")),
            start=Decimal("0.00"),
        )
        return allocation_total + residual_total

    v1_gold_dir = engine_outputs["v1"].output_paths["allocation"].parent
    v2_gold_dir = engine_outputs["v2"].output_paths["allocation"].parent

    assert bu_totals(v1_gold_dir) != bu_totals(v2_gold_dir)
    assert reconcile_total(v1_gold_dir) == DEFAULT_GL_TOTAL_EUR
    assert reconcile_total(v2_gold_dir) == DEFAULT_GL_TOTAL_EUR


def test_rule_version_pinning_is_reproducible_for_same_silver_inputs(engine_outputs) -> None:
    """Re-running the same pinned rule over identical silver should produce identical gold rows."""
    first = engine_outputs["v1"]
    second = engine_outputs["v1_repeat"]
    first_gold_dir = first.output_paths["allocation"].parent
    second_gold_dir = second.output_paths["allocation"].parent
    first_allocation = sorted(
        load_gold_rows(first_gold_dir, "allocation"),
        key=lambda row: (
            row["gl_line_id"],
            row["tower_id"],
            row["app_id"],
            row["bu_id"],
            row["allocated_amount_eur"],
        ),
    )
    first_residual = sorted(
        load_gold_rows(first_gold_dir, "residual"),
        key=lambda row: (
            row["gl_line_id"],
            row["failed_step"],
            row["reason_code"],
            row["amount_eur"],
        ),
    )
    second_allocation = sorted(
        load_gold_rows(second_gold_dir, "allocation"),
        key=lambda row: (
            row["gl_line_id"],
            row["tower_id"],
            row["app_id"],
            row["bu_id"],
            row["allocated_amount_eur"],
        ),
    )
    second_residual = sorted(
        load_gold_rows(second_gold_dir, "residual"),
        key=lambda row: (
            row["gl_line_id"],
            row["failed_step"],
            row["reason_code"],
            row["amount_eur"],
        ),
    )

    assert first.rule_version == second.rule_version == "v1_transactions"
    assert first_allocation == second_allocation
    assert first_residual == second_residual
