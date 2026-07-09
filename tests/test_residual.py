"""Fast tests for residual reporting and reconciliation."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import yaml
from deltalake import DeltaTable

from tech_cost_platform.delta_tables import build_arrow_table, read_delta_table, write_delta_table
from tech_cost_platform.residual import (
    RESIDUAL_DETAIL_TABLE,
    RESIDUAL_REPORT_TABLE,
    ReconciliationError,
    reconcile_rule_version,
)
from tech_cost_platform.residual.report import PCT_QUANTUM
from tech_cost_platform.synth.generate import DEFAULT_GL_TOTAL_EUR


def write_rule_version(test_workspace: Path, version_id: str, payload: dict[str, object]) -> Path:
    """Write a temporary governed rule version for residual tests."""
    rules_dir = test_workspace / "residual-rules" / version_id
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
    """Return a minimal governed-valid rule payload for residual variants."""
    return {
        "version_id": version_id,
        "description": f"Residual integration test rule {version_id}.",
        "created": "2026-07-09",
        "gl_to_tower": {"basis": "cost_center_mapping"},
        "tower_to_app": {"strategy": "consumption", "metric_name": tower_metric},
        "app_to_bu": {"strategy": "consumption", "metric_name": app_metric},
    }


def _build_v1_residual(residual):
    run = residual()
    result = run.build(rule_version_id="v1_transactions")
    return run, result


def _residual_rule_run(residual, test_workspace: Path, *, version_id: str, app_metric: str):
    run = residual()
    rules_dir = write_rule_version(
        test_workspace,
        version_id,
        build_rule_payload(version_id=version_id, app_metric=app_metric),
    )
    result = run.build(rule_version_id=version_id, rules_dir=rules_dir)
    return run, result


def _residual_rule_run_with_tower_metric(
    residual,
    test_workspace: Path,
    *,
    version_id: str,
    tower_metric: str,
    app_metric: str,
):
    run = residual()
    rules_dir = write_rule_version(
        test_workspace,
        version_id,
        build_rule_payload(
            version_id=version_id,
            tower_metric=tower_metric,
            app_metric=app_metric,
        ),
    )
    result = run.build(rule_version_id=version_id, rules_dir=rules_dir)
    return run, result


def test_residual_outputs_materialize_as_valid_delta(residual) -> None:
    """Residual stage should write both detail and report as readable Delta tables."""
    run, result = _build_v1_residual(residual)

    detail_path = result.output_paths[RESIDUAL_DETAIL_TABLE]
    report_path = result.output_paths[RESIDUAL_REPORT_TABLE]

    assert detail_path.exists()
    assert report_path.exists()
    assert (detail_path / "_delta_log").exists()
    assert (report_path / "_delta_log").exists()
    assert DeltaTable(str(detail_path)).to_pyarrow_table().num_rows > 0
    assert DeltaTable(str(report_path)).to_pyarrow_table().num_rows > 0
    assert result.reconciliation.total_gl_eur == DEFAULT_GL_TOTAL_EUR
    assert run.gold_dir == detail_path.parent


def test_reconciliation_passes_exactly_for_v1_transactions(residual) -> None:
    """Good v1 data should reconcile exactly to the governed GL total."""
    run, _ = _build_v1_residual(residual)

    reconciliation = reconcile_rule_version(
        silver_dir=run.silver_dir,
        gold_dir=run.gold_dir,
        rule_version_id="v1_transactions",
    )

    assert reconciliation.total_gl_eur == DEFAULT_GL_TOTAL_EUR
    assert reconciliation.total_allocated_eur + reconciliation.total_residual_eur == DEFAULT_GL_TOTAL_EUR
    assert reconciliation.balanced is True
    assert reconciliation.difference_eur == Decimal("0.00")


def test_reconciliation_fails_on_tampered_residual_input(residual) -> None:
    """Dropping a residual row should cause the reconciliation check to fail loudly."""
    run, _ = _build_v1_residual(residual)
    residual_path = run.gold_dir / "residual"
    residual_table = read_delta_table(residual_path)
    residual_rows = residual_table.to_pylist()
    tampered_rows = residual_rows[1:]

    write_delta_table(
        build_arrow_table(tampered_rows, residual_table.schema),
        residual_path,
        sort_columns=["gl_line_id", "failed_step", "reason_code", "amount_eur"],
    )

    try:
        reconcile_rule_version(
            silver_dir=run.silver_dir,
            gold_dir=run.gold_dir,
            rule_version_id="v1_transactions",
        )
        raise AssertionError("Expected reconciliation to fail on tampered residual input.")
    except ReconciliationError as exc:
        assert exc.result.balanced is False
        assert exc.result.difference_eur != Decimal("0.00")


def test_all_reason_codes_surface_with_expected_failed_steps_and_entities(
    residual,
    test_workspace: Path,
) -> None:
    """Seeded residual cases should surface unchanged in the enriched detail view."""
    v1_run, _ = _build_v1_residual(residual)
    email_run, _ = _residual_rule_run_with_tower_metric(
        residual,
        test_workspace,
        version_id="v_test_email_unattributable",
        tower_metric="named_users",
        app_metric="named_users",
    )
    storage_run, _ = _residual_rule_run(
        residual,
        test_workspace,
        version_id="v_test_storage_driver_zero",
        app_metric="storage_gb",
    )

    v1_detail = read_delta_table(v1_run.gold_dir / RESIDUAL_DETAIL_TABLE).to_pylist()
    email_detail = read_delta_table(email_run.gold_dir / RESIDUAL_DETAIL_TABLE).to_pylist()
    storage_detail = read_delta_table(storage_run.gold_dir / RESIDUAL_DETAIL_TABLE).to_pylist()

    assert any(
        row["cost_center_id"] == "CC-LEGACY"
        and row["reason_code"] == "unmapped"
        and row["failed_step"] == "gl_to_tower"
        for row in v1_detail
    )
    assert any(
        row["app_id"] == "APP-EMAIL"
        and row["reason_code"] == "shared_unattributable"
        and row["failed_step"] == "app_to_bu"
        for row in email_detail
    )
    assert any(
        row["app_id"] == "APP-ANALYTICS"
        and row["reason_code"] == "driver_zero"
        and row["failed_step"] == "app_to_bu"
        for row in storage_detail
    )


def test_residual_detail_and_report_preserve_amounts_without_force_spread(residual) -> None:
    """Residual reporting should preserve amounts exactly and never turn a residual line fully allocated."""
    run, _ = _build_v1_residual(residual)

    detail_rows = read_delta_table(run.gold_dir / RESIDUAL_DETAIL_TABLE).to_pylist()
    report_rows = read_delta_table(run.gold_dir / RESIDUAL_REPORT_TABLE).to_pylist()
    allocation_rows = read_delta_table(run.gold_dir / "allocation").to_pylist()
    fact_rows = read_delta_table(run.silver_dir / "fact_gl_cost").to_pylist()

    detail_total = sum((row["amount_eur"] for row in detail_rows), start=Decimal("0.00"))
    report_total = sum((row["residual_amount_eur"] for row in report_rows), start=Decimal("0.00"))
    allocated_by_gl_line: dict[str, Decimal] = {}
    for row in allocation_rows:
        allocated_by_gl_line[row["gl_line_id"]] = allocated_by_gl_line.get(
            row["gl_line_id"], Decimal("0.00")
        ) + row["allocated_amount_eur"]
    expected_by_gl_line = {row["gl_line_id"]: row["amount_eur"] for row in fact_rows}
    residual_gl_lines = {row["gl_line_id"] for row in detail_rows}

    assert detail_total == report_total
    for gl_line_id in residual_gl_lines:
        assert allocated_by_gl_line.get(gl_line_id, Decimal("0.00")) != expected_by_gl_line[gl_line_id]


def test_driver_zero_is_rule_version_dependent_and_both_versions_reconcile(
    residual,
    test_workspace: Path,
) -> None:
    """Driver-zero residual should appear under storage_gb and differ from v1 while both reconcile."""
    v1_run, v1_result = _build_v1_residual(residual)
    storage_run, storage_result = _residual_rule_run(
        residual,
        test_workspace,
        version_id="v_test_storage_driver_zero",
        app_metric="storage_gb",
    )

    v1_report = read_delta_table(v1_run.gold_dir / RESIDUAL_REPORT_TABLE).to_pylist()
    storage_report = read_delta_table(storage_run.gold_dir / RESIDUAL_REPORT_TABLE).to_pylist()
    v1_driver_zero = sum(
        (row["residual_amount_eur"] for row in v1_report if row["reason_code"] == "driver_zero"),
        start=Decimal("0.00"),
    )
    storage_driver_zero = sum(
        (row["residual_amount_eur"] for row in storage_report if row["reason_code"] == "driver_zero"),
        start=Decimal("0.00"),
    )

    assert v1_result.reconciliation.total_allocated_eur + v1_result.reconciliation.total_residual_eur == DEFAULT_GL_TOTAL_EUR
    assert storage_result.reconciliation.total_allocated_eur + storage_result.reconciliation.total_residual_eur == DEFAULT_GL_TOTAL_EUR
    assert storage_driver_zero > Decimal("0.00")
    assert storage_driver_zero != v1_driver_zero


def test_pct_of_total_gl_uses_deterministic_rounding(residual) -> None:
    """The rounded percentage column should sum to the residual share of total GL deterministically."""
    run, result = _build_v1_residual(residual)
    report_rows = read_delta_table(run.gold_dir / RESIDUAL_REPORT_TABLE).to_pylist()

    pct_total = sum((row["pct_of_total_gl"] for row in report_rows), start=Decimal("0.000000"))
    expected_pct = (result.reconciliation.total_residual_eur / result.reconciliation.total_gl_eur).quantize(
        PCT_QUANTUM
    )

    assert pct_total == expected_pct
