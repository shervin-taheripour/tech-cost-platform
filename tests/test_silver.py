"""Offline tests for the silver conformance layer."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from tech_cost_platform.delta_tables import read_delta_table
from tech_cost_platform.silver.build import SilverDataQualityError
from tech_cost_platform.synth.generate import DEFAULT_GL_TOTAL_EUR

EXPECTED_TABLES = {
    "dim_cost_center",
    "dim_resource_tower",
    "dim_application",
    "dim_business_unit",
    "fact_gl_cost",
    "fact_usage_metric",
}
BAD_COST_CENTER_FIXTURE = Path("tests/fixtures/cost_centers_duplicate_conflict.csv").resolve()


def load_delta_rows(dataframe_root: Path, table_name: str) -> list[dict[str, object]]:
    """Read a Delta table from a per-test output root as Python rows."""
    return read_delta_table(dataframe_root / table_name).to_pylist()


def duplicate_key_count(rows: list[dict[str, object]], key_column: str) -> int:
    """Return the number of duplicated key groups in the supplied rows."""
    counts: dict[object, int] = {}
    for row in rows:
        counts[row[key_column]] = counts.get(row[key_column], 0) + 1
    return sum(1 for count in counts.values() if count > 1)


def unresolved_usage_fk_count(
    rows: list[dict[str, object]],
    dimension_rows: list[dict[str, object]],
    left_column: str,
    right_column: str,
) -> int:
    """Return unresolved usage-metric FK rows."""
    dimension_keys = {row[right_column] for row in dimension_rows}
    return sum(1 for row in rows if row[left_column] not in dimension_keys)


def test_silver_build_writes_all_conformed_tables_and_report_passes(silver) -> None:
    """Silver should produce all conformed Delta outputs on good seeded data."""
    run = silver()
    result = run.build()

    assert set(result.output_paths) == EXPECTED_TABLES
    assert result.dq_report.passed
    for table_name, output_path in result.output_paths.items():
        assert output_path.exists()
        assert len(load_delta_rows(run.silver_dir, table_name)) > 0


def test_silver_primary_keys_are_unique(silver) -> None:
    """Dimensions and the GL fact should each expose unique primary keys."""
    run = silver()
    run.build()

    assert duplicate_key_count(load_delta_rows(run.silver_dir, "dim_cost_center"), "cost_center_id") == 0
    assert duplicate_key_count(load_delta_rows(run.silver_dir, "dim_resource_tower"), "tower_id") == 0
    assert duplicate_key_count(load_delta_rows(run.silver_dir, "dim_application"), "app_id") == 0
    assert duplicate_key_count(load_delta_rows(run.silver_dir, "dim_business_unit"), "bu_id") == 0
    assert duplicate_key_count(load_delta_rows(run.silver_dir, "fact_gl_cost"), "gl_line_id") == 0


def test_silver_join_completeness_preserves_nullable_tower_exception(silver) -> None:
    """All governed joins should resolve, while the intentional null tower remains intact."""
    run = silver()
    run.build()

    dim_cost_center = load_delta_rows(run.silver_dir, "dim_cost_center")
    dim_resource_tower = load_delta_rows(run.silver_dir, "dim_resource_tower")
    dim_application = load_delta_rows(run.silver_dir, "dim_application")
    dim_business_unit = load_delta_rows(run.silver_dir, "dim_business_unit")
    fact_gl_cost = load_delta_rows(run.silver_dir, "fact_gl_cost")
    fact_usage_metric = load_delta_rows(run.silver_dir, "fact_usage_metric")

    cost_center_keys = {row["cost_center_id"] for row in dim_cost_center}
    tower_keys = {row["tower_id"] for row in dim_resource_tower}

    assert sum(1 for row in fact_gl_cost if row["cost_center_id"] not in cost_center_keys) == 0
    assert sum(
        1
        for row in dim_cost_center
        if row["tower_id"] is not None and row["tower_id"] not in tower_keys
    ) == 0
    assert sum(
        1
        for row in fact_gl_cost
        if row["tower_id"] is not None and row["tower_id"] not in tower_keys
    ) == 0
    assert (
        unresolved_usage_fk_count(
            [row for row in fact_usage_metric if row["step"] == "tower_to_app"],
            dim_resource_tower,
            "from_id",
            "tower_id",
        )
        == 0
    )
    assert (
        unresolved_usage_fk_count(
            [row for row in fact_usage_metric if row["step"] == "tower_to_app"],
            dim_application,
            "to_id",
            "app_id",
        )
        == 0
    )
    assert (
        unresolved_usage_fk_count(
            [row for row in fact_usage_metric if row["step"] == "app_to_bu"],
            dim_application,
            "from_id",
            "app_id",
        )
        == 0
    )
    assert (
        unresolved_usage_fk_count(
            [row for row in fact_usage_metric if row["step"] == "app_to_bu"],
            dim_business_unit,
            "to_id",
            "bu_id",
        )
        == 0
    )
    assert sum(1 for row in fact_gl_cost if row["tower_id"] is None) >= 1
    assert {row["cost_center_id"] for row in fact_gl_cost if row["tower_id"] is None} == {"CC-LEGACY"}


def test_silver_reconciliation_holds_at_governed_gl_total(silver) -> None:
    """Silver should preserve the governed GL aggregate after conformance."""
    run = silver()
    run.build()

    fact_gl_cost = load_delta_rows(run.silver_dir, "fact_gl_cost")
    total = sum((row["amount_eur"] for row in fact_gl_cost), start=Decimal("0.00"))

    assert total == DEFAULT_GL_TOTAL_EUR


def test_silver_dq_passes_on_good_data_and_fails_on_conflicting_dimension_duplicate(silver) -> None:
    """Silver DQ should pass on the governed fixture and reject conflicting duplicates."""
    good_run = silver()
    good_result = good_run.build()
    assert good_result.dq_report.passed

    bad_run = silver()
    bad_run.ingest_bronze(source_overrides={"cost_centers": BAD_COST_CENTER_FIXTURE})

    with pytest.raises(SilverDataQualityError) as exc_info:
        bad_run.build_from_bronze()

    failed_check_names = {check.name for check in exc_info.value.report.failed_checks}
    assert "dim_cost_center_conflicting_pk" in failed_check_names
    assert not (bad_run.silver_dir / "dim_cost_center").exists()


def test_silver_preserves_driver_zero_and_divergence_usage_signals(silver) -> None:
    """The usage metric fact should retain the downstream driver edge cases."""
    run = silver()
    run.build()

    fact_usage_metric = load_delta_rows(run.silver_dir, "fact_usage_metric")

    analytics_storage_total = sum(
        (
            row["value"]
            for row in fact_usage_metric
            if row["step"] == "app_to_bu"
            and row["from_id"] == "APP-ANALYTICS"
            and row["metric_name"] == "storage_gb"
        ),
        start=Decimal("0.00"),
    )
    analytics_named_users_total = sum(
        (
            row["value"]
            for row in fact_usage_metric
            if row["step"] == "app_to_bu"
            and row["from_id"] == "APP-ANALYTICS"
            and row["metric_name"] == "named_users"
        ),
        start=Decimal("0.00"),
    )
    email_app_to_bu_rows = sum(
        1
        for row in fact_usage_metric
        if row["step"] == "app_to_bu" and row["from_id"] == "APP-EMAIL"
    )
    email_tower_rows = sum(
        1
        for row in fact_usage_metric
        if row["step"] == "tower_to_app" and row["to_id"] == "APP-EMAIL"
    )

    named_users_rows = [
        row
        for row in fact_usage_metric
        if row["step"] == "app_to_bu"
        and row["from_id"] == "APP-BILLING"
        and row["metric_name"] == "named_users"
    ]
    transactions_rows = [
        row
        for row in fact_usage_metric
        if row["step"] == "app_to_bu"
        and row["from_id"] == "APP-BILLING"
        and row["metric_name"] == "transactions"
    ]

    named_users_total = sum((row["value"] for row in named_users_rows), start=Decimal("0.00"))
    transactions_total = sum((row["value"] for row in transactions_rows), start=Decimal("0.00"))
    named_users_shares = {row["to_id"]: row["value"] / named_users_total for row in named_users_rows}
    transactions_shares = {row["to_id"]: row["value"] / transactions_total for row in transactions_rows}
    top_named_users = max(named_users_shares.items(), key=lambda item: item[1])
    top_transactions = max(transactions_shares.items(), key=lambda item: item[1])

    assert analytics_storage_total == Decimal("0.00")
    assert analytics_named_users_total > Decimal("0.00")
    assert email_tower_rows >= 1
    assert email_app_to_bu_rows == 0
    assert top_named_users[0] != top_transactions[0]
    assert abs(top_named_users[1] - top_transactions[1]) >= Decimal("0.20")
