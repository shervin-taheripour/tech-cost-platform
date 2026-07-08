"""Offline tests for the silver conformance layer."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, functions as F

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


def load_delta_table(dataframe_root: Path, table_name: str, *, spark) -> DataFrame:
    """Read a Delta table from a per-test output root."""
    return spark.read.format("delta").load(str(dataframe_root / table_name))


def duplicate_key_count(dataframe: DataFrame, key_column: str) -> int:
    """Return the number of duplicated key groups in the supplied DataFrame."""
    return dataframe.groupBy(key_column).count().where(F.col("count") > 1).count()


def unresolved_usage_fk_count(
    dataframe: DataFrame, dimension: DataFrame, left_column: str, right_column: str
) -> int:
    """Return unresolved usage-metric FK rows."""
    return dataframe.join(dimension, dataframe[left_column] == dimension[right_column], "left_anti").count()


def test_silver_build_writes_all_conformed_tables_and_report_passes(silver) -> None:
    """Silver should produce all conformed Delta outputs on good seeded data."""
    run = silver()
    result = run.build()

    assert set(result.output_paths) == EXPECTED_TABLES
    assert result.dq_report.passed
    for table_name, output_path in result.output_paths.items():
        assert output_path.exists()
        assert load_delta_table(run.silver_dir, table_name, spark=run.spark).count() > 0


def test_silver_primary_keys_are_unique(silver) -> None:
    """Dimensions and the GL fact should each expose unique primary keys."""
    run = silver()
    run.build()

    assert (
        duplicate_key_count(load_delta_table(run.silver_dir, "dim_cost_center", spark=run.spark), "cost_center_id")
        == 0
    )
    assert (
        duplicate_key_count(
            load_delta_table(run.silver_dir, "dim_resource_tower", spark=run.spark), "tower_id"
        )
        == 0
    )
    assert (
        duplicate_key_count(load_delta_table(run.silver_dir, "dim_application", spark=run.spark), "app_id")
        == 0
    )
    assert (
        duplicate_key_count(load_delta_table(run.silver_dir, "dim_business_unit", spark=run.spark), "bu_id")
        == 0
    )
    assert (
        duplicate_key_count(load_delta_table(run.silver_dir, "fact_gl_cost", spark=run.spark), "gl_line_id")
        == 0
    )


def test_silver_join_completeness_preserves_nullable_tower_exception(silver) -> None:
    """All governed joins should resolve, while the intentional null tower remains intact."""
    run = silver()
    run.build()

    dim_cost_center = load_delta_table(run.silver_dir, "dim_cost_center", spark=run.spark)
    dim_resource_tower = load_delta_table(run.silver_dir, "dim_resource_tower", spark=run.spark)
    dim_application = load_delta_table(run.silver_dir, "dim_application", spark=run.spark)
    dim_business_unit = load_delta_table(run.silver_dir, "dim_business_unit", spark=run.spark)
    fact_gl_cost = load_delta_table(run.silver_dir, "fact_gl_cost", spark=run.spark)
    fact_usage_metric = load_delta_table(run.silver_dir, "fact_usage_metric", spark=run.spark)

    assert fact_gl_cost.join(dim_cost_center, "cost_center_id", "left_anti").count() == 0
    assert dim_cost_center.where("tower_id IS NOT NULL").join(dim_resource_tower, "tower_id", "left_anti").count() == 0
    assert fact_gl_cost.where("tower_id IS NOT NULL").join(dim_resource_tower, "tower_id", "left_anti").count() == 0
    assert (
        unresolved_usage_fk_count(
            fact_usage_metric.where("step = 'tower_to_app'"),
            dim_resource_tower,
            "from_id",
            "tower_id",
        )
        == 0
    )
    assert (
        unresolved_usage_fk_count(
            fact_usage_metric.where("step = 'tower_to_app'"),
            dim_application,
            "to_id",
            "app_id",
        )
        == 0
    )
    assert (
        unresolved_usage_fk_count(
            fact_usage_metric.where("step = 'app_to_bu'"),
            dim_application,
            "from_id",
            "app_id",
        )
        == 0
    )
    assert (
        unresolved_usage_fk_count(
            fact_usage_metric.where("step = 'app_to_bu'"),
            dim_business_unit,
            "to_id",
            "bu_id",
        )
        == 0
    )
    assert fact_gl_cost.where("tower_id IS NULL").count() >= 1
    assert {
        row["cost_center_id"]
        for row in fact_gl_cost.where("tower_id IS NULL").select("cost_center_id").distinct().collect()
    } == {"CC-LEGACY"}


def test_silver_reconciliation_holds_at_governed_gl_total(silver) -> None:
    """Silver should preserve the governed GL aggregate after conformance."""
    run = silver()
    run.build()

    fact_gl_cost = load_delta_table(run.silver_dir, "fact_gl_cost", spark=run.spark)
    total = fact_gl_cost.selectExpr("CAST(sum(amount_eur) AS DECIMAL(18,2)) AS total").collect()[0]["total"]

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

    fact_usage_metric = load_delta_table(run.silver_dir, "fact_usage_metric", spark=run.spark)

    analytics_storage_total = (
        fact_usage_metric.where(
            "step = 'app_to_bu' AND from_id = 'APP-ANALYTICS' AND metric_name = 'storage_gb'"
        )
        .selectExpr("CAST(sum(value) AS DECIMAL(18,2)) AS total")
        .collect()[0]["total"]
    )
    analytics_named_users_total = (
        fact_usage_metric.where(
            "step = 'app_to_bu' AND from_id = 'APP-ANALYTICS' AND metric_name = 'named_users'"
        )
        .selectExpr("CAST(sum(value) AS DECIMAL(18,2)) AS total")
        .collect()[0]["total"]
    )
    email_app_to_bu_rows = fact_usage_metric.where("step = 'app_to_bu' AND from_id = 'APP-EMAIL'").count()
    email_tower_rows = fact_usage_metric.where("step = 'tower_to_app' AND to_id = 'APP-EMAIL'").count()

    named_users_rows = (
        fact_usage_metric.where("step = 'app_to_bu' AND from_id = 'APP-BILLING' AND metric_name = 'named_users'")
        .select("to_id", "value")
        .collect()
    )
    transactions_rows = (
        fact_usage_metric.where("step = 'app_to_bu' AND from_id = 'APP-BILLING' AND metric_name = 'transactions'")
        .select("to_id", "value")
        .collect()
    )

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
