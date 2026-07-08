"""Native Spark data-quality checks for silver conformance."""

from __future__ import annotations

from decimal import Decimal
from typing import Sequence

from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..synth.generate import DEFAULT_GL_TOTAL_EUR
from .conform import SilverConformanceResult


class DQCheckResult(BaseModel):
    """Outcome for a single silver data-quality check."""

    model_config = ConfigDict(frozen=True)

    name: str
    passed: bool
    failure_count: int = Field(ge=0)
    detail: str | None = None


class SilverDQReport(BaseModel):
    """Collection of silver DQ outcomes."""

    model_config = ConfigDict(frozen=True)

    checks: tuple[DQCheckResult, ...]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failed_checks(self) -> tuple[DQCheckResult, ...]:
        return tuple(check for check in self.checks if not check.passed)


def _check(name: str, failure_count: int, detail: str | None = None) -> DQCheckResult:
    return DQCheckResult(
        name=name,
        passed=failure_count == 0,
        failure_count=failure_count,
        detail=detail,
    )


def _duplicate_key_count(dataframe: DataFrame, key_columns: Sequence[str]) -> int:
    return dataframe.groupBy(*key_columns).count().where(F.col("count") > 1).count()


def _conflicting_dimension_key_count(
    dataframe: DataFrame, key_column: str, attribute_columns: Sequence[str]
) -> int:
    attribute_struct = F.struct(*[F.col(column_name) for column_name in attribute_columns])
    return (
        dataframe.groupBy(key_column)
        .agg(F.countDistinct(attribute_struct).alias("variant_count"))
        .where(F.col("variant_count") > 1)
        .count()
    )


def _anti_join_count(
    left: DataFrame,
    right: DataFrame,
    join_pairs: Sequence[tuple[str, str]],
    *,
    ignore_null_left_columns: Sequence[str] | None = None,
) -> int:
    filtered_left = left
    for column_name in ignore_null_left_columns or ():
        filtered_left = filtered_left.where(F.col(column_name).isNotNull())

    left_alias = filtered_left.alias("left")
    right_alias = right.alias("right")
    join_condition = None
    for left_column, right_column in join_pairs:
        candidate = F.col(f"left.{left_column}") == F.col(f"right.{right_column}")
        join_condition = candidate if join_condition is None else join_condition & candidate

    return left_alias.join(right_alias, join_condition, "left_anti").count()


def _non_negative_count(dataframe: DataFrame, column_name: str) -> int:
    return dataframe.where(F.col(column_name) < 0).count()


def _gl_total(dataframe: DataFrame) -> Decimal:
    total = dataframe.selectExpr("CAST(sum(amount_eur) AS DECIMAL(18,2)) AS total").collect()[0]["total"]
    return total if total is not None else Decimal("0.00")


def run_silver_dq_checks(conformance: SilverConformanceResult) -> SilverDQReport:
    """Run the silver DQ suite against conformed candidate and final tables."""
    tables = conformance.tables
    dimension_candidates = conformance.dimension_candidates
    gl_total = _gl_total(tables["fact_gl_cost"])

    checks = (
        _check(
            "dim_cost_center_conflicting_pk",
            _conflicting_dimension_key_count(
                dimension_candidates["dim_cost_center"], "cost_center_id", ("name", "tower_id")
            ),
        ),
        _check(
            "dim_resource_tower_conflicting_pk",
            _conflicting_dimension_key_count(
                dimension_candidates["dim_resource_tower"], "tower_id", ("name", "type")
            ),
        ),
        _check(
            "dim_application_conflicting_pk",
            _conflicting_dimension_key_count(
                dimension_candidates["dim_application"], "app_id", ("name", "criticality")
            ),
        ),
        _check(
            "dim_business_unit_conflicting_pk",
            _conflicting_dimension_key_count(
                dimension_candidates["dim_business_unit"], "bu_id", ("name",)
            ),
        ),
        _check(
            "dim_cost_center_pk_unique",
            _duplicate_key_count(tables["dim_cost_center"], ("cost_center_id",)),
        ),
        _check(
            "dim_resource_tower_pk_unique",
            _duplicate_key_count(tables["dim_resource_tower"], ("tower_id",)),
        ),
        _check(
            "dim_application_pk_unique",
            _duplicate_key_count(tables["dim_application"], ("app_id",)),
        ),
        _check(
            "dim_business_unit_pk_unique",
            _duplicate_key_count(tables["dim_business_unit"], ("bu_id",)),
        ),
        _check(
            "fact_gl_cost_pk_unique",
            _duplicate_key_count(tables["fact_gl_cost"], ("gl_line_id",)),
        ),
        _check(
            "dim_cost_center_tower_fk",
            _anti_join_count(
                tables["dim_cost_center"],
                tables["dim_resource_tower"],
                (("tower_id", "tower_id"),),
                ignore_null_left_columns=("tower_id",),
            ),
            "NULL tower_id is intentionally allowed; only non-null tower keys must resolve.",
        ),
        _check(
            "fact_gl_cost_cost_center_fk",
            _anti_join_count(
                tables["fact_gl_cost"],
                tables["dim_cost_center"],
                (("cost_center_id", "cost_center_id"),),
            ),
        ),
        _check(
            "fact_gl_cost_tower_fk",
            _anti_join_count(
                tables["fact_gl_cost"],
                tables["dim_resource_tower"],
                (("tower_id", "tower_id"),),
                ignore_null_left_columns=("tower_id",),
            ),
            "NULL tower_id is intentionally allowed for the unmapped residual anchor.",
        ),
        _check(
            "fact_usage_metric_tower_to_app_from_fk",
            _anti_join_count(
                tables["fact_usage_metric"].where(F.col("step") == "tower_to_app"),
                tables["dim_resource_tower"],
                (("from_id", "tower_id"),),
            ),
        ),
        _check(
            "fact_usage_metric_tower_to_app_to_fk",
            _anti_join_count(
                tables["fact_usage_metric"].where(F.col("step") == "tower_to_app"),
                tables["dim_application"],
                (("to_id", "app_id"),),
            ),
        ),
        _check(
            "fact_usage_metric_app_to_bu_from_fk",
            _anti_join_count(
                tables["fact_usage_metric"].where(F.col("step") == "app_to_bu"),
                tables["dim_application"],
                (("from_id", "app_id"),),
            ),
        ),
        _check(
            "fact_usage_metric_app_to_bu_to_fk",
            _anti_join_count(
                tables["fact_usage_metric"].where(F.col("step") == "app_to_bu"),
                tables["dim_business_unit"],
                (("to_id", "bu_id"),),
            ),
        ),
        _check(
            "fact_gl_cost_non_negative_amount",
            _non_negative_count(tables["fact_gl_cost"], "amount_eur"),
        ),
        _check(
            "fact_usage_metric_non_negative_value",
            _non_negative_count(tables["fact_usage_metric"], "value"),
        ),
        _check(
            "fact_gl_cost_reconciliation",
            0 if gl_total == DEFAULT_GL_TOTAL_EUR else 1,
            f"expected={DEFAULT_GL_TOTAL_EUR} actual={gl_total}",
        ),
    )
    return SilverDQReport(checks=checks)


def summarize_failed_checks(report: SilverDQReport) -> str:
    """Render a compact failure summary for raised exceptions."""
    lines = ["Silver data-quality checks failed:"]
    for check in report.failed_checks:
        detail = f" detail={check.detail}" if check.detail else ""
        lines.append(f"- {check.name}: failure_count={check.failure_count}{detail}")
    return "\n".join(lines)
