"""Silver-layer cleaning and conformance transforms."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

BRONZE_TABLE_NAMES = (
    "gl_costs",
    "cost_centers",
    "resource_towers",
    "applications",
    "business_units",
    "usage_metrics",
)
SILVER_TABLE_NAMES = (
    "dim_cost_center",
    "dim_resource_tower",
    "dim_application",
    "dim_business_unit",
    "fact_gl_cost",
    "fact_usage_metric",
)
DECIMAL_18_2 = DecimalType(18, 2)


@dataclass(frozen=True)
class SilverConformanceResult:
    """Intermediate normalized inputs plus final conformed outputs."""

    dimension_candidates: Mapping[str, DataFrame]
    tables: Mapping[str, DataFrame]


def _normalize_required_text(column_name: str):
    return F.trim(F.col(column_name))


def _normalize_optional_text(column_name: str):
    trimmed = F.trim(F.col(column_name))
    return F.when(F.length(trimmed) == 0, F.lit(None)).otherwise(trimmed)


def _normalize_period(column_name: str):
    trimmed = F.trim(F.col(column_name))
    return F.when(trimmed.rlike(r"^\d{4}-\d{2}$"), trimmed).otherwise(F.lit(None))


def read_bronze_tables(spark: SparkSession, bronze_dir: Path) -> dict[str, DataFrame]:
    """Load every expected bronze Delta table from the supplied root."""
    bronze_tables: dict[str, DataFrame] = {}

    for table_name in BRONZE_TABLE_NAMES:
        table_path = bronze_dir / table_name
        if not table_path.exists():
            raise FileNotFoundError(f"Expected bronze Delta table for {table_name}: {table_path}")
        bronze_tables[table_name] = spark.read.format("delta").load(str(table_path))

    return bronze_tables


def conform_bronze_tables(bronze_tables: Mapping[str, DataFrame]) -> SilverConformanceResult:
    """Normalize bronze tables into conformed fact and dimension candidates."""
    dim_cost_center_candidate = bronze_tables["cost_centers"].select(
        _normalize_required_text("cost_center_id").alias("cost_center_id"),
        _normalize_required_text("cost_center_name").alias("name"),
        _normalize_optional_text("tower_id").alias("tower_id"),
    )
    dim_resource_tower_candidate = bronze_tables["resource_towers"].select(
        _normalize_required_text("tower_id").alias("tower_id"),
        _normalize_required_text("tower_name").alias("name"),
        _normalize_required_text("tower_type").alias("type"),
    )
    dim_application_candidate = bronze_tables["applications"].select(
        _normalize_required_text("app_id").alias("app_id"),
        _normalize_required_text("app_name").alias("name"),
        _normalize_required_text("business_criticality").alias("criticality"),
    )
    dim_business_unit_candidate = bronze_tables["business_units"].select(
        _normalize_required_text("bu_id").alias("bu_id"),
        _normalize_required_text("bu_name").alias("name"),
    )

    dim_cost_center = dim_cost_center_candidate.dropDuplicates()
    dim_resource_tower = dim_resource_tower_candidate.dropDuplicates()
    dim_application = dim_application_candidate.dropDuplicates()
    dim_business_unit = dim_business_unit_candidate.dropDuplicates()

    fact_gl_cost_base = bronze_tables["gl_costs"].select(
        _normalize_required_text("gl_line_id").alias("gl_line_id"),
        _normalize_period("period").alias("period"),
        _normalize_required_text("gl_account").alias("gl_account"),
        _normalize_required_text("cost_center_id").alias("cost_center_id"),
        F.col("amount_eur").cast(DECIMAL_18_2).alias("amount_eur"),
    )
    fact_gl_cost = fact_gl_cost_base.join(
        dim_cost_center.select("cost_center_id", "tower_id"),
        on="cost_center_id",
        how="left",
    ).select("gl_line_id", "period", "gl_account", "cost_center_id", "amount_eur", "tower_id")

    fact_usage_metric = bronze_tables["usage_metrics"].select(
        _normalize_required_text("metric_id").alias("metric_id"),
        _normalize_period("period").alias("period"),
        _normalize_required_text("step").alias("step"),
        _normalize_required_text("from_id").alias("from_id"),
        _normalize_required_text("to_id").alias("to_id"),
        _normalize_required_text("metric_name").alias("metric_name"),
        F.col("value").cast(DECIMAL_18_2).alias("value"),
    )

    return SilverConformanceResult(
        dimension_candidates={
            "dim_cost_center": dim_cost_center_candidate,
            "dim_resource_tower": dim_resource_tower_candidate,
            "dim_application": dim_application_candidate,
            "dim_business_unit": dim_business_unit_candidate,
        },
        tables={
            "dim_cost_center": dim_cost_center,
            "dim_resource_tower": dim_resource_tower,
            "dim_application": dim_application,
            "dim_business_unit": dim_business_unit,
            "fact_gl_cost": fact_gl_cost,
            "fact_usage_metric": fact_usage_metric,
        },
    )
