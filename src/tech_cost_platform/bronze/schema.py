"""Explicit Spark schemas for bronze CSV ingestion."""

from __future__ import annotations

from pyspark.sql.types import DecimalType, StringType, StructField, StructType

GL_COST_SCHEMA = StructType(
    [
        StructField("gl_line_id", StringType(), True),
        StructField("period", StringType(), True),
        StructField("gl_account", StringType(), True),
        StructField("cost_center_id", StringType(), True),
        StructField("amount_eur", DecimalType(18, 2), True),
        StructField("description", StringType(), True),
    ]
)

COST_CENTER_SCHEMA = StructType(
    [
        StructField("cost_center_id", StringType(), True),
        StructField("cost_center_name", StringType(), True),
        StructField("tower_id", StringType(), True),
    ]
)

RESOURCE_TOWER_SCHEMA = StructType(
    [
        StructField("tower_id", StringType(), True),
        StructField("tower_name", StringType(), True),
        StructField("tower_type", StringType(), True),
    ]
)

APPLICATION_SCHEMA = StructType(
    [
        StructField("app_id", StringType(), True),
        StructField("app_name", StringType(), True),
        StructField("business_criticality", StringType(), True),
    ]
)

BUSINESS_UNIT_SCHEMA = StructType(
    [
        StructField("bu_id", StringType(), True),
        StructField("bu_name", StringType(), True),
    ]
)

USAGE_METRIC_SCHEMA = StructType(
    [
        StructField("metric_id", StringType(), True),
        StructField("period", StringType(), True),
        StructField("step", StringType(), True),
        StructField("from_id", StringType(), True),
        StructField("to_id", StringType(), True),
        StructField("metric_name", StringType(), True),
        StructField("value", DecimalType(18, 2), True),
    ]
)
