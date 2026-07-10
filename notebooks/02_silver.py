# Databricks notebook source
# MAGIC %md
# MAGIC # P-010 — 02 Silver
# MAGIC
# MAGIC Conforms bronze Delta tables into silver using **Spark SQL** — the same
# MAGIC transformation logic as the local `silver/conform.py` DuckDB pass, rewritten
# MAGIC for the Spark DataFrame/SQL API.  No DuckDB, no delta-rs, no local I/O code.
# MAGIC
# MAGIC Silver outputs: `dim_cost_center`, `dim_resource_tower`, `dim_application`,
# MAGIC `dim_business_unit`, `fact_gl_cost` (nullable `tower_id` from LEFT JOIN),
# MAGIC `fact_usage_metric`.
# MAGIC
# MAGIC **Key assertion:** `SUM(fact_gl_cost.amount_eur) == 61813.95` (Decimal exact).
# MAGIC
# MAGIC **Prerequisites:** run `00_setup.py` then `01_synth_and_bronze.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Re-establish setup (if running standalone)

# COMMAND ----------

import sys
from decimal import Decimal
from pathlib import Path

REPO_PATH = str(Path.cwd().parent)   # notebook runs from <repo>/notebooks
SRC_PATH = f"{REPO_PATH}/src"
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

VOLUME_ROOT = "/Volumes/workspace/default/tech_cost_platform"
PATHS = {
    "source": f"{VOLUME_ROOT}/source",
    "bronze": f"{VOLUME_ROOT}/bronze",
    "silver": f"{VOLUME_ROOT}/silver",
    "gold":   f"{VOLUME_ROOT}/gold",
}

from tech_cost_platform.synth.generate import DEFAULT_GL_TOTAL_EUR

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Load bronze tables into Spark SQL temp views

# COMMAND ----------

BRONZE_TABLES = [
    "gl_costs",
    "cost_centers",
    "resource_towers",
    "applications",
    "business_units",
    "usage_metrics",
]

for table_name in BRONZE_TABLES:
    bronze_path = f"{PATHS['bronze']}/{table_name}"
    df = spark.read.format("delta").load(bronze_path)
    df.createOrReplaceTempView(table_name)
    print(f"  {table_name:20s}  rows={df.count():4d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Conform dimensions
# MAGIC
# MAGIC Same SQL transforms as `silver/conform.py`, rewritten for Spark.
# MAGIC Column renames match the local silver schema exactly.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DecimalType

# dim_cost_center — NULLIF empty tower_id → NULL, preserved as nullable
dim_cost_center = spark.sql("""
    SELECT DISTINCT
        TRIM(cost_center_id) AS cost_center_id,
        TRIM(cost_center_name) AS name,
        NULLIF(TRIM(tower_id), '') AS tower_id
    FROM cost_centers
    ORDER BY cost_center_id, name, tower_id
""")
dim_cost_center.write.format("delta").mode("overwrite").save(f"{PATHS['silver']}/dim_cost_center")
dim_cost_center.createOrReplaceTempView("dim_cost_center")
print(f"dim_cost_center:    {dim_cost_center.count()} rows")

# dim_resource_tower
dim_resource_tower = spark.sql("""
    SELECT DISTINCT
        TRIM(tower_id) AS tower_id,
        TRIM(tower_name) AS name,
        TRIM(tower_type) AS type
    FROM resource_towers
    ORDER BY tower_id, name, type
""")
dim_resource_tower.write.format("delta").mode("overwrite").save(f"{PATHS['silver']}/dim_resource_tower")
dim_resource_tower.createOrReplaceTempView("dim_resource_tower")
print(f"dim_resource_tower: {dim_resource_tower.count()} rows")

# dim_application
dim_application = spark.sql("""
    SELECT DISTINCT
        TRIM(app_id) AS app_id,
        TRIM(app_name) AS name,
        TRIM(business_criticality) AS criticality
    FROM applications
    ORDER BY app_id, name, criticality
""")
dim_application.write.format("delta").mode("overwrite").save(f"{PATHS['silver']}/dim_application")
dim_application.createOrReplaceTempView("dim_application")
print(f"dim_application:    {dim_application.count()} rows")

# dim_business_unit
dim_business_unit = spark.sql("""
    SELECT DISTINCT
        TRIM(bu_id) AS bu_id,
        TRIM(bu_name) AS name
    FROM business_units
    ORDER BY bu_id, name
""")
dim_business_unit.write.format("delta").mode("overwrite").save(f"{PATHS['silver']}/dim_business_unit")
dim_business_unit.createOrReplaceTempView("dim_business_unit")
print(f"dim_business_unit:  {dim_business_unit.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Conform facts

# COMMAND ----------

# fact_gl_cost — LEFT JOIN to pick up tower_id from cost_center mapping.
# CC-LEGACY has no tower in dim_cost_center → NULL tower_id propagates forward.
fact_gl_cost = spark.sql("""
    SELECT
        TRIM(gl.gl_line_id)      AS gl_line_id,
        TRIM(gl.period)          AS period,
        TRIM(gl.gl_account)      AS gl_account,
        TRIM(gl.cost_center_id)  AS cost_center_id,
        CAST(gl.amount_eur AS DECIMAL(18,2)) AS amount_eur,
        dim.tower_id             AS tower_id
    FROM gl_costs AS gl
    LEFT JOIN dim_cost_center AS dim
        ON TRIM(gl.cost_center_id) = dim.cost_center_id
    ORDER BY gl_line_id
""")
fact_gl_cost.write.format("delta").mode("overwrite").save(f"{PATHS['silver']}/fact_gl_cost")
fact_gl_cost.createOrReplaceTempView("fact_gl_cost")
print(f"fact_gl_cost:       {fact_gl_cost.count()} rows")

# fact_usage_metric
fact_usage_metric = spark.sql("""
    SELECT
        TRIM(metric_id)   AS metric_id,
        TRIM(period)      AS period,
        TRIM(step)        AS step,
        TRIM(from_id)     AS from_id,
        TRIM(to_id)       AS to_id,
        TRIM(metric_name) AS metric_name,
        CAST(value AS DECIMAL(18,2)) AS value
    FROM usage_metrics
    ORDER BY metric_id
""")
fact_usage_metric.write.format("delta").mode("overwrite").save(f"{PATHS['silver']}/fact_usage_metric")
fact_usage_metric.createOrReplaceTempView("fact_usage_metric")
print(f"fact_usage_metric:  {fact_usage_metric.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · DQ assertions

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, col, count

# Core assertion: GL total must be preserved exactly after cast
gl_total_row = spark.sql("SELECT SUM(amount_eur) AS total FROM fact_gl_cost").collect()[0]
gl_total = Decimal(str(gl_total_row["total"]))
assert gl_total == DEFAULT_GL_TOTAL_EUR, (
    f"Silver GL total mismatch: {gl_total} != {DEFAULT_GL_TOTAL_EUR}"
)
print(f"SUM(fact_gl_cost.amount_eur) == {gl_total} ✓")

# At least one GL line must have NULL tower_id (CC-LEGACY unmapped seeding)
null_tower_gl = spark.sql(
    "SELECT COUNT(*) AS cnt FROM fact_gl_cost WHERE tower_id IS NULL"
).collect()[0]["cnt"]
assert null_tower_gl >= 1, f"Expected unmapped GL lines (CC-LEGACY); got {null_tower_gl}"
print(f"GL lines with NULL tower_id (unmapped CC-LEGACY): {null_tower_gl} ✓")

# Usage metric values must not be negative
negative_metrics = spark.sql(
    "SELECT COUNT(*) AS cnt FROM fact_usage_metric WHERE value < 0"
).collect()[0]["cnt"]
assert negative_metrics == 0, f"Negative metric values found: {negative_metrics}"
print(f"Negative metric values: {negative_metrics} ✓")

print("\nAll silver DQ assertions passed ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver complete
# MAGIC
# MAGIC Six conformed Delta tables written under `{VOLUME_ROOT}/silver/`.
# MAGIC GL total preserved exactly through Spark DECIMAL cast.
# MAGIC `fact_gl_cost.tower_id` is NULL for CC-LEGACY rows — correctly propagates
# MAGIC unmapped residual in the engine notebook.
