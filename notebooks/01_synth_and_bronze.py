# Databricks notebook source

# MAGIC %md
# MAGIC # P-010 — 01 Synth & Bronze
# MAGIC
# MAGIC Calls the **unchanged `synth` module** to write deterministic CSV exports into
# MAGIC the UC volume, then ingests each CSV into a **Spark Delta** bronze table.
# MAGIC Bronze tables use explicit schemas (no schema inference) and `DecimalType(18,2)`
# MAGIC for all money columns.  Pydantic contract validation runs on the driver after
# MAGIC collecting the tiny tables.
# MAGIC
# MAGIC **Prerequisites:** run `00_setup.py` first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Re-establish setup (if running standalone)

# COMMAND ----------

import sys

REPO_PATH = "/Workspace/Repos/shervin-taheripour/tech-cost-platform"
SRC_PATH = f"{REPO_PATH}/src"
RULES_DIR = f"{REPO_PATH}/config/rules"
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

VOLUME_ROOT = "/Volumes/workspace/default/tech_cost_platform"
PATHS = {
    "source": f"{VOLUME_ROOT}/source",
    "bronze": f"{VOLUME_ROOT}/bronze",
    "silver": f"{VOLUME_ROOT}/silver",
    "gold":   f"{VOLUME_ROOT}/gold",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Generate source CSVs (unchanged `synth` module)

# COMMAND ----------

from tech_cost_platform.synth.generate import generate_source_exports, DEFAULT_GL_TOTAL_EUR
from tech_cost_platform.synth.schema import SynthConfig

# SynthConfig with the UC volume as output_dir.
# generate_source_exports uses Path(output_dir).is_absolute() → writes directly to volume.
synth_config = SynthConfig(
    seed=20260107,
    period="2026-01",
    output_dir=PATHS["source"],
)

output_paths = generate_source_exports(config=synth_config)

print(f"Synth complete — seed={synth_config.seed} period={synth_config.period}")
print(f"DEFAULT_GL_TOTAL_EUR = {DEFAULT_GL_TOTAL_EUR}")
for name, path in output_paths.items():
    print(f"  {name:20s} → {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Pydantic contract validation on the driver
# MAGIC
# MAGIC Collect each CSV from the volume and validate every row against the synth
# MAGIC schema models.  These models are byte-for-byte identical to what the local
# MAGIC bronze ingest uses — validating here mirrors the local ingestion boundary
# MAGIC without importing any I/O-layer code.

# COMMAND ----------

import csv
from decimal import Decimal
from pathlib import Path

from tech_cost_platform.synth.schema import (
    GLCost,
    CostCenter,
    ResourceTower,
    Application,
    BusinessUnit,
    UsageMetric,
)

CONTRACTS = {
    "gl_costs":       (GLCost,        ["gl_line_id", "period", "gl_account", "cost_center_id", "amount_eur", "description"]),
    "cost_centers":   (CostCenter,    ["cost_center_id", "cost_center_name", "tower_id"]),
    "resource_towers":(ResourceTower, ["tower_id", "tower_name", "tower_type"]),
    "applications":   (Application,   ["app_id", "app_name", "business_criticality"]),
    "business_units": (BusinessUnit,  ["bu_id", "bu_name"]),
    "usage_metrics":  (UsageMetric,   ["metric_id", "period", "step", "from_id", "to_id", "metric_name", "value"]),
}

validation_summary = {}
for table_name, (model_cls, _columns) in CONTRACTS.items():
    csv_path = Path(output_paths[table_name])
    with csv_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    validated = [model_cls.model_validate(r) for r in rows]
    validation_summary[table_name] = len(validated)
    print(f"  {table_name:20s}  rows={len(validated):4d}  ✓")

gl_total_from_csv = sum(r.amount_eur for r in [GLCost.model_validate(r)
    for r in csv.DictReader(open(output_paths["gl_costs"], encoding="utf-8"))])
assert gl_total_from_csv == DEFAULT_GL_TOTAL_EUR, (
    f"GL total mismatch: csv={gl_total_from_csv} expected={DEFAULT_GL_TOTAL_EUR}"
)
print(f"\nGL total validated: {gl_total_from_csv} == {DEFAULT_GL_TOTAL_EUR} ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Ingest CSVs → Spark Delta bronze tables
# MAGIC
# MAGIC Explicit schemas — no inference.  Money columns use `DecimalType(18,2)`.
# MAGIC Nullable `tower_id` in cost_centers is preserved as a nullable string.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DecimalType,
)

# Schema definitions matching the synth CSV column order exactly.
BRONZE_SCHEMAS = {
    "gl_costs": StructType([
        StructField("gl_line_id",      StringType(),       nullable=False),
        StructField("period",          StringType(),       nullable=False),
        StructField("gl_account",      StringType(),       nullable=False),
        StructField("cost_center_id",  StringType(),       nullable=False),
        StructField("amount_eur",      DecimalType(18, 2), nullable=False),
        StructField("description",     StringType(),       nullable=True),
    ]),
    "cost_centers": StructType([
        StructField("cost_center_id",   StringType(), nullable=False),
        StructField("cost_center_name", StringType(), nullable=False),
        StructField("tower_id",         StringType(), nullable=True),
    ]),
    "resource_towers": StructType([
        StructField("tower_id",   StringType(), nullable=False),
        StructField("tower_name", StringType(), nullable=False),
        StructField("tower_type", StringType(), nullable=False),
    ]),
    "applications": StructType([
        StructField("app_id",               StringType(), nullable=False),
        StructField("app_name",             StringType(), nullable=False),
        StructField("business_criticality", StringType(), nullable=False),
    ]),
    "business_units": StructType([
        StructField("bu_id",   StringType(), nullable=False),
        StructField("bu_name", StringType(), nullable=False),
    ]),
    "usage_metrics": StructType([
        StructField("metric_id",   StringType(),       nullable=False),
        StructField("period",      StringType(),       nullable=False),
        StructField("step",        StringType(),       nullable=False),
        StructField("from_id",     StringType(),       nullable=False),
        StructField("to_id",       StringType(),       nullable=False),
        StructField("metric_name", StringType(),       nullable=False),
        StructField("value",       DecimalType(18, 2), nullable=False),
    ]),
}

# COMMAND ----------

# Ingest each CSV → Spark Delta at PATHS["bronze"]/<table_name>/
for table_name, schema in BRONZE_SCHEMAS.items():
    csv_file = str(output_paths[table_name])
    bronze_path = f"{PATHS['bronze']}/{table_name}"

    df = (
        spark.read
        .option("header", "true")
        .option("emptyValue", None)
        .option("nullValue", "")
        .schema(schema)
        .csv(csv_file)
    )

    df.write.format("delta").mode("overwrite").save(bronze_path)

    count = spark.read.format("delta").load(bronze_path).count()
    print(f"  {table_name:20s}  rows={count:4d}  → {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Bronze sanity checks

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, col

gl_cost_df = spark.read.format("delta").load(f"{PATHS['bronze']}/gl_costs")
bronze_gl_total = gl_cost_df.agg(spark_sum(col("amount_eur")).alias("total")).collect()[0]["total"]

assert Decimal(str(bronze_gl_total)) == DEFAULT_GL_TOTAL_EUR, (
    f"Bronze GL total mismatch: {bronze_gl_total} != {DEFAULT_GL_TOTAL_EUR}"
)
print(f"Bronze GL total: {bronze_gl_total} == {DEFAULT_GL_TOTAL_EUR} ✓")

# Confirm nullable tower_id is preserved
cc_df = spark.read.format("delta").load(f"{PATHS['bronze']}/cost_centers")
null_tower_count = cc_df.filter(col("tower_id").isNull()).count()
print(f"Cost centers with null tower_id (expected ≥1 for CC-LEGACY): {null_tower_count}")
assert null_tower_count >= 1, "Expected at least one cost center with null tower_id"
print("All bronze assertions passed ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze complete
# MAGIC
# MAGIC Six Delta tables written under `{VOLUME_ROOT}/bronze/`.
# MAGIC GL total validated against the deterministic fixture value `61813.95`.
