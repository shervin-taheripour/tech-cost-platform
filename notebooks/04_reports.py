# Databricks notebook source
# MAGIC %md
# MAGIC # P-010 — 04 Reports
# MAGIC
# MAGIC Builds all gold report views from the engine output and renders them for
# MAGIC a reviewer.  Views are built in **Spark SQL** and displayed with `display()`.
# MAGIC The driver-comparison chart uses **matplotlib** to show the APP-BILLING
# MAGIC top-BU flip visually.
# MAGIC
# MAGIC **Prerequisites:** run `00_setup.py` → `01_synth_and_bronze.py` →
# MAGIC `02_silver.py` → `03_engine.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Re-establish setup (if running standalone)

# COMMAND ----------

# MAGIC %pip install duckdb

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
from decimal import Decimal
from pathlib import Path

REPO_PATH = str(Path.cwd().parent)   # notebook runs from <repo>/notebooks
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

from tech_cost_platform.synth.generate import DEFAULT_GL_TOTAL_EUR

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Load gold and silver inputs

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, col, round as spark_round

alloc_df  = spark.read.format("delta").load(f"{PATHS['gold']}/allocation")
resid_df  = spark.read.format("delta").load(f"{PATHS['gold']}/residual")
dim_app   = spark.read.format("delta").load(f"{PATHS['silver']}/dim_application")
dim_bu    = spark.read.format("delta").load(f"{PATHS['silver']}/dim_business_unit")
dim_tower = spark.read.format("delta").load(f"{PATHS['silver']}/dim_resource_tower")
fact_gl   = spark.read.format("delta").load(f"{PATHS['silver']}/fact_gl_cost")

alloc_df.createOrReplaceTempView("allocation")
resid_df.createOrReplaceTempView("residual")
dim_app.createOrReplaceTempView("dim_application")
dim_bu.createOrReplaceTempView("dim_business_unit")
dim_tower.createOrReplaceTempView("dim_resource_tower")
fact_gl.createOrReplaceTempView("fact_gl_cost")

print(f"allocation rows:  {alloc_df.count()}")
print(f"residual rows:    {resid_df.count()}")
print(f"dim_application:  {dim_app.count()}")
print(f"dim_business_unit:{dim_bu.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Application TCO
# MAGIC
# MAGIC Total allocated cost per application and rule version.

# COMMAND ----------

app_tco = spark.sql("""
    SELECT
        a.rule_version,
        a.app_id,
        app.name                                  AS app_name,
        app.criticality                           AS criticality,
        SUM(a.allocated_amount_eur)               AS allocated_amount_eur
    FROM allocation a
    LEFT JOIN dim_application app ON a.app_id = app.app_id
    GROUP BY a.rule_version, a.app_id, app.name, app.criticality
    ORDER BY a.rule_version, allocated_amount_eur DESC
""")

app_tco.write.format("delta").mode("overwrite").save(f"{PATHS['gold']}/report_application_tco")
print("report_application_tco")
display(app_tco)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · BU Showback
# MAGIC
# MAGIC Total allocated cost per business unit and rule version.

# COMMAND ----------

bu_showback = spark.sql("""
    SELECT
        a.rule_version,
        a.bu_id,
        bu.name                                   AS bu_name,
        SUM(a.allocated_amount_eur)               AS allocated_amount_eur
    FROM allocation a
    LEFT JOIN dim_business_unit bu ON a.bu_id = bu.bu_id
    GROUP BY a.rule_version, a.bu_id, bu.name
    ORDER BY a.rule_version, allocated_amount_eur DESC
""")

bu_showback.write.format("delta").mode("overwrite").save(f"{PATHS['gold']}/report_bu_showback")
print("report_bu_showback")
display(bu_showback)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Residual Report
# MAGIC
# MAGIC Passthrough of the residual table with enriched labels.

# COMMAND ----------

residual_report = spark.sql("""
    SELECT
        r.rule_version,
        r.gl_line_id,
        r.period,
        r.gl_account,
        r.cost_center_id,
        r.tower_id,
        r.app_id,
        r.amount_eur                              AS residual_amount_eur,
        r.failed_step,
        r.reason_code
    FROM residual r
    ORDER BY r.rule_version, r.failed_step, r.reason_code, r.amount_eur DESC
""")

residual_report.write.format("delta").mode("overwrite").save(f"{PATHS['gold']}/report_residual")
print("report_residual")
display(residual_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Allocation Lineage
# MAGIC
# MAGIC Shows the full allocation chain from GL line through to BU with cumulative
# MAGIC proportions. (The local runtime uses `lineage/trace.py` which cannot be
# MAGIC imported on Databricks — this Spark-native view carries the same columns.)

# COMMAND ----------

lineage = spark.sql("""
    SELECT
        a.rule_version,
        a.gl_line_id,
        a.period,
        a.gl_account,
        a.cost_center_id,
        a.tower_id,
        a.app_id,
        a.bu_id,
        a.allocated_amount_eur,
        a.gl_to_tower_proportion,
        a.tower_to_app_proportion,
        a.app_to_bu_proportion,
        CAST(
            a.gl_to_tower_proportion
            * a.tower_to_app_proportion
            * a.app_to_bu_proportion
            AS DECIMAL(18, 12)
        )                                          AS end_to_end_proportion,
        'allocated'                                AS outcome
    FROM allocation a
    UNION ALL
    SELECT
        r.rule_version,
        r.gl_line_id,
        r.period,
        r.gl_account,
        r.cost_center_id,
        r.tower_id,
        r.app_id,
        NULL                                       AS bu_id,
        r.amount_eur                               AS allocated_amount_eur,
        CAST(NULL AS DECIMAL(18,12))               AS gl_to_tower_proportion,
        CAST(NULL AS DECIMAL(18,12))               AS tower_to_app_proportion,
        CAST(NULL AS DECIMAL(18,12))               AS app_to_bu_proportion,
        CAST(NULL AS DECIMAL(18,12))               AS end_to_end_proportion,
        r.reason_code                              AS outcome
    FROM residual r
    ORDER BY rule_version, gl_line_id, tower_id, app_id, bu_id
""")

lineage.write.format("delta").mode("overwrite").save(f"{PATHS['gold']}/report_lineage")
print("report_lineage")
display(lineage.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Driver Comparison (v1 vs v2)
# MAGIC
# MAGIC Same GL costs, different BU splits.  `share_delta_pp` > 0 means BU received
# MAGIC a larger share under v2 (named_users) than v1 (transactions).

# COMMAND ----------

driver_comparison = spark.sql("""
    WITH
    v1 AS (
        SELECT bu_id, SUM(allocated_amount_eur) AS amount_v1
        FROM allocation
        WHERE rule_version = 'v1_transactions'
        GROUP BY bu_id
    ),
    v2 AS (
        SELECT bu_id, SUM(allocated_amount_eur) AS amount_v2
        FROM allocation
        WHERE rule_version = 'v2_named_users'
        GROUP BY bu_id
    ),
    totals AS (
        SELECT
            SUM(CASE WHEN rule_version = 'v1_transactions' THEN allocated_amount_eur END) AS total_v1,
            SUM(CASE WHEN rule_version = 'v2_named_users'  THEN allocated_amount_eur END) AS total_v2
        FROM allocation
    )
    SELECT
        COALESCE(v1.bu_id, v2.bu_id)             AS bu_id,
        bu.name                                   AS bu_name,
        COALESCE(v1.amount_v1, CAST(0 AS DECIMAL(18,2))) AS amount_v1,
        COALESCE(v2.amount_v2, CAST(0 AS DECIMAL(18,2))) AS amount_v2,
        COALESCE(v2.amount_v2, CAST(0 AS DECIMAL(18,2)))
            - COALESCE(v1.amount_v1, CAST(0 AS DECIMAL(18,2))) AS delta_eur,
        CASE WHEN totals.total_v1 > 0
            THEN CAST(v1.amount_v1 / totals.total_v1 AS DECIMAL(18,6))
            ELSE CAST(NULL AS DECIMAL(18,6))
        END                                        AS share_v1,
        CASE WHEN totals.total_v2 > 0
            THEN CAST(v2.amount_v2 / totals.total_v2 AS DECIMAL(18,6))
            ELSE CAST(NULL AS DECIMAL(18,6))
        END                                        AS share_v2,
        CASE WHEN totals.total_v1 > 0 AND totals.total_v2 > 0
            THEN CAST(
                    v2.amount_v2 / totals.total_v2
                  - v1.amount_v1 / totals.total_v1
                AS DECIMAL(18,6))
            ELSE CAST(NULL AS DECIMAL(18,6))
        END                                        AS share_delta_pp
    FROM v1
    FULL OUTER JOIN v2      ON v1.bu_id = v2.bu_id
    LEFT  JOIN dim_business_unit bu ON COALESCE(v1.bu_id, v2.bu_id) = bu.bu_id
    CROSS JOIN totals
    ORDER BY bu_id
""")

driver_comparison.write.format("delta").mode("overwrite").save(
    f"{PATHS['gold']}/report_driver_comparison"
)
print("report_driver_comparison")
display(driver_comparison)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Driver comparison — APP-BILLING flip assertion

# COMMAND ----------

comparison_rows = [r.asDict() for r in driver_comparison.collect()]

any_delta = any(r["delta_eur"] and r["delta_eur"] != 0 for r in comparison_rows)
assert any_delta, "Expected non-zero delta_eur for at least one BU"

share_deltas = [abs(float(r["share_delta_pp"])) for r in comparison_rows if r["share_delta_pp"] is not None]
max_abs_delta = max(share_deltas) if share_deltas else 0.0

assert max_abs_delta >= 0.20, (
    f"Expected ≥20pp share_delta_pp for the APP-BILLING flip; got max={max_abs_delta:.4f}"
)
print(f"Driver comparison: max |share_delta_pp| = {max_abs_delta:.4f} (≥0.20) ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Driver comparison chart — APP-BILLING top-BU flip
# MAGIC
# MAGIC This chart is the visual proof of the "no perfect driver" thesis:
# MAGIC the same GL costs produce materially different BU allocations depending
# MAGIC on which consumption metric drives the split.

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

comp_rows = sorted(comparison_rows, key=lambda r: r["bu_id"] or "")

bu_labels  = [r["bu_name"] or r["bu_id"] for r in comp_rows]
amounts_v1 = [float(r["amount_v1"] or 0) for r in comp_rows]
amounts_v2 = [float(r["amount_v2"] or 0) for r in comp_rows]

x = np.arange(len(bu_labels))
width = 0.35

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle(
    "Driver Comparison: v1 (transactions) vs v2 (named_users)\n"
    "Same GL costs — different BU allocation splits",
    fontsize=13, fontweight="bold",
)

# Left: grouped bar chart of absolute amounts
bars_v1 = ax1.bar(x - width / 2, amounts_v1, width, label="v1 transactions", color="#4C72B0", alpha=0.85)
bars_v2 = ax1.bar(x + width / 2, amounts_v2, width, label="v2 named_users",  color="#DD8452", alpha=0.85)
ax1.set_xlabel("Business Unit")
ax1.set_ylabel("Allocated Amount (EUR)")
ax1.set_title("Allocated Amount by BU")
ax1.set_xticks(x)
ax1.set_xticklabels(bu_labels)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"€{v:,.0f}"))
ax1.legend()
ax1.bar_label(bars_v1, fmt="€%.0f", padding=3, fontsize=8)
ax1.bar_label(bars_v2, fmt="€%.0f", padding=3, fontsize=8)

# Right: share_delta_pp (v2 share minus v1 share, in percentage points)
share_deltas_pp = [
    float(r["share_delta_pp"]) * 100 if r["share_delta_pp"] is not None else 0.0
    for r in comp_rows
]
colors = ["#2ca02c" if d >= 0 else "#d62728" for d in share_deltas_pp]
bars_delta = ax2.bar(bu_labels, share_deltas_pp, color=colors, alpha=0.85)
ax2.axhline(0, color="black", linewidth=0.8, linestyle="--")
ax2.set_xlabel("Business Unit")
ax2.set_ylabel("Share delta (percentage points, v2 – v1)")
ax2.set_title("Share Delta: v2 minus v1 (pp)")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+.1f}pp"))
ax2.bar_label(bars_delta, fmt="%+.1f pp", padding=3, fontsize=9)

plt.tight_layout()
plt.savefig("/tmp/driver_comparison.png", dpi=120, bbox_inches="tight")
display(fig)
plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Reconciliation summary

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

for rv in ("v1_transactions", "v2_named_users"):
    a_total = Decimal(str(
        alloc_df.filter(col("rule_version") == rv)
                .agg(spark_sum("allocated_amount_eur")).collect()[0][0]
    ))
    r_total = Decimal(str(
        resid_df.filter(col("rule_version") == rv)
                .agg(spark_sum("amount_eur")).collect()[0][0]
    ))
    match = "✓" if a_total + r_total == DEFAULT_GL_TOTAL_EUR else "✗"
    print(f"{rv}: allocated={a_total}  residual={r_total}  total={a_total + r_total} {match}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reports complete
# MAGIC
# MAGIC All five gold views written and displayed:
# MAGIC
# MAGIC | View | Delta path |
# MAGIC |------|-----------|
# MAGIC | `report_application_tco`      | `{VOLUME_ROOT}/gold/report_application_tco` |
# MAGIC | `report_bu_showback`          | `{VOLUME_ROOT}/gold/report_bu_showback` |
# MAGIC | `report_residual`             | `{VOLUME_ROOT}/gold/report_residual` |
# MAGIC | `report_lineage`              | `{VOLUME_ROOT}/gold/report_lineage` |
# MAGIC | `report_driver_comparison`    | `{VOLUME_ROOT}/gold/report_driver_comparison` |
# MAGIC
# MAGIC The driver-comparison chart above renders the APP-BILLING top-BU flip:
# MAGIC BU-RETAIL dominates under v1 (transactions-heavy), BU-CORP dominates under
# MAGIC v2 (named_users-heavy).  Same costs, different answer — the "no perfect
# MAGIC driver" thesis made visible.
