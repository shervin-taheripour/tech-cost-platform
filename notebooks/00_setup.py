# Databricks notebook source

# MAGIC %md
# MAGIC # P-010 — 00 Setup
# MAGIC
# MAGIC Establishes volume paths, wires the Git-folder repo into `sys.path`, and
# MAGIC defines the `PATHS` constant block used by every subsequent notebook.
# MAGIC
# MAGIC **Run this cell block before running any other P-010 notebook.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Git folder — import path

# COMMAND ----------

import sys

# After cloning the repo into a Databricks Git folder the source tree lives at:
#   /Workspace/Repos/<your-username>/tech-cost-platform/src
#
# To clone:
#   1. Open Workspace > Repos > Add Repo
#   2. URL: https://github.com/shervin-taheripour/tech-cost-platform.git
#   3. Wait for clone to complete.
#
# Alternatively, install the package from the repo:
#   %pip install git+https://github.com/shervin-taheripour/tech-cost-platform.git#subdirectory=src
#
# The Git-folder approach is preferred (no install required, always tracks latest).

REPO_PATH = "/Workspace/Repos/shervin-taheripour/tech-cost-platform"
SRC_PATH = f"{REPO_PATH}/src"
RULES_DIR = f"{REPO_PATH}/config/rules"

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

print(f"sys.path: {SRC_PATH}")
print(f"rules dir: {RULES_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · UC volume paths

# COMMAND ----------

VOLUME_ROOT = "/Volumes/workspace/default/tech_cost_platform"

PATHS = {
    "source": f"{VOLUME_ROOT}/source",
    "bronze": f"{VOLUME_ROOT}/bronze",
    "silver": f"{VOLUME_ROOT}/silver",
    "gold":   f"{VOLUME_ROOT}/gold",
}

for name, path in PATHS.items():
    print(f"  {name:8s} → {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Volume guard

# COMMAND ----------

try:
    entries = dbutils.fs.ls(VOLUME_ROOT)
    print(f"Volume accessible: {VOLUME_ROOT} ({len(entries)} entries)")
except Exception as exc:
    raise RuntimeError(
        f"UC volume not found or not accessible: {VOLUME_ROOT}\n"
        "Create it first with:\n"
        "  CREATE VOLUME workspace.default.tech_cost_platform\n"
        f"Original error: {exc}"
    ) from exc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Portable core smoke-test
# MAGIC
# MAGIC Verifies the three portable modules import correctly from the Git folder.
# MAGIC No `src/` modifications: if this cell fails the Git folder is not wired up
# MAGIC correctly — fix the path above rather than modifying any source file.

# COMMAND ----------

from tech_cost_platform.engine.strategies import (
    compute_strategy_outcome,
    distribute_amount,
    PROPORTION_ONE,
    REASON_SHARED_UNATTRIBUTABLE,
    REASON_DRIVER_ZERO,
)
from tech_cost_platform.rules import RuleRegistry
from tech_cost_platform.synth.generate import (
    generate_source_exports,
    DEFAULT_GL_TOTAL_EUR,
)
from tech_cost_platform.synth.schema import SynthConfig

print("engine.strategies  ✓")
print("rules.RuleRegistry ✓")
print("synth.generate     ✓")
print(f"DEFAULT_GL_TOTAL_EUR = {DEFAULT_GL_TOTAL_EUR}")
print(f"PROPORTION_ONE       = {PROPORTION_ONE}")
print(f"REASON_SHARED_UNATTRIBUTABLE = {REASON_SHARED_UNATTRIBUTABLE!r}")
print(f"REASON_DRIVER_ZERO           = {REASON_DRIVER_ZERO!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Spark Connect check
# MAGIC
# MAGIC Free Edition uses Spark Connect — there is no `SparkContext` and no RDD API.
# MAGIC This cell confirms the session type and guards against accidental `sc.` usage.

# COMMAND ----------

print(f"Spark version: {spark.version}")
print(f"Session type:  {type(spark).__module__}.{type(spark).__qualname__}")

# Confirm Spark Connect (no SparkContext)
try:
    _ = spark.sparkContext
    print("WARNING: SparkContext available — this may not be Spark Connect serverless.")
except AttributeError:
    print("SparkContext not available — confirmed Spark Connect serverless. ✓")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup complete
# MAGIC
# MAGIC `PATHS`, `RULES_DIR`, and `REPO_PATH` are now defined. Run notebooks
# MAGIC 01 → 02 → 03 → 04 in order.
