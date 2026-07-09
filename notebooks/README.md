# P-010 Databricks Notebooks — Local vs Free Edition Deltas

This document records every delta between the local DuckDB/delta-rs runtime
and Databricks Free Edition serverless, as required by P-010 acceptance
criterion 8.

---

## Running the notebooks

Run in order, top-to-bottom within each notebook:

1. `00_setup.py` — volume guard + sys.path wiring
2. `01_synth_and_bronze.py` — synth CSVs → Spark Delta bronze
3. `02_silver.py` — Spark SQL conformance → silver Delta
4. `03_engine.py` — driver-side cascade + cross-runtime assertions
5. `04_reports.py` — gold views + driver-comparison chart

**Git folder setup (required before running):**

1. Workspace → Repos → Add Repo
2. URL: `https://github.com/shervin-taheripour/tech-cost-platform.git`
3. Wait for clone. The notebooks import from `src/` at
   `/Workspace/Repos/shervin-taheripour/tech-cost-platform/src`.

---

## Delta 1 — Spark Connect (no SparkContext)

**Local runtime:** full PySpark with SparkContext, RDD API, Python UDFs via
`udf()`.

**Free Edition serverless:** Spark Connect only.  `spark.sparkContext` raises
`AttributeError`.  No RDD API.  No Python UDFs (Spark Connect does not support
the serialise-to-worker pattern that UDFs require).

**Adapter pattern used:** all Python-side math (cascade, strategies, Decimal
arithmetic) runs on the **driver** after collecting the tiny silver tables via
`.collect()`.  Spark is used only for I/O (reading and writing Delta) and for
the gold-view SQL aggregations — both of which use the DataFrame/SQL API that
works identically in Spark Connect.

---

## Delta 2 — Storage: UC volumes vs local file paths

**Local runtime:** Delta tables written to `data/{bronze,silver,gold}/` under
the repo root.  Paths are `pathlib.Path` objects; `delta-rs` writes them as
POSIX paths on Windows (`/c/Users/...` via WSL-style).

**Free Edition serverless:** Delta tables written to the Unity Catalog volume
`/Volumes/workspace/default/tech_cost_platform/{source,bronze,silver,gold}/`.

*   POSIX paths work directly (Linux-native on serverless).
*   `dbfs:/Volumes/...` is also valid but the POSIX form is preferred.
*   No DBFS mounts are used or required.

---

## Delta 3 — Library installation: no cluster libraries

**Local runtime:** packages installed in a virtualenv
(`.venv/Scripts/pip install -e src/`).

**Free Edition serverless:** no cluster-library install step.  The Git-folder
approach (`/Workspace/Repos/...`) makes `src/` directly importable via
`sys.path.insert(0, SRC_PATH)` — no `%pip install` needed as long as the
portable core's dependencies (`pydantic`, `pyyaml`) are available in the
serverless runtime (they are; both are standard Databricks runtime packages).

`delta-rs`, `duckdb`, and `pyarrow` are NOT imported in the notebooks.  Spark
handles all Delta I/O.

---

## Delta 4 — Decimal / type handling

**Local runtime:** `pyarrow.decimal128(18, 2)` for money; `pyarrow.decimal128(18, 12)`
for proportions.  `deltalake` writes/reads Decimal natively.  Python `Decimal`
objects flow through without coercion.

**Free Edition serverless — key differences and mitigations:**

| Concern | Risk | Mitigation |
|---------|------|-----------|
| Spark CSV reader infers numeric columns as `DoubleType` | Decimal precision lost on read | All CSV reads use **explicit `DecimalType(18,2)` schema** — no inference |
| `SUM()` on `DecimalType` returns `DecimalType` in Spark | Safe | No float cast in aggregations |
| Collecting `DecimalType` column via `.collect()` returns Python `Decimal` | Safe in Spark Connect | Converted with `Decimal(str(value))` before arithmetic |
| `createDataFrame` with Python `Decimal` and explicit `DecimalType` schema | Must not auto-cast to `DoubleType` | Schema is always explicit — Spark preserves precision |
| Spark SQL `/` on `DECIMAL` columns promotes to `DOUBLE` by default | Share percentages lose precision | Shares are display-only (not stored as money); money columns are never divided in Spark SQL |

The cross-runtime assertion in `03_engine.py` uses `Decimal(str(...))` before
comparing to the P-009 reference values — any silent float coercion would show
up as a mismatch and is reported as a **finding**, not papered over.

---

## Delta 5 — Modules not imported in notebooks

The following local I/O modules are **not imported** in any notebook.  They are
reimplemented using Spark DataFrames/SQL where needed, or their pure-Python
logic is inlined:

| Module | Reason not imported | Notebook approach |
|--------|--------------------|--------------------|
| `delta_tables.py` | imports `deltalake`, `pyarrow` | Spark `read/write.format("delta")` |
| `bronze/ingest.py` | reads local files, Arrow schemas | `spark.read.csv` with explicit schema |
| `silver/conform.py` | DuckDB | Spark SQL with same transform logic |
| `silver/dq.py` | DuckDB | Spark SQL assertions inline |
| `engine/cascade.py` | imports `delta_tables`, `runtime` | Cascade logic reimplemented inline in `03_engine.py`; pure functions from `engine.strategies` imported unchanged |
| `residual/*`, `lineage/*` | Arrow/delta-rs | Report views built from gold Delta tables in `04_reports.py` |
| `gold/*` | Arrow/delta-rs | Spark SQL report views in `04_reports.py` |
| `pipeline.py`, `runtime.py` | local path resolution | Paths set explicitly via `PATHS` constant block |

**Portable core imported unchanged (zero modifications to `src/`):**

- `tech_cost_platform.engine.strategies` — pure Python, `Decimal`, no I/O
- `tech_cost_platform.rules` — Pydantic v2 schema + YAML loader
- `tech_cost_platform.synth` — deterministic CSV generator

---

## Delta 6 — `build_usage_index` inlined

`build_usage_index` in `engine/cascade.py` is pure Python (no I/O), but its
module imports `delta_tables` and `runtime` at the top level, which would pull
in `deltalake` and local path code.  The function is therefore **inlined** in
`03_engine.py` rather than imported.  The logic is identical — this is not a
modification to the portable core.

---

## Cross-runtime assertion values

From the P-009 local run (committed `data/gold/allocation`):

| Version | Allocated (EUR) | Residual (EUR) | Sum |
|---------|----------------|---------------|-----|
| `v1_transactions` | 3568.14 | 58245.81 | 61813.95 |
| `v2_named_users`  | 11150.42 | 50663.53 | 61813.95 |

**Note:** `HANDOFF_P-010` listed these values with the `allocated` and
`residual` labels transposed (v1: allocated=58245.81, residual=3568.14).  The
correct values above were verified from the committed Delta table.  The
`03_engine.py` notebook asserts the correct values and reports any discrepancy
as a cross-runtime finding.

---

## Human run log

*To be filled in after the Databricks Free Edition run:*

- [ ] `00_setup.py` — ran / failed
- [ ] `01_synth_and_bronze.py` — ran / failed / timing
- [ ] `02_silver.py` — ran / failed / GL total assertion result
- [ ] `03_engine.py` — ran / failed / cross-runtime assertion result
- [ ] `04_reports.py` — ran / failed / chart rendered
- [ ] Any unexpected errors or Free-Edition-specific behaviour to document
