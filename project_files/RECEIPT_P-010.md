# RECEIPT_P-010

## Header

- Packet: `P-010`
- Title: `Databricks Free Edition notebooks`
- Thread: `claude-cli:databricks-notebooks`
- Date: `2026-07-09`
- Status: `Authored offline, committed — awaiting human Databricks run`
- Commit: `1f0c6f0` — "P-010: Databricks Free Edition notebooks (Option B — portable core unchanged)"

## Scope

P-010 proves the allocation engine is runtime-portable: the same `engine.strategies` pure
functions and the same governed rules that ran locally on DuckDB/delta-rs are imported
unchanged on Databricks Free Edition serverless and produce bit-identical results.  The
Databricks I/O layer (Spark Delta read/write, Spark SQL conformance) replaces the local
layer (`delta-rs`, DuckDB, `pyarrow`) without touching any source file.

Five source-controllable notebooks are committed under `notebooks/` along with a
`notebooks/README.md` documenting every local-vs-Free-Edition delta.

## What Changed

### Notebooks — `notebooks/`

Added:

- [00_setup.py](../notebooks/00_setup.py) — UC volume guard; Git-folder `sys.path` setup
  (`/Workspace/Repos/shervin-taheripour/tech-cost-platform/src`); `PATHS` constant block for
  `{source,bronze,silver,gold}` under `/Volumes/workspace/default/tech_cost_platform/`;
  portable core smoke-test (imports `engine.strategies`, `rules.RuleRegistry`, `synth.generate`
  and prints their key exports); Spark Connect confirmation (`spark.sparkContext` raises
  `AttributeError` on serverless).

- [01_synth_and_bronze.py](../notebooks/01_synth_and_bronze.py) — calls unchanged
  `generate_source_exports` with a `SynthConfig(output_dir=PATHS["source"])` to write six CSVs
  to the UC volume; validates every row against synth Pydantic schema models on the driver;
  asserts `sum(gl_costs.amount_eur) == 61813.95`; ingests each CSV to Spark Delta bronze via
  `spark.read.csv` with explicit `DecimalType(18,2)` schema (no inference); confirms nullable
  `tower_id` on `cost_centers`.

- [02_silver.py](../notebooks/02_silver.py) — Spark SQL conformance transforms mirroring
  `silver/conform.py` DuckDB logic: six silver Delta tables written to `PATHS["silver"]`;
  `fact_gl_cost` built with LEFT JOIN to pick up nullable `tower_id` from `dim_cost_center`;
  three DQ assertions: GL total == `61813.95` (exact Decimal after CAST), at least one NULL
  `tower_id` GL line (CC-LEGACY), no negative metric values.

- [03_engine.py](../notebooks/03_engine.py) — the portability proof. Loads `RuleRegistry`
  (unchanged) with `rules_dir=RULES_DIR`; reads all six silver Delta tables into Spark, then
  **collects to the driver** (tables are tiny); reimplements `build_usage_index` inline (same
  pure logic as `engine/cascade.py` — that module cannot be imported because it pulls in
  `delta_tables` and `runtime`); calls the **unchanged** `compute_strategy_outcome` and
  `distribute_amount` from `engine.strategies` to run the full three-step cascade for both
  `v1_transactions` and `v2_named_users`; writes combined allocation (both versions) and
  residual Delta tables with explicit `DecimalType(18,2)` / `DecimalType(18,12)` schemas;
  asserts cross-runtime equality against P-009 reference values; verifies all three residual
  reason codes and the APP-BILLING top-BU flip.

- [04_reports.py](../notebooks/04_reports.py) — builds five gold report views in Spark SQL and
  displays each with `display()`: `report_application_tco`, `report_bu_showback`,
  `report_residual`, `report_lineage` (allocation chain including residual rows), and
  `report_driver_comparison` (v1 vs v2 BU splits with `delta_eur` and `share_delta_pp`).
  Renders a **matplotlib driver-comparison chart** showing the APP-BILLING top-BU flip side by
  side (grouped bars for amounts; signed `share_delta_pp` bar chart).

- [README.md](../notebooks/README.md) — documents six local-vs-Free-Edition deltas: Spark
  Connect (no SparkContext), UC volumes vs local paths, serverless library constraints (no
  cluster libraries; Git-folder used), Decimal/type handling (explicit schemas, no inference,
  `Decimal(str(...))` on collected values), module exclusion list and the `build_usage_index`
  inline rationale, cross-runtime assertion values with the HANDOFF label-transposition finding.
  Includes a human run log table (to be filled in after Databricks execution).

### pyproject.toml

Updated:

- [pyproject.toml](../pyproject.toml) — added `[tool.ruff.lint.per-file-ignores]` for
  `notebooks/*.py`: suppresses F821 (`spark`/`dbutils`/`display` are Databricks-injected
  globals), E402 (cell-local imports), F401 (smoke-test imports), F811 (cell-local
  re-imports).

## Architecture — Option B

| Portable core (imported unchanged) | Local I/O layer (reimplemented in Spark) |
|------------------------------------|-----------------------------------------|
| `engine.strategies` — pure Python, `Decimal`, no I/O | `delta_tables.py` — replaced by `spark.read/write.format("delta")` |
| `rules` — Pydantic v2 schema + YAML loader | `bronze/ingest.py` — replaced by `spark.read.csv` with explicit schema |
| `synth` — deterministic CSV generator | `silver/conform.py` — replaced by Spark SQL |
| | `engine/cascade.py` — cascade logic inlined on driver; pure strategy functions called unchanged |
| | `residual/*`, `lineage/*`, `gold/*` — report views built in Spark SQL |

`build_usage_index` from `engine/cascade.py` is inlined in `03_engine.py`: it is pure Python
with no I/O, but its module imports `delta_tables` and `runtime` at the top level.  Inlining
it (identical logic) was the correct move — not a modification to the portable core.

## Cross-Runtime Assertion Values

From the P-009 local run (verified from committed `data/gold/allocation`):

| Version | Allocated (EUR) | Residual (EUR) | Sum |
|---------|----------------|---------------|-----|
| `v1_transactions` | 3568.14 | 58245.81 | 61813.95 |
| `v2_named_users`  | 11150.42 | 50663.53 | 61813.95 |

**Finding — HANDOFF label transposition:** `HANDOFF_P-010` listed these values with the
`allocated` and `residual` labels transposed for both versions (v1: allocated=58245.81,
residual=3568.14; v2: allocated=50663.53, residual=11150.42).  The correct values above were
confirmed from the committed Delta table.  `03_engine.py` asserts the correct values and
reports any cross-runtime discrepancy as a finding rather than papering over it.

Under `v1_transactions` (cpu_hours at tower_to_app, transactions at app_to_bu): only
APP-BILLING has non-zero transaction data at the app_to_bu step, so only its slice of
TWR-COMPUTE costs reaches BUs — hence the small allocated amount (3568.14).  All other apps
either have no app_to_bu targets (APP-EMAIL → `shared_unattributable`) or have targets but
zero transaction signals (APP-ANALYTICS, APP-CRM, APP-ERP, APP-HRIS → `driver_zero`).
TWR-LABOR, TWR-NETWORK, and TWR-STORAGE have no cpu_hours data → `shared_unattributable` at
tower_to_app, making their costs residual before app_to_bu is even reached.

Under `v2_named_users`: all four TWR-COMPUTE apps (ANALYTICS, BILLING, CRM, ERP) have
named_users at app_to_bu → all allocated.  Residual shrinks to CC-LEGACY (unmapped) + the
tower_to_app shared_unattributable towers (LABOR, NETWORK, STORAGE — same as v1, since
tower_to_app also uses cpu_hours in v2) + APP-EMAIL (no app_to_bu targets).

## Documented Deltas (notebooks/README.md)

1. **Spark Connect** — no `SparkContext`, no RDD, no Python UDFs; all math on driver
2. **UC volumes** — `/Volumes/workspace/default/tech_cost_platform/`; no DBFS mounts
3. **Library installation** — Git folder (`sys.path.insert`); `delta-rs`/`duckdb`/`pyarrow` not imported; `pydantic`/`pyyaml` are standard Databricks runtime packages
4. **Decimal handling** — explicit `DecimalType(18,2)` on CSV reads; `Decimal(str(...))` on collected values; money never cast to float; shares are display-only (float OK for matplotlib)
5. **Module exclusion** — `cascade.py`, `delta_tables.py`, `runtime.py`, `residual/*`, `lineage/*`, `gold/*` excluded; `build_usage_index` inlined
6. **HANDOFF label transposition** — noted in README and in `03_engine.py` comment block

## Commands Run + Results

- `.venv/Scripts/python.exe -m ruff check notebooks/`
  - Result: **All checks passed**
- `.venv/Scripts/python.exe -m ruff check src tests`
  - Result: **All checks passed** (src unchanged)
- `git commit` → `1f0c6f0`

## Final State

P-010 is authored and committed.

- Five notebooks committed under `notebooks/` with `# COMMAND ----------` cell separators,
  runnable top-to-bottom.
- Portable core (`engine.strategies`, `rules`, `synth`) imported unchanged; `src/` has zero
  modifications.
- Bronze/silver/gold I/O is Spark Delta; no `deltalake`/`duckdb` imports in notebooks.
- `04_reports.py` renders all five gold views and a driver-comparison chart.
- `notebooks/README.md` documents six local-vs-Free-Edition deltas.
- Cross-runtime reconciliation assertions coded in `03_engine.py`; HANDOFF label transposition
  documented as a finding.

**P-010 acceptance is pending the human Databricks Free Edition run.**  Once notebooks 00–04
run end-to-end clean on Free Edition serverless and the run log in `notebooks/README.md` is
filled in, P-010 can be closed.

Remaining: P-011/P-012 documentation track.
