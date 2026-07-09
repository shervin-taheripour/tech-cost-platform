# HANDOFF BUNDLE

## Header
Packet: P-010 — Databricks Free Edition notebooks
CLI Thread: claude-cli:databricks-notebooks  (thread-agnostic)

## Goal
Reproduce the medallion pipeline and final reports as **Databricks Free Edition notebooks**, using **Option B**: the notebooks import the project's **portable domain core unchanged** (`engine/strategies.py`, `rules/`, `synth/`) and implement **only the I/O layer in Spark**. This proves the allocation engine was never runtime-dependent — the same driver math and the same governed rules produce the same numbers on local DuckDB/delta-rs and on Databricks Spark.

The Databricks-fluency signal is supporting, not the headline. The headline is portability: **identical results, different storage engine.**

## Confirmed Environment (do not guess — this is verified)
- Databricks **Free Edition**, **serverless only**. No cluster config, no cluster libraries.
- Compute runs **Spark Connect** (`pyspark.sql.connect.dataframe.DataFrame`). There is **no `SparkContext`**, no RDD API. Use the **DataFrame / SQL API only.** Anything touching `spark.sparkContext.*`, `.rdd`, or `sc.` will fail.
- Unity Catalog is enabled. Catalog: **`workspace`**. Schema: **`default`**.
- Volume created and verified empty:
  **`/Volumes/workspace/default/tech_cost_platform/`**
  (`dbutils.fs.ls` on it returns `[]`.)
- **No DBFS mounts.** UC volume paths only. `dbfs:/Volumes/...` also works but prefer the POSIX form.

## Architecture — Option B (non-negotiable)
**Import unchanged (the portable core — zero modifications):**
- `tech_cost_platform.engine.strategies` — pure Python driver math (`even_spread`, `weighted`, `consumption`, `manual_override`), `Decimal`, no I/O.
- `tech_cost_platform.rules` — Pydantic v2 schema, loader, `RuleRegistry`. Reads YAML from `config/rules/`.
- `tech_cost_platform.synth` — stdlib CSV generator, deterministic at the default seed.

**Do NOT import (these are the local I/O layer, bound to delta-rs/DuckDB):**
- `delta_tables.py`, `bronze/ingest.py`, `silver/conform.py`, `silver/dq.py`, `engine/cascade.py`, `residual/*`, `lineage/*`, `gold/*`, `pipeline.py`, `runtime.py`.
- These are **reimplemented as Spark DataFrame/SQL** in the notebooks. That is the point of the packet.

Getting the code into the workspace: use a **Databricks Git folder** cloning the public repo (`shervin-taheripour/tech-cost-platform`), so notebooks import the portable modules from the cloned source. Document the exact steps. `%pip install` from the repo is an acceptable fallback — document whichever is used.

## Notebooks to Author (`notebooks/`)
Author as `.py` files with Databricks `# COMMAND ----------` cell separators (source-controllable, importable as notebooks). Committed to the repo.

1. **`00_setup.py`** — volume/paths, Git-folder import path setup, a `PATHS` constant block pointing at `/Volumes/workspace/default/tech_cost_platform/{source,bronze,silver,gold}`, and a guard cell that fails loudly if the volume is missing.
2. **`01_synth_and_bronze.py`** — call `synth` (unchanged) to write source CSVs into the volume; ingest to **Spark Delta** bronze tables (`spark.read.csv` with explicit schema → `write.format("delta").save(...)`). Validate with the **existing Pydantic contracts** on the driver (collect the tiny tables), mirroring the local ingestion boundary.
3. **`02_silver.py`** — conform + DQ in Spark SQL. Same conformed model: `dim_cost_center`, `dim_resource_tower`, `dim_application`, `dim_business_unit`, `fact_gl_cost` (nullable `tower_id` preserved), `fact_usage_metric`. Assert `sum(fact_gl_cost.amount_eur) == 61813.95`.
4. **`03_engine.py`** — the proof notebook. Load a `RuleVersion` via `RuleRegistry` (unchanged). Read silver into Spark, collect the tiny driver signals to the driver, call the **unchanged pure strategies** to compute proportions, and write `allocation` + `residual` Delta tables with the same columns and reason codes as local (`unmapped`, `shared_unattributable`, `driver_zero`; `failed_step`; `rule_version`).
5. **`04_reports.py`** — build/read the gold views and render them: application TCO, BU showback, residual, lineage, **driver-comparison (v1 vs v2)**. Use `display()` for tables and at least one chart for the driver comparison. This is the reviewer-facing notebook.

## The Cross-Runtime Assertion (the packet's real payload)
`03_engine.py` (or a dedicated cell in `04_reports.py`) must assert, **on Databricks**:
- `allocated + residual == 61813.95` **exactly** (Decimal) for `v1_transactions`.
- Same for `v2_named_users`.
- All three residual reason codes present, with the seeded cases: `CC-LEGACY` → `unmapped` @ `gl_to_tower`; `APP-EMAIL` → `shared_unattributable` @ `app_to_bu`; `APP-ANALYTICS` → `driver_zero` under a `storage_gb` rule.
- The `v1` vs `v2` BU-level split diverges, with the `APP-BILLING` top-BU flip.
- **Cross-runtime equality:** the per-BU allocated amounts computed on Databricks match the local committed values from P-009. Local reference (from `RECEIPT_P-009`): `v1_transactions` → allocated `58245.81`, residual `3568.14`. `v2_named_users` → allocated `50663.53`, residual `11150.42`. Assert these exactly.

If the numbers differ, that is a **finding**, not something to paper over — report it. Likely culprits would be Decimal→float coercion in a Spark cast, or non-deterministic ordering in remainder distribution. Both are real bugs worth knowing about.

## Constraints / Guardrails
- **Spark Connect only.** DataFrame/SQL API. No `SparkContext`, no RDDs, no `sc.`. No Python UDFs (collect the tiny signal sets to the driver and use the pure strategies — same pattern as local).
- **Decimal discipline.** Money and proportions must not round-trip through `float`/`double`. Use `DecimalType(18, 2)` for money; do not let Spark infer.
- **Portable core unchanged.** If a notebook needs `strategies.py` or `rules/` modified to work, **stop and report** — that would mean the core is not actually portable, which is a finding about the architecture, not a licence to edit it.
- **Do not modify any `src/` code.** P-010 adds notebooks only. If a genuine portability bug surfaces, report it as a follow-up packet.
- **Personal use only.** No client names, no real data, synthetic only.
- **Free Edition is quota-limited.** Keep data tiny (it already is). Don't add volume or long-running jobs.
- **Out of scope:** README/DESIGN prose and diagrams (P-011/P-012). Notebooks may carry brief markdown cells explaining each stage — that is notebook hygiene, not the docs packet.

## Execution Split (learned the hard way)
- **The thread authors the notebooks offline.** It cannot run them; it has no Databricks access.
- **The human runs them in Free Edition**, in order, and reports: what ran, what broke, and every local-vs-Free-Edition delta encountered.
- The thread may run `make lint` on the notebook `.py` files if they're in Ruff's path.
- **P-010 cannot be closed on the thread's word.** Acceptance requires an actual Databricks run.

## Acceptance Criteria
1. All five notebooks committed under `notebooks/`, with cell separators, runnable top-to-bottom.
2. Notebooks run **end-to-end on Free Edition serverless**, human-verified.
3. Data paths use the **UC volume** `/Volumes/workspace/default/tech_cost_platform/`; no DBFS mounts.
4. Notebooks import `engine.strategies`, `rules`, and `synth` **unchanged** from the repo; `src/` has zero modifications.
5. Bronze/silver/gold I/O is Spark Delta; no `deltalake`/`duckdb` imports in notebooks.
6. `04_reports.py` renders all gold views, including a driver-comparison chart.
7. **Cross-runtime reconciliation holds:** on Databricks, `v1` = `58245.81 + 3568.14`, `v2` = `50663.53 + 11150.42`, both `== 61813.95` exactly; three reason codes present; `APP-BILLING` flip reproduced.
8. **All local-vs-Free-Edition deltas documented** in `notebooks/README.md`: Spark Connect (no SparkContext), UC volumes vs local paths, serverless library constraints, and any Decimal/type handling differences.

## Stop When
- The five notebooks are committed and run clean on Free Edition serverless;
- the portable core ran unmodified and produced numbers identical to the local runtime;
- the driver-comparison renders and the flip is visible;
- deltas are documented.
- **Stop — do not write README/DESIGN prose or diagram assets (P-011/P-012); do not modify `src/`.**

## Output Required
1) What changed (what/why)
2) Files changed (paths)
3) Commands run + results — thread: lint. **Human: the Databricks run log** (which notebooks ran, timings, failures).
4) Commit/PR (hash/link)
5) Risks + next steps — explicitly confirm: `src/` untouched; portable core imported unmodified; cross-runtime numbers match P-009 exactly (or name the discrepancy); every Free-Edition delta documented.
