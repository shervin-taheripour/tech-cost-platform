# HANDOFF BUNDLE

## Header
Packet: P-004 — Silver conformance
CLI Thread: codex:silver

## Goal
Build the silver layer on top of bronze Delta: clean and type-normalize, validate with data-quality checks, and join the source tables into conformed **fact** and **dimension** Delta tables that the allocation engine (P-006) will consume. Silver preserves the lineage keys from bronze and the reconciliation chain (GL total must still tie to `61813.95`). It does **not** allocate, apply drivers, compute residual, or author rules — it only produces clean, conformed, engine-ready inputs.

## Start Here (read the current repo before planning)
The repo at its current commit is the source of truth, not the receipts. Before writing anything, read the actual code:
- `src/tech_cost_platform/spark.py` — the SparkSession/Delta bootstrap. **Reuse `build_spark_session()` exactly as-is; do not build your own session.** As of P-000-FIX it carries deliberate startup hardening (in-memory catalog, a Windows-only gateway-tempfile patch, UI off, `127.0.0.1` host). **Do not modify, remove, or "clean up" any of that config** — it is what makes the runtime not hang. Treat `spark.py` as do-not-touch for this packet.
- `tests/conftest.py` — the shared test harness from P-003.1. It provides session-scoped, read-only `synth_data` and a **function-scoped** `bronze_ingest` factory that writes to a fresh per-call output dir. **Reuse these; add a `silver` factory in the same shape (see tests below).**
- `src/tech_cost_platform/bronze/ingest.py` — note the `ingest_bronze_sources(...)` signature (`source_overrides` / `bronze_dir` / `warehouse_dir`); silver reads bronze Delta produced by it.
- `src/tech_cost_platform/pipeline.py` — the staged entrypoint; you'll replace the no-op silver stage.
- `config.yaml`, `Makefile`, `.github/workflows/ci.yml` — conventions to mirror; CI runs `make lint` + `make test` on a clean Linux checkout with **no** pre-staged data.

Then read the P-001→P-003.1 receipts for context and carry-forward lessons, then execute this bundle to its stop-when.

## Execution Rule (learned the hard way — non-negotiable)
Spark cold-start on this machine takes ~80–110s per run; that is normal, not a hang. Because of that:
- **The human runs the Spark tests in their own terminal**, not inside a CLI-thread turn. A thread that runs `make test` itself risks being cut off mid-run (token/turn limits) and reporting a false "hang." The thread's job is to **write the code and the tests**; the human executes the ~100s verification and reports results back.
- When any Spark test is run, use a **generous timeout** (`--timeout=300` or higher). A healthy run is 80–110s; a tight timeout false-fails it. `pytest-timeout` is already configured (default 120s from P-000-FIX) — for silver's heavier runs, pass `--timeout=300` explicitly.
- Do not interpret a long-running Spark test as broken. If unsure, let it run to completion or until the 300s timeout prints a stack trace.

## Repo Targets
- `src/tech_cost_platform/silver/`:
  - `dq.py` — data-quality checks (native Spark aggregations; see constraints).
  - `conform.py` — cleaning, type normalization, joins, dimension conformance, surrogate/lineage-key handling.
  - `build.py` — orchestrates: read bronze Delta → DQ → conform → write silver Delta.
  - `__main__.py` (or `main()`) so `python -m tech_cost_platform.silver` runs the full build.
- `src/tech_cost_platform/pipeline.py` — replace the no-op **silver** stage with the real build (leave gold as no-op).
- `config.yaml` — add silver paths consistent with existing conventions: `silver_dir: data/silver` (bronze_dir already present).
- `tests/test_silver.py` — offline tests.
- `Makefile` — wire the existing `silver` target to the real build.

## Conformed Output (silver Delta tables in `data/silver/`)
Produce a small, explicit conformed model — **facts** and **dimensions** kept separate:

**Dimensions (conformed, deduped, typed):**
- `dim_cost_center` — `cost_center_id` (PK), name, `tower_id` (nullable — preserve the intentional NULL).
- `dim_resource_tower` — `tower_id` (PK), name, type.
- `dim_application` — `app_id` (PK), name, criticality.
- `dim_business_unit` — `bu_id` (PK), name.

**Facts (conformed, typed, lineage-keyed):**
- `fact_gl_cost` — one row per `gl_line_id` (PK, lineage root), with `period`, `gl_account`, `cost_center_id` (FK), `amount_eur` (numeric), and the resolved `tower_id` joined from `dim_cost_center` (nullable — NULL = the unmapped/residual case, carried forward not dropped).
- `fact_usage_metric` — conformed long-form driver signals: `metric_id` (PK), `period`, `step`, `from_id`, `to_id`, `metric_name`, `value` (numeric).

Bridge/relationship rows implied by `usage_metrics` (tower→app, app→bu) stay in `fact_usage_metric`; do not invent new mapping tables — the engine derives splits from these.

## Cleaning / Conformance Rules
- **Type normalization:** ensure `amount_eur` and `usage_metric.value` are proper numerics; ids/strings trimmed of incidental whitespace; period normalized to the canonical `YYYY-MM` form.
- **Deduplication:** dimensions deduped on PK; a duplicate PK with conflicting attributes is a DQ failure (see below), not a silent "last wins."
- **Join completeness:** every `fact_gl_cost.cost_center_id` resolves to `dim_cost_center`; every `fact_usage_metric.from_id`/`to_id` resolves to its dimension. **Exception (must preserve):** `cost_center.tower_id` may be NULL — that unmapped case flows through to `fact_gl_cost.tower_id = NULL`; it is valid, not a join failure.
- **No allocation:** do not compute any cost splits, driver math, or residual here. Silver ends at conformed inputs.

## Data-Quality Checks (`dq.py`)
Checks run as native Spark aggregations on the driver-safe path (no UDFs). Each check yields a pass/fail with a count. Minimum set:
- PK uniqueness on every dimension and on `fact_gl_cost.gl_line_id`.
- Referential integrity for all FKs (with the documented nullable-`tower_id` exception).
- Non-negative `amount_eur` and `value`.
- Reconciliation: `sum(fact_gl_cost.amount_eur)` == `61813.95` (the governed total survives conformance).
- **Behavior on good vs bad data:** on the real seeded data all checks pass; the suite must also prove the checks *fail* on a seeded-bad fixture (e.g. a duplicate dimension PK or a broken FK) — DQ that never fails on bad input isn't DQ.

## Constraints / Guardrails
- **compatibility:** reuse `spark.py`'s session (vendored jars). **Native Spark ops only — no Python UDFs on executors** (carry-forward from P-001/P-003 Windows worker friction). DQ and conformance expressed as DataFrame/SQL ops.
- **Spark session lifecycle (hard rule — this caused a multi-hour hang in P-003.1):** use **one shared SparkSession for the whole test run** (session- or module-scoped fixture, reusing `spark.py`). Do **not** create or `.stop()` a SparkSession per test or inside any function-scoped fixture. Function-scoped isolation applies to **output directories only** (fresh `silver_dir`/`warehouse_dir` per test), never to the session. Creating a session per call hangs on Windows Spark/JVM startup.
- **offline / CI-safe tests via the shared harness (do not reinvent):** silver tests must **not** depend on gitignored runtime output being pre-present on disk (a fresh CI checkout has no `data/source/`, `data/bronze/`, or `data/silver/`). Reuse the P-003.1 `tests/conftest.py` harness: consume the session-scoped read-only `synth_data` and the function-scoped `bronze_ingest` factory to get bronze inputs, and **add a `silver` factory** in the same shape — a function-scoped callable that builds silver into a fresh per-call `silver_dir`/`warehouse_dir` under the gitignored test workspace. Read-only upstream stays shared; every writing test gets its own isolated dir. This is the pattern that fixed the P-003 CI failure; match it, don't rebuild it.
- **governed fixtures (do not alter):** consume bronze as produced by P-003; do not change `synth/` or bronze contracts. The GL total `61813.95` is a contract silver must preserve.
- **style:** mirror repo conventions; Ruff clean; Pydantic v2 where a boundary contract is appropriate (DQ is the silver boundary).
- **windows tests:** project-local paths under `data/`, construct rows via Spark SQL not Python-worker serialization, clean up after.
- **do-not-touch / out-of-scope:**
  - No allocation, drivers, residual, or lineage view — those are P-006/P-007/P-008.
  - No rule authoring (P-005). Do not touch `synth/`, `bronze/` (beyond reading its output), `spark.py`, `notebooks/`, or doc bodies.
  - No documentation obligations — silver produces code + tests + a receipt only (docs are consolidated in P-011/P-012).

## Acceptance Criteria
- **behaviors:**
  - `make PYTHON=<py> silver` reads bronze Delta and writes the conformed fact + dimension Delta tables to `data/silver/`.
  - `make PYTHON=<py> pipeline` runs end-to-end with real synth→bronze→silver stages and a no-op gold stage, exits 0.
  - From a clean checkout (no pre-existing `data/`), the test suite builds its own inputs and passes — i.e. it will pass in GitHub Actions on Linux.
- **verification split:** the thread may run `make lint` itself (fast). The **Spark suite (`make test`, `make silver`, `make pipeline`) is run by the human in-terminal** with `--timeout=300`, and results reported back — do not have the thread run these inside its turn (see Execution Rule). CI on Linux is the independent confirmation.
- **required tests (`tests/test_silver.py`, offline, CI-safe — built on the shared `conftest.py` harness with a new function-scoped `silver` factory; one shared Spark session):**
  1. Conformed fact + dimension tables are produced and readable.
  2. PK uniqueness holds on every dimension and on `fact_gl_cost`.
  3. Join completeness: all FKs resolve except the intentional NULL `tower_id`, which is preserved (≥1 NULL-tower GL row survives).
  4. Reconciliation: `sum(fact_gl_cost.amount_eur)` == `61813.95`.
  5. DQ proves itself: checks pass on good data and **fail** on a seeded-bad fixture (dup PK or broken FK).
  6. Driver signals conform: `fact_usage_metric` retains the multi-driver rows needed downstream (the divergence + driver-zero cases from P-002 survive into silver).

## Stop When
- Conformed fact + dimension Delta tables are produced;
- DQ checks pass on good data and flag seeded bad data;
- lineage keys are preserved from bronze and the `61813.95` reconciliation holds;
- join completeness is asserted (with the nullable-tower exception);
- `make lint`, `make test`, `make silver`, `make pipeline` are green **and** the tests are CI-safe (no dependency on pre-existing gitignored data).
- **Stop — do not start P-005 (rules) or P-006 (engine).**

## Output Required
Return, in this order:
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results (exact commands: `make silver`, `make pipeline`, `make lint`, `make test`, and the clean-input / CI-safe verification)
4) Commit/PR (hash/link, if created)
5) Risks + next steps (confirm reconciliation held at 61813.95 and that tests build their own inputs, for the Dev Ledger receipt)
