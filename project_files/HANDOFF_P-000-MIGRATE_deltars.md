# HANDOFF BUNDLE

## Header
Packet: P-000-MIGRATE — Replace local Spark runtime with delta-rs + DuckDB
CLI Thread: codex:runtime-migration
Priority: **BLOCKING.** P-007 onward is paused until this lands.

## Goal
Remove PySpark/Delta-Spark and the JVM from the local runtime entirely. Replace with **delta-rs (`deltalake`) + DuckDB**: real Delta Lake tables, no JVM. Every existing domain assertion must survive **unchanged** — reconciliation to `61813.95`, no double-counting, the three residual reason codes, and `v1`/`v2` BU-level divergence. This is an infrastructure swap, **not** a logic rewrite.

## Why (context for the thread — do not re-litigate)
The local Spark runtime has been the sole source of failure: winutils/hadoop natives, JVM gateway tempfile hangs, `DELTA_CANNOT_CREATE_LOG_PATH`, Python-worker crashes, and finally `OutOfMemoryError: Java heap space` / `SparkContext was shut down` collapsing the full suite after ~11 minutes. The domain logic passes; the runtime cannot survive its own test suite, and every added packet makes it worse. delta-rs writes the same Delta format with no Spark or JVM.

Databricks fluency is preserved: **P-010 notebooks run on Databricks Free Edition with real Spark.** The local pipeline does not need it.

## Fallback (documented, not implemented)
If a delta-rs limitation blocks a required operation, fall back to **plain Parquet + DuckDB** for that table and record it as a deviation in the receipt. Do **not** reintroduce Spark. Do not silently switch the whole project to Parquet — flag it.

## Scope: what changes, what does NOT
**Migrate (Spark-dependent):**
- `src/tech_cost_platform/bronze/` — Delta writes.
- `src/tech_cost_platform/silver/` — conform + DQ (DataFrame ops → DuckDB SQL / Arrow).
- `src/tech_cost_platform/engine/cascade.py` — the thin Spark adapter.
- `src/tech_cost_platform/pipeline.py` — stage orchestration.
- `tests/conftest.py` — drop Spark fixtures; keep the session-scoped read-only `synth_data` + function-scoped output-dir factories.

**Do NOT touch (already Spark-free — this is the payoff of prior design):**
- `src/tech_cost_platform/engine/strategies.py` — pure driver math. **Zero changes.**
- `src/tech_cost_platform/rules/` — Pydantic + YAML. **Zero changes.**
- `src/tech_cost_platform/synth/` — stdlib CSV. **Zero changes.**
- `config/rules/*.yaml`, `tests/test_strategies.py`, `tests/test_rules.py`.

**Delete outright:**
- `src/tech_cost_platform/spark.py` (incl. `GatewayTempfileProxy`, `configure_windows_hadoop`, jar resolution)
- `jars/` (vendored delta-spark jars)
- `tools/hadoop/` (winutils.exe, hadoop.dll) + its README
- the staged-write helper (`data/_staging/` write-then-move) — it existed **solely** to work around Spark's `_delta_log` failure. delta-rs writes directly; remove the workaround, do not port it.
- `pyspark`, `delta-spark` from `pyproject.toml`
- `tests/test_smoke.py::test_delta_round_trip` → rewrite as a delta-rs round-trip.
- any JDK setup step in `.github/workflows/ci.yml`

## Target Stack
- `deltalake` (delta-rs) — write/read Delta tables by path. No JVM.
- `duckdb` — SQL engine for conform/DQ/joins. Read Delta via the `delta` extension (`delta_scan('path')`) or via `DeltaTable(path).to_pyarrow_dataset()` registered into DuckDB.
- `pyarrow` — the interchange layer.
- Add all three to `pyproject.toml` core deps; pin versions that install cleanly on Windows **and** Linux CI.

## Migration Rules
- **Delta stays real.** Tables written with `write_deltalake(path, arrow_table, mode="overwrite")`. `_delta_log` is produced by delta-rs. Same on-disk format Databricks reads.
- **Money stays exact.** `amount_eur` and proportions remain `Decimal`. Use a Decimal-preserving Arrow type (`decimal128`); do **not** round-trip money through float64. Reconciliation must be exact, not tolerance-based.
- **Determinism preserved.** Deterministic row ordering on write (sort by PK) so outputs remain stable.
- **`strategies.py` is consumed, not rewritten.** The cascade calls the same pure functions with the same signatures.
- **No behavior changes.** Same tables, same columns, same reason codes, same rule semantics (`gl_to_tower` mapping-first with optional `on_unmapped`).
- Zone layout unchanged: `data/source/`, `data/bronze/`, `data/silver/`, `data/gold/`.

## Test Harness
- Delete Spark fixtures. Keep: session-scoped read-only `synth_data`; function-scoped factories (`bronze_ingest`, `silver`, `engine`) each writing to a **fresh per-test output dir** under a gitignored workspace.
- No JVM ⇒ no shared-session gymnastics, no 300s timeouts, no OOM. Keep `pytest-timeout` configured but expect the whole suite in **seconds**.

## Constraints / Guardrails
- **Assertions are frozen.** Existing domain assertions must pass **unchanged**. If a test needs editing, it must be for mechanical API reasons (how a table is read), never to weaken an assertion. Any weakened assertion is a packet failure.
- No Spark, no JVM, no Java anywhere in local runtime, tests, or CI.
- Ruff clean. Pydantic v2 contracts at ingestion boundary retained.
- Do not touch `notebooks/` (P-010 keeps Spark — that's Databricks' runtime).
- No docs obligations (P-011/P-012), **except**: leave a one-line note in the receipt that README must later state "local: delta-rs + DuckDB; Databricks: Spark."

## Execution Rule
The suite should now be fast and JVM-free, so **the thread MAY run `make lint` and the full `make test`** — verify this holds. If any command still takes minutes, stop and report; that means Spark hasn't been fully removed.

## Acceptance Criteria
1. `pip install -e ".[dev]"` in a clean venv succeeds with **no** `pyspark`/`delta-spark`.
2. `git grep -i pyspark` and `git grep -i "delta-spark"` return **nothing** under `src/`, `tests/`, `pyproject.toml`, `.github/`. (`notebooks/` may still reference Spark.)
3. `spark.py`, `jars/`, `tools/hadoop/`, and the staged-write helper are deleted.
4. `make synth`, `make bronze`, `make silver`, `make gold`(if wired), `make pipeline` all run green end-to-end from a clean `data/`.
5. **Full `make test` is green, and completes in under 60 seconds.** (Was 652s + OOM.)
6. Domain assertions pass unchanged:
   - `sum(allocation) + sum(residual) == 61813.95` **exactly** (Decimal).
   - no double-counting: every `gl_line_id` fully accounted for exactly once across allocation + residual.
   - all three residual reason codes produced with correct `failed_step`: `unmapped` (CC-LEGACY @ gl_to_tower), `shared_unattributable` (APP-EMAIL @ app_to_bu), `driver_zero` (APP-ANALYTICS under a storage_gb rule).
   - `v1_transactions` vs `v2_named_users` diverge at BU level; both reconcile.
   - rule-version pinning reproducible.
   - `strategies.py` purity test still passes (now trivially).
7. Tables in `data/bronze|silver|gold/` are **valid Delta tables** — each has a `_delta_log/`, and `DeltaTable(path)` opens it.
8. CI green on Linux with **no JDK setup step**.
9. `tests/test_smoke.py` has a delta-rs round-trip replacing the Spark one.

## Stop When
- Spark is gone from local runtime, tests, deps, and CI;
- all pipeline stages and the full suite are green in **seconds**;
- every frozen domain assertion passes unchanged;
- outputs are real Delta tables;
- CI green without a JDK.
- **Stop — do not start P-007. Do not modify `notebooks/`.**

## Output Required
1) What changed (what/why)
2) Files changed (paths) — including deletions
3) Commands/tests run + results, **with timings** (`make lint`, full `make test`, `make pipeline`)
4) Commit/PR (hash/link)
5) Risks + next steps — explicitly confirm: no pyspark anywhere; `61813.95` exact; three reason codes present; v1/v2 diverge; suite runtime; and any delta-rs limitation that forced a Parquet fallback (name the table).
