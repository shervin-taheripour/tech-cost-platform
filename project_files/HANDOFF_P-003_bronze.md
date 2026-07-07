# HANDOFF BUNDLE

## Header
Packet: P-003 — Bronze ingestion layer
CLI Thread: codex:bronze

## Goal
Ingest the six synthetic source CSVs from `data/source/` **as-received** into Delta tables in `data/bronze/`, guarded by an ingestion-boundary contract (schema + type/null validation) that rejects malformed input before anything is written. Bronze preserves meaning and lineage anchors — it does not clean, join, or conform (that is silver, P-004). This is the raw lineage anchor for depth signal #4.

## Repo Targets
- `src/tech_cost_platform/bronze/`:
  - `contracts.py` — Pydantic v2 models, one per source table = the ingestion boundary contract (expected fields, types, required/nullable rules).
  - `schema.py` — explicit Spark `StructType` per table (typed CSV read; no schema inference).
  - `ingest.py` — read CSV (typed) → validate at the boundary → write Delta. Reuse the existing session helper from `src/tech_cost_platform/spark.py`; **do not** rebuild Spark/Delta config.
  - `__main__.py` (or `main()`) so `python -m tech_cost_platform.bronze` runs the full ingest.
- `src/tech_cost_platform/pipeline.py` — replace the no-op **bronze** stage with the real ingest call (leave silver/gold stages as no-ops).
- `config.yaml` — add bronze paths consistent with the existing `synth:` conventions: `source_dir: data/source`, `bronze_dir: data/bronze`.
- `tests/test_bronze.py` — offline tests (uses vendored Delta jars from P-001).
- `tests/fixtures/gl_costs_malformed.csv` — a deliberately invalid fixture for the negative test.
- `Makefile` — wire the existing `bronze` target to the real ingest.

## Ingestion Contract & Behavior
- **Typed read, no inference.** Read each CSV with its explicit `StructType`. Numeric fields (`amount_eur`, `usage_metrics.value`) parse to numeric; everything else string. Use a read mode that surfaces (does not silently drop) malformed records.
- **Validate at the boundary with Pydantic v2 on the driver.** Data is tiny (~tens of rows) — `collect()` to the driver and validate rows against the per-table Pydantic models. **Do not validate via a Spark UDF / Python-worker path** (carry-forward from P-001's Windows worker friction). Validation runs on the driver only.
- **Reject before writing.** If any table fails validation, raise a clear error and write **no** Delta output for that run (no partial/leaky writes).
- **Preserve meaning.** Do not rename, drop, or re-derive source columns. Additive provenance is allowed: a deterministic `_source_file` column is recommended for lineage. If you add an ingestion timestamp, it must not be asserted in tests and must not affect reconciliation.
- **Honor the intentional NULL.** `cost_centers.tower_id` is legitimately nullable (the unmapped/residual case). The contract must **accept** NULL there — it is valid data, not a violation.

## Constraints / Guardrails
- **compatibility:** reuse `spark.py`'s session (jars already vendored). Native Spark + driver-side Pydantic only — **no Python UDFs on executors**. Offline.
- **governed fixtures (do not alter):** consume `data/source/` as produced by P-002. Do **not** regenerate, edit, or re-seed it. The locked GL total **`61813.95`** and the P-002 file hashes are contracts; bronze must preserve the total exactly.
- **style:** mirror repo conventions; Ruff clean; Pydantic v2 at the boundary (repo convention).
- **windows tests:** write test Delta output under a gitignored project-local path (e.g. a temp subdir of `data/`) or the session warehouse dir rather than OS temp, per the P-002 receipt's temp-dir note; clean up after. Construct any test rows with Spark SQL, not Python-worker serialization.
- **do-not-touch / out-of-scope:**
  - No cleaning, joining, conforming, dedup, or dimension conformance — that is silver (P-004). Bronze is as-received.
  - Do not modify `spark.py`, `synth/`, or the silver/rules/engine/residual/lineage/gold packages.
  - Do not author `allocation_rules` (P-005). Do not touch `notebooks/` (P-010) or write doc bodies (P-011).

## Acceptance Criteria
- **behaviors:**
  - `make PYTHON=<py> bronze` writes all 6 tables as Delta under `data/bronze/`.
  - `make PYTHON=<py> pipeline` runs end-to-end with a **real** bronze stage and no-op silver/gold, exits 0.
  - Pointing bronze at `tests/fixtures/gl_costs_malformed.csv` raises a validation error and writes nothing.
- **required tests (`tests/test_bronze.py`, offline):**
  1. All 6 Delta tables are created and readable.
  2. Row-count reconciliation: each bronze table's row count equals its source CSV row count.
  3. Value preservation: `sum(gl_costs.amount_eur)` in bronze == **`61813.95`**; spot-check ≥1 known `gl_line_id` round-trips unchanged.
  4. Lineage anchor: `gl_line_id` is present and unique in bronze `gl_costs`; all source columns preserved.
  5. Negative test: the malformed fixture is rejected (raises) with no Delta written.
  6. NULL preservation: the intentional NULL `tower_id` (`CC-LEGACY`) survives into bronze (≥1 NULL row), i.e. the unmapped case is intact for downstream residual handling.

## Stop When
- All 6 source tables land in Delta unmodified-in-meaning (row counts + GL total reconcile);
- the ingestion contract validates good data and **rejects** the malformed fixture with no partial write;
- `make lint`, `make test`, `make bronze`, and `make pipeline` are green.
- **Stop — do not start P-004 (silver conformance) or P-005 (rules).**

## Output Required
Return, in this order:
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results (exact commands: `make bronze`, `make pipeline`, `make lint`, `make test`, and the malformed-rejection check)
4) Commit/PR (hash/link, if created)
5) Risks + next steps (flag any Windows Delta-write-in-test friction and confirm the GL total reconciled to 61813.95, for the Dev Ledger receipt)
