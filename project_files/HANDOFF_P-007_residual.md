# HANDOFF BUNDLE

## Header
Packet: P-007 — Residual / unallocated cost handling
CLI Thread: codex:residual

## Goal
Turn the engine's raw `residual` table into a **first-class, quantified, reconciled FinOps metric**. Detect, quantify, and surface cost that does not allocate cleanly — never silently dropped, never force-spread. This is depth signal **#3 (residual handling)** — primary.

The engine (P-006) already emits `data/gold/residual` with `gl_line_id`, `amount_eur`, `failed_step`, `reason_code`, `rule_version`. **P-007 does not re-derive residual.** It aggregates, reconciles, contextualizes, and reports it.

## Start Here (read the current repo before planning)
Runtime is now **delta-rs + DuckDB — no Spark, no JVM.** The suite runs in ~12s.
- `src/tech_cost_platform/delta_tables.py` — the Delta read/write surface (Arrow in/out, deterministic sort-before-write). **Use this; do not call `write_deltalake` directly.**
- `src/tech_cost_platform/engine/cascade.py` — how `allocation` and `residual` are produced; what columns they carry.
- `src/tech_cost_platform/silver/conform.py` — the DuckDB-over-Arrow idiom for transforms. Mirror it.
- `src/tech_cost_platform/rules/` — `RuleRegistry` / `RuleVersion`, for resolving a rule version id.
- `tests/conftest.py` — the harness: session-scoped read-only `synth_data`, function-scoped factories writing to fresh per-test dirs. Add a `residual` factory in the same shape.
- `Makefile`, `config.yaml` — conventions.

Then read the P-005→P-006 and P-000-MIGRATE receipts for context, then execute this bundle to its stop-when.

## Execution Rule
The suite is now fast and JVM-free. **The thread MAY run `make lint` and the full `make test` directly.** If any command takes minutes, stop and report — that means something reintroduced Spark.

## What to Build

### 1. Residual report builder — `src/tech_cost_platform/residual/`
- `report.py` — build the residual report from `data/gold/residual` (+ silver dims for context).
- `reconcile.py` — the reconciliation check, as a reusable, importable function (P-008 will reuse it).
- `__main__.py` / `main()` so `python -m tech_cost_platform.residual` runs it.
- Export a clean surface from `__init__.py`.

### 2. Output: `data/gold/residual_report` (Delta)
A quantified, reviewable report — one row per (`rule_version`, `failed_step`, `reason_code`) with:
- `residual_amount_eur` (Decimal, exact)
- `gl_line_count`
- `pct_of_total_gl` — share of the governed GL total, to a documented rounding rule

Plus, retain a **detail view** (`data/gold/residual_detail`) enriched with context from silver so a reviewer can see *what* failed, not just how much: `gl_line_id`, `amount_eur`, `gl_account`, `cost_center_id`, `cost_center_name`, `failed_step`, `reason_code`, `rule_version`. Join dims from silver; do not invent columns.

### 3. Reconciliation as an explicit, importable check — `reconcile.py`
A function that, for a given `rule_version`, asserts and returns:
- `total_gl_eur` (from silver `fact_gl_cost`)
- `total_allocated_eur` (from `data/gold/allocation`)
- `total_residual_eur` (from `data/gold/residual`)
- `balanced: bool` — `allocated + residual == total_gl` **exactly** (Decimal, no float tolerance)
- `difference_eur` — Decimal, `0` when balanced

It must **raise** (or return an unbalanced result the caller raises on — pick one and be consistent) rather than silently pass when unbalanced. Reconciliation that cannot fail is not reconciliation.

### 4. Reason-code semantics (document in code, assert in tests)
These are produced by the engine; P-007 must surface them accurately and must not merge or re-map them:
- `unmapped` — GL line's cost center has no `tower_id`. Fails at `gl_to_tower`. (Seeded: `CC-LEGACY`.)
- `shared_unattributable` — no usage rows exist for the source under any driver at that step. Fails at `app_to_bu`. (Seeded: `APP-EMAIL`.)
- `driver_zero` — the step's driver metric sums to zero across targets, so no defensible split exists **under that rule version**. (Seeded: `APP-ANALYTICS` under a `storage_gb` rule.)

**`driver_zero` is rule-version-dependent and that is a feature, not noise.** The same cost may be residual under one rule version and allocatable under another. The report must be per-`rule_version` so this is visible, not averaged away.

## Constraints / Guardrails
- **Never force-allocate.** No implicit even-spread of residual, no "other" bucket that absorbs it, no dropping.
- **Do not re-derive residual.** Read the engine's output. If a residual case is missing, that is an engine bug — report it, do not patch around it here.
- **Decimal, exact.** No float. No tolerance-based reconciliation.
- **Use `delta_tables.py`** for all Delta I/O. Close DuckDB connections promptly (Windows overwrite lesson from P-000-MIGRATE).
- **do-not-touch:** `engine/strategies.py`, `engine/cascade.py`, `rules/`, `synth/`, `bronze/`, `silver/`, `delta_tables.py`, `notebooks/`.
- **Out of scope:** lineage view (P-008), the five gold report views (P-009), docs (P-011/P-012). P-007 produces the residual report + reconciliation only.
- Wire a `residual` stage into `pipeline.py` **after** gold/engine, and a `make residual` target. `make pipeline` must still run end-to-end green, twice consecutively.
- Ruff clean.

## Acceptance Criteria
- **behaviors:**
  - `make residual` builds `residual_report` + `residual_detail` from existing gold/silver.
  - `make pipeline` runs green end-to-end **twice in a row** (overwrite regression from P-000-MIGRATE stays fixed).
- **required tests (`tests/test_residual.py`, fast, no JVM):**
  1. Report materializes as valid Delta (`_delta_log` present, `DeltaTable(path)` opens).
  2. **Reconciliation passes:** for `v1_transactions`, `allocated + residual == 61813.95` exactly; `balanced is True`, `difference_eur == 0`.
  3. **Reconciliation can fail:** feed a deliberately tampered allocation/residual input (e.g. drop a residual row in a fixture copy) and assert the check reports unbalanced / raises. A check that never fails is not a check.
  4. **All three reason codes present** with the correct `failed_step`, and the seeded cases resolve to the expected entities in `residual_detail`: `CC-LEGACY` → `unmapped` @ `gl_to_tower`; `APP-EMAIL` → `shared_unattributable` @ `app_to_bu`; `APP-ANALYTICS` → `driver_zero` (under a `storage_gb` rule version — construct it, shipped versions don't use it).
  5. **Nothing dropped, nothing force-spread:** the sum of `residual_detail.amount_eur` equals the sum of `residual_report.residual_amount_eur`, and no residual `gl_line_id` also appears with a full allocation in `data/gold/allocation`.
  6. **Version-dependence visible:** under `v1_transactions` vs a `storage_gb` rule version, the `driver_zero` residual differs — proving residual is a function of the rule version, and both still reconcile to `61813.95`.
  7. `pct_of_total_gl` sums correctly and the rounding rule is deterministic.

## Stop When
- `residual_report` + `residual_detail` materialize as Delta with reason codes and quantified amounts;
- the reconciliation function passes on good data and **fails** on tampered data;
- `allocated + residual == 61813.95` exactly;
- residual is never dropped or force-spread (asserted);
- rule-version-dependence of `driver_zero` is demonstrated;
- `make lint` + full `make test` green, suite still in seconds; `make pipeline` green twice.
- **Stop — do not build the lineage view (P-008) or the gold report views (P-009).**

## Output Required
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results **with timings** (`make lint`, `make residual`, `make pipeline` ×2, full `make test`)
4) Commit/PR (hash/link)
5) Risks + next steps — explicitly confirm: exact `61813.95` reconciliation; the reconciliation check demonstrably fails on tampered input; all three reason codes with correct `failed_step`; nothing force-spread; suite runtime.
