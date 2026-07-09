# HANDOFF BUNDLE

## Header
Packet: P-009 — Gold reports / views
CLI Thread: claude-cli:gold-reports  (thread-agnostic — runs against the repo, not thread history)

## Goal
Build the FinOps output surface: five queryable gold views that make the engine's work legible to a reviewer. These **surface** depth signals #1–#4 — they do not re-derive them. The centerpiece is the **driver-comparison view**, which shows the *same costs* producing *materially different* BU allocations under two rule versions. That is the "no perfect driver" thesis rendered as data.

## Start Here (read the current repo before planning)
Runtime is **delta-rs + DuckDB — no Spark, no JVM.** Full suite runs in ~30s (68 tests).
- `src/tech_cost_platform/delta_tables.py` — the Delta read/write surface (Arrow in/out, deterministic sort-before-write, target-dir removal before rewrite). **Use this; never call `write_deltalake` directly.**
- `src/tech_cost_platform/engine/cascade.py` — `data/gold/allocation` and `data/gold/residual` columns.
- `src/tech_cost_platform/residual/` — `residual_report`, `residual_detail`, `reconciliation`, and the importable `reconcile.py`. **Reuse it; do not write a second reconciliation.**
- `src/tech_cost_platform/lineage/` — `data/gold/lineage` and the `trace_forward` / `trace_backward` API. **Reuse these.**
- `src/tech_cost_platform/silver/conform.py` — the DuckDB-over-Arrow transform idiom. Mirror it.
- `src/tech_cost_platform/rules/` — `RuleRegistry` for resolving rule version ids.
- `tests/conftest.py` — session-scoped read-only `synth_data`; function-scoped factories writing to fresh per-test dirs. Add a `gold_reports` factory in the same shape.

Then read the P-006 → P-008 receipts (esp. the P-008 proportions ruling) before executing.

## Execution Rule
The suite is fast and JVM-free. **Run `make lint` and the full `make test` yourself.** If any command takes minutes, stop and report — something reintroduced Spark. Close DuckDB connections promptly (Windows overwrite lesson, P-000-MIGRATE).

## What to Build — `src/tech_cost_platform/gold/`
- `views.py` — the five view builders.
- `build.py` — orchestrates: read gold/silver inputs → build views → write Delta.
- `__main__.py` / `main()` so `python -m tech_cost_platform.gold_reports` (or your chosen module name) runs it.
- Clean export surface in `__init__.py`.

Wire a `reports` stage into `pipeline.py` after `lineage`, plus a `make reports` target. `make pipeline` must run green end-to-end **twice consecutively**.

## The Five Views (Delta, under `data/gold/`)

Every view carries `rule_version`. A report that silently mixes rule versions is meaningless — allocation results are only comparable within a version.

### 1. `report_application_tco`
Total cost of ownership per application, for a rule version.
`rule_version`, `app_id`, `app_name`, `business_criticality`, `allocated_amount_eur` (Decimal), `pct_of_allocated`, `contributing_gl_line_count`.
Sourced from `allocation` joined to `dim_application`.

### 2. `report_bu_showback`
What each business unit is charged, for a rule version.
`rule_version`, `bu_id`, `bu_name`, `allocated_amount_eur`, `pct_of_allocated`, `contributing_app_count`, `contributing_gl_line_count`.
Sourced from `allocation` joined to `dim_business_unit`.

### 3. `report_residual`
Thin, presentation-level read over the P-007 outputs. **Do not recompute residual.** Surface `residual_report` (by `rule_version`, `failed_step`, `reason_code`) with amounts, line counts, and `pct_of_total_gl`. If a passthrough is all this needs, a passthrough is correct — say so rather than inventing work.

### 4. `report_lineage`
Presentation-level read over `data/gold/lineage`. **Do not recompute lineage.** Should be directly queryable for the two audit questions: *"where did this GL line go?"* and *"what makes up this BU's charge?"* Include the `outcome` column so residual exits are visible alongside allocated paths.

### 5. `report_driver_comparison` — **the centerpiece**
The same GL costs, allocated under **`v1_transactions`** and **`v2_named_users`**, compared at BU level.

One row per `bu_id`:
`bu_id`, `bu_name`, `amount_v1_transactions`, `amount_v2_named_users`, `delta_eur` (v2 − v1), `delta_pct`, `share_v1` (BU's % of allocated under v1), `share_v2`, `share_delta_pp` (percentage-point difference).

Requirements:
- Both rule versions must be run over **identical silver inputs**. The only difference is the `app_to_bu` driver (`transactions` vs `named_users`) — P-005.1 guarantees the versions are otherwise identical.
- Both must reconcile to `61813.95` (allocated + residual). Assert it.
- The view must make the divergence **legible**, not merely present: the seeded `APP-BILLING` case flips its top BU between drivers with ≥20pp share delta (P-002 encoded this deliberately).

Also emit `report_driver_comparison_by_app` at `(app_id, bu_id)` grain if it falls out cheaply — it is what makes the `APP-BILLING` flip visible in a visual (P-012). Optional; skip if it adds real cost.

## Constraints / Guardrails
- **Do not re-derive allocation, residual, or lineage.** Read them. If a needed column is absent, that is an upstream gap — report it, do not backfill here.
- **Do not write a second reconciliation** — import `residual/reconcile.py`.
- **Decimal, exact.** No float. Percentages may be rounded, but the rounding rule must be deterministic and documented; **amounts never lose cents.**
- **Never force-allocate residual into a report.** A BU showback total is *allocated* cost; unallocated cost lives in `report_residual`. The two together equal the GL total. Do not let a report imply 100% allocation when it isn't.
- **Use `delta_tables.py`** for all Delta I/O.
- **do-not-touch:** `engine/`, `residual/`, `lineage/`, `rules/`, `synth/`, `bronze/`, `silver/`, `delta_tables.py`, `notebooks/`.
- **Out of scope:** notebooks (P-010), docs/visuals (P-011/P-012). P-009 produces queryable Delta views + tests only.
- Ruff clean.

## Acceptance Criteria
- **behaviors:**
  - `make reports` builds all five views from existing gold + silver.
  - `make pipeline` runs green end-to-end **twice in a row**.
- **required tests (`tests/test_gold.py`, fast, no JVM):**
  1. All five views materialize as valid Delta (`_delta_log` present; `DeltaTable(path)` opens).
  2. **Totals tie back:** for a given `rule_version`, `sum(report_application_tco.allocated_amount_eur)` == `sum(report_bu_showback.allocated_amount_eur)` == `sum(allocation)`, exactly (Decimal).
  3. **Full reconciliation:** `sum(report_bu_showback) + sum(report_residual)` == `61813.95` exactly. No report implies 100% allocation.
  4. **Driver comparison diverges:** at least one BU has a non-zero `delta_eur` between `v1_transactions` and `v2_named_users`; the seeded `APP-BILLING` case shows a top-BU flip with `share_delta_pp` ≥ 20 for the affected BUs.
  5. **Both versions reconcile:** `v1` and `v2` each satisfy `allocated + residual == 61813.95` exactly — same costs, different splits, both balanced. This is the honest core of the comparison.
  6. **Rule-version isolation:** no view row mixes rule versions; filtering by `rule_version` partitions every view cleanly.
  7. **Residual/lineage views are passthroughs:** their totals equal the P-007/P-008 source tables exactly (proving no recomputation drift).
  8. **Determinism:** two consecutive `make reports` runs produce identical view contents.

## Stop When
- All five views materialize as queryable Delta outputs;
- driver-comparison demonstrably shows divergent BU splits under two rule versions, both reconciling to `61813.95`;
- totals tie back to reconciliation, and no report implies full allocation;
- `make lint` + full `make test` green (seconds); `make pipeline` green twice.
- **Stop — do not build notebooks (P-010) or docs/visuals (P-011/P-012).**

## Output Required
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results **with timings** (`make lint`, `make reports`, `make pipeline` ×2, full `make test`)
4) Commit/PR (hash/link)
5) Risks + next steps — explicitly confirm: all five views built; v1 and v2 both reconcile to `61813.95`; the `APP-BILLING` top-BU flip is present with ≥20pp share delta; residual/lineage views are passthroughs with no drift; suite runtime.
