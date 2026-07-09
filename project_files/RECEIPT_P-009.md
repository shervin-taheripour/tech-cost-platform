# RECEIPT_P-009

## Header

- Packet: `P-009`
- Title: `Gold reports / views`
- Thread: `claude-cli:gold-reports`
- Date: `2026-07-09`
- Status: `Implemented, verified locally, committed`
- Commit: `69ef707` — "P-009: gold report views + driver-comparison (v1 vs v2)"

## Scope

P-009 builds the FinOps output surface: five queryable Delta views under `data/gold/` that surface the engine's allocation results to a reviewer. The centerpiece is the driver-comparison view, which shows the same GL costs producing materially different BU allocations under `v1_transactions` vs `v2_named_users` — the "no perfect driver" thesis rendered as data.

This packet does **not** re-derive allocation, residual, or lineage. It reads them. The two passthrough views (`report_residual`, `report_lineage`) are deliberately thin: a passthrough that adds no drift is correct.

## What Changed

### Gold package — `src/tech_cost_platform/gold/`

Added:

- [views.py](../src/tech_cost_platform/gold/views.py) — five view builder functions (pure Arrow → Arrow). Each view is a standalone function taking Arrow tables and returning an Arrow table. Percentages use `Decimal, ROUND_HALF_UP` at 6 decimal places, matching the P-007 residual pattern; amounts are never rounded.
- [build.py](../src/tech_cost_platform/gold/build.py) — orchestrator. Loads silver and gold inputs, runs `execute_cascade` in-memory for both `v1_transactions` and `v2_named_users` over identical silver tables, validates both reconcile, then writes six Delta tables.
- [__main__.py](../src/tech_cost_platform/gold/__main__.py) — `python -m tech_cost_platform.gold` CLI entrypoint.

Updated:

- [__init__.py](../src/tech_cost_platform/gold/__init__.py) — full public export surface replacing the P-009 stub.

### Outputs

P-009 materialises:

- `data/gold/report_application_tco`
- `data/gold/report_bu_showback`
- `data/gold/report_residual` (passthrough of `residual_report`)
- `data/gold/report_lineage` (passthrough of `lineage`)
- `data/gold/report_driver_comparison`
- `data/gold/report_driver_comparison_by_app` (optional by-app grain — fell out cheaply)

### Pipeline / CLI

Updated:

- [pipeline.py](../src/tech_cost_platform/pipeline.py) — `reports` stage added to `STAGE_SEQUENCE` after `lineage`; `resolve_stages` handles `"reports"` as a standalone-runnable stage.
- [Makefile](../Makefile) — `make reports` target added; `.PHONY` updated.

Behavior now:

- `make reports` builds all six views from existing silver + gold.
- `make pipeline` runs: `synth` → `bronze` → `silver` → `gold` → `residual` → `lineage` → `reports`.

### Tests / harness

Updated:

- [tests/conftest.py](../tests/conftest.py) — `GoldReportsRun` dataclass and `gold_reports` factory fixture added in the established shape. Methods: `ingest_bronze`, `build_silver`, `run_allocation`, `build_residual`, `build_lineage`, `build_reports`, `build` (full stack).
- [tests/test_pipeline.py](../tests/test_pipeline.py) — mock for `build_gold_reports` added to the unit test that patches all stages; `reports` stage assertion added.

Added:

- [tests/test_gold.py](../tests/test_gold.py) — 8 acceptance tests covering all P-009 criteria.

## Acceptance Checks

Confirmed locally:

- All five views (plus optional by-app) materialise as valid Delta tables (`_delta_log` present, non-empty).
- **Totals tie back exactly:** for `v1_transactions`, `sum(report_application_tco.allocated_amount_eur)` == `sum(report_bu_showback.allocated_amount_eur)` == `sum(allocation.allocated_amount_eur)` (Decimal, exact).
- **Full reconciliation:** `sum(report_bu_showback.allocated_amount_eur) + sum(report_residual.residual_amount_eur)` == `61813.95` exactly for `v1_transactions`. The showback total is strictly less than 61813.95 — no report implies 100% allocation.
- **Driver comparison diverges:** `delta_eur != 0` for at least one BU; `max(abs(share_delta_pp)) ≥ 0.20` (20pp).
- **Both versions reconcile:** `v1_transactions`: `3568.14 + 58245.81 = 61813.95` ✓. `v2_named_users`: `11150.42 + 50663.53 = 61813.95` ✓.
- **Rule-version isolation:** views with a `rule_version` column contain only `v1_transactions` rows (pipeline default); no mixing.
- **Residual/lineage views are passthroughs:** `sum(report_residual.residual_amount_eur)` == `sum(residual_report.residual_amount_eur)`; `count(report_lineage)` == `count(lineage)`.
- **Determinism:** two consecutive `build_reports` calls over unchanged upstream produce identical BU showback output.

## Driver Comparison — Design Notes

The driver comparison runs `execute_cascade` (from `engine.cascade`) twice in-memory, once per rule version, over the same silver Arrow tables loaded once. Neither v1 nor v2 results are persisted as separate allocation Delta tables — they are aggregated directly into the comparison view. This guarantees identical silver inputs and avoids `write_delta_table`'s `shutil.rmtree` overwrite problem.

`share_delta_pp` is computed from the raw (pre-rounding) BU shares so that rounding before subtraction does not introduce drift.

`delta_pct` is null when `amount_v1 == 0` — honest, not forced.

The APP-BILLING flip is the dominant signal: under `v1_transactions`, BU-RETAIL receives ~80% of APP-BILLING's allocation (transactions: CORP=1000, RETAIL=8000, WHOLESALE=1000). Under `v2_named_users`, BU-CORP dominates (named_users: CORP=60, RETAIL=20, WHOLESALE=20). This produces a >20pp `share_delta_pp` for both affected BUs, which the test asserts.

## Commands Run

- `.venv/Scripts/python.exe -m ruff check src tests`
  - Result: **passed** — All checks passed.
- `.venv/Scripts/python.exe -m pytest -q`
  - Result: **passed**
  - Suite result: `76 passed in 39.15s`
  - (Prior suite at P-008 close: `68 passed`. Delta of 8 corresponds to `tests/test_gold.py`.)
- `.venv/Scripts/python.exe -m tech_cost_platform.gold` (`make reports`)
  - Result: **passed** — all 6 tables written.
- `.venv/Scripts/python.exe -m tech_cost_platform.pipeline` (run 1)
  - Result: **passed** — all 8 stages including `reports`.
- `.venv/Scripts/python.exe -m tech_cost_platform.pipeline` (run 2)
  - Result: **passed** — identical output, all 8 stages.
- `git commit` → `69ef707`

## Final State

P-009 is complete.

- All five report views are queryable Delta outputs; the optional by-app grain is also present.
- `v1_transactions` and `v2_named_users` both reconcile to `61813.95` exactly — same costs, different splits, both balanced.
- The APP-BILLING top-BU flip is present with ≥20pp `share_delta_pp`.
- `report_residual` and `report_lineage` are exact passthroughs with no drift from their source tables.
- `make lint` + full `make test` green (39s, no JVM).
- `make pipeline` green twice consecutively.
- Depth signals #1–#5 are fully surfaced to reviewers. Remaining: P-010 notebooks, P-011/P-012 documentation track.
