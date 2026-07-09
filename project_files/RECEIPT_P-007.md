# RECEIPT_P-007

Date: 2026-07-09

## Header

- Packet: `P-007`
- Title: `Residual / unallocated cost handling`
- Status: `Implemented and verified locally`

## Scope

P-007 turns the engine's raw `data/gold/residual` output into a first-class FinOps reporting surface.

This packet does **not** re-derive residuals. It:

- reads the engine's `allocation` and `residual` outputs
- reconciles `allocated + residual == total_gl` exactly
- enriches residual detail with silver context
- aggregates quantified residual reporting by:
  - `rule_version`
  - `failed_step`
  - `reason_code`

## What Changed

### Residual package

Added:

- [report.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/residual/report.py)
- [reconcile.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/residual/reconcile.py)
- [__main__.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/residual/__main__.py)

Updated:

- [__init__.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/residual/__init__.py)

### Outputs

P-007 now materializes:

- `data/gold/residual_detail`
- `data/gold/residual_report`
- `data/gold/reconciliation`

`residual_detail` includes:

- `gl_line_id`
- `amount_eur`
- `gl_account`
- `cost_center_id`
- `cost_center_name`
- `failed_step`
- `reason_code`
- `rule_version`
- `app_id`

`residual_report` includes one row per (`rule_version`, `failed_step`, `reason_code`) with:

- `residual_amount_eur`
- `gl_line_count`
- `pct_of_total_gl`

### Pipeline / CLI

Updated:

- [pipeline.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/pipeline.py)
- [Makefile](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/Makefile)

Behavior now:

- `make residual` builds residual outputs from existing silver/gold
- `make pipeline` runs:
  - `synth`
  - `bronze`
  - `silver`
  - `gold`
  - `residual`

### Tests / harness

Updated:

- [tests/conftest.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/conftest.py)
- [tests/test_pipeline.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/test_pipeline.py)

Added:

- [tests/test_residual.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/test_residual.py)

## Reconciliation Contract

P-007 adds an explicit reusable reconciliation function in [reconcile.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/residual/reconcile.py).

For a given `rule_version`, it returns:

- `total_gl_eur`
- `total_allocated_eur`
- `total_residual_eur`
- `balanced`
- `difference_eur`

It raises `ReconciliationError` when balance does not hold exactly.

No float tolerance is used.

## Reason-Code Handling

P-007 surfaces the engine-produced reason codes without remapping:

- `unmapped`
- `shared_unattributable`
- `driver_zero`

Verified seeded cases:

- `CC-LEGACY` -> `unmapped` at `gl_to_tower`
- `APP-EMAIL` -> `shared_unattributable` at `app_to_bu`
- `APP-ANALYTICS` -> `driver_zero` at `app_to_bu` under a `storage_gb` rule version

## Acceptance Checks

Confirmed locally:

- residual outputs materialize as valid Delta tables
- reconciliation passes exactly for `v1_transactions`
- reconciliation fails on tampered residual input
- `allocated + residual == 61813.95` exactly
- residual detail sum equals residual report sum
- no residual line is silently force-spread into a full allocation
- `driver_zero` is rule-version-dependent
- `pct_of_total_gl` uses deterministic rounding and sums correctly

## Commands Run

- `make PYTHON=.\.venv\Scripts\python.exe lint`
  - passed
- `.\\.venv\\Scripts\\python.exe -m pytest tests\\test_residual.py tests\\test_pipeline.py -q`
  - passed
  - `11 passed in 9.12s`
- `make PYTHON=.\.venv\Scripts\python.exe residual`
  - passed
- `make PYTHON=.\.venv\Scripts\python.exe pipeline`
  - passed
- `make PYTHON=.\.venv\Scripts\python.exe pipeline`
  - passed again
- `make PYTHON=.\.venv\Scripts\python.exe test`
  - passed
  - `57 passed in 17.61s`

## Final State

P-007 is complete.

- residual is now a quantified, reviewable, Delta-backed reporting surface
- reconciliation is explicit and reusable
- the check can fail on tampered input
- all three reason codes are preserved accurately
- `make pipeline` remains green on consecutive runs
- full suite remains fast and green on the delta-rs + DuckDB runtime
