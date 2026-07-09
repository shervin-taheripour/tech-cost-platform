# Receipt - P-004

## Header
- Packet: `P-004`
- Title: `Silver conformance layer`
- Thread: `codex:silver`
- Date: `2026-07-08`
- Status: `Implemented and verified locally; 2026-07-09 follow-up moved runtime staging to data/_staging and extracted a shared staged-write helper with lint/targeted checks green`

## Scope
Build the silver layer on top of bronze Delta so the repo produces clean, typed, conformed fact and dimension Delta tables for downstream allocation work. Silver had to preserve bronze lineage keys and the governed GL reconciliation total `61813.95`, while staying strictly out of allocation, rules, residual, and engine scope.

## Outcome
The silver layer is now implemented with:

- native Spark conformance transforms for bronze-to-silver normalization
- explicit conformed output tables for:
  - `dim_cost_center`
  - `dim_resource_tower`
  - `dim_application`
  - `dim_business_unit`
  - `fact_gl_cost`
  - `fact_usage_metric`
- native Spark DQ checks for:
  - conflicting duplicate dimension PKs
  - PK uniqueness
  - FK completeness
  - non-negative measures
  - reconciliation to `61813.95`
- a real silver build entrypoint so `python -m tech_cost_platform.silver` runs the stage
- pipeline wiring so `synth -> bronze -> silver` now runs for real while gold remains no-op
- pipeline lifecycle hardening so bronze and silver share one Spark session inside a single `make pipeline` run
- a shared runtime Delta write helper that stages under neutral gitignored runtime scratch space at `data/_staging/` and then moves the completed table into place
- Windows-safe silver output handling that uses that shared helper for each table written into `data/silver/`
- self-contained offline silver tests built on the shared P-003.1 fixture harness
- a function-scoped `silver` factory in `tests/conftest.py` that reuses the shared Spark session and writes to isolated per-test dirs
- a seeded-bad conflicting-duplicate fixture proving silver DQ actually fails on bad input
- regression tests covering:
  - single-session pipeline orchestration
  - staged runtime output moves into the final target path

The packet stayed within scope:

- no allocation logic
- no rules logic
- no residual logic
- no lineage view work
- no changes to `synth/` contracts
- no changes to `bronze/` behavior beyond reading its output
- no net changes to `spark.py` in the completed packet state

## Files Changed

### Added
- `src/tech_cost_platform/delta_io.py`
- `src/tech_cost_platform/silver/__main__.py`
- `src/tech_cost_platform/silver/build.py`
- `src/tech_cost_platform/silver/conform.py`
- `src/tech_cost_platform/silver/dq.py`
- `tests/test_pipeline.py`
- `tests/test_silver.py`
- `tests/test_silver_build.py`
- `tests/fixtures/cost_centers_duplicate_conflict.csv`
- `project_files/RECEIPT_P-004.md`

### Updated
- `Makefile`
- `config.yaml`
- `src/tech_cost_platform/pipeline.py`
- `tests/conftest.py`

## Commands Run

### Lint
- `make PYTHON=.\\.venv\\Scripts\\python.exe lint`
  - Result: passed
- `.\\.venv\\Scripts\\python.exe -m ruff check src tests`
  - Result: passed

### Follow-up maintenance checks
- `git grep -n "test-runs" -- src/`
  - Result: no matches
- `.\\.venv\\Scripts\\python.exe -m pytest tests\\test_silver_build.py tests\\test_pipeline.py -q`
  - Result: passed
  - Suite result: `2 passed`

### Human-run Spark verification
Per the current P-004 handoff, the Spark-heavy commands were run in the human terminal rather than inside the CLI-thread turn.

- `Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force`
  - Result: used before reruns to clear stale JVM state
- `Remove-Item -Recurse -Force data\\source,data\\bronze,data\\silver,data\\test-runs,data\\warehouse,data\\spark-local,data\\python-temp -ErrorAction SilentlyContinue`
  - Result: passed
- `make PYTHON=.\\.venv\\Scripts\\python.exe pipeline`
  - Result: passed
  - Output confirmed:
    - `stage=synth status=completed`
    - `stage=bronze status=completed`
    - `stage=silver status=completed`
    - `stage=gold status=no-op`
    - `pipeline status=completed`
- `$env:PYTEST_ADDOPTS='--timeout=300'; make PYTHON=.\\.venv\\Scripts\\python.exe test`
  - Result: passed
  - Suite result: `19 passed in 203.81s (0:03:23)`
- `Remove-Item Env:PYTEST_ADDOPTS`
  - Result: passed

### Standalone silver target
- `make PYTHON=.\\.venv\\Scripts\\python.exe silver`
  - Result: not separately re-run after the final write-path fix
  - Note: the same silver build path completed successfully inside the clean-input `make pipeline` run

## Acceptance Check

### Implemented
- silver build orchestration exists and is wired to `python -m tech_cost_platform.silver`
- pipeline silver stage is now real
- conformed fact and dimension tables are defined explicitly
- DQ checks are implemented as native Spark aggregations
- the nullable-`tower_id` exception is preserved in the design and tests
- self-contained silver tests build their own inputs via the shared fixture harness
- the seeded bad fixture proves conflicting duplicate dimension PKs fail DQ
- clean-input / clean-input-from-scratch proof passed locally
- `make lint` is green
- `make pipeline` is green
- `make test` is green

### Remaining Note
- standalone `make silver` was not re-run separately after the final Windows write-path fix, although the same silver build path completed successfully inside `make pipeline`
- the `2026-07-09` follow-up staging-path refactor was intentionally verified with lint + targeted non-Spark checks only; the human still re-runs `make pipeline`, `make silver`, and `make test` in-terminal per packet rule
- GitHub Actions confirmation remains pending until the current state is pushed and CI runs

## Silver Behavior Implemented
- bronze Delta is read table-by-table from the expected governed inputs
- ids and text are trimmed
- blank optional tower ids normalize to `NULL`
- period values normalize to canonical `YYYY-MM`
- `amount_eur` and `value` are cast to numeric decimals
- dimensions are deduped, but conflicting duplicate PK variants fail DQ instead of silently winning
- `fact_gl_cost` preserves one row per `gl_line_id` and carries forward the nullable unmapped tower case
- `fact_usage_metric` preserves the long-form multi-driver signals required by downstream allocation

## Deviations / Notes for Dev Ledger
- The current handoff explicitly required that `spark.py` remain untouched because it carries the P-000-FIX Windows startup hardening. Any accidental drift there was backed out during packet cleanup, so the final packet state leaves `spark.py` unchanged.
- The current handoff explicitly split verification responsibility: code and tests are authored in-thread, but long-running Spark verification is executed by the human in-terminal with `--timeout=300` to avoid false hang reports.
- The silver tests follow the P-003.1 harness pattern exactly: one shared Spark session for the full run, read-only upstream synth data at session scope, and fresh per-test bronze/silver/warehouse output dirs for every writing test.
- During human-run verification, `make pipeline` initially failed on Windows with `DELTA_CANNOT_CREATE_LOG_PATH` when Delta attempted to create `_delta_log` directly under the canonical runtime output paths. The final fix was twofold:
  - share one Spark session across bronze and silver inside pipeline execution
  - stage silver Delta writes and move the completed table directories into `data/silver/`
- Diagnostic probes showed the issue was not silver-model-specific: trivial one-row Delta writes could succeed under the test harness workspace while failing under the canonical runtime output path, which is why the final workaround targeted the write path rather than the conformance logic.
- The `2026-07-09` follow-up moved runtime staging out of the test harness scratch path into neutral runtime scratch space at `data/_staging/`, and extracted the write-then-move logic into shared helper `src/tech_cost_platform/delta_io.py` so the future gold layer can reuse the same Windows workaround.
- `.gitignore` already covered the new staging root via `data/*`, so no ignore rule change was required.

## Reconciliation Lock
- Silver DQ and tests assert that `sum(fact_gl_cost.amount_eur) == 61813.95`
- The passing `19`-test suite includes the silver reconciliation assertion, so the implemented path was confirmed locally on this machine

## Risks
- Because Spark cold-start is intentionally slow on this machine, any verification run without a generous timeout risks a false failure.
- Standalone `make silver` was not re-run separately after the final path fix, though the same build logic completed inside `make pipeline`.
- After the `2026-07-09` refactor, the human still needs to re-run `make pipeline`, standalone `make silver`, and `make test` in-terminal to reconfirm the unchanged runtime behavior.
- GitHub Actions has not yet confirmed the current packet state on Linux.

## Next Steps
- Re-run:
  - `make PYTHON=.\\.venv\\Scripts\\python.exe pipeline`
  - `make PYTHON=.\\.venv\\Scripts\\python.exe silver`
  - `$env:PYTEST_ADDOPTS='--timeout=300'; make PYTHON=.\\.venv\\Scripts\\python.exe test`
- Push the current state and record the GitHub Actions result for the Dev Ledger
- Carry the verified silver outputs forward into P-005 / P-006 work
