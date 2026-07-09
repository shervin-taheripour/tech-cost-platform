# Receipt - P-005.1 / P-006

## Header
- Packet: `P-005.1` + `P-006`
- Title: `Mapping-first rules contract + allocation engine core`
- Thread: `codex:rules-mapping` / `codex:engine`
- Date: `2026-07-09`
- Status: `Implemented; targeted engine verification green; full repo suite still not fully green on this Windows machine`

## Scope
These two packets were executed together, in order:

1. `P-005.1` corrected the `gl_to_tower` contract so it is explicitly mapping-first, with an optional governed `on_unmapped` fallback.
2. `P-006` implemented the actual three-step allocation engine (`gl -> tower -> app -> bu`) with residual handling, strategy-driven split math, and gold Delta outputs.

The work had to preserve the P-000 Spark hardening, stay out of P-007/P-008/P-009 scope, and keep strategy math pure Python with Spark as a thin adapter.

## Outcome

### P-005.1
The rules contract is now:

- `gl_to_tower` uses `basis: cost_center_mapping`
- a bare top-level split `strategy` at `gl_to_tower` is rejected
- optional `on_unmapped` fallback is supported and versioned
- `on_unmapped: consumption` is explicitly rejected because synth emits no `gl_to_tower` usage metrics
- shipped versions now declare no fallback, so `CC-LEGACY` still exits as `unmapped` residual by default

### P-006
The engine core is now implemented with:

- pure Python strategy math in `src/tech_cost_platform/engine/strategies.py`
- Spark adapter/orchestration in `src/tech_cost_platform/engine/cascade.py`
- 4 strategy types implemented:
  - `even_spread`
  - `weighted`
  - `consumption`
  - `manual_override`
- first-class residual output with:
  - `failed_step`
  - `reason_code`
  - `rule_version`
- 2 gold outputs:
  - `allocation`
  - `residual`
- rule-version-aware divergence between `v1_transactions` and `v2_named_users`
- exact reconciliation checks to `61813.95`
- engine test harness support in `tests/conftest.py`
- dedicated pure strategy tests and Spark engine integration tests

## Files Changed

### Added
- `src/tech_cost_platform/engine/cascade.py`
- `src/tech_cost_platform/engine/strategies.py`
- `tests/test_engine.py`
- `tests/test_strategies.py`
- `tests/fixtures/rules_gl_to_tower_invalid_bare_strategy.yaml`
- `project_files/RECEIPT_P-005.1_P-006.md`

### Updated
- `config/rules/v1_transactions.yaml`
- `config/rules/v2_named_users.yaml`
- `src/tech_cost_platform/bronze/ingest.py`
- `src/tech_cost_platform/engine/__init__.py`
- `src/tech_cost_platform/rules/schema.py`
- `tests/conftest.py`
- `tests/test_rules.py`
- `tests/test_smoke.py`

## Commands Run

### Thread-run non-Spark checks
- `make PYTHON=.\\.venv\\Scripts\\python.exe lint`
  - Result: passed
- `.\\.venv\\Scripts\\python.exe -m pytest tests\\test_rules.py -q`
  - Result: passed
  - Suite result: `15 passed`
- `.\\.venv\\Scripts\\python.exe -m pytest tests\\test_strategies.py -q`
  - Result: passed
  - Suite result: `8 passed`

### Human-run targeted Spark verification
- `.\\.venv\\Scripts\\python.exe -m pytest tests\\test_engine.py -x -vv --timeout=300`
  - Final result after reboot and harness refactors: passed
  - Suite result: `5 passed in 181.74s (0:03:01)`
- `.\\.venv\\Scripts\\python.exe -m pytest tests\\test_engine.py::test_rule_version_pinning_is_reproducible_for_same_silver_inputs -vv --timeout=300`
  - Result after reboot: passed
  - Suite result: `1 passed in 94.99s`

### Human-run full-suite verification
- `$env:PYTEST_ADDOPTS='--timeout=300'; make PYTHON=.\\.venv\\Scripts\\python.exe test`
  - Current result: failed
  - Suite result: `45 passed, 2 failed in 652.07s (0:10:52)`

## Acceptance Check

### P-005.1 accepted locally
- shipped rule versions load with `gl_to_tower.basis == cost_center_mapping`
- both shipped versions declare no `on_unmapped`
- invalid bare `gl_to_tower.strategy` is rejected with guidance toward `on_unmapped`
- valid `on_unmapped: weighted` loads
- invalid `on_unmapped: consumption` is rejected
- surgical difference between `v1` and `v2` is preserved

### P-006 accepted for targeted engine scope
- engine strategies implemented and tested
- `strategies.py` contains zero `pyspark` imports
- direct engine suite now passes when run on its own
- engine assertions proven in targeted run:
  - allocation + residual outputs written
  - reconciliation holds to `61813.95`
  - no double-counting
  - seeded residual reasons surface
  - `v1` vs `v2` diverge at BU level
  - pinned rerun is reproducible

### Not yet accepted for repo-wide green
- full `make test` is still not green on this Windows machine

## Bug Fix Attempts and What They Meant

### 1. Bronze direct Delta writes still hit Windows `_delta_log` failures
Initial full-suite failures showed bronze still writing Delta directly to canonical output paths.

Fix applied:
- `src/tech_cost_platform/bronze/ingest.py` was changed to reuse the shared staged-write helper rather than calling `.save()` directly on the final path.

Effect:
- bronze-specific `_delta_log` path failures stopped being the first blocker.

### 2. Engine gold output crashed in the Python worker
The first engine failure moved to gold output writing:

- `allocation_df` / `residual_df` were being created through `spark.createDataFrame(...)` from Python tuples
- Spark failed with `Python worker exited unexpectedly (crashed)` / `EOFException`

Fix applied:
- engine output serialization was changed so the tiny driver-side gold rows are staged as CSV and then read back into Spark with an explicit schema before Delta write

Effect:
- the direct engine suite stopped crashing at gold write.

### 3. Engine reproducibility test timed out repeatedly
The remaining engine-only failure became the last pinning test. Several attempts were made:

- first attempt:
  - reuse one prepared silver snapshot
  - result: still timed out
- second attempt:
  - try to create a fresh Spark session just for the last test
  - result: insufficient, because `SparkSession.builder.getOrCreate()` reused an existing session
- third attempt:
  - reduce Spark sharing by moving the general `spark` fixture from session scope to module scope
  - collapse engine integration tests onto one shared bronze/silver preparation plus reused gold outputs

Effect:
- after reboot, `tests/test_engine.py` passed cleanly end-to-end.

## Current Situation
The current state is mixed:

- `P-005.1` itself is done
- `P-006` engine implementation itself is done
- direct engine verification is green
- full repo verification is not yet green

The remaining full-suite failures are not rule-schema assertion failures and not engine-logic assertion failures. They are late-run Spark runtime stability failures on this Windows machine.

From the latest full-suite log:

- `tests/test_silver.py::test_silver_preserves_driver_zero_and_divergence_usage_signals`
  - failed while writing bronze Delta
  - Spark reported `SparkContext was shut down`
- `tests/test_smoke.py::test_delta_round_trip`
  - failed afterward because the Py4J / JVM connection was already dead
- late in the same run, Spark logged:
  - `java.lang.OutOfMemoryError: Java heap space`

Interpretation:
- the engine module is no longer the only problem
- the long full-suite Spark workload is still capable of destabilizing the JVM
- once Spark dies late in the run, downstream Spark tests fail secondarily

## Plain-English Situation Summary
The packets are implemented, and the engine works when exercised directly. The repo is now past the original rule-contract bug, past the Windows `_delta_log` bronze bug, and past the engine Python-worker crash. The remaining issue is broader full-suite Spark stability under Windows: after a long run, the JVM can still fall over, which then causes later Spark tests to fail even though the packet-specific assertions already passed in isolation.

So the honest state is:

- feature work: mostly complete
- targeted packet verification: green
- repo-wide stability verification: still incomplete

## Risks
- Full-suite Spark runs on this Windows machine remain fragile under long runtimes.
- `make test` can still fail after many Spark-heavy tests even when targeted packet suites pass.
- The present instability looks infrastructure/runtime-related rather than P-005.1/P-006 business-logic-related, but it still blocks a clean “all green” closeout.

## Next Steps
- Decide whether to treat this as a follow-up Spark-runtime-hardening task rather than a P-005.1/P-006 logic defect.
- If the goal is full-suite green on Windows, the next likely work area is broader Spark test-runtime reduction / memory hardening, not more rules or engine math changes.
- Re-run:
  - `.\.venv\\Scripts\\python.exe -m pytest tests\\test_engine.py -x -vv --timeout=300`
  - `$env:PYTEST_ADDOPTS='--timeout=300'; make PYTHON=.\\.venv\\Scripts\\python.exe test`

