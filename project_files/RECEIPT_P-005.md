# Receipt - P-005

## Header
- Packet: `P-005`
- Title: `Versioned allocation-rule schema + loader + registry`
- Thread: `codex:rules`
- Date: `2026-07-09`
- Status: `Implemented and verified locally for Spark-free checks`

## Scope
Define governed allocation rules as versioned config artifacts rather than hardcoded logic. P-005 had to ship a schema, YAML loader, and registry for whole-cascade rule versions, while staying fully Spark-free and leaving driver math to P-006.

## Outcome
The rules layer is now implemented with:

- Pydantic v2 models for governed whole-cascade rule versions
- strict `extra="forbid"` validation for version metadata and step definitions
- validation of strategy-specific params:
  - `even_spread` carries no params
  - `weighted` requires non-empty non-negative `weights`
  - `consumption` requires `metric_name`
  - `manual_override` requires non-empty non-negative `proportions` summing to `1.0`
- step-aware validation of consumption metrics against synth’s actual emitted `usage_metrics`
- a YAML loader that raises `RuleValidationError` on malformed or invalid files
- a registry that discovers versions from `config/rules/`, lists ids, resolves by `version_id`, and errors on unknown ids
- exactly two shipped rule versions:
  - `v1_transactions`
  - `v2_named_users`
- a surgical-difference test proving the two shipped versions are identical for `gl_to_tower` and `tower_to_app`, and differ only at `app_to_bu`

The packet stayed within scope:

- no SparkSession
- no Delta
- no DataFrames
- no `spark.py` changes
- no engine math
- no pipeline wiring changes
- no changes to `synth/`, `bronze/`, or `silver/` logic

## Files Changed

### Added
- `config/rules/v1_transactions.yaml`
- `config/rules/v2_named_users.yaml`
- `src/tech_cost_platform/rules/schema.py`
- `src/tech_cost_platform/rules/loader.py`
- `src/tech_cost_platform/rules/registry.py`
- `tests/test_rules.py`
- `tests/fixtures/rules_malformed.yaml`
- `project_files/RECEIPT_P-005.md`

### Updated
- `config.yaml`
- `src/tech_cost_platform/rules/__init__.py`

## Commands Run

### Lint
- `make PYTHON=.\\.venv\\Scripts\\python.exe lint`
  - Result: passed

### Rules tests
- `.\\.venv\\Scripts\\python.exe -m pytest tests\\test_rules.py -q`
  - Result: passed
  - Suite result: `12 passed`

### Full Spark suite intentionally not run in-thread
Per the handoff, the thread did not run:

- `make test`
- `make pipeline`
- `make silver`

## Acceptance Check

### Passed
- both shipped rule versions load and validate from `config/rules/`
- `RuleRegistry` lists both ids and resolves each by `version_id`
- unknown version ids raise a clear `RuleValidationError`
- the malformed rule fixture is rejected
- the two shipped versions differ only at `app_to_bu`
- all shipped `metric_name` references exist in synth’s emitted `usage_metrics` for the relevant step
- loading the same version twice yields equal `RuleVersion` objects
- `make lint` is green
- `pytest tests/test_rules.py` is green

## Rule Versions Shipped
- `v1_transactions`
  - `gl_to_tower`: `weighted`
  - `tower_to_app`: `consumption` on `cpu_hours`
  - `app_to_bu`: `consumption` on `transactions`
- `v2_named_users`
  - `gl_to_tower`: identical to `v1_transactions`
  - `tower_to_app`: identical to `v1_transactions`
  - `app_to_bu`: `consumption` on `named_users`

## Deviations / Notes for Dev Ledger
- Rule validation uses synth’s real emitted usage-metric surface rather than a separately invented whitelist, so the config schema stays aligned with the governed fixture contract.
- `gl_to_tower` is intentionally non-consumption in the shipped versions because synth emits no `gl_to_tower` usage metrics.
- The rules packet remained fully Spark-free; the only verification run in-thread was lint plus the dedicated rules test module.

## Risks
- The current state is locally verified for the Spark-free rules surface only; the human still runs the full repo `make test` and push/CI confirmation as the packet requires.
- Future engine work must preserve the “whole cascade pinned by one version id” contract; introducing per-step versioning later would break the governed comparison model established here.

## Next Steps
- Human runs:
  - `make PYTHON=.\\.venv\\Scripts\\python.exe test`
- Push and confirm GitHub Actions is green
- P-006 can now consume `RuleRegistry` / `RuleVersion` and implement the actual driver math without redefining the config surface
