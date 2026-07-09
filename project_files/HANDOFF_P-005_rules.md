# HANDOFF BUNDLE

## Header
Packet: P-005 — Versioned allocation-rule schema + loader + registry
CLI Thread: codex:rules

## Goal
Define allocation rules as **governed, versioned config artifacts** rather than hardcoded logic. Ship a rule schema (which driver applies at which cascade step), a validating loader, and a version registry that resolves a version by id. This is depth signal **#5 (rule versioning)** — primary — and it is what lets P-006 swap drivers per step and P-009 prove driver divergence.

**This packet is deliberately Spark-free.** Rules are config + Pydantic + file I/O. No SparkSession, no Delta, no DataFrames. Tests run in milliseconds.

## Start Here (read the current repo before planning)
The repo at its current commit is the source of truth. Read:
- `config.yaml` — runtime config conventions; you will add a `rules:` block.
- `src/tech_cost_platform/bronze/contracts.py` — the repo's Pydantic v2 idiom. Mirror its style.
- `src/tech_cost_platform/synth/` — the driver signals that rules must reference. In particular `usage_metrics` has `step` ∈ {`tower_to_app`, `app_to_bu`} and `metric_name` ∈ {`cpu_hours`, `storage_gb`, `named_users`, `ticket_count`, `transactions`, `headcount`, `revenue_share`, …}. **Rule driver references must be consistent with what synth actually emits** — read the generator, don't assume.
- `tests/conftest.py` — the shared harness. Rules tests need **none** of its Spark fixtures.
- `Makefile`, `.github/workflows/ci.yml` — conventions.

Then read the P-001→P-004 receipts for context, then execute this bundle to its stop-when.

## Execution Rule (carry-forward, non-negotiable)
- **The thread writes code and tests. The human runs verification in their own terminal.** Do not run the Spark suite in-thread.
- This packet's own tests are Spark-free and fast — the thread MAY run `pytest tests/test_rules.py` and `make lint` directly, since neither starts a JVM. Do **not** run `make test` (full suite pulls in Spark), `make pipeline`, or `make silver`.

## The Cascade (fixed — do not redesign)
Three steps, each with its **own** driver:
1. `gl_to_tower` — GL cost lines → resource towers
2. `tower_to_app` — towers → applications
3. `app_to_bu` — applications → business units

## Versioning Model (decided)
- **A rule version covers the WHOLE cascade.** One version id = a complete, self-contained definition of all three steps. There is no per-step versioning. Pinning a version id must reproduce a prior allocation exactly.
- Version ids are stable strings. Each version carries metadata: `version_id`, `description`, `created` (a fixed date string, not `now()` — determinism), and the three step definitions.

## Driver Strategies (the vocabulary rules may reference)
The rule schema must express, per step, which strategy applies and its parameters. Strategies (P-006 implements the math; P-005 only defines/validates the config surface):
- `even_spread` — equal split across targets. No metric needed.
- `weighted` — split by static weights declared in the rule itself (e.g. tower weights). Params: a mapping of target → weight.
- `consumption` — split by a usage metric from `usage_metrics`. Params: `metric_name` (must be a metric synth actually emits for that step).
- `manual_override` — explicit fixed proportions declared in the rule. Params: mapping of target → proportion.

Schema validation must enforce, at minimum:
- strategy name is one of the known strategies;
- `consumption` requires a `metric_name`, and that metric must be valid **for that step** (a `tower_to_app` step cannot reference an `app_to_bu`-only metric);
- `weighted` / `manual_override` require non-empty params with non-negative values; `manual_override` proportions must sum to 1.0 within a small tolerance;
- `even_spread` must NOT carry a `metric_name`;
- unknown/extra fields are rejected (Pydantic `extra="forbid"`) — rules are governed artifacts, typos must fail loudly;
- every one of the three cascade steps is present exactly once.

## The Two Shipped Rule Versions (required — seeds the P-009 driver-comparison)
Ship **exactly two** versions in `config/rules/`, and they must be **surgically different**:
- `v1_transactions` — `gl_to_tower`: (your choice, e.g. `weighted` or `consumption`); `tower_to_app`: a consumption driver; `app_to_bu`: `consumption` on **`transactions`**.
- `v2_named_users` — **identical to `v1_transactions` in `gl_to_tower` and `tower_to_app`**; differs **only** in `app_to_bu`, which uses `consumption` on **`named_users`**.

The isolation is the point: P-009 must be able to claim the *only* cause of divergent BU splits is the app→BU driver choice. If the versions differ anywhere else, the comparison proves nothing. **A test must assert this**: the two versions' `gl_to_tower` and `tower_to_app` definitions are equal, and their `app_to_bu` definitions differ.

(These two metrics are the divergence pair P-002 deliberately encoded — `APP-BILLING` flips its top BU between `transactions` and `named_users` with ≥20pp share delta. Do not invent different metrics.)

## Repo Targets
- `config/rules/v1_transactions.yaml`
- `config/rules/v2_named_users.yaml`
- `src/tech_cost_platform/rules/`:
  - `schema.py` — Pydantic v2 models: `RuleVersion`, `StepRule`, strategy params. `extra="forbid"`.
  - `loader.py` — load + validate a rule YAML into a `RuleVersion`; raise a clear `RuleValidationError` on malformed input.
  - `registry.py` — discover rule versions from `config/rules/`, resolve by `version_id`, list available versions, raise on unknown id.
  - `__init__.py` — export the public surface (`load_rule_version`, `RuleRegistry`, `RuleVersion`, `RuleValidationError`).
- `config.yaml` — add a `rules:` block: `rules_dir: config/rules`, `default_version: v1_transactions`.
- `tests/test_rules.py` — Spark-free tests.
- `tests/fixtures/rules_malformed.yaml` — a deliberately invalid rule for the negative test.

## Constraints / Guardrails
- **Spark-free:** no `spark.py` import, no SparkSession, no Delta, no DataFrame. If you find yourself importing pyspark, stop — you've left scope.
- **Determinism:** no `datetime.now()` in rule metadata or loading. Fixed date strings in the YAML.
- **do-not-touch:** `spark.py` (carries P-000-FIX hardening), `synth/`, `bronze/`, `silver/`, `pipeline.py`, `conftest.py` Spark fixtures. Do not wire rules into the pipeline — the engine (P-006) consumes them, not this packet.
- **No engine math.** Define and validate the config surface only. Do not implement `even_spread`/`weighted`/`consumption`/`manual_override` split logic — that is P-006.
- **No docs obligations** — code + tests + receipt only (docs consolidate in P-011/P-012).
- **style:** mirror repo conventions; Ruff clean; Pydantic v2 as in `bronze/contracts.py`.

## Acceptance Criteria
- **behaviors:**
  - Both rule versions load and validate from `config/rules/`.
  - `RuleRegistry` lists both versions and resolves each by `version_id`; resolving an unknown id raises a clear error.
  - A malformed rule (`tests/fixtures/rules_malformed.yaml`) is **rejected** with `RuleValidationError`.
  - Loading is reproducible: loading the same version twice yields equal objects (a version pin is stable).
- **required tests (`tests/test_rules.py`, Spark-free, fast):**
  1. Both shipped versions load; each has exactly the three cascade steps, each with a valid strategy.
  2. **Surgical-difference test:** `v1_transactions` and `v2_named_users` have *equal* `gl_to_tower` and `tower_to_app` step definitions, and *differing* `app_to_bu` definitions (`transactions` vs `named_users`).
  3. Registry: lists both ids; resolves each; unknown id raises.
  4. Schema rejects, each with its own assertion: unknown strategy name; `consumption` missing `metric_name`; `consumption` referencing a metric invalid for that step; `manual_override` proportions not summing to 1.0; `even_spread` carrying a `metric_name`; an extra/unknown field; a missing cascade step.
  5. Driver references are real: every `metric_name` referenced by a shipped rule exists in synth's emitted `usage_metrics` for that step (assert against the synth schema/constants — **no Spark**, read the generator's definitions).
  6. Determinism: loading a version twice produces equal `RuleVersion` objects.
- **verification split:** thread runs `make lint` and `pytest tests/test_rules.py` (both fast, no JVM). Human runs the full `make test` + push for CI.

## Stop When
- Two surgically-different rule versions load from `config/rules/`;
- schema validation rejects each malformed case above;
- the registry resolves a version by id and errors on unknown ids;
- a version pin is reproducible;
- `make lint` and `pytest tests/test_rules.py` are green.
- **Stop — do not implement driver math, do not touch the engine, do not start P-006.**

## Output Required
Return, in this order:
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results (`make lint`, `pytest tests/test_rules.py` — do NOT run the Spark suite)
4) Commit/PR (hash/link, if created)
5) Risks + next steps (confirm the two versions differ *only* at `app_to_bu`, and that all referenced metrics exist in synth — for the Dev Ledger receipt)
