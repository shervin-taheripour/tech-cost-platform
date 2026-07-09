# HANDOFF BUNDLE

## Header
Packet: P-005.1 — `gl_to_tower` mapping-first semantics + explicit `on_unmapped` fallback
CLI Thread: codex:rules-mapping

## Goal
Correct a semantic gap in the P-005 rule schema. `gl_to_tower` must be **mapping-first**: GL cost follows its cost-center's `tower_id`. The declared strategy is **not** the primary allocation basis for that step — it is an *optional, explicitly opted-in fallback* for cost the mapping cannot resolve. By default, unmapped cost becomes **residual**, never force-spread.

This encodes the governance decision ("do we spread unmapped cost, or report it?") as versioned config rather than hardcoded behavior. **Spark-free packet** — schema, YAML, tests only.

## Background (why this exists)
P-005 shipped `gl_to_tower: weighted` in both rule versions. Read literally, that means "spread all GL cost across towers by static weights" — which would ignore the `cost_centers.tower_id` mapping that synth deliberately generates, and would silently force-spread the `CC-LEGACY` null case that P-002 seeded as the `unmapped` residual fixture. Both are wrong for a TBM-credible model: the mapping is the allocation basis, and unallocatable cost must surface, not disappear into a weighted spread.

## The Corrected `gl_to_tower` Contract
1. **Mapping is primary.** A GL line allocates to the tower given by its cost center's `tower_id`. 100% of that line to that tower. This is a direct mapping, not a split.
2. **Unresolved cost (null `tower_id`) hits the fallback decision:**
   - If the rule declares **no** `on_unmapped` → the cost exits the cascade as residual with reason code `unmapped`. **This is the default.**
   - If the rule declares `on_unmapped: <strategy>` → the unmapped cost is spread across towers by that strategy (e.g. `weighted` with declared tower weights). This is an explicit, versioned, auditable choice.
3. **Nothing is ever dropped.** Whichever branch runs, `allocated + residual` must equal the input total.

## Schema Changes (`src/tech_cost_platform/rules/schema.py`)
- `gl_to_tower` step no longer takes a top-level allocation `strategy` as its primary basis. Model it explicitly as a mapping step, e.g.:
  - `basis: "cost_center_mapping"` (only valid value for this step — makes the mapping explicit and auditable rather than implied)
  - `on_unmapped:` **optional**. When absent → residual. When present → a full strategy spec (same strategy vocabulary + param validation as other steps: `even_spread`, `weighted`, `consumption`, `manual_override`).
- `tower_to_app` and `app_to_bu` are **unchanged** — they remain strategy-driven split steps.
- Validation:
  - `gl_to_tower` must not carry a bare split `strategy` at the top level (reject with a clear message pointing at `on_unmapped`).
  - If `on_unmapped` is present, its strategy + params validate exactly as elsewhere (`consumption` requires a step-valid `metric_name`; `weighted` requires non-empty non-negative weights; `manual_override` proportions sum to 1.0; `even_spread` carries no metric).
  - `on_unmapped: consumption` is **invalid** at `gl_to_tower` — synth emits no `gl_to_tower` usage metrics. Reject it explicitly.
  - Keep `extra="forbid"` throughout.

## YAML Changes (`config/rules/`)
Both shipped versions must express **mapping-first with NO fallback**, so the `CC-LEGACY` null lands in residual as `unmapped` (preserving the P-002 seeded case and P-007's demo):
- `v1_transactions.yaml` — `gl_to_tower: { basis: cost_center_mapping }` (no `on_unmapped`).
- `v2_named_users.yaml` — **identical** `gl_to_tower` and `tower_to_app`; still differs **only** at `app_to_bu` (`transactions` vs `named_users`).

The surgical-difference guarantee from P-005 must survive this change.

## Repo Targets
- `src/tech_cost_platform/rules/schema.py` (amend)
- `config/rules/v1_transactions.yaml`, `config/rules/v2_named_users.yaml` (amend)
- `src/tech_cost_platform/rules/loader.py` / `registry.py` (only if the schema change requires it)
- `tests/test_rules.py` (extend)
- `tests/fixtures/` — add a fixture exercising an invalid `gl_to_tower` (e.g. bare `strategy`, or `on_unmapped: consumption`)

## Constraints / Guardrails
- **Spark-free.** No pyspark import, no SparkSession, no Delta.
- **do-not-touch:** `spark.py`, `synth/`, `bronze/`, `silver/`, `pipeline.py`, Spark fixtures in `conftest.py`.
- **No engine math.** Do not implement mapping or split logic — P-006 consumes this contract.
- No docs obligations (docs consolidate in P-011/P-012).
- Ruff clean; Pydantic v2 idiom per `bronze/contracts.py`.

## Acceptance Criteria
- **required tests (`tests/test_rules.py`, Spark-free, fast):**
  1. Both shipped versions load; `gl_to_tower` has `basis: cost_center_mapping` and **no** `on_unmapped`.
  2. Surgical-difference still holds: `v1` and `v2` are equal at `gl_to_tower` and `tower_to_app`, differ only at `app_to_bu`.
  3. A rule declaring `on_unmapped: weighted` (with valid weights) **loads and validates** — proving the fallback capability exists and is versioned.
  4. A rule declaring `on_unmapped: consumption` at `gl_to_tower` is **rejected** (no `gl_to_tower` usage metrics exist in synth).
  5. A `gl_to_tower` carrying a bare top-level split `strategy` is **rejected** with a message directing the author to `on_unmapped`.
  6. All prior P-005 validation tests still pass (unknown strategy, missing `metric_name`, `manual_override` sum, `even_spread` with metric, extra field, missing step, registry resolve/unknown-id, determinism).
- **verification split:** the thread runs `make lint` and `pytest tests/test_rules.py` (fast, no JVM). The thread must **not** run `make test`, `make pipeline`, or `make silver`. The human runs the full suite + push/CI.

## Stop When
- `gl_to_tower` is mapping-first with optional, validated `on_unmapped`;
- both shipped versions declare no fallback (unmapped → residual);
- the fallback capability is proven loadable by test, and invalid fallbacks are rejected;
- surgical difference preserved;
- `make lint` + `pytest tests/test_rules.py` green.
- **Stop — do not implement engine math, do not start P-006.**

## Output Required
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results (`make lint`, `pytest tests/test_rules.py` only)
4) Commit/PR (hash/link, if created)
5) Risks + next steps (confirm shipped versions have no fallback, so `CC-LEGACY` still becomes `unmapped` residual)
