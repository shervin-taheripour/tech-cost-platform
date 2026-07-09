# HANDOFF BUNDLE

## Header
Packet: P-006 — Allocation engine core (THE HEADLINE)
CLI Thread: codex:engine

## Goal
Implement the multi-tier cascading allocation engine: **GL → resource towers → applications → business units**, where each step uses its own driver, drivers are pluggable strategies selected from the versioned rule config (P-005/P-005.1), and cost that cannot be allocated **exits the cascade into a first-class residual output** — never dropped, never silently force-spread.

This is depth signal **#1 (multi-step, step-specific drivers)** and **#2 (driver variety / "no perfect driver")**. It is the core proof of the entire project. Do not compress it.

## Architectural Requirement (non-negotiable — read carefully)
**The driver math is pure Python. Spark is a thin adapter around it.**

A driver strategy answers exactly one question: *given a set of targets and their signals, what proportion of cost goes to each?* That is arithmetic over plain data — no DataFrame required.

- `src/tech_cost_platform/engine/strategies.py` — **zero pyspark imports.** Each strategy is a pure function with the signature shape:
  `(targets: Sequence[str], signals: Mapping[str, Decimal] | None, params: ...) -> dict[str, Decimal]`
  returning proportions that sum to exactly `1.0` (or signalling "cannot allocate" — see below).
- `src/tech_cost_platform/engine/cascade.py` — the Spark layer: reads silver, resolves rules, applies the pure strategies per step, emits Delta outputs.

Why: the split math is the domain proof and must be unit-tested in milliseconds without a JVM. This is better engineering *and* it keeps the test suite affordable. If `strategies.py` imports pyspark, the packet has failed.

## Start Here (read the current repo before planning)
- `src/tech_cost_platform/rules/` — `RuleRegistry`, `RuleVersion`, step definitions. **Consume this contract; do not redefine it.**
- `config/rules/*.yaml` — the two shipped versions.
- `src/tech_cost_platform/silver/` — the conformed inputs: `fact_gl_cost` (incl. nullable `tower_id`), `fact_usage_metric` (long-form: `step`, `from_id`, `to_id`, `metric_name`, `value`), and the four `dim_*` tables.
- `src/tech_cost_platform/silver/build.py` — **reuse the shared staged-write Delta helper** (the `data/_staging/` write-then-move). Do not write Delta directly; the Windows `_delta_log` failure will bite you.
- `src/tech_cost_platform/spark.py` — **do-not-touch.** Reuse `build_spark_session()` as-is (carries P-000-FIX hardening).
- `tests/conftest.py` — shared harness: session-scoped read-only `synth_data`, function-scoped factories writing to fresh per-test dirs. Add an `engine` factory in the same shape.

## The Cascade — step semantics
**Step 1 · `gl_to_tower` — mapping-first (NOT a split).**
- Each `fact_gl_cost` row allocates 100% to the tower given by its cost center's `tower_id`.
- `tower_id IS NULL` → the rule decides:
  - no `on_unmapped` declared (both shipped versions) → **residual**, reason `unmapped`, `failed_step = gl_to_tower`.
  - `on_unmapped: <strategy>` declared → spread across towers by that strategy.

**Step 2 · `tower_to_app` — strategy split** using the rule's strategy (shipped: `consumption` on `cpu_hours`), signals drawn from `fact_usage_metric` where `step = 'tower_to_app'`.

**Step 3 · `app_to_bu` — strategy split** using the rule's strategy (shipped: `consumption` on `transactions` for `v1`, `named_users` for `v2`), signals from `fact_usage_metric` where `step = 'app_to_bu'`.

## Driver Strategies (implement all four)
All four must be implemented and swappable via rule config, **even though the shipped versions only exercise `weighted`-as-fallback and `consumption`.** The Definition of Done requires ≥3; ship 4. Each gets its own unit tests.
- `even_spread` — equal proportions across targets.
- `weighted` — proportions ∝ declared static weights.
- `consumption` — proportions ∝ the usage metric's values across targets.
- `manual_override` — proportions taken directly from the rule (already validated to sum to 1.0).

**Cannot-allocate semantics (critical):** a strategy must signal — not guess — when no defensible split exists:
- `consumption` where the metric sums to **zero** across all targets → cannot allocate → residual reason `driver_zero`.
- any step where the target set is **empty** (no usage rows at all for that `from_id`) → cannot allocate → residual reason `shared_unattributable`.
- Never fall back to even-spread implicitly. Never divide by zero. Never emit NaN.

Return proportions as `Decimal`, not float, and ensure they sum to exactly 1.0 (allocate any rounding remainder deterministically — e.g. to the largest-share target, or the lexicographically-first on ties). Rounding must be deterministic and documented.

## Residual is a first-class engine output (decided)
The engine emits a `residual` table alongside `allocation`. Cost exits the cascade at the step where it fails, tagged — it is not carried forward as nulls, and it is not dropped.

Residual row: `gl_line_id`, `amount_eur`, `failed_step` (`gl_to_tower`|`tower_to_app`|`app_to_bu`), `reason_code` (`unmapped`|`shared_unattributable`|`driver_zero`), `rule_version`.

**`driver_zero` is rule-version-dependent** — the same cost may be residual under one rule version and allocatable under another. That is the "no perfect driver" story; preserve it, don't normalize it away.

P-007 will quantify/report on this table. P-006 must produce it and assert reconciliation.

## Outputs (Delta, via the staged-write helper)
- `data/gold/allocation` — the cascaded result. Must carry lineage: `gl_line_id`, `tower_id`, `app_id`, `bu_id`, `allocated_amount_eur`, `rule_version`, and the per-step proportion applied (so P-008 can trace and reconstruct).
- `data/gold/residual` — as specified above.

Engine entry point: `run_allocation(silver_dir, rule_version_id, gold_dir) -> AllocationResult`.

## Constraints / Guardrails
- **`strategies.py` imports zero pyspark.** Enforced by test (see below).
- **No Python UDFs on executors.** Cascade uses native DataFrame/SQL ops; the pure strategies are applied on the driver over collected, tiny signal sets (data is intentionally small).
- **No double-counting.** Each GL euro flows to exactly one terminal outcome: an allocated BU row, or a residual row. Asserted.
- **Decimal, not float.** Money and proportions.
- **Reuse the staged-write Delta helper** from silver; do not call `.save()` directly on canonical output paths.
- **do-not-touch:** `spark.py`, `synth/`, `bronze/`, `silver/` logic, `rules/` schema. Read them; don't change them.
- **Out of scope:** residual *reporting* (P-007), lineage *view* (P-008), gold report views (P-009), notebooks (P-010), docs (P-011/P-012).

## Execution Rule (carry-forward, non-negotiable)
- The thread may run `make lint` and `pytest tests/test_strategies.py` — pure Python, no JVM, fast.
- The thread must **NOT** run `make test`, `make pipeline`, `make silver`, or any Spark-touching test. The human runs those in-terminal with `--timeout=300`.

## Acceptance Criteria
- **required tests — pure, fast (`tests/test_strategies.py`, Spark-free):**
  1. `even_spread`: N targets → each exactly `1/N`; proportions sum to 1.0.
  2. `weighted`: proportions ∝ weights; zero-weight target gets 0; all-zero weights → cannot-allocate signal.
  3. `consumption`: proportions ∝ metric values; **metric sums to zero → cannot-allocate with `driver_zero`**; empty target set → cannot-allocate with `shared_unattributable`.
  4. `manual_override`: returns the declared proportions unchanged.
  5. Every strategy returns `Decimal` proportions summing to **exactly** 1.0 (test the rounding-remainder rule explicitly).
  6. **Purity test:** assert `strategies.py` imports no pyspark (e.g. inspect the module source / `sys.modules` after import).
- **required tests — Spark cascade (`tests/test_engine.py`, uses the shared harness + an `engine` factory; human-run):**
  7. Full cascade under `v1_transactions` produces `allocation` + `residual` Delta outputs.
  8. **Reconciliation:** `sum(allocation.allocated_amount_eur) + sum(residual.amount_eur) == 61813.95` exactly (Decimal, no float tolerance hand-waving).
  9. **No double-counting:** every `gl_line_id` in `fact_gl_cost` appears in the terminal outcome exactly once in aggregate — its GL amount is fully accounted for across allocation rows + residual rows, and no `gl_line_id` contributes to both a full allocation and a residual for the same amount.
  10. **Seeded residual cases all appear**, each with the right reason and step: `CC-LEGACY` → `unmapped` @ `gl_to_tower`; `APP-EMAIL` → `shared_unattributable` @ `app_to_bu`; `APP-ANALYTICS` under a `storage_gb` rule → `driver_zero` (construct a test rule version for this; shipped versions don't use `storage_gb`).
  11. **Version-dependence:** running `v1_transactions` vs `v2_named_users` over identical silver yields **different BU-level allocations** (the divergence P-002 seeded at `APP-BILLING`), while both reconcile to `61813.95`.
  12. Rule-version pinning is reproducible: same silver + same `rule_version_id` → identical allocation output.

## Stop When
- Costs cascade across all 3 steps with per-step driver selection from rule config;
- all 4 strategies implemented, pure, unit-tested, swappable;
- residual emitted with correct `failed_step` + `reason_code` for all three seeded cases;
- reconciliation `allocated + residual == 61813.95` asserted and passing;
- no double-counting asserted;
- `v1` vs `v2` demonstrably diverge at BU level;
- `make lint` + `pytest tests/test_strategies.py` green (thread); full suite green (human).
- **Stop — do not build the residual report (P-007), lineage view (P-008), or gold views (P-009).**

## Output Required
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results (`make lint`, `pytest tests/test_strategies.py` — NOT the Spark suite)
4) Commit/PR (hash/link, if created)
5) Risks + next steps — explicitly confirm: `strategies.py` has zero pyspark imports; reconciliation holds at 61813.95; all three residual reason codes are produced; v1/v2 diverge at BU level.
