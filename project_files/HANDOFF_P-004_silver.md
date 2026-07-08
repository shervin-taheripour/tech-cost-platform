# HANDOFF BUNDLE

## Header
Packet: P-004 — Silver conformance
CLI Thread: codex:silver

## Goal
Build the silver layer on top of bronze Delta: clean and type-normalize, validate with data-quality checks, and join the source tables into conformed **fact** and **dimension** Delta tables that the allocation engine (P-006) will consume. Silver preserves the lineage keys from bronze and the reconciliation chain (GL total must still tie to `61813.95`). It does **not** allocate, apply drivers, compute residual, or author rules — it only produces clean, conformed, engine-ready inputs.

## Repo Targets
- `src/tech_cost_platform/silver/`:
  - `dq.py` — data-quality checks (native Spark aggregations; see constraints).
  - `conform.py` — cleaning, type normalization, joins, dimension conformance, surrogate/lineage-key handling.
  - `build.py` — orchestrates: read bronze Delta → DQ → conform → write silver Delta.
  - `__main__.py` (or `main()`) so `python -m tech_cost_platform.silver` runs the full build.
- `src/tech_cost_platform/pipeline.py` — replace the no-op **silver** stage with the real build (leave gold as no-op).
- `config.yaml` — add silver paths consistent with existing conventions: `silver_dir: data/silver` (bronze_dir already present).
- `tests/test_silver.py` — offline tests.
- `Makefile` — wire the existing `silver` target to the real build.

## Conformed Output (silver Delta tables in `data/silver/`)
Produce a small, explicit conformed model — **facts** and **dimensions** kept separate:

**Dimensions (conformed, deduped, typed):**
- `dim_cost_center` — `cost_center_id` (PK), name, `tower_id` (nullable — preserve the intentional NULL).
- `dim_resource_tower` — `tower_id` (PK), name, type.
- `dim_application` — `app_id` (PK), name, criticality.
- `dim_business_unit` — `bu_id` (PK), name.

**Facts (conformed, typed, lineage-keyed):**
- `fact_gl_cost` — one row per `gl_line_id` (PK, lineage root), with `period`, `gl_account`, `cost_center_id` (FK), `amount_eur` (numeric), and the resolved `tower_id` joined from `dim_cost_center` (nullable — NULL = the unmapped/residual case, carried forward not dropped).
- `fact_usage_metric` — conformed long-form driver signals: `metric_id` (PK), `period`, `step`, `from_id`, `to_id`, `metric_name`, `value` (numeric).

Bridge/relationship rows implied by `usage_metrics` (tower→app, app→bu) stay in `fact_usage_metric`; do not invent new mapping tables — the engine derives splits from these.

## Cleaning / Conformance Rules
- **Type normalization:** ensure `amount_eur` and `usage_metric.value` are proper numerics; ids/strings trimmed of incidental whitespace; period normalized to the canonical `YYYY-MM` form.
- **Deduplication:** dimensions deduped on PK; a duplicate PK with conflicting attributes is a DQ failure (see below), not a silent "last wins."
- **Join completeness:** every `fact_gl_cost.cost_center_id` resolves to `dim_cost_center`; every `fact_usage_metric.from_id`/`to_id` resolves to its dimension. **Exception (must preserve):** `cost_center.tower_id` may be NULL — that unmapped case flows through to `fact_gl_cost.tower_id = NULL`; it is valid, not a join failure.
- **No allocation:** do not compute any cost splits, driver math, or residual here. Silver ends at conformed inputs.

## Data-Quality Checks (`dq.py`)
Checks run as native Spark aggregations on the driver-safe path (no UDFs). Each check yields a pass/fail with a count. Minimum set:
- PK uniqueness on every dimension and on `fact_gl_cost.gl_line_id`.
- Referential integrity for all FKs (with the documented nullable-`tower_id` exception).
- Non-negative `amount_eur` and `value`.
- Reconciliation: `sum(fact_gl_cost.amount_eur)` == `61813.95` (the governed total survives conformance).
- **Behavior on good vs bad data:** on the real seeded data all checks pass; the suite must also prove the checks *fail* on a seeded-bad fixture (e.g. a duplicate dimension PK or a broken FK) — DQ that never fails on bad input isn't DQ.

## Constraints / Guardrails
- **compatibility:** reuse `spark.py`'s session (vendored jars). **Native Spark ops only — no Python UDFs on executors** (carry-forward from P-001/P-003 Windows worker friction). DQ and conformance expressed as DataFrame/SQL ops.
- **offline / CI-safe tests (carry-forward from the P-003 CI failure):** silver tests must **not** depend on gitignored runtime output being pre-present on disk. A fresh CI checkout has no `data/source/` or `data/bronze/`. Tests must build their own inputs — generate synth + run bronze in a fixture/setup, or construct small bronze Delta fixtures directly — under a project-local gitignored path (e.g. `data/test-runs/…`), not OS temp. This is the exact gap that turned CI red on P-003; do not reintroduce it.
- **governed fixtures (do not alter):** consume bronze as produced by P-003; do not change `synth/` or bronze contracts. The GL total `61813.95` is a contract silver must preserve.
- **style:** mirror repo conventions; Ruff clean; Pydantic v2 where a boundary contract is appropriate (DQ is the silver boundary).
- **windows tests:** project-local paths under `data/`, construct rows via Spark SQL not Python-worker serialization, clean up after.
- **do-not-touch / out-of-scope:**
  - No allocation, drivers, residual, or lineage view — those are P-006/P-007/P-008.
  - No rule authoring (P-005). Do not touch `synth/`, `bronze/` (beyond reading its output), `spark.py`, `notebooks/`, or doc bodies.
  - No documentation obligations — silver produces code + tests + a receipt only (docs are consolidated in P-011/P-012).

## Acceptance Criteria
- **behaviors:**
  - `make PYTHON=<py> silver` reads bronze Delta and writes the conformed fact + dimension Delta tables to `data/silver/`.
  - `make PYTHON=<py> pipeline` runs end-to-end with real synth→bronze→silver stages and a no-op gold stage, exits 0.
  - From a clean checkout (no pre-existing `data/`), the test suite builds its own inputs and passes — i.e. it will pass in GitHub Actions on Linux.
- **required tests (`tests/test_silver.py`, offline, CI-safe):**
  1. Conformed fact + dimension tables are produced and readable.
  2. PK uniqueness holds on every dimension and on `fact_gl_cost`.
  3. Join completeness: all FKs resolve except the intentional NULL `tower_id`, which is preserved (≥1 NULL-tower GL row survives).
  4. Reconciliation: `sum(fact_gl_cost.amount_eur)` == `61813.95`.
  5. DQ proves itself: checks pass on good data and **fail** on a seeded-bad fixture (dup PK or broken FK).
  6. Driver signals conform: `fact_usage_metric` retains the multi-driver rows needed downstream (the divergence + driver-zero cases from P-002 survive into silver).

## Stop When
- Conformed fact + dimension Delta tables are produced;
- DQ checks pass on good data and flag seeded bad data;
- lineage keys are preserved from bronze and the `61813.95` reconciliation holds;
- join completeness is asserted (with the nullable-tower exception);
- `make lint`, `make test`, `make silver`, `make pipeline` are green **and** the tests are CI-safe (no dependency on pre-existing gitignored data).
- **Stop — do not start P-005 (rules) or P-006 (engine).**

## Output Required
Return, in this order:
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results (exact commands: `make silver`, `make pipeline`, `make lint`, `make test`, and the clean-input / CI-safe verification)
4) Commit/PR (hash/link, if created)
5) Risks + next steps (confirm reconciliation held at 61813.95 and that tests build their own inputs, for the Dev Ledger receipt)
