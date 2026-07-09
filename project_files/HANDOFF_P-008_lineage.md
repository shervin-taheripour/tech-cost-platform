# HANDOFF BUNDLE

## Header
Packet: P-008 — Reconciliation & lineage
CLI Thread: claude-cli:lineage  (thread-agnostic — runs against the repo, not thread history)

## Goal
Source-to-allocation traceability: **every euro at BU/app level traces back to its originating GL line(s)**, with the rule version and the proportion applied at each cascade hop. Prove it with a round-trip reconciliation (GL → gold → GL) and a committed **worked example**. This is depth signal **#4 (lineage / reconciliation)** — primary, and the governance/auditability signal for the whole project.

The engine (P-006) already carries `gl_line_id`, `tower_id`, `app_id`, `bu_id`, the per-step proportion applied, and `rule_version` on `data/gold/allocation`. **Lineage is a read over data that is already threaded correctly — not a reconstruction.** If a trace cannot be resolved from existing columns, that is an engine gap: report it, do not backfill it here.

## Start Here (read the current repo before planning)
Runtime is **delta-rs + DuckDB — no Spark, no JVM.** Full suite runs in ~18s.
- `src/tech_cost_platform/delta_tables.py` — the Delta read/write surface (Arrow in/out, deterministic sort-before-write, target-dir removal before rewrite). **Use this; never call `write_deltalake` directly.**
- `src/tech_cost_platform/engine/cascade.py` — what `allocation` and `residual` carry, and how proportions are recorded per step.
- `src/tech_cost_platform/residual/reconcile.py` — **reuse this.** It already returns `total_gl_eur`, `total_allocated_eur`, `total_residual_eur`, `balanced`, `difference_eur`, and raises `ReconciliationError`. Do not write a second reconciliation.
- `src/tech_cost_platform/silver/conform.py` — the DuckDB-over-Arrow transform idiom. Mirror it.
- `src/tech_cost_platform/rules/` — `RuleRegistry` / `RuleVersion`.
- `tests/conftest.py` — session-scoped read-only `synth_data`; function-scoped factories writing to fresh per-test dirs. Add a `lineage` factory in the same shape.

Then read the P-006, P-007, and P-000-MIGRATE receipts for context, then execute this bundle to its stop-when.

## Execution Rule
The suite is fast and JVM-free. **Run `make lint` and the full `make test` yourself.** If any command takes minutes, stop and report — something reintroduced Spark. Close DuckDB connections promptly (Windows overwrite lesson from P-000-MIGRATE).

## What to Build

### 1. `src/tech_cost_platform/lineage/`
- `trace.py` — the lineage resolvers (see API below).
- `build.py` — materializes the lineage view(s) as Delta.
- `__main__.py` / `main()` so `python -m tech_cost_platform.lineage` runs it.
- Clean export surface in `__init__.py`.

### 2. Output: `data/gold/lineage` (Delta)
The **edge-level** trace: one row per contributing path from a GL line to a terminal consumer, for a given rule version.

Columns (at minimum): `gl_line_id`, `gl_account`, `cost_center_id`, `tower_id`, `app_id`, `bu_id`, `gl_amount_eur`, `prop_gl_to_tower`, `prop_tower_to_app`, `prop_app_to_bu`, `allocated_amount_eur`, `rule_version`.

Invariant to assert: for each `gl_line_id`, `gl_amount_eur × (product of the three proportions)` equals `allocated_amount_eur` for that path, and the sum of `allocated_amount_eur` across a GL line's paths **plus** any residual for that line equals its `gl_amount_eur`. Exactly. `Decimal`, no float.

**Residual lines belong in lineage too.** A GL line that exits at `gl_to_tower` as `unmapped` has a lineage row with null downstream ids and a residual outcome — its trace is "it stopped here, and why." Do **not** exclude residual from the lineage view; a lineage that only shows successful allocations is not an audit trail. Include an `outcome` column (`allocated` | `residual`) and, when residual, the `reason_code` and `failed_step`.

### 3. Trace API (`trace.py`)
Two directions, both taking a `rule_version`:
- `trace_forward(gl_line_id)` → all contributing paths and terminal outcomes for that GL line (including residual exit).
- `trace_backward(bu_id=..., app_id=...)` → the contributing GL lines and amounts behind a given BU (or app) figure, with the proportions applied at each hop.

These are the audit questions a controller actually asks: *"where did this GL line go?"* and *"what makes up this BU's charge?"*

### 4. Round-trip reconciliation
Reusing `reconcile.py`, add a **lineage-level** round trip: rebuild the GL total by summing lineage `allocated_amount_eur` + residual, per rule version, and assert it equals `61813.95` exactly. This is GL → gold → GL. It must **fail** if lineage drops or duplicates a path.

### 5. Worked example (as data, not prose)
Materialize `examples/lineage_worked_example.json` (or `.md` table generated from real output — your call, committed to the repo): pick **one** GL line that fans out across multiple towers/apps/BUs, and **one** residual line (`CC-LEGACY`). Show each hop, the driver used, the proportion applied, and the terminal amounts, with real numbers from an actual run at `v1_transactions`.

**Numbers must come from a real run.** Do not hand-write figures. This artifact feeds P-012's lineage visual and the blog post, so it must be reproducible from the pipeline.

## Constraints / Guardrails
- **Do not re-derive allocation or residual.** Read `data/gold/allocation` and `data/gold/residual`. If a needed column is absent, report it as an engine gap.
- **Do not write a second reconciliation** — import `residual/reconcile.py`.
- **Decimal, exact.** No float, no tolerance.
- **Use `delta_tables.py`** for all Delta I/O.
- **do-not-touch:** `engine/strategies.py`, `engine/cascade.py`, `residual/` (beyond importing `reconcile`), `rules/`, `synth/`, `bronze/`, `silver/`, `delta_tables.py`, `notebooks/`.
- **Out of scope:** the five gold report views (P-009), notebooks (P-010), docs (P-011/P-012). The worked example is a *data artifact*, not documentation prose.
- Wire a `lineage` stage into `pipeline.py` after `residual`, plus a `make lineage` target. `make pipeline` must run green end-to-end **twice consecutively**.
- Ruff clean.

## Acceptance Criteria
- **behaviors:**
  - `make lineage` builds `data/gold/lineage` from existing gold + silver.
  - `make pipeline` runs green end-to-end twice in a row.
- **required tests (`tests/test_lineage.py`, fast, no JVM):**
  1. `lineage` materializes as valid Delta (`_delta_log` present; `DeltaTable(path)` opens).
  2. **Path arithmetic:** for every allocated row, `gl_amount_eur × prop₁ × prop₂ × prop₃ == allocated_amount_eur` exactly (Decimal).
  3. **Per-GL-line completeness:** for every `gl_line_id`, sum of its lineage `allocated_amount_eur` + its residual == its `gl_amount_eur`. Exactly. No line leaks, none is double-counted.
  4. **Round trip:** summing lineage across all lines reproduces `61813.95` exactly, per rule version.
  5. **Round trip can fail:** with a tampered lineage fixture (drop a path, or duplicate one), the round-trip check raises / reports unbalanced.
  6. **Residual is traceable:** `CC-LEGACY`'s GL line appears in lineage with `outcome = residual`, `reason_code = unmapped`, `failed_step = gl_to_tower`, and null downstream ids.
  7. **`trace_backward`** for a BU returns contributing GL lines whose amounts sum to that BU's total in `data/gold/allocation`.
  8. **`trace_forward`** for a fanned-out GL line returns all its terminal outcomes, summing to that line's GL amount.
  9. **Version-aware:** the same BU traced under `v1_transactions` vs `v2_named_users` resolves to different contributing amounts (the seeded `APP-BILLING` divergence), while both round-trip to `61813.95`.
  10. The committed worked example matches a fresh run's output (regenerate and compare — it must not drift).

## Stop When
- `data/gold/lineage` materializes with per-hop proportions and an `outcome` column covering both allocated and residual;
- every gold BU/app figure resolves to contributing GL lines via `trace_backward`, and every GL line resolves forward;
- round-trip GL → gold → GL reconciles to `61813.95` exactly, and **fails** on tampered input;
- the worked example is committed, generated from a real run, and covers one fanned-out line + one residual line;
- `make lint` + full `make test` green (seconds); `make pipeline` green twice.
- **Stop — do not build the gold report views (P-009) or touch notebooks/docs.**

## Output Required
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results **with timings** (`make lint`, `make lineage`, `make pipeline` ×2, full `make test`)
4) Commit/PR (hash/link)
5) Risks + next steps — explicitly confirm: exact round-trip to `61813.95`; the round-trip check fails on tampered input; residual lines appear in lineage with reason codes; `trace_backward` ties to allocation totals; the worked example was generated from a real run; suite runtime.
