# HANDOFF BUNDLE

## Header
Packet: P-003.1 — Self-contained test harness + bronze test retrofit
CLI Thread: codex:test-harness

## Goal
Make the test suite self-contained (Option A): tests build their own inputs instead of depending on gitignored runtime data or test-execution order. Establish a **shared** fixture harness that every layer's tests reuse, retrofit the bronze tests onto it, and remove the "generate synth before tests" step from CI so self-containment is actually proven on a clean checkout. **Product code is not touched** — this is a test + workflow packet.

## Background (why)
P-003's bronze tests read `data/source/` (synth output), which is gitignored and absent on a fresh checkout — and pytest ran `test_bronze` before `test_synth` generated it, turning CI red. The interim fix added a synth-generation step to `ci.yml`. That works but leaves the suite dependent on external setup and hides ordering coupling. We are now standardizing on self-contained tests, which requires both the retrofit and the removal of that CI step.

## Repo Targets
- `tests/conftest.py` — new shared fixtures (see below).
- `tests/test_bronze.py` — retrofit to use the shared fixtures.
- `.github/workflows/ci.yml` — remove the synth/data-prep step so tests run with no pre-staged data.
- `.gitignore` — confirm the fixture workspace path is ignored (add if missing).

## Shared Fixtures (`tests/conftest.py`)
**Fixture-scope discipline is a hard requirement.** Cache expensive, read-only, deterministic upstream work at session scope; give every test that *writes* tables its own isolated, function-scoped output dir. Do **not** create a single shared mutable bronze artifact that all bronze tests read/write — that reintroduces order-dependence and cross-test bleed through the fixtures.

All paths are **project-local gitignored** (e.g. under `data/test-runs/<unique>/`), not OS temp (Windows carry-forward).

- `test_workspace` — **session-scoped.** The gitignored temp root. Cleaned up after the session.
- `synth_data(test_workspace)` — **session-scoped, read-only after generation.** Runs the synth generator at the **default seed** once; returns the source dir. Safe to share because nothing mutates it; deterministic ⇒ the governed GL total `61813.95` holds. Do not regenerate per test.
- `bronze_ingest(synth_data)` — **function-scoped factory**, not a prebuilt table set. It's a callable that runs `ingest_bronze_sources(...)` into a **fresh per-call `bronze_dir`/`warehouse_dir`** (mapped under `test_workspace`, e.g. via `tmp_path`), using the existing `source_overrides` / `bronze_dir` / `warehouse_dir` params (no product change). Each test that needs bronze output calls the factory and gets its own isolated space; reads/writes never collide.

Rationale to preserve: the malformed-input test asserts *nothing was written* (`delta_written=False`), so it **must** ingest into its own isolated bronze dir — sharing a dir with a test that legitimately writes tables makes that assertion meaningless or order-dependent.

Design the harness so later packets add the same shape — read-only upstream cached at session scope, and function-scoped `silver`/`gold` factories that write to fresh per-test dirs — in the same file. This is the reuse point for P-004→P-009.

## Constraints / Guardrails
- **do-not-touch product code:** no changes to `synth/`, `bronze/` (`contracts.py`/`schema.py`/`ingest.py`), `spark.py`, `pipeline.py`, or `config.yaml`. If a test needs a custom source/bronze dir, use the existing `ingest_bronze_sources` params — do not add hooks to product code.
- **CI-safe is the whole point:** after this packet, a bare `pytest` (or `make test`) on a clean checkout with **no** pre-existing `data/` must pass. `ci.yml` must no longer stage data before tests.
- **ordering independence:** running `pytest tests/test_bronze.py` alone (without `test_synth.py`) must pass.
- **windows/offline carry-forwards:** project-local gitignored paths (not OS temp), Spark SQL not Python-worker serialization for any row construction, native Spark only, offline.
- **determinism:** fixtures use the default seed; the `61813.95` reconciliation must still hold in the retrofitted tests.
- **scope:** do not modify other layers' tests, add features, or touch docs. Bronze test retrofit + shared harness + ci.yml only.

## Acceptance Criteria
- **behaviors:**
  - `tests/conftest.py` provides `synth_data` and `bronze_tables` (plus the workspace fixture), writing to a gitignored project-local path.
  - `tests/test_bronze.py` uses the fixtures and no longer assumes any pre-existing `data/`.
  - `ci.yml` has **no** data-prep/synth step before the test job.
- **required checks:**
  1. Clean-checkout proof: with `data/` deleted (or on a fresh clone), `make lint` + `make test` pass locally.
  2. Ordering proof: `pytest tests/test_bronze.py` passes in isolation.
  3. Isolation proof: `synth_data` is session-scoped and read-only; bronze output is function-scoped — each writing test ingests into its own fresh `bronze_dir`. No two tests share a writable bronze dir. The malformed-rejection test uses its **own** isolated bronze dir and its `delta_written=False` assertion is checked against that dir only. Randomizing test order (e.g. `pytest -p no:randomly` off / `pytest-randomly` if available) does not change results.
  4. CI proof: push turns Actions green with the data-prep step removed.
  5. No product diff: `git diff` shows changes only under `tests/`, `.github/workflows/ci.yml`, and `.gitignore`.
  6. Reconciliation intact: bronze tests still assert `sum(gl_costs.amount_eur) == 61813.95` via the fixture-built data.

## Stop When
- Shared `conftest.py` fixtures exist and are used by the bronze tests;
- tests pass from a clean checkout and in isolation (no ordering/data dependency);
- the CI data-prep step is removed and CI is green on push;
- no product code changed.
- **Stop — do not start P-004.**

## Output Required
Return, in this order:
1) What changed (what/why)
2) Files changed (paths) — should be tests-only + `ci.yml` + `.gitignore`
3) Commands/tests run + results (exact commands: clean-checkout `make test`, isolated `pytest tests/test_bronze.py`, and the CI run link/status)
4) Commit/PR (hash/link, if created)
5) Risks + next steps (confirm no product diff and that CI is green with no data-prep step, for the Dev Ledger receipt)
