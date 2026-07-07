# Implementation Packet Plan — tech-cost-platform

Strategist-authored packet outline. Each packet maps to **one** CLI thread and has a **Stop when** condition.
These are outlines, not full handoff bundles — expand any one into `HANDOFF_BUNDLE_TEMPLATE.md` form before handing to a CLI thread.

**Core proof = P-005 → P-008** (rules, engine, residual, lineage). Do not compress these; they are the headline. Everything else is supporting.

Suggested order: P-001 → P-002 → P-003 → P-004 → P-005 → P-006 → P-007 → P-008 → P-009 → P-010 → P-011. P-012 is slow-track.

---

## P-001 — Repo scaffold + tooling (mirror finance-data-platform conventions)
- **CLI thread:** claude-cli:scaffold
- **Goal:** Stand up `tech-cost-platform` as a sibling to `finance-data-platform` — same project conventions — minus orchestration/cloud, plus a local Spark + Delta bootstrap. Empty-but-runnable, ready to push to GitHub.
- **Conventions to mirror (from finance-data-platform):**
  - Python 3.11+; `src/tech_cost_platform/` src-layout with subpackages: `synth/`, `bronze/`, `silver/`, `rules/`, `engine/`, `residual/`, `lineage/`, `gold/`.
  - `pyproject.toml` with a `[dev]` extra (`ruff`, `pytest`); core deps `pyspark`, `delta-spark`, `pydantic` (v2), `pyyaml`. Install via `pip install -e ".[dev]"`.
  - `Makefile` using the `make PYTHON=.venv/bin/python3 <target>` pattern; targets: `synth`, `bronze`, `silver`, `gold`, `pipeline`, `test`, `lint`.
  - Ruff for lint; `pytest` suite that is fully offline / fixture-based.
  - `.github/workflows/ci.yml` running lint + test on every push; CI + Python-version + License badges in README.
  - `config.yaml` as runtime config source of truth, plus `config/rules/` for versioned allocation rules.
  - `docs/` with `architecture.md` (mermaid) and `DESIGN.md`; `CHANGELOG.md`; `LICENSE` (MIT); `.gitignore`; `.env.example`.
  - Pydantic v2 enforced at the ingestion boundary (matches the sibling's explicit-contract style).
  - `data/` zones (gitignored) + `examples/` for committed sample output.
- **Intentional divergences from the sibling (justified — note in README):**
  - Storage zones named `bronze/silver/gold`, not `raw/staged/curated` — medallion is the Databricks idiom this demo showcases.
  - Delta Lake (not plain Parquet + DuckDB) — the lakehouse is the point.
  - No Docker / Airflow / S3 / CloudFront — no orchestration or cloud publish in scope (size discipline).
- **Runtime (decided):** local-first PySpark + `delta-spark` is canonical; Databricks Free Edition is the showcase target (P-010). No pre-commit (not used by the sibling).
- **Depth signals:** none (foundation).
- **Stop when:** `python -m venv .venv && pip install -e ".[dev]"` succeeds in a clean venv; `make lint` and `make test` pass on a trivial smoke test; a no-op pipeline entry point runs end-to-end; CI is green on first push.

## P-002 — Synthetic source-data generator
- **CLI thread:** codex:synth-data
- **Goal:** Deterministic generator for all source tables, intentionally engineered to exercise the depth signals.
- **Repo targets:** `src/.../synth/`, `data/raw/` outputs, `tests/test_synth.py`.
- **Depth signals:** enables #2, #3, #4.
- **Design intent (must encode):** some costs do NOT cleanly allocate; different drivers yield visibly different splits; every figure traceable to a GL line.
- **Stop when:** seeded run produces all 7 source tables reproducibly (byte-stable for a fixed seed); generated data contains documented "non-clean" cases (unmapped cost, shared-but-unattributable cost); tests assert the awkward cases exist.

## P-003 — Bronze ingestion layer
- **CLI thread:** codex:bronze
- **Goal:** Ingest synthetic exports as-received into Delta with an ingestion-boundary contract.
- **Repo targets:** `src/.../bronze/`, `tests/test_bronze.py`.
- **Depth signals:** supports #4 (raw lineage anchor).
- **Stop when:** all source tables land in Delta unmodified-in-meaning; ingestion contract (schema + basic type/null checks) validates and rejects a deliberately malformed fixture; row counts reconcile to source.

## P-004 — Silver conformance
- **CLI thread:** codex:silver
- **Goal:** Clean, validate, join, and conform cost records + dimension tables; apply data-quality checks.
- **Repo targets:** `src/.../silver/`, `tests/test_silver.py`.
- **Depth signals:** supports #4.
- **Stop when:** conformed fact + dimension Delta tables produced; DQ checks pass on good data and flag seeded bad data; surrogate/lineage keys preserved from bronze; join completeness asserted.

## P-005 — Versioned allocation-rule schema + loader
- **CLI thread:** claude-cli:rules
- **Goal:** Define the allocation-rule config (which driver applies at which step), a loader, and a rule-version registry. Rules are governed artifacts, not hardcoded logic.
- **Repo targets:** `config/rules/`, `src/.../rules/`, `tests/test_rules.py`.
- **Depth signals:** **#5 (rule versioning)** — primary.
- **Stop when:** ≥2 named rule versions load from config; schema validation rejects malformed rules; registry resolves a version by id; a version pin is reproducible.

## P-006 — Allocation engine core (the headline)
- **CLI thread:** claude-cli:engine
- **Goal:** Multi-tier cascade GL → towers → apps → business units, each step using its own driver; pluggable driver strategies.
- **Repo targets:** `src/.../engine/`, `tests/test_engine.py`.
- **Depth signals:** **#1 (multi-step, step-specific drivers)** and **#2 (driver variety / "no perfect driver")** — primary.
- **Driver strategies (min 3):** even-spread, weighted, consumption-based, manual-override.
- **Stop when:** costs cascade across ≥3 steps with per-step driver selection from rule config; ≥3 driver strategies implemented and swappable; unit tests verify each strategy's split math; no double-counting across tiers (asserted).

## P-007 — Residual / unallocated handling
- **CLI thread:** claude-cli:residual
- **Goal:** Detect, quantify, and surface cost that does not allocate cleanly, as an explicit first-class metric.
- **Repo targets:** `src/.../residual/`, `tests/test_residual.py`.
- **Depth signals:** **#3 (residual handling)** — primary.
- **Stop when:** residual table produced with reason codes (unmapped / shared-unattributable / driver-zero); reconciliation test passes: allocated + residual == total GL (within tolerance); residual is never silently dropped or force-spread.

## P-008 — Reconciliation & lineage
- **CLI thread:** claude-cli:lineage
- **Goal:** Source-to-allocation traceability — every gold euro at BU/app level traces back to GL line(s).
- **Repo targets:** `src/.../lineage/`, `tests/test_lineage.py`.
- **Depth signals:** **#4 (lineage/reconciliation)** — primary.
- **Stop when:** lineage view resolves any BU/app figure to contributing GL lines with the rule version applied; round-trip reconciliation (GL → gold → GL) balances; a worked example is documented.

## P-009 — Gold reports / views
- **CLI thread:** codex:gold-reports
- **Goal:** Build the 5–6 FinOps outputs from engine + residual + lineage tables.
- **Repo targets:** `src/.../gold/`, `tests/test_gold.py`.
- **Depth signals:** surfaces #1–#4.
- **Views:** application TCO; business-unit showback; residual/unallocated report; source-to-allocation lineage; driver-comparison (same costs, two drivers, materially different BU splits).
- **Stop when:** all views materialize as Delta/queryable outputs; driver-comparison demonstrably shows divergent results; totals tie back to reconciliation.

## P-010 — Databricks Free Edition notebooks
- **CLI thread:** codex:databricks-notebooks
- **Goal:** Reproduce the pipeline + final reports as Databricks Free Edition notebooks (serverless; UC volumes, not DBFS mounts).
- **Repo targets:** `notebooks/`.
- **Depth signals:** demonstrates Databricks fluency (supporting).
- **Stop when:** notebooks run end-to-end on Free Edition serverless; data paths use UC volumes; final-report notebook renders all gold views; any local-vs-Free-Edition deltas documented.

## P-011 — Docs: README, DESIGN, CHANGELOG, architecture diagram
- **CLI thread:** claude-cli:docs
- **Goal:** Canonical repo docs, including the positioning claim and honest scope statement.
- **Repo targets:** `README.md`, `DESIGN.md`, `CHANGELOG.md`, `docs/architecture.*`.
- **Depth signals:** documents all five.
- **Stop when:** README contains architecture diagram, the Apptio positioning claim, business context, and the explicit "does / does not claim" statement; DESIGN explains engine model + driver abstraction + rule-versioning; CHANGELOG initialized.

## P-012 — Blog drafts (slow track, optional)
- **CLI thread:** claude-cli:blog
- **Goal:** Draft the three tie-in posts from repo artifacts.
- **Repo targets:** `docs/blog/` (or external).
- **Depth signals:** content, not code.
- **Posts:** "Why IT Cost Controlling Is Really a Data Engineering Problem"; "The Data Platform Behind IT Cost Transparency"; "Allocation Rules Need Version Control".
- **Stop when:** three outlines drafted with worked examples drawn from the repo. (Defer until P-001–P-011 land.)
