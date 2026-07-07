# Project Brief — tech-cost-platform (TBM-Style Cost Allocation Demo on Databricks)

## Snapshot
- **Project Name:** TBM-style-cost-allocation-on-Databricks (FinOps / IT Cost Intelligence demo)
- **Owner:** `Shervin Taheripour; github.com/shervin-taheripour`
- **Start Date:** scope authored 2026-06-26; build start week of 2026-06-29 (next week)
- **Repo / Workspace:** local git repo `TBM-style-cost-allocation-on-Databricks/`, later pushed to GitHub `github.com/shervin-taheripour/TBM-style-cost-allocation-on-Databricks`
- **Primary Environment:**
  - **Authoring / test runtime (canonical for reviewers):** Python 3.11, PySpark 3.5.x, `delta-spark`, `pytest`, run locally — no Databricks account required.
  - **Showcase / deployment target:** Databricks **Free Edition** (serverless, quota-limited). NB: Community Edition was retired ~2026-01-01; Free Edition is the replacement. Personal-use-only terms.
- **Execution Tools:** Claude Code CLI + Codex CLI (per-packet implementation threads), VS Code, git. Strategist + Dev Ledger threads in the web project.

## Objective
Demonstrate genuine domain depth in IT cost modeling (TBM/ITFM/FinOps) by implementing a multi-tier **cascading allocation engine** in transparent, auditable code on a Databricks medallion lakehouse. The engine — not the plumbing — is the proof. Positioning: this shows the allocation modeling is understood deeply enough to build and govern it in-house; it does **not** claim to replace Apptio's standardized taxonomy or enterprise engine.

## Non-Goals
- Will NOT build the full TBM taxonomy — only a representative slice (GL → 3–4 towers → 5–6 apps → 3 BUs).
- Will NOT do large-data-volume or enterprise-scale performance work. Sophistication is shown by structure and driver logic, not row counts.
- Will NOT implement a trivial single-step cost-center → cost-type mapping (proves syntax, not domain depth; would undercut positioning).
- Will NOT replicate or replace Apptio wholesale.
- Will NOT use confidential or real cost data — synthetic only.

## Deliverables
- [ ] Locally-runnable PySpark + Delta medallion pipeline (bronze → silver → gold)
- [ ] Deterministic synthetic source-data generator (designed to exercise residual, driver-divergence, and lineage)
- [ ] **Multi-tier allocation engine** with step-specific drivers and pluggable driver strategies
- [ ] Versioned allocation-rule config (YAML/JSON) + rule loader/registry
- [ ] Gold reports (5–6): app TCO, BU showback, residual/unallocated report, source-to-allocation lineage view, driver-comparison view
- [ ] Databricks Free Edition notebooks reproducing the pipeline + final reports
- [ ] README with architecture diagram, positioning claim, business context, and honest "does / does not claim" statement
- [ ] DESIGN, CHANGELOG (and RELEASE if a tagged version is cut)

## Success Criteria (Definition of Done)
- [ ] Functional criteria:
  - Costs cascade GL → towers → apps → business units across ≥3 distinct steps, each with its **own** driver.
  - At least 3 driver strategies implemented (e.g. even-spread, weighted, consumption-based, manual override) and swappable per step via config.
  - Driver-comparison view shows the **same** costs producing materially different BU allocations under two different drivers.
  - Residual/unallocated cost is detected, quantified, and surfaced as an explicit metric (never silently dropped or force-allocated).
  - Every gold-level BU/app figure traces back to its originating GL line(s) via a lineage view.
  - Allocation rules are versioned; switching rule-version reproduces a prior allocation result.
- [ ] Quality criteria:
  - Reconciliation: sum of allocated + residual == total GL cost (within a defined tolerance), asserted in tests.
  - Synthetic data is deterministic (seeded) and includes deliberately non-clean cases.
  - `pytest` suite green; core engine logic unit-tested independent of Spark session where practical.
  - `pip install -e .` + a single command runs the full pipeline locally end-to-end.
- [ ] Documentation criteria:
  - [ ] README updated (architecture diagram + positioning claim + honest scope statement present)
  - [ ] DESIGN updated (engine model, driver abstraction, rule-versioning approach)
  - [ ] RELEASE updated (if a version is cut)
  - [ ] CHANGELOG updated
- [ ] Release criteria (if shipping a tagged demo):
  - [ ] Version bump strategy defined (semver, start 0.1.0)
  - [ ] Package built and installed from artifact (wheel) in a clean venv
  - [ ] Smoke test passed (full pipeline runs from installed package)

## Constraints & Guardrails
- **Hard constraints:**
  - No paid Databricks workspace, no cloud/cluster cost — Free Edition or local only.
  - No confidential/real data.
  - Engine + pipeline must run locally without a Databricks account (reviewer reproducibility).
  - Repo conventions mirror the sibling `finance-data-platform`: src layout, `pyproject.toml` + `[dev]` extra, `Makefile`, Ruff, offline `pytest`, GitHub Actions CI (+ badges), MIT license, Pydantic v2 ingestion contracts, `config.yaml` + `config/`.
- **Soft constraints:**
  - Rule definitions externalized to versioned config (nice-to-have but on-thesis).
  - Architecture diagram in README.
- **Do-not-touch:**
  - `finance-data-platform` repo (the V3 centerpiece) — this is a separate companion repo.
  - No real client names or data anywhere in repo/notebooks.

## Assumptions
- Databricks **Free Edition** is the available free tier (Community Edition retired ~2026-01-01). Confirmed: this is a pure demo with no link to a client or business, so Free Edition's personal-use-only terms are satisfied.
- **Runtime decided:** local-first PySpark + `delta-spark` is canonical (reviewers run it without Databricks); Free Edition is the showcase target.
- Repo is a **sibling** to `finance-data-platform` and mirrors its conventions, with deliberate divergences: `bronze/silver/gold` zone names (medallion is the Databricks idiom being showcased) instead of `raw/staged/curated`; Delta Lake instead of Parquet + DuckDB; no Docker/Airflow/cloud publish.
- Core value is demonstrated by the allocation model's structure and driver logic; data stays small and synthetic.
- Repo will be public on GitHub as a portfolio piece; reviewers may run it locally.
- Implementation timing is a day-by-day capacity call in Hamburg; build begins the week of 2026-06-29.

## Risks / Unknowns
- **Free Edition limits:** serverless-only, library/quota constraints, UC volumes instead of DBFS mounts → notebooks need path/runtime adaptation vs. legacy CE. Mitigated by keeping core logic in portable local modules.
- **Personal-use terms** of Free Edition — resolved: pure demo, no client identity in the artifact.
- **Allocation correctness:** cascading + residual + reconciliation must balance; subtle double-counting or leakage risk. Mitigated by reconciliation tests as acceptance gates.
- **Scope creep** toward full taxonomy / volume / "Apptio clone." Mitigated by Non-Goals + per-packet "Stop when".
- **Capacity:** build may not happen soon; brief must stand alone as a documented artifact regardless.

## Architecture Outline (high-level)
- **Key modules/components:**
  - `synth/` — deterministic synthetic data generator (gl_costs, resource_towers, applications, business_units, cost_centers, usage_metrics, allocation_rules)
  - `bronze/` — raw ingest as-received into Delta + ingestion-boundary contracts/validation
  - `silver/` — clean/validate/join, conformed cost records + dimensions, data-quality checks
  - `rules/` — versioned rule schema, loader, rule-version registry (depth signal #5)
  - `engine/` — multi-tier cascade + driver strategy abstraction (depth signals #1, #2)
  - `residual/` — detect/quantify/surface unallocated cost (depth signal #3)
  - `lineage/` — source-to-allocation traceability (depth signal #4)
  - `gold/` — report/view builders (TCO, showback, residual, lineage, driver-comparison)
  - `notebooks/` — Databricks Free Edition reproduction
- **Data flow / control flow:** medallion — bronze (raw Delta) → silver (conformed Delta) → gold (allocation engine output + FinOps reports). Engine consumes silver + versioned rules; emits gold allocation, residual, and lineage tables.
- **Interfaces / API surfaces:**
  - Rule config schema (YAML/JSON): which driver applies at which step, with version metadata.
  - Driver strategy interface (pluggable: even-spread / weighted / consumption-based / manual-override).
  - Engine entry point (silver tables + rule version → gold tables).
  - Gold report views consumed by notebooks.

## Working Agreements
- **Thread roles:**
  - Strategist/Architect: decisions + packets + review
  - Dev Ledger: packet log + execution receipts + checklist state
- **Packet discipline:**
  - Every implementation change must map to a Packet ID (P-###) and exactly one CLI thread.
  - Every Packet must have a receipt (tests + files + outcome) and a "Stop when" condition.
- **Source of truth:**
  - Code + repo docs (README/DESIGN/RELEASE/CHANGELOG) are canonical.
  - Web project stores decisions + packets + receipts + status.

## Links
- Repo: `<github.com/shervin-taheripour/TBM-style-cost-allocation-on-Databricks>`
- Issue tracker: `<TBD>`
- Design doc (repo): `DESIGN.md` _(to be created in P-011)_
- Release checklist (repo): `RELEASE.md` _(if shipping)_
- Devlog/Changelog (repo): `CHANGELOG.md` _(to be created in P-011)_
