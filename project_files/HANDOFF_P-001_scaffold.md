# HANDOFF BUNDLE

## Header
Packet: P-001 — Repo scaffold + tooling (mirror finance-data-platform conventions)
CLI Thread: claude-cli:scaffold

## Goal
Stand up `tech-cost-platform` as a sibling to `finance-data-platform` — same project conventions, minus orchestration/cloud, plus a working local Spark + Delta bootstrap. The result must be empty-but-runnable and ready to push to GitHub: a clean-venv install works, lint/test/pipeline targets run green, and CI passes on first push. No domain logic in this packet — scaffolding only.

## Repo Targets
Create the following tree (src layout mirrors the sibling; module dirs are stubs to be filled by later packets):

```
tech-cost-platform/
├── .github/workflows/ci.yml
├── src/tech_cost_platform/
│   ├── __init__.py
│   ├── spark.py                 # local SparkSession + Delta bootstrap helper
│   ├── pipeline.py              # no-op end-to-end entrypoint (stub stages)
│   ├── synth/__init__.py        # P-002
│   ├── bronze/__init__.py       # P-003
│   ├── silver/__init__.py       # P-004
│   ├── rules/__init__.py        # P-005
│   ├── engine/__init__.py       # P-006
│   ├── residual/__init__.py     # P-007
│   ├── lineage/__init__.py      # P-008
│   └── gold/__init__.py         # P-009
├── tests/
│   └── test_smoke.py
├── config.yaml                  # runtime config source of truth (root, per sibling)
├── config/
│   └── rules/.gitkeep           # versioned allocation rules land here in P-005
├── data/                        # gitignored zones (bronze/ silver/ gold/ created at runtime)
├── examples/.gitkeep            # committed sample output (populated later)
├── notebooks/.gitkeep           # Databricks Free Edition notebooks (P-010)
├── docs/
│   ├── architecture.md          # stub: header + "TBD — see P-011"
│   └── DESIGN.md                # stub: header + "TBD — see P-011"
├── CHANGELOG.md                 # Keep a Changelog format; 0.1.0 "Unreleased" scaffold entry
├── README.md                    # SKELETON only (badges + quickstart + structure + divergences)
├── LICENSE                      # MIT
├── Makefile
├── pyproject.toml
├── .gitignore
└── .env.example
```

## Constraints / Guardrails
- **compatibility:**
  - Python 3.11+.
  - Pin `pyspark` to 3.5.x and `delta-spark` to the version compatible with it (per Delta's Spark-compatibility matrix). Do not guess — verify the pair by actually starting a Delta-enabled session (see acceptance).
  - `spark.py` must build a working local SparkSession with Delta enabled, with **no Databricks account and no network** required. Reviewers run this locally.
  - Tests are fully offline / fixture-based (no live calls), matching the sibling.
- **style:**
  - Mirror `finance-data-platform` conventions: `src/` layout, `pyproject.toml` with a `[dev]` extra (`ruff`, `pytest`), `Makefile` using the `make PYTHON=.venv/bin/python3 <target>` pattern, Ruff for lint, offline `pytest`, GitHub Actions CI + README badges (CI / Python 3.11+ / License), MIT license.
  - Core deps: `pyspark`, `delta-spark`, `pydantic` (v2), `pyyaml`. (Pydantic is wired as a dependency now; the actual ingestion contracts come in P-003.)
  - Makefile targets: `synth`, `bronze`, `silver`, `gold`, `pipeline`, `test`, `lint`. For this packet the data-stage targets may call the no-op `pipeline` stubs; `test` → `pytest`, `lint` → `ruff check`.
- **performance/security:** no secrets committed; `.env.example` only (placeholder keys, no values). No performance work.
- **do-not-touch / out-of-scope:**
  - Do **not** read from, modify, or copy code out of the `finance-data-platform` repo — mirror conventions only, from the brief/plan.
  - No Docker / Airflow / S3 / CloudFront (no orchestration or cloud publish in scope).
  - No pre-commit (the sibling doesn't use it).
  - No allocation/domain logic, no synthetic data, no rules content, no notebooks. Those are P-002…P-010.
  - Do **not** write full README/DESIGN/architecture bodies — skeletons/stubs only; canonical docs are P-011. (README still needs badges + quickstart so CI badge resolves; leave the Apptio positioning claim as a marked `<!-- P-011 -->` placeholder.)
  - Storage zones are named `bronze/silver/gold` (not `raw/staged/curated`) — this is an intentional divergence from the sibling; note it in the README skeleton.

## Acceptance Criteria
- **behaviors:**
  - Clean-venv install succeeds: `python -m venv .venv && .venv/bin/python -m pip install -e ".[dev]"`.
  - `make PYTHON=.venv/bin/python3 lint` runs Ruff with zero errors.
  - `make PYTHON=.venv/bin/python3 test` runs pytest green.
  - `make PYTHON=.venv/bin/python3 pipeline` runs the no-op entrypoint end-to-end: builds the Spark+Delta session, iterates the stub stages (bronze→silver→gold as no-ops), exits 0.
  - CI is green on first push: workflow provisions Python 3.11 **and a JDK** (PySpark needs a JVM), installs `.[dev]`, runs lint + test.
  - README renders with CI + Python + License badges.
- **required tests (`tests/test_smoke.py`):**
  1. Import test: `tech_cost_platform` and each subpackage import without error.
  2. Spark+Delta round-trip: build the session via `spark.py`, write a 1-row DataFrame to a Delta table under `tmp_path`, read it back, assert the row survives (proves `delta-spark` is correctly wired, not just installed).

## Stop When
- `python -m venv .venv && pip install -e ".[dev]"` succeeds in a clean venv;
- `make lint` and `make test` pass on the smoke test;
- the no-op `make pipeline` runs end-to-end and exits 0;
- CI is green on the first push.
Nothing beyond scaffold is implemented. Stop here — do not begin P-002.

## Output Required
Return, in this order:
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results (exact commands, incl. the clean-venv install, lint, test, pipeline run, and CI link/status)
4) Commit/PR (hash/link, if created)
5) Risks + next steps (flag any pyspark/delta version-pin friction, JDK-in-CI setup, or deviations for the Dev Ledger receipt)
