# HANDOFF BUNDLE

## Header
Packet: P-002 — Synthetic source-data generator
CLI Thread: codex:synth-data

## Goal
Build a deterministic (seeded) generator that emits the synthetic **source exports** the pipeline ingests — the simulated GL/vendor/usage extracts that bronze (P-003) will later read as-received. The data must be intentionally engineered to exercise three depth signals downstream: residual/unallocated cost (#3), driver divergence / "no perfect driver" (#2), and end-to-end lineage GL→tower→app→BU (#4). Small data, deep structure. No Spark, no Delta, no allocation logic in this packet.

## Repo Targets
- `src/tech_cost_platform/synth/` — the generator:
  - `schema.py` — column definitions for each source table (plain dataclasses or Pydantic v2 models; used only to shape/validate the generator's own output, NOT the ingestion contract — that's P-003).
  - `generate.py` — the deterministic build logic.
  - `__main__.py` (or a `main()` entry) so `python -m tech_cost_platform.synth` runs it.
- `config.yaml` — add a `synth:` block (see below). This is the seed/config source of truth.
- `data/source/` — output dir for the generated CSVs (gitignored; regenerable from seed).
- `tests/test_synth.py` — determinism + design-intent assertions.
- `Makefile` — wire the existing `synth` stub target to `python -m tech_cost_platform.synth`.

## Source Tables (6 — CSV, written to `data/source/`)
Grain and keys below are the spec. IDs are stable strings (lineage anchors). One period by default (`2026-01`), period-configurable.

**1. `gl_costs.csv`** — the cost facts (~40–60 lines)
| column | type | notes |
|---|---|---|
| gl_line_id | str (PK) | stable, e.g. `GL-000001` — the lineage root |
| period | str | e.g. `2026-01` |
| gl_account | str | e.g. `6000` Salaries, `7000` Cloud, `7100` Software |
| cost_center_id | str (FK → cost_centers) | |
| amount_eur | decimal(2) | positive |
| description | str | |

**2. `cost_centers.csv`** (~6)
| column | type | notes |
|---|---|---|
| cost_center_id | str (PK) | |
| cost_center_name | str | |
| tower_id | str (FK → resource_towers, **nullable**) | step-1 GL→tower mapping basis; ≥1 row NULL to force an *unmapped* residual |

**3. `resource_towers.csv`** (3–4) — `TWR-COMPUTE`, `TWR-STORAGE`, `TWR-NETWORK`, `TWR-LABOR`
| column | type |
|---|---|
| tower_id | str (PK) |
| tower_name | str |
| tower_type | str |

**4. `applications.csv`** (5–6) — e.g. `APP-CRM`, `APP-ERP`, `APP-BILLING`, `APP-ANALYTICS`, `APP-EMAIL`, `APP-HRIS`
| column | type |
|---|---|
| app_id | str (PK) |
| app_name | str |
| business_criticality | str (low/med/high) |

**5. `business_units.csv`** (3) — `BU-RETAIL`, `BU-WHOLESALE`, `BU-CORP`
| column | type |
|---|---|
| bu_id | str (PK) |
| bu_name | str |

**6. `usage_metrics.csv`** — the driver signals, tidy/long so the engine can swap drivers per step
| column | type | notes |
|---|---|---|
| metric_id | str (PK) | |
| period | str | |
| step | str | `tower_to_app` or `app_to_bu` — which cascade step this signal serves |
| from_id | str | tower_id (for `tower_to_app`) or app_id (for `app_to_bu`) |
| to_id | str | app_id or bu_id |
| metric_name | str | e.g. `cpu_hours`, `storage_gb`, `named_users`, `ticket_count`, `transactions`, `headcount`, `revenue_share` |
| value | decimal | ≥ 0 |

Multiple `metric_name`s per `from_id` are required — that is what lets P-006 swap drivers and P-009 show divergence.

## Design Intent — MUST encode (tests assert these)
Encode all four. Suggested concrete fixtures given; the thread may choose different IDs **as long as the asserted property holds**.
- **Unmapped (residual reason `unmapped`):** ≥1 `gl_costs` line on a `cost_center` whose `tower_id` is NULL → cannot enter the cascade. (e.g. `CC-LEGACY`.)
- **Shared-unattributable (reason `shared_unattributable`):** ≥1 app (or tower) that carries cost but has **no** `usage_metrics` rows under any driver for its step → shared but unattributable. (e.g. `APP-EMAIL` at `app_to_bu`.)
- **Driver-zero (reason `driver_zero`):** ≥1 `from_id` with cost where a specific consumption driver sums to 0 for the period → unallocatable *under that driver* (but allocatable under even-spread — this is the point of #2). (e.g. `storage_gb` = 0 for `APP-ANALYTICS`.)
- **Driver divergence (for #2):** ≥1 app whose `app_to_bu` split flips materially between two drivers — the top BU under driver A differs from the top BU under driver B, and the top-BU share differs by ≥ 20 percentage points. (e.g. `APP-BILLING`: `transactions` favors `BU-RETAIL` ~70/20/10, `named_users` favors `BU-CORP` ~20/20/60.)

## Constraints / Guardrails
- **compatibility:**
  - **Pure Python (stdlib `csv` + optional `numpy`). No SparkSession, no Delta, no pandas requirement.** This deliberately keeps P-002 off the Windows Spark path that cost P-001 time.
  - **Determinism is a hard requirement.** Seed from `config.yaml`. Same seed ⇒ byte-identical CSVs. That means: seeded RNG only (no wall clock, no UUIDs, no set-ordering), rows sorted by PK, fixed column order, fixed decimal formatting (amounts to 2 dp), and **explicit `\n` (LF) newlines written with `newline=''`** so output is byte-stable across Windows/Linux (carry-over lesson from P-001).
- **style:** mirror repo conventions; Ruff clean; offline. Schema models may be Pydantic v2 (on-thesis) but keep them describing the generator's *output shape* only.
- **performance/security:** none material; tiny data; no secrets.
- **do-not-touch / out-of-scope:**
  - Do **not** modify `spark.py`, `pipeline.py`, or other P-001 scaffold beyond adding the `synth` module, the `config.yaml` `synth:` block, and the Makefile `synth` wiring.
  - Do **not** ingest into Delta / write to `data/bronze/` — that is P-003.
  - Do **not** author `allocation_rules` — see decision note; rules are governed config owned by P-005, not synthesized here.
  - Do **not** implement any allocation/driver math — the engine is P-006. P-002 only provides the *signals* the engine will consume.

## Config addition (`config.yaml`)
```yaml
synth:
  seed: 20260107
  period: "2026-01"
  output_dir: "data/source"
```

## Acceptance Criteria
- **behaviors:**
  - `make PYTHON=<py> synth` writes all 6 CSVs to `data/source/`.
  - Determinism: two consecutive runs with the same seed produce byte-identical files (assert via content hash).
  - Total `gl_costs.amount_eur` for the default seed equals a fixed, committed expected value (locks the aggregate for later reconciliation targets).
- **required tests (`tests/test_synth.py`, offline, no Spark):**
  1. Generator runs and emits all 6 files with the specified columns.
  2. Determinism: hash of each file is stable across two runs at the default seed.
  3. Referential integrity: every FK resolves (except the intentional NULL `tower_id`), and every `usage_metrics.from_id`/`to_id` exists in its dimension.
  4. Design-intent assertions — each of the four cases above is present: ≥1 `unmapped`, ≥1 `shared_unattributable`, ≥1 `driver_zero`, and the divergence case meets the flip + ≥20pp threshold (computed directly from `usage_metrics` proportions, no engine needed).
  5. Aggregate lock: total GL amount == committed expected value.

## Stop When
- Seeded `make synth` produces all 6 source tables reproducibly (byte-stable at the default seed);
- the four documented non-clean/divergence cases exist and are asserted by tests;
- `make lint` and `make test` are green;
- nothing beyond generation is built. **Stop — do not start P-003 (bronze ingestion) or P-005 (rules).**

## Output Required
Return, in this order:
1) What changed (what/why)
2) Files changed (paths)
3) Commands/tests run + results (exact commands: `make synth`, `make lint`, `make test`, and the determinism/hash check)
4) Commit/PR (hash/link, if created)
5) Risks + next steps (flag any determinism/line-ending friction and the committed expected-GL-total value, for the Dev Ledger receipt)
