# Receipt - P-008

## Header
- Packet: `P-008`
- Title: `Reconciliation & lineage`
- Thread: `codex:lineage`
- Date: `2026-07-09`
- Status: `Implemented, verified locally, committed and pushed`
- Commit: `8a06566` — "P-008: source-to-allocation lineage + round-trip reconciliation"
- Pushed: `a86c3ee..8a06566  main -> main`

## Provenance Note
The executing CLI thread exhausted its session before emitting a receipt. This receipt was reconstructed by the Strategist thread from **verified repo evidence** — `git status`, `git diff --stat`, the commit manifest, and a green local test run — not from a thread's self-report. Claims below are grounded in observed output. Items that could not be verified from that evidence are marked explicitly as **unverified**.

## Scope
Source-to-allocation traceability: every euro at BU/app level traces back to its originating GL line(s), with the rule version and the proportion applied at each cascade hop. Round-trip reconciliation (GL → gold → GL) plus a committed worked example. Depth signal **#4 (lineage / reconciliation)** — primary.

Lineage is a read over data the engine already threads correctly (`gl_line_id`, per-hop proportions, `rule_version`); it does not re-derive allocation or residual.

## Files Changed (from commit manifest — 12 files, +1424 / −11)

### Added
- `src/tech_cost_platform/lineage/trace.py`
- `src/tech_cost_platform/lineage/build.py`
- `src/tech_cost_platform/lineage/__main__.py`
- `tests/test_lineage.py`
- `examples/lineage_worked_example.json`
- `project_files/HANDOFF_P-008_lineage.md`
- `project_files/RECEIPT_P-007.md`

### Updated
- `src/tech_cost_platform/lineage/__init__.py`
- `src/tech_cost_platform/pipeline.py` (lineage stage wired after residual)
- `Makefile` (`make lineage` target)
- `tests/conftest.py` (+119 — `lineage` factory in the established fixture shape)
- `tests/test_pipeline.py` (+79)

## Commands Run + Results

- `make PYTHON=.\.venv\Scripts\python.exe test`
  - Result: **passed**
  - Suite result: `68 passed in 30.46s`
  - (Prior suite at P-007 close: `57 passed`. Delta of ~11 tests corresponds to `tests/test_lineage.py` plus pipeline additions.)
- `git commit` → `8a06566`
- `git push` → `a86c3ee..8a06566  main -> main`

**Unverified:** `make lint`, `make lineage` standalone, and consecutive `make pipeline` runs were not observed in this session's output. The full suite (which exercises the pipeline and lineage build via fixtures) is green.

## Design Decision Recorded — Proportions Are Descriptive, Not Reconstructive

Mid-packet, the thread correctly identified that the handoff's stated invariant was **arithmetically impossible** and stopped for a decision. Evidence it surfaced from real gold data:

- `GL-000009`, `gl_amount_eur = 1957.99`
- `prop_gl_to_tower = 1.000000000000`, `prop_tower_to_app = 0.320000000000`, `prop_app_to_bu = 0.100000000000`
- product = `62.65568`; recorded `allocated_amount_eur = 62.66`

**Ruling (Strategist):** the engine is correct; no engine-gap packet. Exact cent-level reconciliation and exact proportion-product equality are **mutually exclusive**. The engine rounds at each hop and distributes remainders deterministically so money ties out exactly to `61813.95`. Stored proportions therefore *describe the split*; `allocated_amount_eur` is *authoritative*.

The handoff's acceptance test #2 was replaced with:
- **2a — Completeness (exact, Decimal):** for every `gl_line_id`, sum of lineage `allocated_amount_eur` + its residual == its `gl_amount_eur`, exactly. No leakage, no double-counting.
- **2b — Proportion consistency (bounded):** `gl_amount × prop₁ × prop₂ × prop₃` differs from `allocated_amount_eur` by at most one cent per rounding hop. Catches a materially wrong proportion; tolerates rounding.

Round-trip tests (#4, #5) remained exact-Decimal assertions.

This staged-rounding-with-remainder-distribution behavior is a **domain strength, not a caveat**: real allocation engines round at each tier and must still tie out. It should be surfaced deliberately in DESIGN (P-011) and in the worked example (P-012), showing the product, the rounded amount, and the remainder handling.

## Design Decisions Carried From the Handoff
- **Residual lines appear in the lineage view.** A GL line exiting at `gl_to_tower` as `unmapped` has a lineage row with null downstream ids, `outcome = residual`, plus `reason_code` and `failed_step`. A lineage covering only successful allocations would not be an audit trail.
- **Single reconciliation implementation.** `residual/reconcile.py` (P-007) is imported and reused; no second reconciliation was written.
- **Worked example generated from a real run** (`examples/lineage_worked_example.json`), not hand-written — it feeds P-012's lineage visual and the blog post.

## Risks / Follow-ups
- **P1 — Verify the unverified.** Run `make lint`, `make lineage`, and `make pipeline` twice consecutively; confirm green. Record CI status for commit `8a06566`.
- **P2 — `residual_detail.app_id` (carried from P-007).** For `unmapped` lines (failing at `gl_to_tower`, before any app is known) `app_id` must be NULL. Confirm when building P-012 visuals; a populated value would mean an app was inferred for cost that never reached the app tier.
- **P2 — Delta history.** `delta_tables.py` removes the target directory before rewrite (P-000-MIGRATE fix), so each write is a table replacement rather than a Delta overwrite transaction; version history is not retained across runs. Fine for this demo — but README/DESIGN must not claim Delta time-travel as a feature.

## Ledger Interpretation
- Depth signals #1–#5 are now all implemented: multi-step drivers (P-006), driver variety (P-006), residual (P-007), lineage/reconciliation (P-008), rule versioning (P-005/P-005.1).
- Remaining: P-009 gold report views, P-010 Databricks Free Edition notebooks, P-011/P-012 documentation track.
