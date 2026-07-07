# PACKET_PLAN.md — Documentation restructure (drop-in)

Replace everything from **P-011 onward** in `PACKET_PLAN.md` with the three packets below, and update the suggested-order line + add the consolidation note.

---

## Suggested order (updated)
P-001 → P-002 → P-003 → P-004 → P-005 → P-006 → P-007 → P-008 → P-009 → P-010 → P-011 → P-012. P-013 is slow-track.

## Documentation note (add near the top of the plan)
Documentation is **consolidated**, not sprinkled. Implementation packets P-001–P-010 carry **no** documentation obligations beyond producing their own Dev Ledger receipt. All docs are authored in the documentation track: **P-011** (canonical repo text docs) and **P-012** (visuals, diagrams, implementation walkthrough, optional deck). CHANGELOG is reconstructed in P-011 from the receipt log.

---

## P-011 — Canonical repo docs (README, DESIGN, CHANGELOG)
- **CLI thread:** claude-cli:docs
- **Goal:** Author the canonical, ships-in-repo written documentation. README (positioning claim, honest "does / does not claim" scope statement, business context, quickstart, in-README architecture/mermaid diagram); DESIGN (engine model, driver-strategy abstraction, rule-versioning approach, medallion rationale, the intentional divergences from `finance-data-platform`); CHANGELOG (reconstructed from the P-001→P-010 receipt log). These are the source-of-truth docs that gate Definition of Done.
- **Repo targets:** `README.md`, `docs/DESIGN.md`, `CHANGELOG.md`, `docs/architecture.md`.
- **Depth signals:** documents all five (in prose).
- **Source material:** the Dev Ledger receipts (explicit context) + the merged brief/plan. Do not invent history — synthesize from receipts.
- **Stop when:** README contains the architecture diagram, the Apptio positioning claim, business context, and the explicit does/does-not-claim statement; DESIGN explains engine model + driver abstraction + rule-versioning + the sibling divergences; CHANGELOG is reconstructed from receipts with an initial `0.1.0` entry; all internal doc links resolve.

## P-012 — Documentation assets: visuals, diagrams & implementation walkthrough (+ optional deck)
- **CLI thread:** claude-cli:docs-visuals
- **Goal:** Produce the presentable documentation of the important implementation steps as visual and portfolio artifacts: (a) rendered diagram images for GitHub embedding, (b) an implementation-walkthrough doc synthesized from the receipts and gold outputs, and (c) an **optional** PPTX summary deck.
- **Repo targets:** `docs/assets/` (committed PNG/SVG diagram images), `docs/implementation-walkthrough.md`, `assets/` referenced by README, and (optional) `docs/deck/tech-cost-platform.pptx`.
- **Scope — visuals (one per depth signal, min):**
  - medallion flow (source → bronze → silver → gold);
  - allocation cascade GL → towers → apps → BUs with the per-step driver on each arrow (#1);
  - driver-divergence (same costs, two drivers, materially different BU splits — numbers from the actual P-009 driver-comparison view) (#2);
  - residual/unallocated breakdown by reason code (#3);
  - source-to-allocation lineage (a worked GL-line → BU trace) (#4).
- **Scope — walkthrough:** documents the important implementation steps and decisions (the depth-signal build, the residual/reconciliation model, rule versioning) with worked examples drawn from real repo output, not invented figures.
- **Scope — deck (optional):** positioning claim + honest scope, architecture, the five signals, one worked allocation example. Use the pptx skill if built.
- **Depth signals:** visually surfaces all five.
- **Constraints / do-not-touch:** GitHub-embeddable formats only (PNG/SVG images; mermaid where GitHub renders it natively); **relative links only** (no external image hosting); pull all worked numbers from actual gold outputs — do **not** fabricate figures; keep diagram source (mermaid/script) in-repo where practical for reproducibility; do not modify engine/pipeline code.
- **Depends on:** P-009 (gold reports must exist for real figures) and P-011 (framing/positioning stable). Diagram assets can begin once P-009 lands even if the deck slips.
- **Stop when:** each of the five depth signals has ≥1 committed, GitHub-renderable visual with a stable relative link usable from README; the implementation-walkthrough documents the important steps with worked examples from real output; if built, the deck renders and covers positioning + architecture + the five signals + a worked example.

## P-013 — Blog drafts (slow track, optional)
- **CLI thread:** claude-cli:blog
- **Goal:** Draft the three tie-in posts from repo artifacts.
- **Repo targets:** `docs/blog/` (or external).
- **Depth signals:** content, not code.
- **Posts:** "Why IT Cost Controlling Is Really a Data Engineering Problem"; "The Data Platform Behind IT Cost Transparency"; "Allocation Rules Need Version Control".
- **Stop when:** three outlines drafted with worked examples drawn from the repo. (Defer until P-001–P-012 land.)
