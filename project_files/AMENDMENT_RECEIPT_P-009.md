# AMENDMENT — RECEIPT_P-009 (2026-07-09)

**Append this to `project_files/RECEIPT_P-009.md`. Do not edit the original body — the amendment records the correction and its provenance.**

---

## Correction — Allocated / Residual Labels Were Transposed

### What was wrong
The original P-009 receipt (and, subsequently, `HANDOFF_P-010`) recorded the reconciliation as:

- ~~`v1_transactions`: allocated 58,245.81 + residual 3,568.14~~
- ~~`v2_named_users`: allocated 50,663.53 + residual 11,150.42~~

The **sums were correct** (both `= 61,813.95`), but the **allocated and residual labels were swapped**.

### Corrected values
Confirmed by `claude-cli:databricks-notebooks` during P-010, read directly from the committed `data/gold/allocation` and `data/gold/residual` Delta tables:

| Rule version | Allocated (EUR) | Residual (EUR) | Total |
|---|---:|---:|---:|
| `v1_transactions` | **3,568.14** | **58,245.81** | 61,813.95 |
| `v2_named_users` | **11,150.42** | **50,663.53** | 61,813.95 |

Reconciliation to `61,813.95` was never in doubt and remains exact. **No code defect.** This is a documentation error in the receipt and in `HANDOFF_P-010`, introduced by the Strategist thread and corrected here.

### Why this matters more than a typo
The corrected figures invert the headline finding. Under `v1_transactions`, **the majority of cost does not allocate** (58,245.81 of 61,813.95 ≈ 94% residual). Under `v2_named_users`, residual falls to ≈ 82%. Changing one driver at one step nearly **triples** allocatable cost (3,568.14 → 11,150.42).

### Root cause in the data (not a defect — the seeded design working)
- `tower_to_app` uses `cpu_hours` in **both** rule versions. `cpu_hours` exists only for `TWR-COMPUTE`. Therefore `TWR-LABOR`, `TWR-NETWORK`, and `TWR-STORAGE` never reach the application tier at all → `shared_unattributable` at `tower_to_app`, before `app_to_bu` is ever evaluated. This is the single largest residual contributor and it is **identical across both versions**.
- `CC-LEGACY` → `unmapped` at `gl_to_tower` (constant across versions).
- `APP-EMAIL` → `shared_unattributable` at `app_to_bu` (no targets under any driver).
- Under `v1_transactions`: only `APP-BILLING` carries `transactions` signal, so `APP-ANALYTICS`, `APP-CRM`, `APP-ERP`, `APP-HRIS` → `driver_zero`.
- Under `v2_named_users`: all four `TWR-COMPUTE` apps carry `named_users`, so all allocate. Residual shrinks accordingly.

### Interpretation — this is the strongest form of the thesis
The driver choice does not merely redistribute cost between business units. **It determines how much cost is allocatable at all.** A demo that showed only "the bars moved" would be a weaker claim than what this data actually demonstrates: driver selection is a modeling judgment with a first-order effect on allocation coverage, and the uncovered remainder must be reported, not hidden.

This is a **feature of the synthetic design** (P-002 deliberately seeded sparse driver coverage), not an artifact to be tuned away. Do **not** "fix" the synth data to make allocation look more complete.

---

## Downstream Actions Required

- **P-010 (`HANDOFF_P-010`, `03_engine.py`, `notebooks/README.md`)** — already carries the corrected values; the transposition is documented there as a finding. No further action.
- **P-011 (README / DESIGN)** — must quote the **corrected** figures. The residual story is a headline, not a footnote. State plainly that under the shipped default rule version most cost is residual, and explain why (sparse driver coverage at `tower_to_app`).
- **P-012 (visuals / walkthrough / deck)** — the residual-by-reason-code visual and the driver-comparison visual must both use the corrected values. The most compelling single chart is **allocated vs residual under v1 vs v2**, showing coverage nearly tripling on one driver change.
- **P-013 (blog)** — "Allocation Rules Need Version Control" now has a concrete, quantified worked example: one config change, 3,568.14 → 11,150.42 allocatable.

## Process Note
The error was caught by a downstream CLI thread reading the committed Delta tables rather than trusting the upstream receipt. That is the ledger working as designed: **repo state is canonical, receipts are secondary.** Any figure quoted in P-011/P-012 must be regenerated from a real run, not copied from a receipt.
