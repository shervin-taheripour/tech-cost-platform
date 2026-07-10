# Design

This project is a deliberately small but fully governed demonstration of a TBM-style cost allocation engine. The point is not to show a large taxonomy or a polished UI. The point is to show that the allocation problem itself is understood well enough to implement transparently, reconcile exactly, and vary under version control.

## 1. The Cascade Model

The cascade is:

1. `gl_to_tower`
2. `tower_to_app`
3. `app_to_bu`

Each step can use a different driver. Money enters as GL lines in `fact_gl_cost` and either reaches terminal BU allocations or exits as residual at the step where the model can no longer allocate honestly.

## 2. `gl_to_tower` Is Mapping-First

The first step is not a split strategy. It is a mapping:

- a GL line follows its cost center's `tower_id`
- if the cost center has no mapped tower, the line is residual by default
- a fallback can be opted into explicitly via `on_unmapped`

That matters because an unmapped fallback is not a technical convenience. It is a governance decision, and the decision belongs in versioned config, not hidden in code.

## 3. Driver Strategy Abstraction

The allocation engine's strategy layer is four pure functions expressed through governed rule config:

- `even_spread`
- `weighted`
- `consumption`
- `manual_override`

These live in [src/tech_cost_platform/engine/strategies.py](../src/tech_cost_platform/engine/strategies.py). They do no I/O, use `Decimal`, and are swappable per cascade step. The runtime adapter feeds them materialized records; the strategies themselves are portable.

## 4. Cannot-Allocate Is a Signal, Never a Guess

If a `consumption` strategy has targets but the selected metric sums to zero, the engine returns `driver_zero`.

It does not silently fall back to even-spread.

That is a deliberate modeling choice: a system that invents an allocation where no signal exists produces a neat report at the cost of false precision. This engine records the uncertainty honestly.

## 5. Residual Reason Codes

The engine uses three residual classes:

- `unmapped`
  - the model had no mapping at `gl_to_tower`
- `driver_zero`
  - targets exist, but the selected driver sums to zero
- `shared_unattributable`
  - the step has no downstream targets at all

Under the shipped rule versions, the default live run surfaces:

- `unmapped`
- `driver_zero`

It does not surface `shared_unattributable` in the default `v1_transactions` run. That class is still real and covered by tests, but the seeded default path never reaches it because the relevant apps receive no cost before that step.

## 6. Rule Versioning

A rule version pins the whole cascade, not a single parameter.

The two shipped versions are intentionally surgical:

- `v1_transactions`
- `v2_named_users`

They differ only at `app_to_bu`. That makes the comparison a controlled experiment: same source costs, same silver inputs, same tower-to-app logic, one changed driver at one step.

## 7. Staged Rounding and Remainder Distribution

The engine rounds money at each hop and distributes remainders deterministically so totals reconcile exactly.

That means:

- exact cent-level reconciliation is guaranteed
- the stored per-hop proportions are descriptive of the split shape
- `gl_amount * p1 * p2 * p3` is close to `allocated_amount_eur`, but not identical at the cent level

The lineage layer therefore treats `allocated_amount_eur` as authoritative and bounds proportion drift instead of pretending both exact arithmetic and exact staged reconciliation can hold simultaneously.

That is not a compromise. It is the correct accounting choice.

## 8. Why the Local Runtime Is `delta-rs + DuckDB`

The engine itself was never Spark-dependent.

Earlier in the project, local PySpark on Windows created runtime cost with little domain value:

- JVM startup and session churn
- Hadoop/winutils friction
- Delta `_delta_log` write issues
- long test times
- eventual OOM and instability during repeated local runs

Migrating the local runtime to `delta-rs + DuckDB` removed the JVM entirely while preserving all domain assertions. Databricks remains the lakehouse target, but local development now optimizes for reproducibility and reviewer ergonomics rather than platform theater.

That migration is part of the engineering story, not an implementation footnote.

## 9. Intentional Divergences from the Sibling Platform

This repo intentionally keeps a different scope and shape from the sibling `finance-data-platform`:

- uses `bronze / silver / gold` medallion naming
- uses Delta format
- does not include Docker, Airflow, or cloud publish infrastructure
- stays focused on the allocation engine, rule versioning, residual handling, lineage, and reporting

The result is a smaller repo, but a sharper portfolio artifact.
