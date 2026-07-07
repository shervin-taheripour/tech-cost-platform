# Receipt - P-003

## Header
- Packet: `P-003`
- Title: `Bronze ingestion layer`
- Thread: `codex:bronze`
- Date: `2026-07-07`
- Status: `Implemented and verified locally`

## Scope
Ingest the 6 synthetic source CSVs from `data/source/` into Delta under `data/bronze/` with an ingestion-boundary contract that validates schema, types, and nullability before any write occurs. Bronze had to preserve source meaning and lineage anchors without cleaning, joining, or conforming.

## Outcome
The bronze layer is now implemented with:

- explicit Pydantic v2 ingestion contracts for each source table
- explicit Spark `StructType` schemas for typed CSV reads
- driver-side validation only
- all-or-nothing ingestion behavior
- Delta output for all 6 source tables under `data/bronze/`
- a real bronze stage in the pipeline
- offline bronze tests, including malformed-input rejection and null preservation

The packet stayed within scope:

- no silver logic
- no rules logic
- no cleaning, conforming, deduplication, or joins
- no changes to `spark.py`
- no changes to `synth/`

## Files Changed

### Added
- `src/tech_cost_platform/bronze/contracts.py`
- `src/tech_cost_platform/bronze/schema.py`
- `src/tech_cost_platform/bronze/ingest.py`
- `src/tech_cost_platform/bronze/__main__.py`
- `tests/test_bronze.py`
- `tests/fixtures/gl_costs_malformed.csv`

### Updated
- `src/tech_cost_platform/bronze/__init__.py`
- `src/tech_cost_platform/pipeline.py`
- `config.yaml`
- `Makefile`

## Commands Run

### Bronze ingest
- `make PYTHON=.\\.venv\\Scripts\\python.exe bronze`
  - Result: passed
  - Wrote 6 Delta tables:
    - `data/bronze/gl_costs`
    - `data/bronze/cost_centers`
    - `data/bronze/resource_towers`
    - `data/bronze/applications`
    - `data/bronze/business_units`
    - `data/bronze/usage_metrics`

### Pipeline
- `make PYTHON=.\\.venv\\Scripts\\python.exe pipeline`
  - Result: passed
  - `synth` remained no-op
  - `bronze` ran for real
  - `silver` and `gold` remained no-op

### Lint
- `make PYTHON=.\\.venv\\Scripts\\python.exe lint`
  - Result: passed

### Tests
- `make PYTHON=.\\.venv\\Scripts\\python.exe test`
  - Result: passed
  - Suite result: `11 passed`

### Explicit malformed-rejection check
- Command used:

```powershell
@'
from pathlib import Path
from tech_cost_platform.bronze.ingest import TABLE_SPECS, BronzeValidationError, ingest_bronze_sources
import shutil

root = Path('data/test-runs/manual-malformed-check').resolve()
shutil.rmtree(root, ignore_errors=True)
root.mkdir(parents=True, exist_ok=True)

try:
    ingest_bronze_sources(
        bronze_dir=root / 'bronze',
        warehouse_dir=root / 'warehouse',
        source_overrides={'gl_costs': Path('tests/fixtures/gl_costs_malformed.csv').resolve()},
    )
    raise SystemExit('expected BronzeValidationError')
except BronzeValidationError as exc:
    wrote_any = any((root / 'bronze' / spec.table_name).exists() for spec in TABLE_SPECS)
    print('malformed fixture rejected')
    print(f'delta_written={wrote_any}')
    print(str(exc).splitlines()[0])
'@ | .\.venv\Scripts\python.exe -
```

- Result: passed
- Output confirmed:
  - `malformed fixture rejected`
  - `delta_written=False`
  - `Bronze validation failed for gl_costs.`

## Acceptance Check

### Passed
- All 6 source tables land in Delta under `data/bronze/`
- Bronze tables are readable
- Source row counts reconcile exactly to bronze row counts
- `gl_costs.amount_eur` reconciles exactly to `61813.95`
- `gl_line_id` is present and unique in bronze `gl_costs`
- source columns are preserved
- intentional NULL `tower_id` for `CC-LEGACY` survives bronze
- malformed input is rejected before any Delta write occurs
- `make lint`, `make test`, `make bronze`, and `make pipeline` are green

## Ingestion Behavior Implemented
- CSV header must match the expected source columns exactly
- CSV read uses explicit Spark schema, not inference
- validation happens on the driver via Pydantic after typed read
- validation errors abort the run before any Delta table is written
- bronze preserves source columns as-received and adds `_source_file` as additive provenance

## Deviations / Notes for Dev Ledger
- Validation was intentionally kept off Spark executors to avoid the Windows Python-worker path that caused friction earlier.
- Tests use project-local paths under `data/test-runs/` rather than OS temp to avoid the Windows temp-directory issues already observed in this repo.
- Actual Spark/Delta bronze verification had to be run outside the sandbox for reliable local completion on Windows, even though the code itself remained unchanged.

## Reconciliation Lock
- Bronze `gl_costs.amount_eur` total preserved exactly: `61813.95`

## Risks
- Main Windows-specific friction remains local Spark/Delta execution inside the sandbox; the implementation passed in normal local execution outside that wrapper.
- Because bronze is intentionally as-received, malformed-source handling depends on strict boundary validation. Any future source-shape change in `P-002` should be treated as a governed contract change and reflected here deliberately.

## Next Steps
- P-004 can now build silver on top of stable bronze Delta tables
- Silver should assume bronze is the raw lineage anchor and preserve the GL-total reconciliation chain from `61813.95`
