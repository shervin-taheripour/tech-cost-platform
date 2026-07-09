# RECEIPT_P-000-MIGRATE_deltars

Date: 2026-07-09

## Scope

Replaced the local JVM/Spark runtime with:

- `deltalake`
- `duckdb`
- `pyarrow`

The migration preserved the governed domain behavior while removing:

- `pyspark`
- `delta-spark`
- `src/tech_cost_platform/spark.py`
- `src/tech_cost_platform/delta_io.py`
- bundled `jars/`
- bundled `tools/hadoop/`
- JDK setup from CI

## What Changed

### Shared runtime

- Added [runtime.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/runtime.py) for repo-root and repo-path resolution.
- Added [delta_tables.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/delta_tables.py) for:
  - Arrow table construction
  - deterministic sort-before-write
  - Delta read/write via `DeltaTable(...)` and `write_deltalake(...)`

### Bronze

- Rewrote [bronze/ingest.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/bronze/ingest.py) to:
  - read CSVs with `csv.DictReader`
  - validate rows with existing Pydantic contracts
  - build Arrow tables directly
  - write real Delta tables with delta-rs
- Replaced Spark schemas in [bronze/schema.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/bronze/schema.py) with Arrow schemas.

### Silver

- Rewrote [silver/conform.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/silver/conform.py) to:
  - read bronze Delta tables as Arrow
  - run conformance transforms in DuckDB
- Rewrote [silver/dq.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/silver/dq.py) to run DQ checks over Arrow/Python rows.
- Rewrote [silver/build.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/silver/build.py) to write silver outputs directly with delta-rs.

### Engine / Gold

- Rewrote [engine/cascade.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/engine/cascade.py) so:
  - silver inputs are read from Delta as Arrow
  - the pure strategy math remains unchanged
  - allocation/residual outputs are materialized as Arrow
  - gold is written as real Delta via delta-rs
- Simplified [engine/__init__.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/engine/__init__.py) to the non-Spark runtime surface.

### Pipeline / CLI / CI

- Updated [pipeline.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/pipeline.py) to remove shared Spark lifecycle and run directly on the new runtime.
- Updated [Makefile](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/Makefile):
  - `make gold` is now gold-only from existing `data/silver/`
  - `make pipeline` remains the end-to-end run
- Updated [pyproject.toml](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/pyproject.toml) to replace Spark dependencies with `deltalake`, `duckdb`, and `pyarrow`.
- Updated [.github/workflows/ci.yml](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/.github/workflows/ci.yml) to remove JDK setup.

### Tests

- Rewrote the Spark-dependent harness in [tests/conftest.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/conftest.py).
- Rewrote runtime-dependent tests to use Delta/Arrow instead of Spark:
  - [tests/test_bronze.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/test_bronze.py)
  - [tests/test_silver.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/test_silver.py)
  - [tests/test_engine.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/test_engine.py)
  - [tests/test_smoke.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/test_smoke.py)
  - [tests/test_silver_build.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/test_silver_build.py)
  - [tests/test_pipeline.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/test_pipeline.py)

## Overwrite Bug

During verification, a Windows rerun defect surfaced in the rewrite path.

Actual exception text observed:

`OSError: Generic LocalFileSystem error: Unable to open file C:\Users\sherv\workingdir\Projects\tech-cost-platform\data\bronze\gl_costs\part-00001-...snappy.parquet#1: Zugriff verweigert (os error 5)`

Also observed during concurrent/invalid verification runs:

`FileNotFoundError: Object at location C:\Users\sherv\workingdir\Projects\tech-cost-platform\data\bronze\business_units\_delta_log\00000000000000000000.json not found: The system cannot find the file specified. (os error 2)`

Resolution:

- The real fix was applied in [delta_tables.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/src/tech_cost_platform/delta_tables.py):
  - remove the existing target directory before writing the new Delta table
  - ensure read paths materialize to Arrow and close DuckDB connections promptly
- The `make gold` change was *not* used to hide this bug.
- A regression test was added in [tests/test_pipeline.py](/C:/Users/sherv/workingdir/Projects/tech-cost-platform/tests/test_pipeline.py) that runs the full pipeline twice over non-empty data and rechecks reconciliation.

## Verification

Commands verified successfully:

- `make PYTHON=.\.venv\Scripts\python.exe lint`
- `python -m pip install -e ".[dev]"`
- `make PYTHON=.\.venv\Scripts\python.exe synth`
- `make PYTHON=.\.venv\Scripts\python.exe bronze`
- `make PYTHON=.\.venv\Scripts\python.exe silver`
- `make PYTHON=.\.venv\Scripts\python.exe gold`
- `make PYTHON=.\.venv\Scripts\python.exe pipeline`
- `make PYTHON=.\.venv\Scripts\python.exe pipeline`
- `make PYTHON=.\.venv\Scripts\python.exe test`

Observed results:

- `make gold` ran gold-only and completed successfully.
- `make pipeline` completed successfully twice in a row.
- full test suite passed:
  - `49 passed in 12.36s`

Additional acceptance checks:

- `git grep -i pyspark -- src tests pyproject.toml .github`
  - no matches
- `git grep -i "delta-spark" -- src tests pyproject.toml .github`
  - no matches

## Final State

P-000-MIGRATE is complete.

- Spark/JVM is gone from local runtime, test harness, package dependencies, and CI.
- Bronze, silver, and gold write real Delta tables via delta-rs.
- Silver conformance and DQ run on Arrow + DuckDB.
- The governed engine behavior remains intact, including:
  - reconciliation at `61813.95`
  - no double counting
  - residual reason codes
  - v1/v2 rule divergence
  - reproducible rule pinning
