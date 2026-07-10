# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-10

### Added

- Deterministic synthetic source-data generator with seeded edge cases and exact GL total `61,813.95`.
- Bronze ingest with explicit contracts and Delta output.
- Silver conformance and DQ checks with preserved unmapped and zero-signal cases.
- Governed rule schema, loader, and registry with shipped `v1_transactions` and `v2_named_users`.
- Pure strategy layer plus allocation engine cascade with exact reconciliation and residual handling.
- Residual reporting and explicit reconciliation outputs.
- Lineage tracing, round-trip validation, and a committed worked example from a real run.
- Gold report views, including driver comparison between the two shipped rule versions.
- Databricks Free Edition notebooks proving the same strategy core and rule files reproduce the monetary results on Spark Connect.
- Canonical README, DESIGN, architecture diagram, and public-release repo cleanup.

### Changed

- Local runtime migrated from PySpark-on-Windows to `delta-rs + DuckDB` while preserving domain behavior and test assertions.
- `make gold` became a true gold-only stage against existing silver, while `make pipeline` stayed end-to-end.
- Pipeline orchestration expanded to include residual, lineage, and report stages.
- Databricks notebooks were updated to use dynamic Git-folder path resolution and corrected live residual expectations.

### Fixed

- Windows Delta overwrite path instability by replacing direct rewrites with deterministic local table replacement.
- Bronze and gold write paths to survive repeated local runs.
- P-009 documentation error that transposed allocated and residual totals.
- P-010 notebook expectation that incorrectly looked for `shared_unattributable` in the default `v1_transactions` run.
- `tech_cost_platform.engine` import chain so `engine.strategies` no longer eagerly pulls the DuckDB-backed local runtime adapter.
