# Receipt - P-002

## Header
- Packet: `P-002`
- Title: `Synthetic source-data generator`
- Thread: `codex:synth-data`
- Date: `2026-07-07`
- Status: `Implemented and verified locally`

## Scope
Build a deterministic, seeded generator that emits the synthetic source exports the later bronze layer will ingest as-received. The generator had to remain pure Python, offline, and intentionally encode the awkward cases needed for downstream residual handling, driver divergence, and lineage demonstrations.

## Outcome
`tech_cost_platform.synth` now generates 6 CSV source tables into `data/source/` from the `synth:` block in `config.yaml`.

Implemented:

- output-shape schema models for all 6 source tables
- deterministic generation logic with seeded stable values
- module CLI entrypoint so `python -m tech_cost_platform.synth` works
- `Makefile` wiring for `make synth`
- offline tests for:
  - file emission and column shape
  - byte-stable determinism across runs
  - referential integrity
  - required design-intent cases
  - locked aggregate GL total

The packet stayed within scope:

- no Spark
- no Delta
- no bronze ingestion
- no allocation logic
- no rules authoring
- no changes to `spark.py` or `pipeline.py`

## Files Changed

### Added
- `src/tech_cost_platform/synth/schema.py`
- `src/tech_cost_platform/synth/generate.py`
- `src/tech_cost_platform/synth/__main__.py`
- `tests/test_synth.py`

### Updated
- `src/tech_cost_platform/synth/__init__.py`
- `config.yaml`
- `Makefile`

## Commands Run

### Generator run
- `make PYTHON=.\\.venv\\Scripts\\python.exe synth`
  - Result: passed
  - Wrote:
    - `data/source/gl_costs.csv`
    - `data/source/cost_centers.csv`
    - `data/source/resource_towers.csv`
    - `data/source/applications.csv`
    - `data/source/business_units.csv`
    - `data/source/usage_metrics.csv`
  - Reported fixed aggregate: `gl_total_eur=61813.95`

### Lint
- `make PYTHON=.\\.venv\\Scripts\\python.exe lint`
  - Result: passed

### Tests
- `make PYTHON=.\\.venv\\Scripts\\python.exe test`
  - Result: passed
  - Suite result: `7 passed`

### Explicit determinism / hash check
- Command used:

```powershell
make PYTHON=.\.venv\Scripts\python.exe synth; $before = Get-ChildItem data\source\*.csv | Sort-Object Name | ForEach-Object { [pscustomobject]@{ Name = $_.Name; Hash = (Get-FileHash $_.FullName -Algorithm SHA256).Hash } }; make PYTHON=.\.venv\Scripts\python.exe synth; $after = Get-ChildItem data\source\*.csv | Sort-Object Name | ForEach-Object { [pscustomobject]@{ Name = $_.Name; Hash = (Get-FileHash $_.FullName -Algorithm SHA256).Hash } }; if (Compare-Object $before $after) { throw 'Hash mismatch across synth runs.' } else { $after | Format-Table -AutoSize }
```

- Result: passed
- Hashes matched across both runs:
  - `applications.csv` -> `91D07CDBE242CE86E80709331DCEA71897735CC27B00A05D47FA5A2BFCD978AC`
  - `business_units.csv` -> `7934500F72798D417476EF948DD813581B489AB254BD61F74425D18F7EEE5742`
  - `cost_centers.csv` -> `59142C522174AD273DA8388B6C81BEAD0BFB816CD67FA21022ECF8B5A2CD17D8`
  - `gl_costs.csv` -> `455A64FB5FB30546519143FF80BC65A4B026B9681EBC572E4CD6BFBA2F53BDA0`
  - `resource_towers.csv` -> `A8FA2D5573EE85D84F7CA18FFB055CD912CA716E5DDAAC4DD507F6697F58ACEB`
  - `usage_metrics.csv` -> `D0ACEED1B9D72676CF21AA99C385EC368811F83C3E1881DF4E2714C1745F49CA`

## Acceptance Check

### Passed
- `make synth` writes all 6 required CSVs to `data/source/`
- generator is deterministic at the default seed
- output is byte-stable across repeated runs
- required columns are present for every table
- referential integrity holds across source tables
- all 4 design-intent cases are encoded and asserted:
  - unmapped
  - shared unattributable
  - driver zero
  - driver divergence
- total `gl_costs.amount_eur` is locked to the committed expected value
- `make lint` is green
- `make test` is green

## Design-Intent Cases Encoded
- **Unmapped:** at least one GL line lands on `CC-LEGACY`, whose `tower_id` is null.
- **Shared unattributable:** `APP-EMAIL` receives tower-level usage but has no `app_to_bu` usage rows.
- **Driver zero:** `APP-ANALYTICS` has `storage_gb` values of `0` for `app_to_bu`, while `named_users` remains positive.
- **Driver divergence:** `APP-BILLING` flips top BU between `transactions` and `named_users`, with a top-share delta of at least 20 percentage points.

## Deviations / Notes for Dev Ledger
- The generator uses deterministic hashing (`sha256`) rather than runtime RNG state, which makes the fixture stable without depending on Python hash randomization or OS-specific ordering.
- Output writing is explicitly LF-terminated with fixed column order and fixed 2-decimal formatting to satisfy the byte-identical requirement across runs.
- The existing `make test` command was used for verification rather than only `tests/test_synth.py`, so the packet was validated against both the new synth tests and the existing repo smoke tests.

## Committed Lock Value
- `gl_costs.amount_eur` aggregate at default seed: `61813.95`

## Risks
- The determinism promise depends on preserving:
  - field order
  - row sort order
  - LF newlines
  - fixed decimal formatting
  - stable seeded value derivation
- Any intentional fixture change will likely alter the file hashes and the locked GL total, so downstream packets should treat those as governed fixture contracts.
- Windows sandbox temp-directory behavior still affects direct in-sandbox pytest runs in this workspace; full `make test` verification succeeded locally outside that temp restriction path.

## Next Steps
- P-003 can ingest the generated CSVs from `data/source/` as-received
- Bronze should treat these outputs as the canonical seeded source exports for downstream reconciliation and lineage tests
