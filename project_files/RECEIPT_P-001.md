# Receipt - P-001

## Header
- Packet: `P-001`
- Title: `Repo scaffold + tooling`
- Thread: `claude-cli:scaffold`
- Date: `2026-07-07`
- Status: `Implemented locally; CI prepared but not yet exercised on GitHub`

## Scope
Stand up `tech-cost-platform` as an empty-but-runnable sibling to `finance-data-platform`, mirroring repo conventions while keeping the new repo local-first, offline-capable, Spark + Delta based, and limited to scaffold-only work.

## Outcome
`tech-cost-platform` now has a working scaffold with:

- `src/tech_cost_platform/` package layout and stage stubs
- local Spark + Delta bootstrap
- no-op pipeline entrypoint
- offline smoke tests
- Ruff, pytest, Make targets, MIT license, docs skeletons, README skeleton, and GitHub Actions CI
- vendored Delta runtime jars for offline use
- vendored Windows Hadoop native files so Delta round-trip works locally on Windows

No changes were made inside `finance-data-platform`.
`project_files/` was preserved.

## Files Changed

### Added
- `.github/workflows/ci.yml`
- `src/tech_cost_platform/__init__.py`
- `src/tech_cost_platform/spark.py`
- `src/tech_cost_platform/pipeline.py`
- `src/tech_cost_platform/synth/__init__.py`
- `src/tech_cost_platform/bronze/__init__.py`
- `src/tech_cost_platform/silver/__init__.py`
- `src/tech_cost_platform/rules/__init__.py`
- `src/tech_cost_platform/engine/__init__.py`
- `src/tech_cost_platform/residual/__init__.py`
- `src/tech_cost_platform/lineage/__init__.py`
- `src/tech_cost_platform/gold/__init__.py`
- `tests/test_smoke.py`
- `config.yaml`
- `docs/architecture.md`
- `docs/DESIGN.md`
- `CHANGELOG.md`
- `README.md`
- `LICENSE`
- `Makefile`
- `pyproject.toml`
- `.gitignore`
- `.env.example`
- `config/rules/.gitkeep`
- `examples/.gitkeep`
- `notebooks/.gitkeep`
- `data/.gitkeep`
- `jars/delta-spark_2.12-3.3.2.jar`
- `jars/delta-storage-3.3.2.jar`
- `tools/hadoop/bin/winutils.exe`
- `tools/hadoop/bin/hadoop.dll`
- `tools/hadoop/README.md`

### Updated During Verification / Refinement
- `src/tech_cost_platform/spark.py`
- `tests/test_smoke.py`
- `README.md`
- `pyproject.toml`
- `.gitignore`

## Commands Run

### Reference / inspection
- `Get-Content -Raw tech-cost-platform\project_files\HANDOFF_P-001_scaffold.md`
- one-time structure-only inspection of `finance-data-platform`

### Install / scaffold verification
- `.\\.venv\\Scripts\\python.exe -m pip install -e ".[dev]"`
  - Result: passed
- `make PYTHON=.\\.venv\\Scripts\\python.exe lint`
  - Result: passed
- `make PYTHON=.\\.venv\\Scripts\\python.exe test`
  - Result: passed
- `make PYTHON=.\\.venv\\Scripts\\python.exe pipeline`
  - Result: passed

### Clean-install check
- `python -m venv data\\venv-smoke`
  - Result: passed
- `data\\venv-smoke\\Scripts\\python.exe -m pip install -e ".[dev]"`
  - Result: passed
- temporary clean venv removed after verification

### Runtime dependency verification
- checked Java availability locally
  - Result: `OpenJDK 17` present
- verified Delta / Spark compatibility against Delta docs
  - Result: `delta-spark 3.3.2` is in the `Spark 3.5.x` compatibility band
- inspected installed `delta-spark` package contents
  - Result: Python wrapper only; runtime jars had to be vendored separately

## Acceptance Check

### Passed
- Clean editable install works in the existing project venv
- Clean editable install also works in a throwaway fresh venv
- Ruff runs clean
- Smoke tests pass
- Spark + Delta local session builds successfully
- Delta round-trip test passes locally on Windows
- No-op pipeline runs end-to-end and exits `0`
- README skeleton, CI workflow, MIT license, Makefile, config, docs skeletons, and stage stubs are present

### Prepared But Not Yet Externally Proven
- GitHub Actions workflow is written and should run `lint` + `test` with Python `3.11` and JDK `17`
- CI cannot be marked green until the repo is initialized/pushed and the workflow runs on GitHub

## Deviations / Notes for Dev Ledger
- The repo is Windows-hosted, so local offline Delta round-trip required vendored Hadoop native binaries:
  - `tools/hadoop/bin/winutils.exe`
  - `tools/hadoop/bin/hadoop.dll`
- Source used for those Windows natives:
  - `cdarlint/winutils`
  - Hadoop native version chosen: `3.3.5`
- Rationale:
  - PySpark ships Hadoop Java jars but not the Windows native DLL needed for local Delta filesystem operations.
  - Linux CI should not need these files, but Windows local execution does.
- Delta runtime jars were vendored for offline use:
  - `delta-spark_2.12-3.3.2.jar`
  - `delta-storage-3.3.2.jar`
- The smoke test was refined to create its one-row DataFrame with Spark SQL instead of Python worker serialization, keeping the test focused on Delta wiring and avoiding unnecessary Windows local worker friction.

## Risks
- The Windows Hadoop native files are third-party binaries, not shipped by PySpark itself.
- The vendored native version is `3.3.5` while PySpark 3.5.8 bundles Hadoop `3.3.4` Java jars; this worked locally, but it is the main compatibility point to watch.
- CI status is still unverified until first push.

## Next Steps
- Initialize git in `tech-cost-platform` if desired
- Add GitHub remote
- Push once to exercise `.github/workflows/ci.yml`
- Record CI run URL/status in a follow-up receipt note or packet log update
