# tech-cost-platform

[![CI](https://img.shields.io/badge/CI-GitHub_Actions-2088FF?logo=githubactions&logoColor=white)](.github/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-2EA44F)](LICENSE)

<!-- P-011 -->

Scaffold repository for a local-first tech cost pipeline built on Spark and Delta Lake.

## Quickstart

Windows:

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]"
make PYTHON=.venv\Scripts\python.exe lint
make PYTHON=.venv\Scripts\python.exe test
make PYTHON=.venv\Scripts\python.exe pipeline
```

Unix-like shells:

```bash
python -m venv .venv
.venv/bin/python3 -m pip install -e ".[dev]"
make PYTHON=.venv/bin/python3 lint
make PYTHON=.venv/bin/python3 test
make PYTHON=.venv/bin/python3 pipeline
```

## Structure

```text
src/tech_cost_platform/
  spark.py          local Spark + Delta session bootstrap
  pipeline.py       no-op scaffold pipeline entrypoint
  synth/            stage stubs for later packets
  bronze/
  silver/
  rules/
  engine/
  residual/
  lineage/
  gold/
tests/
  test_smoke.py     import + Delta round-trip smoke coverage
config.yaml         runtime config source of truth
config/rules/       versioned rules directory placeholder
docs/               stub documentation for later packets
project_files/      handoff and planning materials kept intact
```

## Intentional Divergences

- Storage zones are named `bronze`, `silver`, and `gold`.
- This scaffold is local-first and does not include Docker, orchestration, or cloud publishing.
- Delta runtime jars are kept in-repo so local Spark does not need a Databricks account or Maven access at run time.
- Windows local Spark also uses vendored Hadoop native files under `tools/hadoop/` so Delta round-trip tests work offline on Windows.
