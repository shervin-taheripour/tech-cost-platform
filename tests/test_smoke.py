"""Smoke tests for the scaffolded repository."""

from __future__ import annotations

from importlib import import_module

import pyarrow as pa
from deltalake import DeltaTable

from tech_cost_platform.delta_tables import write_delta_table

MODULES = [
    "tech_cost_platform",
    "tech_cost_platform.runtime",
    "tech_cost_platform.delta_tables",
    "tech_cost_platform.pipeline",
    "tech_cost_platform.synth",
    "tech_cost_platform.bronze",
    "tech_cost_platform.silver",
    "tech_cost_platform.rules",
    "tech_cost_platform.engine",
    "tech_cost_platform.residual",
    "tech_cost_platform.lineage",
    "tech_cost_platform.gold",
]


def test_modules_import() -> None:
    """All scaffold modules should import cleanly."""
    for module_name in MODULES:
        import_module(module_name)


def test_delta_round_trip(tmp_path) -> None:
    """A Delta write/read round-trip proves the local delta-rs runtime is wired correctly."""
    table_path = tmp_path / "delta-table"
    table = pa.table({"id": [1], "status": ["ok"]})

    write_delta_table(table, table_path, sort_columns=["id"])

    rows = DeltaTable(str(table_path)).to_pyarrow_table().to_pylist()

    assert rows == [{"id": 1, "status": "ok"}]
