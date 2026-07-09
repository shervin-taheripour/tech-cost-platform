"""Arrow and Delta Lake helpers for the local runtime."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from .runtime import resolve_repo_path

MONEY_TYPE = pa.decimal128(18, 2)
PROPORTION_TYPE = pa.decimal128(18, 12)


def build_arrow_table(rows: Iterable[Mapping[str, object]], schema: pa.Schema) -> pa.Table:
    """Build a typed Arrow table from Python mappings."""
    materialized_rows = list(rows)
    arrays = [
        pa.array([row.get(field.name) for row in materialized_rows], type=field.type)
        for field in schema
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def sort_arrow_table(table: pa.Table, sort_columns: Sequence[str] | None = None) -> pa.Table:
    """Return a deterministically ordered Arrow table."""
    if not sort_columns or table.num_rows == 0:
        return table
    return table.sort_by([(column_name, "ascending") for column_name in sort_columns])


def read_delta_table(table_path: str | Path) -> pa.Table:
    """Read a Delta table from disk into an Arrow table."""
    resolved_path = resolve_repo_path(table_path)
    return DeltaTable(str(resolved_path)).to_pyarrow_table()


def write_delta_table(
    table: pa.Table,
    table_path: str | Path,
    *,
    sort_columns: Sequence[str] | None = None,
) -> Path:
    """Write an Arrow table to a Delta target with deterministic ordering."""
    resolved_path = resolve_repo_path(table_path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    prepared_table = sort_arrow_table(table.combine_chunks(), sort_columns)
    shutil.rmtree(resolved_path, ignore_errors=True)
    write_deltalake(str(resolved_path), prepared_table, mode="overwrite")
    return resolved_path
