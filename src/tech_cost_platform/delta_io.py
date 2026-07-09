"""Shared Delta filesystem helpers for runtime output tables."""

from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame

from .spark import repo_root


def build_runtime_staging_root() -> Path:
    """Return the neutral runtime staging root used for staged Delta writes."""
    return repo_root() / "data" / "_staging" / uuid4().hex[:8]


def write_delta_table_staged(dataframe: DataFrame, target_path: str | Path) -> Path:
    """Write to staging then move into place because Windows Delta cannot create `_delta_log` directly under canonical runtime output paths."""
    resolved_target_path = Path(target_path)
    resolved_target_path.parent.mkdir(parents=True, exist_ok=True)

    staging_root = build_runtime_staging_root()
    staging_path = staging_root / resolved_target_path.name
    staging_root.mkdir(parents=True, exist_ok=True)

    try:
        shutil.rmtree(resolved_target_path, ignore_errors=True)
        shutil.rmtree(staging_path, ignore_errors=True)
        dataframe.write.format("delta").mode("overwrite").save(str(staging_path))
        shutil.move(str(staging_path), str(resolved_target_path))
        return resolved_target_path
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
