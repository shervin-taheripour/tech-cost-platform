"""Unit tests for silver build filesystem behavior."""

from __future__ import annotations

from pathlib import Path

from tech_cost_platform import delta_io
from tech_cost_platform.silver import build


class FakeDataFrameWriter:
    """Minimal DataFrameWriter stand-in for filesystem behavior tests."""

    def __init__(self, dataframe: "FakeDataFrame") -> None:
        self.dataframe = dataframe
        self._format = None
        self._mode = None

    def format(self, value: str) -> "FakeDataFrameWriter":
        self._format = value
        return self

    def mode(self, value: str) -> "FakeDataFrameWriter":
        self._mode = value
        return self

    def save(self, path: str) -> None:
        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)
        (target / "_delta_log").mkdir(parents=True, exist_ok=True)
        (target / "_delta_log" / "00000000000000000000.json").write_text("{}", encoding="utf-8")


class FakeDataFrame:
    """Small object exposing the write interface used by write_silver_tables."""

    def __init__(self) -> None:
        self.write = FakeDataFrameWriter(self)


def test_write_silver_tables_stages_then_moves_outputs(monkeypatch, test_workspace) -> None:
    """Silver writes should land in staging first and then move into the final target."""
    silver_dir = test_workspace / "silver-target"
    staging_root = test_workspace / "_staging-root"
    tables = {table_name: FakeDataFrame() for table_name in build.SILVER_TABLE_NAMES}

    monkeypatch.setattr(delta_io, "build_runtime_staging_root", lambda: staging_root)

    output_paths = build.write_silver_tables(tables, silver_dir)

    assert set(output_paths) == set(build.SILVER_TABLE_NAMES)
    for table_name, output_path in output_paths.items():
        assert output_path == silver_dir / table_name
        assert output_path.exists()
        assert (output_path / "_delta_log" / "00000000000000000000.json").exists()
    assert not staging_root.exists()
