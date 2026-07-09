"""Unit tests for silver build filesystem behavior."""

from __future__ import annotations

import pyarrow as pa
from deltalake import DeltaTable

from tech_cost_platform.silver import build


def _sample_tables() -> dict[str, pa.Table]:
    return {
        "dim_cost_center": pa.table(
            {"cost_center_id": ["CC-1"], "name": ["Cost Center"], "tower_id": ["TWR-1"]}
        ),
        "dim_resource_tower": pa.table(
            {"tower_id": ["TWR-1"], "name": ["Tower"], "type": ["infra"]}
        ),
        "dim_application": pa.table(
            {"app_id": ["APP-1"], "name": ["App"], "criticality": ["high"]}
        ),
        "dim_business_unit": pa.table({"bu_id": ["BU-1"], "name": ["BU"]}),
        "fact_gl_cost": pa.table(
            {
                "gl_line_id": ["GL-1"],
                "period": ["2026-01"],
                "gl_account": ["7000"],
                "cost_center_id": ["CC-1"],
                "amount_eur": [1],
                "tower_id": ["TWR-1"],
            }
        ),
        "fact_usage_metric": pa.table(
            {
                "metric_id": ["M-1"],
                "period": ["2026-01"],
                "step": ["tower_to_app"],
                "from_id": ["TWR-1"],
                "to_id": ["APP-1"],
                "metric_name": ["cpu_hours"],
                "value": [1],
            }
        ),
    }


def test_write_silver_tables_writes_delta_outputs_directly(test_workspace) -> None:
    """Silver writes should land at their final targets as readable Delta tables."""
    silver_dir = test_workspace / "silver-target"
    tables = _sample_tables()

    output_paths = build.write_silver_tables(tables, silver_dir)

    assert set(output_paths) == set(build.SILVER_TABLE_NAMES)
    for table_name, output_path in output_paths.items():
        assert output_path == silver_dir / table_name
        assert output_path.exists()
        assert (output_path / "_delta_log").exists()
        rows = DeltaTable(str(output_path)).to_pyarrow_table().to_pylist()
        assert rows == tables[table_name].to_pylist()
