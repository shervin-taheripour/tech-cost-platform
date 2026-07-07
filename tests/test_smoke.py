"""Smoke tests for the scaffolded repository."""

from __future__ import annotations

from importlib import import_module

from tech_cost_platform.spark import build_spark_session

MODULES = [
    "tech_cost_platform",
    "tech_cost_platform.spark",
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
    """A Delta write/read round-trip proves the local Spark bootstrap is wired correctly."""
    spark = build_spark_session(
        app_name="tech-cost-platform-test",
        warehouse_dir=tmp_path / "warehouse",
    )
    table_path = tmp_path / "delta-table"

    try:
        dataframe = spark.sql("SELECT 1 AS id, 'ok' AS status")
        dataframe.write.format("delta").mode("overwrite").save(str(table_path))

        rows = spark.read.format("delta").load(str(table_path)).collect()

        assert len(rows) == 1
        assert rows[0]["id"] == 1
        assert rows[0]["status"] == "ok"
    finally:
        spark.stop()
