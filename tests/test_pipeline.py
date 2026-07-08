"""Unit tests for pipeline orchestration behavior."""

from __future__ import annotations

from tech_cost_platform import pipeline


class DummySparkSession:
    """Small stand-in that lets the test observe lifecycle handling."""

    def __init__(self) -> None:
        self.stop_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1


def test_pipeline_reuses_one_spark_session_for_bronze_and_silver(monkeypatch, test_workspace) -> None:
    """Bronze and silver should share one Spark session inside a single pipeline run."""
    config = pipeline.RuntimeConfig()
    dummy_spark = DummySparkSession()
    recorded_calls: list[tuple[str, object, object]] = []

    monkeypatch.setattr(pipeline, "repo_root", lambda: test_workspace)
    monkeypatch.setattr(pipeline, "load_config", lambda config_path=None: config)
    monkeypatch.setattr(
        pipeline,
        "build_spark_session",
        lambda **kwargs: recorded_calls.append(("build_spark_session", kwargs["warehouse_dir"], None)) or dummy_spark,
    )
    monkeypatch.setattr(
        pipeline,
        "generate_source_exports",
        lambda config_path=None: recorded_calls.append(("synth", config_path, None)),
    )
    monkeypatch.setattr(
        pipeline,
        "ingest_bronze_sources",
        lambda **kwargs: recorded_calls.append(("bronze", kwargs["spark"], kwargs["warehouse_dir"])),
    )
    monkeypatch.setattr(
        pipeline,
        "build_silver_tables",
        lambda **kwargs: recorded_calls.append(("silver", kwargs["spark"], kwargs["warehouse_dir"])),
    )

    result = pipeline.run_pipeline()

    expected_warehouse_dir = test_workspace / config.paths.data / "warehouse"

    assert result == 0
    assert recorded_calls[0] == ("build_spark_session", expected_warehouse_dir, None)
    assert ("bronze", dummy_spark, expected_warehouse_dir) in recorded_calls
    assert ("silver", dummy_spark, expected_warehouse_dir) in recorded_calls
    assert dummy_spark.stop_calls == 1
