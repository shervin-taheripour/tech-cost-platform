"""Unit tests for pipeline orchestration behavior."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import yaml

from tech_cost_platform import pipeline
from tech_cost_platform.delta_tables import read_delta_table
from tech_cost_platform.rules.registry import RulesConfig
from tech_cost_platform.synth.generate import DEFAULT_GL_TOTAL_EUR


def test_pipeline_runs_all_stages_with_default_rule_version(monkeypatch, test_workspace) -> None:
    """Pipeline should execute synth through residual with repo-relative runtime paths."""
    config = pipeline.RuntimeConfig()
    rules_config = RulesConfig()
    recorded_calls: list[tuple[str, object, object]] = []

    monkeypatch.setattr(pipeline, "repo_root", lambda: test_workspace)
    monkeypatch.setattr(pipeline, "load_config", lambda config_path=None: config)
    monkeypatch.setattr(pipeline, "load_rules_config", lambda config_path=None: rules_config)
    monkeypatch.setattr(
        pipeline,
        "generate_source_exports",
        lambda config_path=None: recorded_calls.append(("synth", config_path, None)),
    )
    monkeypatch.setattr(
        pipeline,
        "ingest_bronze_sources",
        lambda **kwargs: recorded_calls.append(("bronze", kwargs.get("config_path"), kwargs.get("bronze_dir"))),
    )
    monkeypatch.setattr(
        pipeline,
        "build_silver_tables",
        lambda **kwargs: recorded_calls.append(("silver", kwargs.get("config_path"), kwargs.get("silver_dir"))),
    )
    monkeypatch.setattr(
        pipeline,
        "run_allocation",
        lambda **kwargs: recorded_calls.append(
            ("gold", kwargs["rule_version_id"], kwargs["gold_dir"])
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "build_residual_outputs",
        lambda **kwargs: recorded_calls.append(
            ("residual", kwargs["rule_version_id"], kwargs["gold_dir"])
        ),
    )

    result = pipeline.run_pipeline()

    expected_gold_dir = test_workspace / config.paths.gold

    assert result == 0
    assert recorded_calls[0] == ("synth", None, None)
    assert ("bronze", None, None) in recorded_calls
    assert ("silver", None, None) in recorded_calls
    assert ("gold", rules_config.default_version, expected_gold_dir) in recorded_calls
    assert ("residual", rules_config.default_version, expected_gold_dir) in recorded_calls


def test_pipeline_gold_stage_runs_gold_only(monkeypatch, test_workspace) -> None:
    """Selecting the gold stage should run allocation against existing silver only."""
    config = pipeline.RuntimeConfig()
    rules_config = RulesConfig()
    recorded_calls: list[tuple[str, object, object]] = []

    monkeypatch.setattr(pipeline, "repo_root", lambda: test_workspace)
    monkeypatch.setattr(pipeline, "load_config", lambda config_path=None: config)
    monkeypatch.setattr(pipeline, "load_rules_config", lambda config_path=None: rules_config)
    monkeypatch.setattr(
        pipeline,
        "generate_source_exports",
        lambda config_path=None: recorded_calls.append(("synth", config_path, None)),
    )
    monkeypatch.setattr(
        pipeline,
        "ingest_bronze_sources",
        lambda **kwargs: recorded_calls.append(("bronze", kwargs.get("config_path"), kwargs.get("bronze_dir"))),
    )
    monkeypatch.setattr(
        pipeline,
        "build_silver_tables",
        lambda **kwargs: recorded_calls.append(("silver", kwargs.get("config_path"), kwargs.get("silver_dir"))),
    )
    monkeypatch.setattr(
        pipeline,
        "run_allocation",
        lambda **kwargs: recorded_calls.append(
            ("gold", kwargs["rule_version_id"], kwargs["gold_dir"])
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "build_residual_outputs",
        lambda **kwargs: recorded_calls.append(
            ("residual", kwargs["rule_version_id"], kwargs["gold_dir"])
        ),
    )

    result = pipeline.run_pipeline(target_stage="gold")

    expected_gold_dir = test_workspace / config.paths.gold

    assert result == 0
    assert recorded_calls == [("gold", rules_config.default_version, expected_gold_dir)]


def test_pipeline_residual_stage_runs_residual_only(monkeypatch, test_workspace) -> None:
    """Selecting the residual stage should build residual outputs only."""
    config = pipeline.RuntimeConfig()
    rules_config = RulesConfig()
    recorded_calls: list[tuple[str, object, object]] = []

    monkeypatch.setattr(pipeline, "repo_root", lambda: test_workspace)
    monkeypatch.setattr(pipeline, "load_config", lambda config_path=None: config)
    monkeypatch.setattr(pipeline, "load_rules_config", lambda config_path=None: rules_config)
    monkeypatch.setattr(
        pipeline,
        "generate_source_exports",
        lambda config_path=None: recorded_calls.append(("synth", config_path, None)),
    )
    monkeypatch.setattr(
        pipeline,
        "ingest_bronze_sources",
        lambda **kwargs: recorded_calls.append(("bronze", kwargs.get("config_path"), kwargs.get("bronze_dir"))),
    )
    monkeypatch.setattr(
        pipeline,
        "build_silver_tables",
        lambda **kwargs: recorded_calls.append(("silver", kwargs.get("config_path"), kwargs.get("silver_dir"))),
    )
    monkeypatch.setattr(
        pipeline,
        "run_allocation",
        lambda **kwargs: recorded_calls.append(
            ("gold", kwargs["rule_version_id"], kwargs["gold_dir"])
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "build_residual_outputs",
        lambda **kwargs: recorded_calls.append(
            ("residual", kwargs["rule_version_id"], kwargs["gold_dir"])
        ),
    )

    result = pipeline.run_pipeline(target_stage="residual")

    expected_gold_dir = test_workspace / config.paths.gold

    assert result == 0
    assert recorded_calls == [("residual", rules_config.default_version, expected_gold_dir)]


def _write_runtime_config(path: Path) -> None:
    """Write an isolated runtime config for pipeline rerun regression coverage."""
    data_root = path.parent / "runtime-data"
    source_dir = data_root / "source"
    bronze_dir = data_root / "bronze"
    silver_dir = data_root / "silver"
    gold_dir = data_root / "gold"
    rules_dir = Path("config/rules").resolve()
    examples_dir = data_root / "examples"

    payload = {
        "synth": {
            "seed": 20260107,
            "period": "2026-01",
            "output_dir": source_dir.as_posix(),
        },
        "bronze": {
            "source_dir": source_dir.as_posix(),
            "bronze_dir": bronze_dir.as_posix(),
        },
        "silver": {
            "silver_dir": silver_dir.as_posix(),
        },
        "rules": {
            "rules_dir": rules_dir.as_posix(),
            "default_version": "v1_transactions",
        },
        "paths": {
            "data": data_root.as_posix(),
            "bronze": bronze_dir.as_posix(),
            "silver": silver_dir.as_posix(),
            "gold": gold_dir.as_posix(),
            "rules": rules_dir.as_posix(),
            "examples": examples_dir.as_posix(),
        },
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_pipeline_can_run_twice_consecutively_and_still_reconcile(test_workspace: Path) -> None:
    """The full pipeline should succeed twice over a non-empty data root."""
    config_path = test_workspace / "pipeline-rerun.yaml"
    _write_runtime_config(config_path)

    assert pipeline.run_pipeline(config_path=config_path) == 0
    assert pipeline.run_pipeline(config_path=config_path) == 0

    config = pipeline.load_config(config_path)
    rules_config = pipeline.load_rules_config(config_path)
    gold_dir = Path(config.paths.gold)
    allocation_rows = read_delta_table(gold_dir / "allocation").to_pylist()
    residual_rows = read_delta_table(gold_dir / "residual").to_pylist()

    allocation_total = sum((row["allocated_amount_eur"] for row in allocation_rows), start=Decimal("0.00"))
    residual_total = sum((row["amount_eur"] for row in residual_rows), start=Decimal("0.00"))

    assert allocation_total + residual_total == DEFAULT_GL_TOTAL_EUR
    assert rules_config.default_version == "v1_transactions"
