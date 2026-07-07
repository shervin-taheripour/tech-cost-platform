"""Pipeline entrypoint with a real bronze stage and stub downstream stages."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from .bronze.ingest import ingest_bronze_sources
from .spark import repo_root

STAGE_SEQUENCE = ("synth", "bronze", "silver", "gold")


class SparkConfig(BaseModel):
    """Runtime Spark settings."""

    app_name: str = "tech-cost-platform"
    master: str = "local[*]"


class PathsConfig(BaseModel):
    """Repository-relative storage and config paths."""

    data: str = "data"
    bronze: str = "data/bronze"
    silver: str = "data/silver"
    gold: str = "data/gold"
    rules: str = "config/rules"
    examples: str = "examples"


class RuntimeConfig(BaseModel):
    """Root config loaded from config.yaml."""

    spark: SparkConfig = Field(default_factory=SparkConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)


def load_config(config_path: Path | None = None) -> RuntimeConfig:
    """Load the repo-root YAML config into a typed runtime model."""
    resolved_path = config_path or repo_root() / "config.yaml"
    with resolved_path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    return RuntimeConfig.model_validate(raw_config)


def resolve_stages(target_stage: str | None) -> list[str]:
    """Return the no-op stages to execute through the requested stage."""
    if target_stage is None:
        return list(STAGE_SEQUENCE)
    if target_stage not in STAGE_SEQUENCE:
        valid = ", ".join(STAGE_SEQUENCE)
        raise ValueError(f"Unknown stage '{target_stage}'. Expected one of: {valid}")
    return list(STAGE_SEQUENCE[: STAGE_SEQUENCE.index(target_stage) + 1])


def ensure_paths(config: RuntimeConfig, root: Path) -> dict[str, Path]:
    """Create the directories needed by the scaffold."""
    paths = {
        "data": root / config.paths.data,
        "bronze": root / config.paths.bronze,
        "silver": root / config.paths.silver,
        "gold": root / config.paths.gold,
        "rules": root / config.paths.rules,
        "examples": root / config.paths.examples,
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def run_pipeline(target_stage: str | None = None, config_path: Path | None = None) -> int:
    """Run the pipeline through the requested stage."""
    root = repo_root()
    config = load_config(config_path)
    ensure_paths(config, root)

    print("[tech-cost-platform] pipeline status=started")
    for stage_name in resolve_stages(target_stage):
        if stage_name == "bronze":
            ingest_bronze_sources(config_path=config_path)
            print("[tech-cost-platform] stage=bronze status=completed")
        else:
            print(f"[tech-cost-platform] stage={stage_name} status=no-op")
    print("[tech-cost-platform] pipeline status=completed")
    return 0


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the scaffold pipeline."""
    parser = argparse.ArgumentParser(description="Run the scaffold tech cost pipeline.")
    parser.add_argument(
        "--stage",
        choices=STAGE_SEQUENCE,
        help="Run the no-op pipeline through the selected stage.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Optional path to a runtime config.yaml file.",
    )
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    return run_pipeline(target_stage=args.stage, config_path=args.config)


if __name__ == "__main__":
    raise SystemExit(main())
