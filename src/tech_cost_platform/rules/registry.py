"""Registry for discovering and resolving versioned rule artifacts."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict

from .loader import RuleValidationError, load_rule_version
from .schema import NonEmptyStr, RuleVersion


def repo_root() -> Path:
    """Return the repository root without importing Spark bootstrap code."""
    return Path(__file__).resolve().parents[3]


def resolve_repo_path(path_value: str | Path) -> Path:
    """Resolve a repo-relative path into an absolute filesystem path."""
    path = Path(path_value)
    return path if path.is_absolute() else repo_root() / path


class RulesConfig(BaseModel):
    """Runtime config for versioned rule discovery."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    rules_dir: str = "config/rules"
    default_version: NonEmptyStr = "v1_transactions"


def load_rules_config(config_path: Path | None = None) -> RulesConfig:
    """Load the rules block from config.yaml."""
    resolved_path = config_path or repo_root() / "config.yaml"
    with resolved_path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    return RulesConfig.model_validate(raw_config.get("rules", {}))


class RuleRegistry:
    """Discover and resolve governed rule versions by id."""

    def __init__(
        self,
        *,
        rules_dir: str | Path | None = None,
        default_version: str | None = None,
        config_path: Path | None = None,
    ) -> None:
        config = load_rules_config(config_path)
        self.rules_dir = resolve_repo_path(rules_dir or config.rules_dir)
        self.default_version = default_version or config.default_version
        self._versions = self._discover_versions()

    def _discover_versions(self) -> dict[str, RuleVersion]:
        if not self.rules_dir.exists():
            raise RuleValidationError(f"Rules directory not found: {self.rules_dir}")

        rule_files = sorted(self.rules_dir.glob("*.yaml")) + sorted(self.rules_dir.glob("*.yml"))
        if not rule_files:
            raise RuleValidationError(f"No rule files were found in {self.rules_dir}")

        versions: dict[str, RuleVersion] = {}
        for rule_file in rule_files:
            version = load_rule_version(rule_file)
            if version.version_id in versions:
                raise RuleValidationError(
                    f"Duplicate rule version_id '{version.version_id}' in {self.rules_dir}"
                )
            versions[version.version_id] = version
        return versions

    @property
    def available_versions(self) -> tuple[str, ...]:
        return tuple(sorted(self._versions))

    def list_versions(self) -> tuple[str, ...]:
        """Return the available rule version ids in sorted order."""
        return self.available_versions

    def resolve(self, version_id: str) -> RuleVersion:
        """Return the requested rule version or raise a clear error."""
        try:
            return self._versions[version_id]
        except KeyError as exc:
            available = ", ".join(self.available_versions)
            raise RuleValidationError(
                f"Unknown rule version '{version_id}'. Available versions: {available}"
            ) from exc

    def resolve_default(self) -> RuleVersion:
        """Return the configured default rule version."""
        return self.resolve(self.default_version)
