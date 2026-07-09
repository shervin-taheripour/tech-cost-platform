"""Rule YAML loading and validation."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError as PydanticValidationError

from .schema import RuleVersion


class RuleValidationError(ValueError):
    """Raised when a governed rule artifact is malformed or invalid."""


def load_rule_version(path: str | Path) -> RuleVersion:
    """Load a rule YAML file and validate it into a RuleVersion."""
    rule_path = Path(path)

    try:
        with rule_path.open("r", encoding="utf-8") as handle:
            raw_rule = yaml.safe_load(handle) or {}
    except FileNotFoundError as exc:
        raise RuleValidationError(f"Rule file not found: {rule_path}") from exc
    except OSError as exc:
        raise RuleValidationError(f"Could not read rule file {rule_path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise RuleValidationError(f"Could not parse rule file {rule_path}: {exc}") from exc

    try:
        return RuleVersion.model_validate(raw_rule)
    except PydanticValidationError as exc:
        raise RuleValidationError(f"Rule validation failed for {rule_path}: {exc}") from exc
