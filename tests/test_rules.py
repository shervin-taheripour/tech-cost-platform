"""Spark-free tests for governed versioned allocation rules."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tech_cost_platform.rules import RuleRegistry, RuleValidationError, load_rule_version
from tech_cost_platform.rules.schema import CASCADE_STEP_NAMES
from tech_cost_platform.synth.generate import DEFAULT_SYNTH_CONFIG, build_usage_metrics

RULES_FIXTURE_PATH = Path("tests/fixtures/rules_malformed.yaml").resolve()
SHIPPED_VERSION_IDS = ("v1_transactions", "v2_named_users")
KNOWN_STRATEGIES = {"even_spread", "weighted", "consumption", "manual_override"}


def build_valid_rule_payload(
    *,
    version_id: str = "test_rule",
    app_to_bu_metric: str = "transactions",
) -> dict[str, object]:
    """Return a governed-valid rule payload for negative-case mutations."""
    return {
        "version_id": version_id,
        "description": "Valid rule payload for Spark-free tests.",
        "created": "2026-07-09",
        "gl_to_tower": {
            "strategy": "weighted",
            "weights": {
                "TWR-COMPUTE": 4,
                "TWR-LABOR": 3,
                "TWR-NETWORK": 2,
                "TWR-STORAGE": 2,
            },
        },
        "tower_to_app": {
            "strategy": "consumption",
            "metric_name": "cpu_hours",
        },
        "app_to_bu": {
            "strategy": "consumption",
            "metric_name": app_to_bu_metric,
        },
    }


def write_rule_fixture(
    test_workspace: Path,
    case_name: str,
    payload: dict[str, object],
) -> Path:
    """Write a temporary rule fixture under the project-local test workspace."""
    fixture_path = test_workspace / "rules-fixtures" / f"{case_name}.yaml"
    fixture_path.parent.mkdir(parents=True, exist_ok=True)
    fixture_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return fixture_path


def test_shipped_versions_load_with_exactly_three_cascade_steps() -> None:
    """Each shipped rule version should load and define the whole cascade."""
    registry = RuleRegistry()

    for version_id in SHIPPED_VERSION_IDS:
        version = registry.resolve(version_id)
        assert tuple(version.steps) == CASCADE_STEP_NAMES
        assert version.gl_to_tower.strategy in KNOWN_STRATEGIES
        assert version.tower_to_app.strategy in KNOWN_STRATEGIES
        assert version.app_to_bu.strategy in KNOWN_STRATEGIES


def test_shipped_versions_differ_only_at_app_to_bu() -> None:
    """The driver-comparison pair must be surgically isolated to app_to_bu only."""
    registry = RuleRegistry()
    v1 = registry.resolve("v1_transactions")
    v2 = registry.resolve("v2_named_users")

    assert v1.gl_to_tower == v2.gl_to_tower
    assert v1.tower_to_app == v2.tower_to_app
    assert v1.app_to_bu != v2.app_to_bu
    assert v1.app_to_bu.metric_name == "transactions"
    assert v2.app_to_bu.metric_name == "named_users"


def test_registry_lists_versions_and_resolves_unknown_ids_cleanly() -> None:
    """Registry discovery should be deterministic and fail clearly on unknown ids."""
    registry = RuleRegistry()

    assert registry.list_versions() == SHIPPED_VERSION_IDS
    assert registry.resolve("v1_transactions").version_id == "v1_transactions"
    assert registry.resolve("v2_named_users").version_id == "v2_named_users"

    with pytest.raises(RuleValidationError) as exc_info:
        registry.resolve("v9_missing")

    assert "Unknown rule version 'v9_missing'" in str(exc_info.value)


def test_loader_rejects_deliberately_malformed_fixture() -> None:
    """The seeded malformed fixture should fail validation loudly."""
    with pytest.raises(RuleValidationError) as exc_info:
        load_rule_version(RULES_FIXTURE_PATH)

    assert "app_to_bu" in str(exc_info.value)


@pytest.mark.parametrize(
    ("case_name", "mutator", "expected_text"),
    [
        (
            "unknown_strategy",
            lambda payload: payload["tower_to_app"].update({"strategy": "magic_beans"}),
            "magic_beans",
        ),
        (
            "consumption_missing_metric",
            lambda payload: payload["tower_to_app"].pop("metric_name"),
            "metric_name",
        ),
        (
            "invalid_step_metric",
            lambda payload: payload["tower_to_app"].update({"metric_name": "transactions"}),
            "tower_to_app metric_name 'transactions' is invalid",
        ),
        (
            "manual_override_not_one",
            lambda payload: payload.update(
                {
                    "app_to_bu": {
                        "strategy": "manual_override",
                        "proportions": {
                            "BU-CORP": 0.50,
                            "BU-RETAIL": 0.20,
                            "BU-WHOLESALE": 0.20,
                        },
                    }
                }
            ),
            "manual_override proportions must sum to 1.0",
        ),
        (
            "even_spread_metric_name",
            lambda payload: payload.update(
                {"gl_to_tower": {"strategy": "even_spread", "metric_name": "cpu_hours"}}
            ),
            "metric_name",
        ),
        (
            "extra_unknown_field",
            lambda payload: payload["app_to_bu"].update({"unexpected": "typo"}),
            "unexpected",
        ),
    ],
)
def test_schema_rejects_each_required_malformed_case(
    test_workspace: Path,
    case_name: str,
    mutator,
    expected_text: str,
) -> None:
    """Every governed malformed case should fail with a case-specific assertion."""
    payload = build_valid_rule_payload(version_id=case_name)
    mutator(payload)
    fixture_path = write_rule_fixture(test_workspace, case_name, payload)

    with pytest.raises(RuleValidationError) as exc_info:
        load_rule_version(fixture_path)

    assert expected_text in str(exc_info.value)


def test_shipped_rule_driver_references_exist_in_synth_usage_metrics() -> None:
    """Every shipped consumption metric must exist in synth output for its step."""
    registry = RuleRegistry()
    real_metrics_by_step: dict[str, set[str]] = {}
    for metric in build_usage_metrics(DEFAULT_SYNTH_CONFIG.period):
        real_metrics_by_step.setdefault(metric.step, set()).add(metric.metric_name)

    for version_id in SHIPPED_VERSION_IDS:
        version = registry.resolve(version_id)
        for step_name, step_rule in version.steps.items():
            metric_name = getattr(step_rule, "metric_name", None)
            if metric_name is None:
                continue
            assert metric_name in real_metrics_by_step[step_name]


def test_loading_same_version_twice_is_deterministic() -> None:
    """Pinned versions should load reproducibly into equal objects."""
    rules_dir = Path("config/rules").resolve()
    first = load_rule_version(rules_dir / "v1_transactions.yaml")
    second = load_rule_version(rules_dir / "v1_transactions.yaml")

    assert first == second
