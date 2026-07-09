"""Governed schema for versioned allocation rules."""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from ..synth.generate import DEFAULT_SYNTH_CONFIG, build_usage_metrics

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
DateStr = Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}$")]
CascadeStepName = Literal["gl_to_tower", "tower_to_app", "app_to_bu"]
StrategyName = Literal["even_spread", "weighted", "consumption", "manual_override"]
CASCADE_STEP_NAMES: tuple[CascadeStepName, ...] = ("gl_to_tower", "tower_to_app", "app_to_bu")
PROPORTION_TOLERANCE = Decimal("0.000001")


def _collect_valid_usage_metrics_by_step() -> dict[str, tuple[str, ...]]:
    metrics_by_step: dict[str, set[str]] = {}
    for metric in build_usage_metrics(DEFAULT_SYNTH_CONFIG.period):
        metrics_by_step.setdefault(metric.step, set()).add(metric.metric_name)
    return {
        step_name: tuple(sorted(metric_names))
        for step_name, metric_names in sorted(metrics_by_step.items())
    }


VALID_USAGE_METRICS_BY_STEP = _collect_valid_usage_metrics_by_step()


class RulesModel(BaseModel):
    """Shared base model for governed rule artifacts."""

    model_config = ConfigDict(frozen=True, extra="forbid")


def _validate_non_negative_mapping(values: dict[str, Decimal], field_name: str) -> None:
    if not values:
        raise ValueError(f"{field_name} must not be empty.")
    if any(value < 0 for value in values.values()):
        raise ValueError(f"{field_name} values must be non-negative.")


class EvenSpreadRule(RulesModel):
    strategy: Literal["even_spread"]


class WeightedRule(RulesModel):
    strategy: Literal["weighted"]
    weights: dict[NonEmptyStr, Decimal]

    @model_validator(mode="after")
    def validate_weights(self) -> "WeightedRule":
        _validate_non_negative_mapping(self.weights, "weights")
        return self


class ConsumptionRule(RulesModel):
    strategy: Literal["consumption"]
    metric_name: NonEmptyStr


class ManualOverrideRule(RulesModel):
    strategy: Literal["manual_override"]
    proportions: dict[NonEmptyStr, Decimal]

    @model_validator(mode="after")
    def validate_proportions(self) -> "ManualOverrideRule":
        _validate_non_negative_mapping(self.proportions, "proportions")
        total = sum(self.proportions.values(), start=Decimal("0.00"))
        if abs(total - Decimal("1.0")) > PROPORTION_TOLERANCE:
            raise ValueError("manual_override proportions must sum to 1.0.")
        return self


StepRule = Annotated[
    EvenSpreadRule | WeightedRule | ConsumptionRule | ManualOverrideRule,
    Field(discriminator="strategy"),
]


class RuleVersion(RulesModel):
    """A complete, pinned allocation-rule definition for the full cascade."""

    version_id: NonEmptyStr
    description: NonEmptyStr
    created: DateStr
    gl_to_tower: StepRule
    tower_to_app: StepRule
    app_to_bu: StepRule

    @property
    def steps(self) -> dict[CascadeStepName, StepRule]:
        return {
            "gl_to_tower": self.gl_to_tower,
            "tower_to_app": self.tower_to_app,
            "app_to_bu": self.app_to_bu,
        }

    @model_validator(mode="after")
    def validate_consumption_metrics(self) -> "RuleVersion":
        for step_name, step_rule in self.steps.items():
            if not isinstance(step_rule, ConsumptionRule):
                continue

            valid_metric_names = VALID_USAGE_METRICS_BY_STEP.get(step_name, ())
            if step_rule.metric_name not in valid_metric_names:
                if not valid_metric_names:
                    raise ValueError(
                        f"{step_name} does not support consumption metrics; "
                        "synth emits no usage metrics for that step."
                    )
                valid_metric_text = ", ".join(valid_metric_names)
                raise ValueError(
                    f"{step_name} metric_name '{step_rule.metric_name}' is invalid. "
                    f"Expected one of: {valid_metric_text}."
                )

        return self
