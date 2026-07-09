"""Pure allocation strategy math with zero Spark dependencies."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Mapping, Sequence

from ..rules.schema import ConsumptionRule, EvenSpreadRule, ManualOverrideRule, StrategyRule, WeightedRule

PROPORTION_ONE = Decimal("1.0")
PROPORTION_QUANTUM = Decimal("0.000000000001")
MONEY_QUANTUM = Decimal("0.01")
REASON_DRIVER_ZERO = "driver_zero"
REASON_SHARED_UNATTRIBUTABLE = "shared_unattributable"


@dataclass(frozen=True)
class StrategyOutcome:
    """Result of attempting to compute a target split for one strategy application."""

    proportions: dict[str, Decimal]
    reason_code: str | None = None

    @property
    def allocatable(self) -> bool:
        return self.reason_code is None


def _canonicalize_targets(targets: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted(dict.fromkeys(targets)))


def _normalize_to_exact_one(raw_values: Mapping[str, Decimal]) -> dict[str, Decimal]:
    total = sum(raw_values.values(), start=Decimal("0.0"))
    rounded: dict[str, Decimal] = {}
    ranking: list[tuple[Decimal, Decimal, str]] = []

    for target, raw_value in raw_values.items():
        raw_share = raw_value / total
        rounded_share = raw_share.quantize(PROPORTION_QUANTUM, rounding=ROUND_DOWN)
        rounded[target] = rounded_share
        ranking.append((raw_share - rounded_share, raw_share, target))

    remainder = PROPORTION_ONE - sum(rounded.values(), start=Decimal("0.0"))
    increments = int((remainder / PROPORTION_QUANTUM).to_integral_value())
    ranking.sort(key=lambda item: (-item[0], -item[1], item[2]))

    for index in range(increments):
        rounded[ranking[index][2]] += PROPORTION_QUANTUM

    return rounded


def even_spread(
    targets: Sequence[str],
    signals: Mapping[str, Decimal] | None = None,
    *,
    params: Mapping[str, Decimal] | None = None,
) -> StrategyOutcome:
    """Return equal proportions across the supplied targets."""
    del signals, params
    canonical_targets = _canonicalize_targets(targets)
    if not canonical_targets:
        return StrategyOutcome({}, REASON_SHARED_UNATTRIBUTABLE)
    return StrategyOutcome(_normalize_to_exact_one({target: Decimal("1.0") for target in canonical_targets}))


def weighted(
    targets: Sequence[str],
    signals: Mapping[str, Decimal] | None = None,
    *,
    params: Mapping[str, Decimal],
) -> StrategyOutcome:
    """Return proportions based on explicit static weights."""
    del signals
    canonical_targets = _canonicalize_targets(targets)
    if not canonical_targets:
        return StrategyOutcome({}, REASON_SHARED_UNATTRIBUTABLE)

    raw_weights = {target: params.get(target, Decimal("0.0")) for target in canonical_targets}
    total = sum(raw_weights.values(), start=Decimal("0.0"))
    if total == Decimal("0.0"):
        return StrategyOutcome({}, REASON_DRIVER_ZERO)

    return StrategyOutcome(_normalize_to_exact_one(raw_weights))


def consumption(
    targets: Sequence[str],
    signals: Mapping[str, Decimal] | None,
    *,
    params: Mapping[str, Decimal] | None = None,
) -> StrategyOutcome:
    """Return proportions based on usage-metric signal values."""
    del params
    canonical_targets = _canonicalize_targets(targets)
    if not canonical_targets:
        return StrategyOutcome({}, REASON_SHARED_UNATTRIBUTABLE)

    safe_signals = signals or {}
    raw_signals = {target: safe_signals.get(target, Decimal("0.0")) for target in canonical_targets}
    total = sum(raw_signals.values(), start=Decimal("0.0"))
    if total == Decimal("0.0"):
        return StrategyOutcome({}, REASON_DRIVER_ZERO)

    return StrategyOutcome(_normalize_to_exact_one(raw_signals))


def manual_override(
    targets: Sequence[str],
    signals: Mapping[str, Decimal] | None = None,
    *,
    params: Mapping[str, Decimal],
) -> StrategyOutcome:
    """Return the declared proportions unchanged."""
    del targets, signals
    return StrategyOutcome(dict(params))


def compute_strategy_outcome(
    rule: StrategyRule,
    targets: Sequence[str],
    *,
    signals: Mapping[str, Decimal] | None = None,
) -> StrategyOutcome:
    """Dispatch to the pure strategy implementation for the supplied rule."""
    if isinstance(rule, EvenSpreadRule):
        return even_spread(targets, signals)
    if isinstance(rule, WeightedRule):
        return weighted(targets, signals, params=rule.weights)
    if isinstance(rule, ConsumptionRule):
        return consumption(targets, signals)
    if isinstance(rule, ManualOverrideRule):
        return manual_override(targets, signals, params=rule.proportions)
    raise TypeError(f"Unsupported strategy rule: {type(rule)!r}")


def distribute_amount(amount: Decimal, proportions: Mapping[str, Decimal]) -> dict[str, Decimal]:
    """Distribute a money amount across targets and preserve the exact total to cents."""
    ranked_targets = sorted(proportions)
    rounded: dict[str, Decimal] = {}
    ranking: list[tuple[Decimal, Decimal, str]] = []

    for target in ranked_targets:
        raw_amount = amount * proportions[target]
        rounded_amount = raw_amount.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
        rounded[target] = rounded_amount
        ranking.append((raw_amount - rounded_amount, raw_amount, target))

    remainder = amount - sum(rounded.values(), start=Decimal("0.00"))
    increments = int((remainder / MONEY_QUANTUM).to_integral_value())
    ranking.sort(key=lambda item: (-item[0], -item[1], item[2]))

    for index in range(increments):
        rounded[ranking[index][2]] += MONEY_QUANTUM

    return rounded
