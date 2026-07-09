"""Pure fast tests for allocation strategy math."""

from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

from tech_cost_platform.engine import strategies
from tech_cost_platform.engine.strategies import (
    REASON_DRIVER_ZERO,
    REASON_SHARED_UNATTRIBUTABLE,
    consumption,
    distribute_amount,
    even_spread,
    manual_override,
    weighted,
)


def test_even_spread_returns_equal_decimal_proportions_summing_to_one() -> None:
    """Equal-target splits should be exact Decimals that reconcile to 1.0."""
    outcome = even_spread(("BU-B", "BU-A", "BU-C"))

    assert outcome.allocatable
    assert outcome.proportions == {
        "BU-A": Decimal("0.333333333334"),
        "BU-B": Decimal("0.333333333333"),
        "BU-C": Decimal("0.333333333333"),
    }
    assert sum(outcome.proportions.values(), start=Decimal("0.0")) == Decimal("1.0")
    assert all(isinstance(value, Decimal) for value in outcome.proportions.values())


def test_weighted_respects_static_weights_and_zero_weight_targets() -> None:
    """Static weights should drive proportions and preserve zero-weight exclusions."""
    outcome = weighted(
        ("APP-C", "APP-B", "APP-A"),
        params={
            "APP-A": Decimal("5"),
            "APP-B": Decimal("3"),
            "APP-C": Decimal("0"),
        },
    )

    assert outcome.allocatable
    assert outcome.proportions == {
        "APP-A": Decimal("0.625000000000"),
        "APP-B": Decimal("0.375000000000"),
        "APP-C": Decimal("0E-12"),
    }
    assert sum(outcome.proportions.values(), start=Decimal("0.0")) == Decimal("1.0")


def test_weighted_all_zero_weights_cannot_allocate() -> None:
    """Weighted strategies with no positive signal should exit as driver_zero."""
    outcome = weighted(
        ("TWR-COMPUTE", "TWR-LABOR"),
        params={
            "TWR-COMPUTE": Decimal("0"),
            "TWR-LABOR": Decimal("0"),
        },
    )

    assert not outcome.allocatable
    assert outcome.proportions == {}
    assert outcome.reason_code == REASON_DRIVER_ZERO


def test_consumption_uses_metric_values_and_signals_driver_zero_on_zero_sum() -> None:
    """Consumption should follow usage values and refuse zero-sum metrics."""
    outcome = consumption(
        ("BU-A", "BU-B", "BU-C"),
        {
            "BU-A": Decimal("10"),
            "BU-B": Decimal("30"),
            "BU-C": Decimal("60"),
        },
    )
    zero_sum = consumption(
        ("BU-A", "BU-B"),
        {
            "BU-A": Decimal("0"),
            "BU-B": Decimal("0"),
        },
    )

    assert outcome.allocatable
    assert outcome.proportions == {
        "BU-A": Decimal("0.100000000000"),
        "BU-B": Decimal("0.300000000000"),
        "BU-C": Decimal("0.600000000000"),
    }
    assert zero_sum.reason_code == REASON_DRIVER_ZERO


def test_consumption_empty_targets_signal_shared_unattributable() -> None:
    """A split step with no downstream target rows should become shared_unattributable."""
    outcome = consumption((), {})

    assert not outcome.allocatable
    assert outcome.proportions == {}
    assert outcome.reason_code == REASON_SHARED_UNATTRIBUTABLE


def test_manual_override_returns_declared_proportions_unchanged() -> None:
    """Manual override should hand back the governed proportions verbatim."""
    params = {
        "BU-CORP": Decimal("0.15"),
        "BU-RETAIL": Decimal("0.35"),
        "BU-WHOLESALE": Decimal("0.50"),
    }

    outcome = manual_override(tuple(params), params=params)

    assert outcome.allocatable
    assert outcome.proportions == params


def test_distribute_amount_assigns_cents_deterministically() -> None:
    """Money rounding remainders should be assigned deterministically to preserve totals."""
    outcome = even_spread(("A", "B", "C"))
    distributed = distribute_amount(Decimal("0.05"), outcome.proportions)

    assert distributed == {
        "A": Decimal("0.02"),
        "B": Decimal("0.02"),
        "C": Decimal("0.01"),
    }
    assert sum(distributed.values(), start=Decimal("0.00")) == Decimal("0.05")


def test_strategies_module_imports_no_pyspark() -> None:
    """The pure strategy module must not import pyspark anywhere."""
    source = Path(strategies.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source, filename=strategies.__file__)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_names = [alias.name for alias in node.names]
            assert all(not name.startswith("pyspark") for name in imported_names)
        if isinstance(node, ast.ImportFrom):
            module_name = node.module or ""
            assert not module_name.startswith("pyspark")
