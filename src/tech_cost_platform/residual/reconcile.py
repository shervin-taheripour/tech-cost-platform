"""Explicit reconciliation checks for allocation plus residual outputs."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from ..delta_tables import read_delta_table
from ..runtime import resolve_repo_path


@dataclass(frozen=True)
class ReconciliationResult:
    """Exact reconciliation totals for one rule version."""

    rule_version: str
    total_gl_eur: Decimal
    total_allocated_eur: Decimal
    total_residual_eur: Decimal
    balanced: bool
    difference_eur: Decimal


class ReconciliationError(ValueError):
    """Raised when allocation plus residual does not exactly balance to total GL."""

    def __init__(self, result: ReconciliationResult):
        self.result = result
        super().__init__(
            "Reconciliation failed for "
            f"{result.rule_version}: total_gl={result.total_gl_eur} "
            f"allocated={result.total_allocated_eur} residual={result.total_residual_eur} "
            f"difference={result.difference_eur}"
        )


def reconcile_rule_version(
    *,
    silver_dir: str | Path,
    gold_dir: str | Path,
    rule_version_id: str,
) -> ReconciliationResult:
    """Return and enforce exact reconciliation for one rule version."""
    resolved_silver_dir = resolve_repo_path(silver_dir)
    resolved_gold_dir = resolve_repo_path(gold_dir)

    fact_gl_cost_rows = read_delta_table(resolved_silver_dir / "fact_gl_cost").to_pylist()
    allocation_rows = [
        row
        for row in read_delta_table(resolved_gold_dir / "allocation").to_pylist()
        if row["rule_version"] == rule_version_id
    ]
    residual_rows = [
        row
        for row in read_delta_table(resolved_gold_dir / "residual").to_pylist()
        if row["rule_version"] == rule_version_id
    ]

    total_gl = sum((row["amount_eur"] for row in fact_gl_cost_rows), start=Decimal("0.00"))
    total_allocated = sum(
        (row["allocated_amount_eur"] for row in allocation_rows),
        start=Decimal("0.00"),
    )
    total_residual = sum((row["amount_eur"] for row in residual_rows), start=Decimal("0.00"))
    difference = total_gl - total_allocated - total_residual

    result = ReconciliationResult(
        rule_version=rule_version_id,
        total_gl_eur=total_gl,
        total_allocated_eur=total_allocated,
        total_residual_eur=total_residual,
        balanced=difference == Decimal("0.00"),
        difference_eur=difference,
    )
    if not result.balanced:
        raise ReconciliationError(result)
    return result
