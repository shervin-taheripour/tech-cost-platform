"""Lineage tracing over authoritative gold outputs.

Terminal cents come from staged rounding with deterministic remainder distribution in the
allocation engine. The stored proportions explain the split shape, but they are not a
lossless cent-level reconstruction formula. In lineage, allocated amounts are authoritative;
the proportion-product check is bounded consistency, not exact equality.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from ..delta_tables import read_delta_table
from ..residual.reconcile import reconcile_rule_version
from ..rules import RuleVersion
from ..runtime import resolve_repo_path

ROUNDING_DRIFT_PER_HOP = Decimal("0.01")
ROUNDING_HOPS = 3
MAX_PROPORTION_DRIFT_EUR = ROUNDING_DRIFT_PER_HOP * ROUNDING_HOPS


@dataclass(frozen=True)
class LineageRoundTripResult:
    """Exact round-trip totals derived from the lineage table."""

    rule_version: str
    total_gl_eur: Decimal
    total_lineage_allocated_eur: Decimal
    total_lineage_residual_eur: Decimal
    balanced: bool
    difference_eur: Decimal


class LineageValidationError(ValueError):
    """Raised when lineage drops, duplicates, or materially distorts a trace."""

    def __init__(self, message: str, *, result: LineageRoundTripResult | None = None):
        self.result = result
        super().__init__(message)


def load_lineage_rows(lineage_dir: str | Path) -> list[dict[str, object]]:
    """Read the lineage Delta table as Python rows."""
    return read_delta_table(resolve_repo_path(lineage_dir)).to_pylist()


def trace_forward(
    *,
    lineage_dir: str | Path,
    rule_version_id: str,
    gl_line_id: str,
) -> list[dict[str, object]]:
    """Return all terminal lineage outcomes for one GL line and rule version."""
    return [
        row
        for row in load_lineage_rows(lineage_dir)
        if row["rule_version"] == rule_version_id and row["gl_line_id"] == gl_line_id
    ]


def trace_backward(
    *,
    lineage_dir: str | Path,
    rule_version_id: str,
    bu_id: str | None = None,
    app_id: str | None = None,
) -> list[dict[str, object]]:
    """Return the contributing allocated lineage rows behind a BU or app total."""
    if bu_id is None and app_id is None:
        raise ValueError("trace_backward requires bu_id or app_id.")

    rows = [
        row
        for row in load_lineage_rows(lineage_dir)
        if row["rule_version"] == rule_version_id and row["outcome"] == "allocated"
    ]
    if bu_id is not None:
        rows = [row for row in rows if row["bu_id"] == bu_id]
    if app_id is not None:
        rows = [row for row in rows if row["app_id"] == app_id]
    return rows


def validate_lineage_per_line(rows: Iterable[dict[str, object]]) -> None:
    """Assert exact per-GL completeness from lineage alone."""
    totals: dict[str, Decimal] = {}
    expected: dict[str, Decimal] = {}
    for row in rows:
        gl_line_id = row["gl_line_id"]
        expected[gl_line_id] = row["gl_amount_eur"]
        totals[gl_line_id] = totals.get(gl_line_id, Decimal("0.00")) + row["terminal_amount_eur"]

    for gl_line_id, gl_amount in expected.items():
        if totals[gl_line_id] != gl_amount:
            raise LineageValidationError(
                f"Lineage per-line completeness failed for {gl_line_id}: "
                f"expected={gl_amount} actual={totals[gl_line_id]}"
            )


def validate_proportion_consistency(rows: Iterable[dict[str, object]]) -> None:
    """Assert bounded consistency between explanatory proportions and authoritative cents."""
    for row in rows:
        if row["outcome"] != "allocated":
            continue
        product = (
            row["gl_amount_eur"]
            * row["prop_gl_to_tower"]
            * row["prop_tower_to_app"]
            * row["prop_app_to_bu"]
        )
        drift = abs(product - row["allocated_amount_eur"])
        if drift > MAX_PROPORTION_DRIFT_EUR:
            raise LineageValidationError(
                f"Lineage proportion drift too large for {row['gl_line_id']}: "
                f"product={product} allocated={row['allocated_amount_eur']} drift={drift}"
            )


def validate_lineage_round_trip(
    *,
    silver_dir: str | Path,
    gold_dir: str | Path,
    lineage_dir: str | Path,
    rule_version_id: str,
) -> LineageRoundTripResult:
    """Assert exact GL -> gold -> GL round-trip using lineage plus residual outcomes."""
    gold_reconciliation = reconcile_rule_version(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        rule_version_id=rule_version_id,
    )
    lineage_rows = [
        row
        for row in load_lineage_rows(lineage_dir)
        if row["rule_version"] == rule_version_id
    ]
    allocated_total = sum(
        (
            row["allocated_amount_eur"]
            for row in lineage_rows
            if row["allocated_amount_eur"] is not None
        ),
        start=Decimal("0.00"),
    )
    residual_total = sum(
        (
            row["residual_amount_eur"]
            for row in lineage_rows
            if row["residual_amount_eur"] is not None
        ),
        start=Decimal("0.00"),
    )
    difference = gold_reconciliation.total_gl_eur - allocated_total - residual_total
    result = LineageRoundTripResult(
        rule_version=rule_version_id,
        total_gl_eur=gold_reconciliation.total_gl_eur,
        total_lineage_allocated_eur=allocated_total,
        total_lineage_residual_eur=residual_total,
        balanced=difference == Decimal("0.00"),
        difference_eur=difference,
    )
    if not result.balanced:
        raise LineageValidationError(
            "Lineage round-trip failed for "
            f"{rule_version_id}: total_gl={result.total_gl_eur} "
            f"allocated={allocated_total} residual={residual_total} "
            f"difference={difference}",
            result=result,
        )
    if allocated_total != gold_reconciliation.total_allocated_eur:
        raise LineageValidationError(
            f"Lineage allocated total drifted from gold allocation for {rule_version_id}: "
            f"lineage={allocated_total} gold={gold_reconciliation.total_allocated_eur}",
            result=result,
        )
    if residual_total != gold_reconciliation.total_residual_eur:
        raise LineageValidationError(
            f"Lineage residual total drifted from gold residual for {rule_version_id}: "
            f"lineage={residual_total} gold={gold_reconciliation.total_residual_eur}",
            result=result,
        )
    return result


def build_worked_example_payload(
    *,
    lineage_rows: list[dict[str, object]],
    rule: RuleVersion,
) -> dict[str, object]:
    """Build the committed worked example from a real lineage run."""
    allocated_rows = [row for row in lineage_rows if row["outcome"] == "allocated"]
    residual_rows = [row for row in lineage_rows if row["outcome"] == "residual"]

    fanout_gl_line_id = next(
        gl_line_id
        for gl_line_id in sorted({row["gl_line_id"] for row in allocated_rows})
        if sum(1 for row in allocated_rows if row["gl_line_id"] == gl_line_id) > 1
    )
    fanout_rows = [row for row in allocated_rows if row["gl_line_id"] == fanout_gl_line_id]
    residual_row = next(
        row
        for row in residual_rows
        if row["cost_center_id"] == "CC-LEGACY"
    )

    allocated_paths = []
    for row in fanout_rows:
        raw_product = (
            row["gl_amount_eur"]
            * row["prop_gl_to_tower"]
            * row["prop_tower_to_app"]
            * row["prop_app_to_bu"]
        )
        allocated_paths.append(
            {
                "gl_line_id": row["gl_line_id"],
                "tower_id": row["tower_id"],
                "app_id": row["app_id"],
                "bu_id": row["bu_id"],
                "gl_amount_eur": str(row["gl_amount_eur"]),
                "prop_gl_to_tower": str(row["prop_gl_to_tower"]),
                "prop_tower_to_app": str(row["prop_tower_to_app"]),
                "prop_app_to_bu": str(row["prop_app_to_bu"]),
                "raw_product_eur": str(raw_product),
                "rounded_terminal_amount_eur": str(row["allocated_amount_eur"]),
                "difference_from_raw_eur": str(row["allocated_amount_eur"] - raw_product),
            }
        )

    return {
        "rule_version": rule.version_id,
        "rounding_note": (
            "Terminal cents derive from staged rounding with deterministic remainder "
            "distribution. Proportions explain the split shape; allocated amounts are authoritative."
        ),
        "allocated_example": {
            "gl_line_id": fanout_gl_line_id,
            "gl_account": fanout_rows[0]["gl_account"],
            "cost_center_id": fanout_rows[0]["cost_center_id"],
            "gl_amount_eur": str(fanout_rows[0]["gl_amount_eur"]),
            "gl_to_tower_basis": rule.gl_to_tower.basis,
            "tower_to_app_driver": getattr(rule.tower_to_app, "metric_name", None),
            "app_to_bu_driver": getattr(rule.app_to_bu, "metric_name", None),
            "paths": allocated_paths,
            "terminal_total_eur": str(
                sum((row["allocated_amount_eur"] for row in fanout_rows), start=Decimal("0.00"))
            ),
        },
        "residual_example": {
            "gl_line_id": residual_row["gl_line_id"],
            "gl_account": residual_row["gl_account"],
            "cost_center_id": residual_row["cost_center_id"],
            "gl_amount_eur": str(residual_row["gl_amount_eur"]),
            "outcome": residual_row["outcome"],
            "reason_code": residual_row["reason_code"],
            "failed_step": residual_row["failed_step"],
            "tower_id": residual_row["tower_id"],
            "app_id": residual_row["app_id"],
            "bu_id": residual_row["bu_id"],
            "terminal_amount_eur": str(residual_row["terminal_amount_eur"]),
        },
    }


def write_worked_example(path: str | Path, payload: dict[str, object]) -> Path:
    """Persist the worked example as committed JSON."""
    resolved_path = resolve_repo_path(path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return resolved_path
