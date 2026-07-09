"""Arrow-backed adapter for the pure Python allocation engine."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Mapping

import pyarrow as pa

from ..delta_tables import (
    MONEY_TYPE,
    PROPORTION_TYPE,
    build_arrow_table,
    read_delta_table,
    write_delta_table,
)
from ..rules import RuleRegistry
from ..rules.schema import ManualOverrideRule, RuleVersion, StrategyRule, WeightedRule
from ..runtime import resolve_repo_path
from ..silver.conform import SILVER_TABLE_NAMES
from .strategies import (
    PROPORTION_ONE,
    REASON_SHARED_UNATTRIBUTABLE,
    StrategyOutcome,
    compute_strategy_outcome,
    distribute_amount,
)

SILVER_REQUIRED_TABLES = SILVER_TABLE_NAMES
GOLD_ALLOCATION_TABLE = "allocation"
GOLD_RESIDUAL_TABLE = "residual"

ALLOCATION_SCHEMA = pa.schema(
    [
        ("gl_line_id", pa.string()),
        ("period", pa.string()),
        ("gl_account", pa.string()),
        ("cost_center_id", pa.string()),
        ("tower_id", pa.string()),
        ("app_id", pa.string()),
        ("bu_id", pa.string()),
        ("allocated_amount_eur", MONEY_TYPE),
        ("rule_version", pa.string()),
        ("gl_to_tower_proportion", PROPORTION_TYPE),
        ("tower_to_app_proportion", PROPORTION_TYPE),
        ("app_to_bu_proportion", PROPORTION_TYPE),
    ]
)
RESIDUAL_SCHEMA = pa.schema(
    [
        ("gl_line_id", pa.string()),
        ("period", pa.string()),
        ("gl_account", pa.string()),
        ("cost_center_id", pa.string()),
        ("tower_id", pa.string()),
        ("app_id", pa.string()),
        ("amount_eur", MONEY_TYPE),
        ("failed_step", pa.string()),
        ("reason_code", pa.string()),
        ("rule_version", pa.string()),
    ]
)
ALLOCATION_SORT_COLUMNS = [
    "gl_line_id",
    "tower_id",
    "app_id",
    "bu_id",
    "allocated_amount_eur",
]
RESIDUAL_SORT_COLUMNS = [
    "gl_line_id",
    "failed_step",
    "reason_code",
    "amount_eur",
]


@dataclass(frozen=True)
class AllocationResult:
    """Successful gold-layer output paths for one allocation engine run."""

    output_paths: dict[str, Path]
    rule_version: str


class AllocationValidationError(ValueError):
    """Raised when required engine inputs or reconciliation checks fail."""


@dataclass(frozen=True)
class GLLineRecord:
    gl_line_id: str
    period: str
    gl_account: str
    cost_center_id: str
    amount_eur: Decimal
    tower_id: str | None


@dataclass(frozen=True)
class TowerFlow:
    gl_line_id: str
    period: str
    gl_account: str
    cost_center_id: str
    tower_id: str
    amount_eur: Decimal
    gl_to_tower_proportion: Decimal


@dataclass(frozen=True)
class AppFlow:
    gl_line_id: str
    period: str
    gl_account: str
    cost_center_id: str
    tower_id: str
    app_id: str
    amount_eur: Decimal
    gl_to_tower_proportion: Decimal
    tower_to_app_proportion: Decimal


@dataclass(frozen=True)
class AllocationRow:
    gl_line_id: str
    period: str
    gl_account: str
    cost_center_id: str
    tower_id: str
    app_id: str
    bu_id: str
    allocated_amount_eur: Decimal
    rule_version: str
    gl_to_tower_proportion: Decimal
    tower_to_app_proportion: Decimal
    app_to_bu_proportion: Decimal


@dataclass(frozen=True)
class ResidualRow:
    gl_line_id: str
    period: str
    gl_account: str
    cost_center_id: str
    tower_id: str | None
    app_id: str | None
    amount_eur: Decimal
    failed_step: str
    reason_code: str
    rule_version: str


@dataclass(frozen=True)
class UsageIndex:
    """Indexed usage signals for one silver usage fact table."""

    targets_by_step_from: dict[str, dict[str, tuple[str, ...]]]
    signals_by_step_from_metric: dict[str, dict[str, dict[str, dict[str, Decimal]]]]


def read_silver_tables(silver_dir: Path) -> dict[str, pa.Table]:
    """Load the required silver Delta tables from disk."""
    tables: dict[str, pa.Table] = {}
    for table_name in SILVER_REQUIRED_TABLES:
        table_path = silver_dir / table_name
        if not table_path.exists():
            raise FileNotFoundError(f"Expected silver Delta table for {table_name}: {table_path}")
        tables[table_name] = read_delta_table(table_path)
    return tables


def build_usage_index(usage_rows: Iterable[dict[str, object]]) -> UsageIndex:
    """Index silver usage metrics by step, source id, and metric name."""
    targets_by_step_from: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    signals_by_step_from_metric: dict[str, dict[str, dict[str, dict[str, Decimal]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: Decimal("0.00"))))
    )

    for row in usage_rows:
        step = str(row["step"])
        from_id = str(row["from_id"])
        to_id = str(row["to_id"])
        metric_name = str(row["metric_name"])
        value = row["value"] or Decimal("0.00")
        targets_by_step_from[step][from_id].add(to_id)
        signals_by_step_from_metric[step][from_id][metric_name][to_id] += value

    return UsageIndex(
        targets_by_step_from={
            step: {
                from_id: tuple(sorted(targets))
                for from_id, targets in sorted(from_map.items())
            }
            for step, from_map in sorted(targets_by_step_from.items())
        },
        signals_by_step_from_metric={
            step: {
                from_id: {
                    metric_name: dict(sorted(target_map.items()))
                    for metric_name, target_map in sorted(metric_map.items())
                }
                for from_id, metric_map in sorted(from_map.items())
            }
            for step, from_map in sorted(signals_by_step_from_metric.items())
        },
    )


def _strategy_targets_for_split_step(
    usage_targets: tuple[str, ...],
    rule: StrategyRule,
) -> tuple[str, ...]:
    if isinstance(rule, ManualOverrideRule):
        return tuple(sorted(rule.proportions))
    if isinstance(rule, WeightedRule):
        return tuple(sorted(set(usage_targets) | set(rule.weights)))
    return usage_targets


def _consumption_signals(
    usage_index: UsageIndex,
    *,
    step_name: str,
    from_id: str,
    metric_name: str,
    targets: tuple[str, ...],
) -> dict[str, Decimal]:
    raw_metric_map = (
        usage_index.signals_by_step_from_metric.get(step_name, {})
        .get(from_id, {})
        .get(metric_name, {})
    )
    return {target: raw_metric_map.get(target, Decimal("0.00")) for target in targets}


def _to_gl_line_records(gl_cost_rows: Iterable[dict[str, object]]) -> list[GLLineRecord]:
    return [
        GLLineRecord(
            gl_line_id=str(row["gl_line_id"]),
            period=str(row["period"]),
            gl_account=str(row["gl_account"]),
            cost_center_id=str(row["cost_center_id"]),
            amount_eur=row["amount_eur"],
            tower_id=row["tower_id"],
        )
        for row in sorted(gl_cost_rows, key=lambda item: item["gl_line_id"])
    ]


def _distribute_tower_flows(
    gl_lines: list[GLLineRecord],
    rule: RuleVersion,
    tower_ids: tuple[str, ...],
) -> tuple[list[TowerFlow], list[ResidualRow]]:
    tower_flows: list[TowerFlow] = []
    residual_rows: list[ResidualRow] = []
    fallback_rule = rule.gl_to_tower.on_unmapped

    for gl_line in gl_lines:
        if gl_line.tower_id is not None:
            tower_flows.append(
                TowerFlow(
                    gl_line_id=gl_line.gl_line_id,
                    period=gl_line.period,
                    gl_account=gl_line.gl_account,
                    cost_center_id=gl_line.cost_center_id,
                    tower_id=gl_line.tower_id,
                    amount_eur=gl_line.amount_eur,
                    gl_to_tower_proportion=PROPORTION_ONE,
                )
            )
            continue

        if fallback_rule is None:
            residual_rows.append(
                ResidualRow(
                    gl_line_id=gl_line.gl_line_id,
                    period=gl_line.period,
                    gl_account=gl_line.gl_account,
                    cost_center_id=gl_line.cost_center_id,
                    tower_id=None,
                    app_id=None,
                    amount_eur=gl_line.amount_eur,
                    failed_step="gl_to_tower",
                    reason_code="unmapped",
                    rule_version=rule.version_id,
                )
            )
            continue

        outcome = compute_strategy_outcome(fallback_rule, tower_ids)
        if not outcome.allocatable:
            residual_rows.append(
                ResidualRow(
                    gl_line_id=gl_line.gl_line_id,
                    period=gl_line.period,
                    gl_account=gl_line.gl_account,
                    cost_center_id=gl_line.cost_center_id,
                    tower_id=None,
                    app_id=None,
                    amount_eur=gl_line.amount_eur,
                    failed_step="gl_to_tower",
                    reason_code=outcome.reason_code or "driver_zero",
                    rule_version=rule.version_id,
                )
            )
            continue

        distributed_amounts = distribute_amount(gl_line.amount_eur, outcome.proportions)
        for tower_id, amount_eur in distributed_amounts.items():
            if amount_eur == Decimal("0.00"):
                continue
            tower_flows.append(
                TowerFlow(
                    gl_line_id=gl_line.gl_line_id,
                    period=gl_line.period,
                    gl_account=gl_line.gl_account,
                    cost_center_id=gl_line.cost_center_id,
                    tower_id=tower_id,
                    amount_eur=amount_eur,
                    gl_to_tower_proportion=outcome.proportions[tower_id],
                )
            )

    return tower_flows, residual_rows


def _distribute_app_flows(
    tower_flows: list[TowerFlow],
    rule: RuleVersion,
    usage_index: UsageIndex,
) -> tuple[list[AppFlow], list[ResidualRow]]:
    app_flows: list[AppFlow] = []
    residual_rows: list[ResidualRow] = []
    outcome_cache: dict[str, tuple[tuple[str, ...], StrategyOutcome]] = {}

    for tower_flow in tower_flows:
        if tower_flow.tower_id not in outcome_cache:
            usage_targets = usage_index.targets_by_step_from.get("tower_to_app", {}).get(
                tower_flow.tower_id, ()
            )
            if not usage_targets:
                outcome_cache[tower_flow.tower_id] = (
                    (),
                    StrategyOutcome({}, REASON_SHARED_UNATTRIBUTABLE),
                )
            else:
                targets = _strategy_targets_for_split_step(usage_targets, rule.tower_to_app)
                signals = None
                if hasattr(rule.tower_to_app, "metric_name"):
                    signals = _consumption_signals(
                        usage_index,
                        step_name="tower_to_app",
                        from_id=tower_flow.tower_id,
                        metric_name=rule.tower_to_app.metric_name,
                        targets=targets,
                    )
                outcome_cache[tower_flow.tower_id] = (
                    targets,
                    compute_strategy_outcome(rule.tower_to_app, targets, signals=signals),
                )

        _, outcome = outcome_cache[tower_flow.tower_id]
        if not outcome.allocatable:
            residual_rows.append(
                ResidualRow(
                    gl_line_id=tower_flow.gl_line_id,
                    period=tower_flow.period,
                    gl_account=tower_flow.gl_account,
                    cost_center_id=tower_flow.cost_center_id,
                    tower_id=tower_flow.tower_id,
                    app_id=None,
                    amount_eur=tower_flow.amount_eur,
                    failed_step="tower_to_app",
                    reason_code=outcome.reason_code or "driver_zero",
                    rule_version=rule.version_id,
                )
            )
            continue

        distributed_amounts = distribute_amount(tower_flow.amount_eur, outcome.proportions)
        for app_id, amount_eur in distributed_amounts.items():
            if amount_eur == Decimal("0.00"):
                continue
            app_flows.append(
                AppFlow(
                    gl_line_id=tower_flow.gl_line_id,
                    period=tower_flow.period,
                    gl_account=tower_flow.gl_account,
                    cost_center_id=tower_flow.cost_center_id,
                    tower_id=tower_flow.tower_id,
                    app_id=app_id,
                    amount_eur=amount_eur,
                    gl_to_tower_proportion=tower_flow.gl_to_tower_proportion,
                    tower_to_app_proportion=outcome.proportions[app_id],
                )
            )

    return app_flows, residual_rows


def _distribute_bu_allocations(
    app_flows: list[AppFlow],
    rule: RuleVersion,
    usage_index: UsageIndex,
) -> tuple[list[AllocationRow], list[ResidualRow]]:
    allocation_rows: list[AllocationRow] = []
    residual_rows: list[ResidualRow] = []
    outcome_cache: dict[str, tuple[tuple[str, ...], StrategyOutcome]] = {}

    for app_flow in app_flows:
        if app_flow.app_id not in outcome_cache:
            usage_targets = usage_index.targets_by_step_from.get("app_to_bu", {}).get(app_flow.app_id, ())
            if not usage_targets:
                outcome_cache[app_flow.app_id] = ((), StrategyOutcome({}, REASON_SHARED_UNATTRIBUTABLE))
            else:
                targets = _strategy_targets_for_split_step(usage_targets, rule.app_to_bu)
                signals = None
                if hasattr(rule.app_to_bu, "metric_name"):
                    signals = _consumption_signals(
                        usage_index,
                        step_name="app_to_bu",
                        from_id=app_flow.app_id,
                        metric_name=rule.app_to_bu.metric_name,
                        targets=targets,
                    )
                outcome_cache[app_flow.app_id] = (
                    targets,
                    compute_strategy_outcome(rule.app_to_bu, targets, signals=signals),
                )

        _, outcome = outcome_cache[app_flow.app_id]
        if not outcome.allocatable:
            residual_rows.append(
                ResidualRow(
                    gl_line_id=app_flow.gl_line_id,
                    period=app_flow.period,
                    gl_account=app_flow.gl_account,
                    cost_center_id=app_flow.cost_center_id,
                    tower_id=app_flow.tower_id,
                    app_id=app_flow.app_id,
                    amount_eur=app_flow.amount_eur,
                    failed_step="app_to_bu",
                    reason_code=outcome.reason_code or "driver_zero",
                    rule_version=rule.version_id,
                )
            )
            continue

        distributed_amounts = distribute_amount(app_flow.amount_eur, outcome.proportions)
        for bu_id, amount_eur in distributed_amounts.items():
            if amount_eur == Decimal("0.00"):
                continue
            allocation_rows.append(
                AllocationRow(
                    gl_line_id=app_flow.gl_line_id,
                    period=app_flow.period,
                    gl_account=app_flow.gl_account,
                    cost_center_id=app_flow.cost_center_id,
                    tower_id=app_flow.tower_id,
                    app_id=app_flow.app_id,
                    bu_id=bu_id,
                    allocated_amount_eur=amount_eur,
                    rule_version=rule.version_id,
                    gl_to_tower_proportion=app_flow.gl_to_tower_proportion,
                    tower_to_app_proportion=app_flow.tower_to_app_proportion,
                    app_to_bu_proportion=outcome.proportions[bu_id],
                )
            )

    return allocation_rows, residual_rows


def execute_cascade(
    tables: Mapping[str, pa.Table],
    rule: RuleVersion,
) -> tuple[list[AllocationRow], list[ResidualRow]]:
    """Run the multi-step allocation cascade over materialized silver inputs."""
    gl_lines = _to_gl_line_records(tables["fact_gl_cost"].to_pylist())
    usage_index = build_usage_index(tables["fact_usage_metric"].to_pylist())
    tower_ids = tuple(sorted(row["tower_id"] for row in tables["dim_resource_tower"].to_pylist()))

    tower_flows, residual_rows = _distribute_tower_flows(gl_lines, rule, tower_ids)
    app_flows, tower_residuals = _distribute_app_flows(tower_flows, rule, usage_index)
    allocation_rows, app_residuals = _distribute_bu_allocations(app_flows, rule, usage_index)

    return allocation_rows, [*residual_rows, *tower_residuals, *app_residuals]


def validate_reconciliation(
    gl_lines: list[GLLineRecord],
    allocation_rows: list[AllocationRow],
    residual_rows: list[ResidualRow],
) -> None:
    """Assert the cascade preserves every input GL euro exactly once in aggregate."""
    input_total = sum((row.amount_eur for row in gl_lines), start=Decimal("0.00"))
    allocation_total = sum((row.allocated_amount_eur for row in allocation_rows), start=Decimal("0.00"))
    residual_total = sum((row.amount_eur for row in residual_rows), start=Decimal("0.00"))

    if input_total != allocation_total + residual_total:
        raise AllocationValidationError(
            f"Allocation reconciliation failed: input={input_total} "
            f"allocated={allocation_total} residual={residual_total}"
        )

    totals_by_gl_line: dict[str, Decimal] = defaultdict(lambda: Decimal("0.00"))
    expected_by_gl_line = {row.gl_line_id: row.amount_eur for row in gl_lines}

    for row in allocation_rows:
        totals_by_gl_line[row.gl_line_id] += row.allocated_amount_eur
    for row in residual_rows:
        totals_by_gl_line[row.gl_line_id] += row.amount_eur

    for gl_line_id, expected_total in expected_by_gl_line.items():
        if totals_by_gl_line[gl_line_id] != expected_total:
            raise AllocationValidationError(
                f"GL line {gl_line_id} is not fully reconciled: "
                f"expected={expected_total} actual={totals_by_gl_line[gl_line_id]}"
            )


def build_allocation_table(rows: list[AllocationRow]) -> pa.Table:
    """Create the allocation Arrow table for Delta output."""
    return build_arrow_table(
        (
            {
                "gl_line_id": row.gl_line_id,
                "period": row.period,
                "gl_account": row.gl_account,
                "cost_center_id": row.cost_center_id,
                "tower_id": row.tower_id,
                "app_id": row.app_id,
                "bu_id": row.bu_id,
                "allocated_amount_eur": row.allocated_amount_eur,
                "rule_version": row.rule_version,
                "gl_to_tower_proportion": row.gl_to_tower_proportion,
                "tower_to_app_proportion": row.tower_to_app_proportion,
                "app_to_bu_proportion": row.app_to_bu_proportion,
            }
            for row in rows
        ),
        ALLOCATION_SCHEMA,
    )


def build_residual_table(rows: list[ResidualRow]) -> pa.Table:
    """Create the residual Arrow table for Delta output."""
    return build_arrow_table(
        (
            {
                "gl_line_id": row.gl_line_id,
                "period": row.period,
                "gl_account": row.gl_account,
                "cost_center_id": row.cost_center_id,
                "tower_id": row.tower_id,
                "app_id": row.app_id,
                "amount_eur": row.amount_eur,
                "failed_step": row.failed_step,
                "reason_code": row.reason_code,
                "rule_version": row.rule_version,
            }
            for row in rows
        ),
        RESIDUAL_SCHEMA,
    )


def write_gold_tables(
    allocation_rows: list[AllocationRow],
    residual_rows: list[ResidualRow],
    gold_dir: Path,
) -> dict[str, Path]:
    """Write the gold allocation and residual tables to Delta."""
    gold_dir.mkdir(parents=True, exist_ok=True)
    allocation_table = build_allocation_table(allocation_rows)
    residual_table = build_residual_table(residual_rows)

    return {
        GOLD_ALLOCATION_TABLE: write_delta_table(
            allocation_table,
            gold_dir / GOLD_ALLOCATION_TABLE,
            sort_columns=ALLOCATION_SORT_COLUMNS,
        ),
        GOLD_RESIDUAL_TABLE: write_delta_table(
            residual_table,
            gold_dir / GOLD_RESIDUAL_TABLE,
            sort_columns=RESIDUAL_SORT_COLUMNS,
        ),
    }


def run_allocation(
    *,
    silver_dir: str | Path,
    gold_dir: str | Path,
    rule_version_id: str | None = None,
    rules_dir: str | Path | None = None,
) -> AllocationResult:
    """Read silver, apply the versioned rules, and write allocation plus residual gold outputs."""
    resolved_silver_dir = resolve_repo_path(silver_dir)
    resolved_gold_dir = resolve_repo_path(gold_dir)

    registry = RuleRegistry(rules_dir=rules_dir)
    rule = registry.resolve(rule_version_id) if rule_version_id is not None else registry.resolve_default()

    try:
        tables = read_silver_tables(resolved_silver_dir)
        gl_lines = _to_gl_line_records(tables["fact_gl_cost"].to_pylist())
        allocation_rows, residual_rows = execute_cascade(tables, rule)
        validate_reconciliation(gl_lines, allocation_rows, residual_rows)
        output_paths = write_gold_tables(allocation_rows, residual_rows, resolved_gold_dir)
        return AllocationResult(output_paths=output_paths, rule_version=rule.version_id)
    except FileNotFoundError as exc:
        raise AllocationValidationError(str(exc)) from exc
