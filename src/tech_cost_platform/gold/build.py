"""Orchestrate gold report views: read gold/silver inputs, build views, write Delta."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from ..delta_tables import read_delta_table, write_delta_table
from ..engine.cascade import execute_cascade
from ..rules import RuleRegistry
from ..runtime import repo_root, resolve_repo_path
from .views import (
    REPORT_APP_TCO_SORT_COLUMNS,
    REPORT_APP_TCO_TABLE,
    REPORT_BU_SHOWBACK_SORT_COLUMNS,
    REPORT_BU_SHOWBACK_TABLE,
    REPORT_DRIVER_COMPARISON_BY_APP_SORT_COLUMNS,
    REPORT_DRIVER_COMPARISON_BY_APP_TABLE,
    REPORT_DRIVER_COMPARISON_SORT_COLUMNS,
    REPORT_DRIVER_COMPARISON_TABLE,
    REPORT_LINEAGE_SORT_COLUMNS,
    REPORT_LINEAGE_TABLE,
    REPORT_RESIDUAL_SORT_COLUMNS,
    REPORT_RESIDUAL_TABLE,
    build_report_application_tco,
    build_report_bu_showback,
    build_report_driver_comparison,
    build_report_driver_comparison_by_app,
    build_report_lineage,
    build_report_residual,
)


@dataclass(frozen=True)
class GoldReportsResult:
    """Gold report output paths plus driver-comparison reconciliation totals."""

    output_paths: dict[str, Path]
    gl_total_eur: Decimal
    v1_allocated_eur: Decimal
    v1_residual_eur: Decimal
    v2_allocated_eur: Decimal
    v2_residual_eur: Decimal


def _load_silver_inputs(silver_dir: Path) -> dict[str, pa.Table]:
    required = {
        "dim_application": silver_dir / "dim_application",
        "dim_business_unit": silver_dir / "dim_business_unit",
        "dim_resource_tower": silver_dir / "dim_resource_tower",
        "fact_gl_cost": silver_dir / "fact_gl_cost",
        "fact_usage_metric": silver_dir / "fact_usage_metric",
    }
    tables: dict[str, pa.Table] = {}
    for name, path in required.items():
        if not path.exists():
            raise FileNotFoundError(f"Expected silver table {name}: {path}")
        tables[name] = read_delta_table(path)
    return tables


def _load_gold_inputs(gold_dir: Path) -> dict[str, pa.Table]:
    required = {
        "allocation": gold_dir / "allocation",
        "residual_report": gold_dir / "residual_report",
        "lineage": gold_dir / "lineage",
    }
    tables: dict[str, pa.Table] = {}
    for name, path in required.items():
        if not path.exists():
            raise FileNotFoundError(f"Expected gold table {name}: {path}")
        tables[name] = read_delta_table(path)
    return tables


def _validate_cascade_reconciliation(
    allocation_rows: list,
    residual_rows: list,
    gl_total: Decimal,
    version_id: str,
) -> tuple[Decimal, Decimal]:
    """Assert allocation + residual == gl_total and return the two totals."""
    allocated = sum((r.allocated_amount_eur for r in allocation_rows), start=Decimal("0.00"))
    residual = sum((r.amount_eur for r in residual_rows), start=Decimal("0.00"))
    if allocated + residual != gl_total:
        raise ValueError(
            f"Driver comparison reconciliation failed for {version_id}: "
            f"allocated={allocated} residual={residual} "
            f"sum={allocated + residual} expected={gl_total}"
        )
    return allocated, residual


def build_gold_reports(
    *,
    silver_dir: str | Path,
    gold_dir: str | Path,
    rules_dir: str | Path | None = None,
) -> GoldReportsResult:
    """Read gold and silver inputs, build five report views, and write to Delta."""
    resolved_silver_dir = resolve_repo_path(silver_dir)
    resolved_gold_dir = resolve_repo_path(gold_dir)

    silver = _load_silver_inputs(resolved_silver_dir)
    gold = _load_gold_inputs(resolved_gold_dir)

    app_tco = build_report_application_tco(gold["allocation"], silver["dim_application"])
    bu_showback = build_report_bu_showback(gold["allocation"], silver["dim_business_unit"])
    report_residual = build_report_residual(gold["residual_report"])
    report_lineage = build_report_lineage(gold["lineage"])

    registry = RuleRegistry(rules_dir=rules_dir)
    v1_rule = registry.resolve("v1_transactions")
    v2_rule = registry.resolve("v2_named_users")

    cascade_inputs = {
        "fact_gl_cost": silver["fact_gl_cost"],
        "fact_usage_metric": silver["fact_usage_metric"],
        "dim_resource_tower": silver["dim_resource_tower"],
    }

    v1_allocation_rows, v1_residual_rows = execute_cascade(cascade_inputs, v1_rule)
    v2_allocation_rows, v2_residual_rows = execute_cascade(cascade_inputs, v2_rule)

    gl_total = sum(
        (row["amount_eur"] for row in silver["fact_gl_cost"].to_pylist()),
        start=Decimal("0.00"),
    )
    v1_allocated_eur, v1_residual_eur = _validate_cascade_reconciliation(
        v1_allocation_rows, v1_residual_rows, gl_total, "v1_transactions"
    )
    v2_allocated_eur, v2_residual_eur = _validate_cascade_reconciliation(
        v2_allocation_rows, v2_residual_rows, gl_total, "v2_named_users"
    )

    driver_comparison = build_report_driver_comparison(
        v1_allocation=v1_allocation_rows,
        v2_allocation=v2_allocation_rows,
        dim_business_unit=silver["dim_business_unit"],
    )
    driver_comparison_by_app = build_report_driver_comparison_by_app(
        v1_allocation=v1_allocation_rows,
        v2_allocation=v2_allocation_rows,
        dim_application=silver["dim_application"],
        dim_business_unit=silver["dim_business_unit"],
    )

    output_paths: dict[str, Path] = {}
    for table_name, table, sort_cols in [
        (REPORT_APP_TCO_TABLE, app_tco, REPORT_APP_TCO_SORT_COLUMNS),
        (REPORT_BU_SHOWBACK_TABLE, bu_showback, REPORT_BU_SHOWBACK_SORT_COLUMNS),
        (REPORT_RESIDUAL_TABLE, report_residual, REPORT_RESIDUAL_SORT_COLUMNS),
        (REPORT_LINEAGE_TABLE, report_lineage, REPORT_LINEAGE_SORT_COLUMNS),
        (REPORT_DRIVER_COMPARISON_TABLE, driver_comparison, REPORT_DRIVER_COMPARISON_SORT_COLUMNS),
        (
            REPORT_DRIVER_COMPARISON_BY_APP_TABLE,
            driver_comparison_by_app,
            REPORT_DRIVER_COMPARISON_BY_APP_SORT_COLUMNS,
        ),
    ]:
        output_paths[table_name] = write_delta_table(
            table,
            resolved_gold_dir / table_name,
            sort_columns=sort_cols,
        )

    return GoldReportsResult(
        output_paths=output_paths,
        gl_total_eur=gl_total,
        v1_allocated_eur=v1_allocated_eur,
        v1_residual_eur=v1_residual_eur,
        v2_allocated_eur=v2_allocated_eur,
        v2_residual_eur=v2_residual_eur,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold report views from existing gold and silver data.")
    parser.add_argument("--silver-dir", type=Path, help="Optional silver Delta directory override.")
    parser.add_argument("--gold-dir", type=Path, help="Optional gold Delta directory override.")
    parser.add_argument("--rules-dir", type=Path, help="Optional rules directory override.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint for gold reports building."""
    args = parse_args()
    result = build_gold_reports(
        silver_dir=args.silver_dir or repo_root() / "data" / "silver",
        gold_dir=args.gold_dir or repo_root() / "data" / "gold",
        rules_dir=args.rules_dir,
    )
    print(
        "[tech-cost-platform] reports status=completed "
        f"gl_total={result.gl_total_eur} "
        f"v1_allocated={result.v1_allocated_eur} v1_residual={result.v1_residual_eur} "
        f"v2_allocated={result.v2_allocated_eur} v2_residual={result.v2_residual_eur}"
    )
    for table_name, output_path in result.output_paths.items():
        print(f"[tech-cost-platform] reports table={table_name} path={output_path}")
    return 0
