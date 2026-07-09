"""Residual detail and summary reporting."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import duckdb
import pyarrow as pa

from ..delta_tables import MONEY_TYPE, build_arrow_table, read_delta_table, write_delta_table
from ..rules import RuleRegistry
from ..runtime import repo_root, resolve_repo_path
from .reconcile import ReconciliationResult, reconcile_rule_version

RESIDUAL_REPORT_TABLE = "residual_report"
RESIDUAL_DETAIL_TABLE = "residual_detail"
PCT_QUANTUM = Decimal("0.000001")
PCT_TYPE = pa.decimal128(18, 6)
RESIDUAL_REPORT_SCHEMA = pa.schema(
    [
        ("rule_version", pa.string()),
        ("failed_step", pa.string()),
        ("reason_code", pa.string()),
        ("residual_amount_eur", MONEY_TYPE),
        ("gl_line_count", pa.int64()),
        ("pct_of_total_gl", PCT_TYPE),
    ]
)
RECONCILIATION_SCHEMA = pa.schema(
    [
        ("rule_version", pa.string()),
        ("total_gl_eur", MONEY_TYPE),
        ("total_allocated_eur", MONEY_TYPE),
        ("total_residual_eur", MONEY_TYPE),
        ("balanced", pa.bool_()),
        ("difference_eur", MONEY_TYPE),
    ]
)
RESIDUAL_DETAIL_SORT_COLUMNS = [
    "rule_version",
    "failed_step",
    "reason_code",
    "gl_line_id",
]
RESIDUAL_REPORT_SORT_COLUMNS = [
    "rule_version",
    "failed_step",
    "reason_code",
]
RECONCILIATION_SORT_COLUMNS = ["rule_version"]


@dataclass(frozen=True)
class ResidualReportResult:
    """Residual reporting outputs for one build."""

    output_paths: dict[str, Path]
    reconciliation: ReconciliationResult


def _fetch_arrow_table(connection: duckdb.DuckDBPyConnection, query: str) -> pa.Table:
    """Execute a DuckDB query and return its Arrow result."""
    return connection.execute(query).arrow().read_all()


def _quantize_pct(value: Decimal) -> Decimal:
    """Round percentage shares deterministically to six decimal places."""
    return value.quantize(PCT_QUANTUM, rounding=ROUND_HALF_UP)


def load_residual_inputs(
    *,
    silver_dir: Path,
    gold_dir: Path,
) -> dict[str, pa.Table]:
    """Load the silver and gold tables needed for residual reporting."""
    required_tables = {
        "fact_gl_cost": silver_dir / "fact_gl_cost",
        "dim_cost_center": silver_dir / "dim_cost_center",
        "allocation": gold_dir / "allocation",
        "residual": gold_dir / "residual",
    }
    tables: dict[str, pa.Table] = {}
    for table_name, table_path in required_tables.items():
        if not table_path.exists():
            raise FileNotFoundError(f"Expected Delta table for {table_name}: {table_path}")
        tables[table_name] = read_delta_table(table_path)
    return tables


def build_residual_detail_table(
    *,
    fact_gl_cost: pa.Table,
    dim_cost_center: pa.Table,
    residual: pa.Table,
) -> pa.Table:
    """Enrich engine residual rows with governed silver context."""
    connection = duckdb.connect()
    try:
        connection.register("fact_gl_cost", fact_gl_cost)
        connection.register("dim_cost_center", dim_cost_center)
        connection.register("residual", residual)
        return _fetch_arrow_table(
            connection,
            """
            SELECT
                residual.gl_line_id,
                residual.amount_eur,
                gl.gl_account,
                residual.cost_center_id,
                dim.name AS cost_center_name,
                residual.failed_step,
                residual.reason_code,
                residual.rule_version,
                residual.app_id
            FROM residual
            INNER JOIN fact_gl_cost AS gl
                ON residual.gl_line_id = gl.gl_line_id
            LEFT JOIN dim_cost_center AS dim
                ON residual.cost_center_id = dim.cost_center_id
            ORDER BY
                residual.rule_version,
                residual.failed_step,
                residual.reason_code,
                residual.gl_line_id
            """
        )
    finally:
        connection.close()


def build_residual_report_table(
    detail_table: pa.Table,
    *,
    total_gl_eur: Decimal,
) -> pa.Table:
    """Aggregate the residual detail into the quantified report rows."""
    rows = detail_table.to_pylist()
    grouped: dict[tuple[str, str, str], dict[str, object]] = {}
    for row in rows:
        key = (row["rule_version"], row["failed_step"], row["reason_code"])
        bucket = grouped.setdefault(
            key,
            {
                "rule_version": row["rule_version"],
                "failed_step": row["failed_step"],
                "reason_code": row["reason_code"],
                "residual_amount_eur": Decimal("0.00"),
                "gl_line_ids": set(),
            },
        )
        bucket["residual_amount_eur"] += row["amount_eur"]
        bucket["gl_line_ids"].add(row["gl_line_id"])

    report_rows = []
    for key in sorted(grouped):
        bucket = grouped[key]
        residual_amount = bucket["residual_amount_eur"]
        pct_of_total_gl = _quantize_pct(residual_amount / total_gl_eur)
        report_rows.append(
            {
                "rule_version": bucket["rule_version"],
                "failed_step": bucket["failed_step"],
                "reason_code": bucket["reason_code"],
                "residual_amount_eur": residual_amount,
                "gl_line_count": len(bucket["gl_line_ids"]),
                "pct_of_total_gl": pct_of_total_gl,
            }
        )
    return build_arrow_table(report_rows, RESIDUAL_REPORT_SCHEMA)


def build_reconciliation_table(result: ReconciliationResult) -> pa.Table:
    """Materialize the reconciliation result as a Delta-backed table."""
    return build_arrow_table(
        [
            {
                "rule_version": result.rule_version,
                "total_gl_eur": result.total_gl_eur,
                "total_allocated_eur": result.total_allocated_eur,
                "total_residual_eur": result.total_residual_eur,
                "balanced": result.balanced,
                "difference_eur": result.difference_eur,
            }
        ],
        RECONCILIATION_SCHEMA,
    )


def build_residual_outputs(
    *,
    silver_dir: str | Path,
    gold_dir: str | Path,
    rule_version_id: str | None = None,
    rules_dir: str | Path | None = None,
) -> ResidualReportResult:
    """Build residual detail, report, and reconciliation outputs."""
    resolved_silver_dir = resolve_repo_path(silver_dir)
    resolved_gold_dir = resolve_repo_path(gold_dir)

    registry = RuleRegistry(rules_dir=rules_dir)
    rule = registry.resolve(rule_version_id) if rule_version_id is not None else registry.resolve_default()
    reconciliation = reconcile_rule_version(
        silver_dir=resolved_silver_dir,
        gold_dir=resolved_gold_dir,
        rule_version_id=rule.version_id,
    )
    tables = load_residual_inputs(silver_dir=resolved_silver_dir, gold_dir=resolved_gold_dir)
    detail_table = build_residual_detail_table(
        fact_gl_cost=tables["fact_gl_cost"],
        dim_cost_center=tables["dim_cost_center"],
        residual=tables["residual"],
    )
    report_table = build_residual_report_table(
        detail_table,
        total_gl_eur=reconciliation.total_gl_eur,
    )
    reconciliation_table = build_reconciliation_table(reconciliation)

    output_paths = {
        RESIDUAL_DETAIL_TABLE: write_delta_table(
            detail_table,
            resolved_gold_dir / RESIDUAL_DETAIL_TABLE,
            sort_columns=RESIDUAL_DETAIL_SORT_COLUMNS,
        ),
        RESIDUAL_REPORT_TABLE: write_delta_table(
            report_table,
            resolved_gold_dir / RESIDUAL_REPORT_TABLE,
            sort_columns=RESIDUAL_REPORT_SORT_COLUMNS,
        ),
        "reconciliation": write_delta_table(
            reconciliation_table,
            resolved_gold_dir / "reconciliation",
            sort_columns=RECONCILIATION_SORT_COLUMNS,
        ),
    }
    return ResidualReportResult(output_paths=output_paths, reconciliation=reconciliation)


def parse_args() -> argparse.Namespace:
    """Parse CLI args for the residual reporting stage."""
    parser = argparse.ArgumentParser(description="Build quantified residual reporting outputs.")
    parser.add_argument("--silver-dir", type=Path, help="Optional silver Delta directory override.")
    parser.add_argument("--gold-dir", type=Path, help="Optional gold Delta directory override.")
    parser.add_argument("--rule-version", help="Optional rule version override.")
    parser.add_argument("--rules-dir", type=Path, help="Optional rules directory override.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint for residual reporting."""
    args = parse_args()
    result = build_residual_outputs(
        silver_dir=args.silver_dir or repo_root() / "data" / "silver",
        gold_dir=args.gold_dir or repo_root() / "data" / "gold",
        rule_version_id=args.rule_version,
        rules_dir=args.rules_dir,
    )
    print(
        "[tech-cost-platform] residual status=completed "
        f"balanced={result.reconciliation.balanced} "
        f"difference_eur={result.reconciliation.difference_eur}"
    )
    print(
        "[tech-cost-platform] residual "
        f"total_gl_eur={result.reconciliation.total_gl_eur} "
        f"total_allocated_eur={result.reconciliation.total_allocated_eur} "
        f"total_residual_eur={result.reconciliation.total_residual_eur}"
    )
    for table_name, output_path in result.output_paths.items():
        print(f"[tech-cost-platform] residual table={table_name} path={output_path}")
    return 0
