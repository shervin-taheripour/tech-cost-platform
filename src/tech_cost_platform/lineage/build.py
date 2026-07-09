"""Materialize lineage views from existing gold and silver outputs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa

from ..delta_tables import MONEY_TYPE, read_delta_table, write_delta_table
from ..residual.reconcile import ReconciliationResult
from ..rules import RuleRegistry
from ..runtime import repo_root, resolve_repo_path
from .trace import (
    MAX_PROPORTION_DRIFT_EUR,
    build_worked_example_payload,
    validate_lineage_per_line,
    validate_lineage_round_trip,
    validate_proportion_consistency,
    write_worked_example,
)

LINEAGE_TABLE = "lineage"
LINEAGE_SCHEMA = pa.schema(
    [
        ("gl_line_id", pa.string()),
        ("period", pa.string()),
        ("gl_account", pa.string()),
        ("cost_center_id", pa.string()),
        ("cost_center_name", pa.string()),
        ("tower_id", pa.string()),
        ("app_id", pa.string()),
        ("bu_id", pa.string()),
        ("gl_amount_eur", MONEY_TYPE),
        ("prop_gl_to_tower", pa.decimal128(18, 12)),
        ("prop_tower_to_app", pa.decimal128(18, 12)),
        ("prop_app_to_bu", pa.decimal128(18, 12)),
        ("allocated_amount_eur", MONEY_TYPE),
        ("residual_amount_eur", MONEY_TYPE),
        ("terminal_amount_eur", MONEY_TYPE),
        ("rule_version", pa.string()),
        ("outcome", pa.string()),
        ("reason_code", pa.string()),
        ("failed_step", pa.string()),
    ]
)
LINEAGE_SORT_COLUMNS = [
    "rule_version",
    "outcome",
    "gl_line_id",
    "tower_id",
    "app_id",
    "bu_id",
    "reason_code",
]


@dataclass(frozen=True)
class LineageBuildResult:
    """Lineage output paths plus the exact round-trip reconciliation."""

    output_paths: dict[str, Path]
    reconciliation: ReconciliationResult
    worked_example_path: Path


def _fetch_arrow_table(connection: duckdb.DuckDBPyConnection, query: str) -> pa.Table:
    """Execute a DuckDB query and return its Arrow result."""
    return connection.execute(query).arrow().read_all()


def load_lineage_inputs(
    *,
    silver_dir: Path,
    gold_dir: Path,
) -> dict[str, pa.Table]:
    """Load the silver and gold tables needed for lineage."""
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


def build_lineage_table(
    *,
    fact_gl_cost: pa.Table,
    dim_cost_center: pa.Table,
    allocation: pa.Table,
    residual: pa.Table,
) -> pa.Table:
    """Build the authoritative lineage edge table from existing gold outputs."""
    connection = duckdb.connect()
    try:
        connection.register("fact_gl_cost", fact_gl_cost)
        connection.register("dim_cost_center", dim_cost_center)
        connection.register("allocation", allocation)
        connection.register("residual", residual)
        return _fetch_arrow_table(
            connection,
            """
            SELECT
                gl.gl_line_id,
                gl.period,
                gl.gl_account,
                gl.cost_center_id,
                dim.name AS cost_center_name,
                alloc.tower_id,
                alloc.app_id,
                alloc.bu_id,
                gl.amount_eur AS gl_amount_eur,
                alloc.gl_to_tower_proportion AS prop_gl_to_tower,
                alloc.tower_to_app_proportion AS prop_tower_to_app,
                alloc.app_to_bu_proportion AS prop_app_to_bu,
                alloc.allocated_amount_eur,
                CAST(NULL AS DECIMAL(18,2)) AS residual_amount_eur,
                alloc.allocated_amount_eur AS terminal_amount_eur,
                alloc.rule_version,
                'allocated' AS outcome,
                CAST(NULL AS VARCHAR) AS reason_code,
                CAST(NULL AS VARCHAR) AS failed_step
            FROM allocation AS alloc
            INNER JOIN fact_gl_cost AS gl
                ON alloc.gl_line_id = gl.gl_line_id
            LEFT JOIN dim_cost_center AS dim
                ON gl.cost_center_id = dim.cost_center_id

            UNION ALL

            SELECT
                gl.gl_line_id,
                gl.period,
                gl.gl_account,
                gl.cost_center_id,
                dim.name AS cost_center_name,
                residual.tower_id,
                residual.app_id,
                CAST(NULL AS VARCHAR) AS bu_id,
                gl.amount_eur AS gl_amount_eur,
                CAST(NULL AS DECIMAL(18,12)) AS prop_gl_to_tower,
                CAST(NULL AS DECIMAL(18,12)) AS prop_tower_to_app,
                CAST(NULL AS DECIMAL(18,12)) AS prop_app_to_bu,
                CAST(NULL AS DECIMAL(18,2)) AS allocated_amount_eur,
                residual.amount_eur AS residual_amount_eur,
                residual.amount_eur AS terminal_amount_eur,
                residual.rule_version,
                'residual' AS outcome,
                residual.reason_code,
                residual.failed_step
            FROM residual
            INNER JOIN fact_gl_cost AS gl
                ON residual.gl_line_id = gl.gl_line_id
            LEFT JOIN dim_cost_center AS dim
                ON gl.cost_center_id = dim.cost_center_id
            ORDER BY rule_version, outcome, gl_line_id, tower_id, app_id, bu_id, reason_code
            """
        )
    finally:
        connection.close()


def build_lineage_outputs(
    *,
    silver_dir: str | Path,
    gold_dir: str | Path,
    examples_dir: str | Path,
    rule_version_id: str | None = None,
    rules_dir: str | Path | None = None,
) -> LineageBuildResult:
    """Build the lineage view and the worked example."""
    resolved_silver_dir = resolve_repo_path(silver_dir)
    resolved_gold_dir = resolve_repo_path(gold_dir)
    resolved_examples_dir = resolve_repo_path(examples_dir)

    registry = RuleRegistry(rules_dir=rules_dir)
    rule = registry.resolve(rule_version_id) if rule_version_id is not None else registry.resolve_default()

    tables = load_lineage_inputs(silver_dir=resolved_silver_dir, gold_dir=resolved_gold_dir)
    lineage_table = build_lineage_table(
        fact_gl_cost=tables["fact_gl_cost"],
        dim_cost_center=tables["dim_cost_center"],
        allocation=tables["allocation"],
        residual=tables["residual"],
    )
    output_path = write_delta_table(
        lineage_table,
        resolved_gold_dir / LINEAGE_TABLE,
        sort_columns=LINEAGE_SORT_COLUMNS,
    )

    persisted_rows = [
        row for row in read_delta_table(output_path).to_pylist() if row["rule_version"] == rule.version_id
    ]
    validate_lineage_per_line(persisted_rows)
    validate_proportion_consistency(persisted_rows)
    round_trip = validate_lineage_round_trip(
        silver_dir=resolved_silver_dir,
        gold_dir=resolved_gold_dir,
        lineage_dir=output_path,
        rule_version_id=rule.version_id,
    )

    worked_example_payload = build_worked_example_payload(lineage_rows=persisted_rows, rule=rule)
    worked_example_path = write_worked_example(
        resolved_examples_dir / "lineage_worked_example.json",
        worked_example_payload,
    )
    return LineageBuildResult(
        output_paths={LINEAGE_TABLE: output_path},
        reconciliation=round_trip,
        worked_example_path=worked_example_path,
    )


def parse_args() -> argparse.Namespace:
    """Parse CLI args for the lineage stage."""
    parser = argparse.ArgumentParser(description="Build lineage outputs from existing gold and silver data.")
    parser.add_argument("--silver-dir", type=Path, help="Optional silver Delta directory override.")
    parser.add_argument("--gold-dir", type=Path, help="Optional gold Delta directory override.")
    parser.add_argument("--examples-dir", type=Path, help="Optional examples directory override.")
    parser.add_argument("--rule-version", help="Optional rule version override.")
    parser.add_argument("--rules-dir", type=Path, help="Optional rules directory override.")
    return parser.parse_args()


def main() -> int:
    """CLI entrypoint for lineage building."""
    args = parse_args()
    result = build_lineage_outputs(
        silver_dir=args.silver_dir or repo_root() / "data" / "silver",
        gold_dir=args.gold_dir or repo_root() / "data" / "gold",
        examples_dir=args.examples_dir or repo_root() / "examples",
        rule_version_id=args.rule_version,
        rules_dir=args.rules_dir,
    )
    print(
        "[tech-cost-platform] lineage status=completed "
        f"balanced={result.reconciliation.balanced} "
        f"difference_eur={result.reconciliation.difference_eur} "
        f"max_proportion_drift_eur={MAX_PROPORTION_DRIFT_EUR}"
    )
    for table_name, output_path in result.output_paths.items():
        print(f"[tech-cost-platform] lineage table={table_name} path={output_path}")
    print(f"[tech-cost-platform] lineage worked_example={result.worked_example_path}")
    return 0
