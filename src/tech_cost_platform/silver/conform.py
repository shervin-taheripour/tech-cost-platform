"""Silver-layer cleaning and conformance transforms."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import duckdb
import pyarrow as pa

from ..delta_tables import MONEY_TYPE, read_delta_table

BRONZE_TABLE_NAMES = (
    "gl_costs",
    "cost_centers",
    "resource_towers",
    "applications",
    "business_units",
    "usage_metrics",
)
SILVER_TABLE_NAMES = (
    "dim_cost_center",
    "dim_resource_tower",
    "dim_application",
    "dim_business_unit",
    "fact_gl_cost",
    "fact_usage_metric",
)
DECIMAL_18_2 = MONEY_TYPE

_NORMALIZE_PERIOD_SQL = "trim(period)"


@dataclass(frozen=True)
class SilverConformanceResult:
    """Intermediate normalized inputs plus final conformed outputs."""

    dimension_candidates: Mapping[str, pa.Table]
    tables: Mapping[str, pa.Table]


def _fetch_arrow_table(connection: duckdb.DuckDBPyConnection, query: str) -> pa.Table:
    """Execute a DuckDB query and return its Arrow result."""
    return connection.execute(query).arrow().read_all()


def read_bronze_tables(bronze_dir: Path) -> dict[str, pa.Table]:
    """Load every expected bronze Delta table from the supplied root."""
    bronze_tables: dict[str, pa.Table] = {}

    for table_name in BRONZE_TABLE_NAMES:
        table_path = bronze_dir / table_name
        if not table_path.exists():
            raise FileNotFoundError(f"Expected bronze Delta table for {table_name}: {table_path}")
        bronze_tables[table_name] = read_delta_table(table_path)

    return bronze_tables


def conform_bronze_tables(bronze_tables: Mapping[str, pa.Table]) -> SilverConformanceResult:
    """Normalize bronze tables into conformed fact and dimension candidates."""
    connection = duckdb.connect()
    try:
        for table_name, table in bronze_tables.items():
            connection.register(table_name, table)

        dim_cost_center_candidate = _fetch_arrow_table(
            connection,
            """
            SELECT
                trim(cost_center_id) AS cost_center_id,
                trim(cost_center_name) AS name,
                NULLIF(trim(tower_id), '') AS tower_id
            FROM cost_centers
            """,
        )
        dim_resource_tower_candidate = _fetch_arrow_table(
            connection,
            """
            SELECT
                trim(tower_id) AS tower_id,
                trim(tower_name) AS name,
                trim(tower_type) AS type
            FROM resource_towers
            """,
        )
        dim_application_candidate = _fetch_arrow_table(
            connection,
            """
            SELECT
                trim(app_id) AS app_id,
                trim(app_name) AS name,
                trim(business_criticality) AS criticality
            FROM applications
            """,
        )
        dim_business_unit_candidate = _fetch_arrow_table(
            connection,
            """
            SELECT
                trim(bu_id) AS bu_id,
                trim(bu_name) AS name
            FROM business_units
            """,
        )

        connection.register("dim_cost_center_candidate", dim_cost_center_candidate)
        connection.register("dim_resource_tower_candidate", dim_resource_tower_candidate)
        connection.register("dim_application_candidate", dim_application_candidate)
        connection.register("dim_business_unit_candidate", dim_business_unit_candidate)

        dim_cost_center = _fetch_arrow_table(
            connection,
            """
            SELECT DISTINCT cost_center_id, name, tower_id
            FROM dim_cost_center_candidate
            ORDER BY cost_center_id, name, tower_id
            """,
        )
        dim_resource_tower = _fetch_arrow_table(
            connection,
            """
            SELECT DISTINCT tower_id, name, type
            FROM dim_resource_tower_candidate
            ORDER BY tower_id, name, type
            """,
        )
        dim_application = _fetch_arrow_table(
            connection,
            """
            SELECT DISTINCT app_id, name, criticality
            FROM dim_application_candidate
            ORDER BY app_id, name, criticality
            """,
        )
        dim_business_unit = _fetch_arrow_table(
            connection,
            """
            SELECT DISTINCT bu_id, name
            FROM dim_business_unit_candidate
            ORDER BY bu_id, name
            """,
        )

        connection.register("dim_cost_center", dim_cost_center)

        fact_gl_cost = _fetch_arrow_table(
            connection,
            f"""
            SELECT
                trim(gl.gl_line_id) AS gl_line_id,
                {_NORMALIZE_PERIOD_SQL} AS period,
                trim(gl.gl_account) AS gl_account,
                trim(gl.cost_center_id) AS cost_center_id,
                CAST(gl.amount_eur AS DECIMAL(18,2)) AS amount_eur,
                dim.tower_id AS tower_id
            FROM gl_costs AS gl
            LEFT JOIN dim_cost_center AS dim
                ON trim(gl.cost_center_id) = dim.cost_center_id
            ORDER BY gl_line_id
            """,
        )
        fact_usage_metric = _fetch_arrow_table(
            connection,
            f"""
            SELECT
                trim(metric_id) AS metric_id,
                {_NORMALIZE_PERIOD_SQL} AS period,
                trim(step) AS step,
                trim(from_id) AS from_id,
                trim(to_id) AS to_id,
                trim(metric_name) AS metric_name,
                CAST(value AS DECIMAL(18,2)) AS value
            FROM usage_metrics
            ORDER BY metric_id
            """,
        )
    finally:
        connection.close()

    return SilverConformanceResult(
        dimension_candidates={
            "dim_cost_center": dim_cost_center_candidate,
            "dim_resource_tower": dim_resource_tower_candidate,
            "dim_application": dim_application_candidate,
            "dim_business_unit": dim_business_unit_candidate,
        },
        tables={
            "dim_cost_center": dim_cost_center,
            "dim_resource_tower": dim_resource_tower,
            "dim_application": dim_application,
            "dim_business_unit": dim_business_unit,
            "fact_gl_cost": fact_gl_cost,
            "fact_usage_metric": fact_usage_metric,
        },
    )
