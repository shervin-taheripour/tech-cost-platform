"""Gold report view builders — five presentation-layer views over engine outputs."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

import duckdb
import pyarrow as pa

from ..delta_tables import MONEY_TYPE, build_arrow_table

PCT_TYPE = pa.decimal128(18, 6)
PCT_QUANTUM = Decimal("0.000001")

REPORT_APP_TCO_TABLE = "report_application_tco"
REPORT_BU_SHOWBACK_TABLE = "report_bu_showback"
REPORT_RESIDUAL_TABLE = "report_residual"
REPORT_LINEAGE_TABLE = "report_lineage"
REPORT_DRIVER_COMPARISON_TABLE = "report_driver_comparison"
REPORT_DRIVER_COMPARISON_BY_APP_TABLE = "report_driver_comparison_by_app"

REPORT_APP_TCO_SCHEMA = pa.schema(
    [
        ("rule_version", pa.string()),
        ("app_id", pa.string()),
        ("app_name", pa.string()),
        ("business_criticality", pa.string()),
        ("allocated_amount_eur", MONEY_TYPE),
        ("pct_of_allocated", PCT_TYPE),
        ("contributing_gl_line_count", pa.int64()),
    ]
)
REPORT_BU_SHOWBACK_SCHEMA = pa.schema(
    [
        ("rule_version", pa.string()),
        ("bu_id", pa.string()),
        ("bu_name", pa.string()),
        ("allocated_amount_eur", MONEY_TYPE),
        ("pct_of_allocated", PCT_TYPE),
        ("contributing_app_count", pa.int64()),
        ("contributing_gl_line_count", pa.int64()),
    ]
)
REPORT_DRIVER_COMPARISON_SCHEMA = pa.schema(
    [
        ("bu_id", pa.string()),
        ("bu_name", pa.string()),
        ("amount_v1_transactions", MONEY_TYPE),
        ("amount_v2_named_users", MONEY_TYPE),
        ("delta_eur", MONEY_TYPE),
        ("delta_pct", PCT_TYPE),
        ("share_v1", PCT_TYPE),
        ("share_v2", PCT_TYPE),
        ("share_delta_pp", PCT_TYPE),
    ]
)
REPORT_DRIVER_COMPARISON_BY_APP_SCHEMA = pa.schema(
    [
        ("app_id", pa.string()),
        ("app_name", pa.string()),
        ("bu_id", pa.string()),
        ("bu_name", pa.string()),
        ("amount_v1_transactions", MONEY_TYPE),
        ("amount_v2_named_users", MONEY_TYPE),
        ("delta_eur", MONEY_TYPE),
    ]
)

REPORT_APP_TCO_SORT_COLUMNS = ["rule_version", "app_id"]
REPORT_BU_SHOWBACK_SORT_COLUMNS = ["rule_version", "bu_id"]
REPORT_RESIDUAL_SORT_COLUMNS = ["rule_version", "failed_step", "reason_code"]
REPORT_LINEAGE_SORT_COLUMNS = [
    "rule_version",
    "outcome",
    "gl_line_id",
    "tower_id",
    "app_id",
    "bu_id",
    "reason_code",
]
REPORT_DRIVER_COMPARISON_SORT_COLUMNS = ["bu_id"]
REPORT_DRIVER_COMPARISON_BY_APP_SORT_COLUMNS = ["app_id", "bu_id"]


def _pct(value: Decimal) -> Decimal:
    return value.quantize(PCT_QUANTUM, rounding=ROUND_HALF_UP)


def _fetch_arrow(conn: duckdb.DuckDBPyConnection, sql: str) -> pa.Table:
    return conn.execute(sql).arrow().read_all()


def build_report_application_tco(
    allocation: pa.Table,
    dim_application: pa.Table,
) -> pa.Table:
    """Application TCO: allocated amount and GL contribution count per app per rule version."""
    conn = duckdb.connect()
    try:
        conn.register("allocation", allocation)
        conn.register("dim_application", dim_application)
        agg = _fetch_arrow(
            conn,
            """
            SELECT
                alloc.rule_version,
                alloc.app_id,
                COALESCE(dim.name, alloc.app_id) AS app_name,
                COALESCE(dim.criticality, '') AS business_criticality,
                SUM(alloc.allocated_amount_eur) AS allocated_amount_eur,
                COUNT(DISTINCT alloc.gl_line_id) AS contributing_gl_line_count
            FROM allocation AS alloc
            LEFT JOIN dim_application AS dim ON alloc.app_id = dim.app_id
            GROUP BY alloc.rule_version, alloc.app_id, dim.name, dim.criticality
            ORDER BY alloc.rule_version, alloc.app_id
            """,
        )
    finally:
        conn.close()

    rows = agg.to_pylist()
    totals: dict[str, Decimal] = {}
    for row in rows:
        totals.setdefault(row["rule_version"], Decimal("0.00"))
        totals[row["rule_version"]] += row["allocated_amount_eur"]

    return build_arrow_table(
        [
            {
                "rule_version": row["rule_version"],
                "app_id": row["app_id"],
                "app_name": row["app_name"],
                "business_criticality": row["business_criticality"],
                "allocated_amount_eur": row["allocated_amount_eur"],
                "pct_of_allocated": _pct(row["allocated_amount_eur"] / totals[row["rule_version"]]),
                "contributing_gl_line_count": row["contributing_gl_line_count"],
            }
            for row in rows
        ],
        REPORT_APP_TCO_SCHEMA,
    )


def build_report_bu_showback(
    allocation: pa.Table,
    dim_business_unit: pa.Table,
) -> pa.Table:
    """BU showback: allocated amount and contribution counts per BU per rule version."""
    conn = duckdb.connect()
    try:
        conn.register("allocation", allocation)
        conn.register("dim_business_unit", dim_business_unit)
        agg = _fetch_arrow(
            conn,
            """
            SELECT
                alloc.rule_version,
                alloc.bu_id,
                COALESCE(dim.name, alloc.bu_id) AS bu_name,
                SUM(alloc.allocated_amount_eur) AS allocated_amount_eur,
                COUNT(DISTINCT alloc.app_id) AS contributing_app_count,
                COUNT(DISTINCT alloc.gl_line_id) AS contributing_gl_line_count
            FROM allocation AS alloc
            LEFT JOIN dim_business_unit AS dim ON alloc.bu_id = dim.bu_id
            GROUP BY alloc.rule_version, alloc.bu_id, dim.name
            ORDER BY alloc.rule_version, alloc.bu_id
            """,
        )
    finally:
        conn.close()

    rows = agg.to_pylist()
    totals: dict[str, Decimal] = {}
    for row in rows:
        totals.setdefault(row["rule_version"], Decimal("0.00"))
        totals[row["rule_version"]] += row["allocated_amount_eur"]

    return build_arrow_table(
        [
            {
                "rule_version": row["rule_version"],
                "bu_id": row["bu_id"],
                "bu_name": row["bu_name"],
                "allocated_amount_eur": row["allocated_amount_eur"],
                "pct_of_allocated": _pct(row["allocated_amount_eur"] / totals[row["rule_version"]]),
                "contributing_app_count": row["contributing_app_count"],
                "contributing_gl_line_count": row["contributing_gl_line_count"],
            }
            for row in rows
        ],
        REPORT_BU_SHOWBACK_SCHEMA,
    )


def build_report_residual(residual_report: pa.Table) -> pa.Table:
    """Passthrough: residual_report is the correct presentation surface — no recomputation."""
    return residual_report


def build_report_lineage(lineage: pa.Table) -> pa.Table:
    """Passthrough: lineage is directly queryable for forward and backward audit traces."""
    return lineage


def build_report_driver_comparison(
    *,
    v1_allocation: list,
    v2_allocation: list,
    dim_business_unit: pa.Table,
) -> pa.Table:
    """Same costs allocated under v1_transactions vs v2_named_users, compared at BU level."""
    v1_by_bu: dict[str, Decimal] = {}
    for row in v1_allocation:
        v1_by_bu[row.bu_id] = v1_by_bu.get(row.bu_id, Decimal("0.00")) + row.allocated_amount_eur

    v2_by_bu: dict[str, Decimal] = {}
    for row in v2_allocation:
        v2_by_bu[row.bu_id] = v2_by_bu.get(row.bu_id, Decimal("0.00")) + row.allocated_amount_eur

    bu_names: dict[str, str] = {r["bu_id"]: r["name"] for r in dim_business_unit.to_pylist()}
    v1_total = sum(v1_by_bu.values(), start=Decimal("0.00"))
    v2_total = sum(v2_by_bu.values(), start=Decimal("0.00"))
    all_bu_ids = sorted(set(v1_by_bu) | set(v2_by_bu))

    result_rows = []
    for bu_id in all_bu_ids:
        amt_v1 = v1_by_bu.get(bu_id, Decimal("0.00"))
        amt_v2 = v2_by_bu.get(bu_id, Decimal("0.00"))
        delta_eur = amt_v2 - amt_v1
        raw_share_v1 = amt_v1 / v1_total if v1_total else Decimal("0")
        raw_share_v2 = amt_v2 / v2_total if v2_total else Decimal("0")
        result_rows.append(
            {
                "bu_id": bu_id,
                "bu_name": bu_names.get(bu_id, bu_id),
                "amount_v1_transactions": amt_v1,
                "amount_v2_named_users": amt_v2,
                "delta_eur": delta_eur,
                "delta_pct": _pct(delta_eur / amt_v1) if amt_v1 != Decimal("0.00") else None,
                "share_v1": _pct(raw_share_v1),
                "share_v2": _pct(raw_share_v2),
                "share_delta_pp": _pct(raw_share_v2 - raw_share_v1),
            }
        )
    return build_arrow_table(result_rows, REPORT_DRIVER_COMPARISON_SCHEMA)


def build_report_driver_comparison_by_app(
    *,
    v1_allocation: list,
    v2_allocation: list,
    dim_application: pa.Table,
    dim_business_unit: pa.Table,
) -> pa.Table:
    """Driver comparison at (app_id, bu_id) grain — makes the APP-BILLING flip visible."""
    v1_by_key: dict[tuple[str, str], Decimal] = {}
    for row in v1_allocation:
        key = (row.app_id, row.bu_id)
        v1_by_key[key] = v1_by_key.get(key, Decimal("0.00")) + row.allocated_amount_eur

    v2_by_key: dict[tuple[str, str], Decimal] = {}
    for row in v2_allocation:
        key = (row.app_id, row.bu_id)
        v2_by_key[key] = v2_by_key.get(key, Decimal("0.00")) + row.allocated_amount_eur

    app_names: dict[str, str] = {r["app_id"]: r["name"] for r in dim_application.to_pylist()}
    bu_names: dict[str, str] = {r["bu_id"]: r["name"] for r in dim_business_unit.to_pylist()}
    all_keys = sorted(set(v1_by_key) | set(v2_by_key))

    return build_arrow_table(
        [
            {
                "app_id": app_id,
                "app_name": app_names.get(app_id, app_id),
                "bu_id": bu_id,
                "bu_name": bu_names.get(bu_id, bu_id),
                "amount_v1_transactions": v1_by_key.get((app_id, bu_id), Decimal("0.00")),
                "amount_v2_named_users": v2_by_key.get((app_id, bu_id), Decimal("0.00")),
                "delta_eur": v2_by_key.get((app_id, bu_id), Decimal("0.00"))
                - v1_by_key.get((app_id, bu_id), Decimal("0.00")),
            }
            for app_id, bu_id in all_keys
        ],
        REPORT_DRIVER_COMPARISON_BY_APP_SCHEMA,
    )
